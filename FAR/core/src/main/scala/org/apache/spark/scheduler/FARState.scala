/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// scalastyle:off
package org.apache.spark.scheduler

import org.apache.log4j.LogManager
import org.apache.spark.{NarrowDependency, ShuffleDependency, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.{RDDBlockId, StorageLevel}

import scala.collection.mutable
import scala.collection.mutable.{ArrayStack, HashMap, HashSet, Queue, Set}

private[spark] class FARState(sc: SparkContext,
                              dagScheduler: DAGScheduler) extends Logging {

  private val logger = LogManager.getLogger("FARState")

  val pendingTasks = new mutable.HashSet[RDDBlockId]()
  val activeTasks = new mutable.HashSet[RDDBlockId]()
  val finishedTasks = new mutable.HashSet[RDDBlockId]()

  val rddIdToRDD = new mutable.HashMap[Int, RDD[_]]()
  val rddGraph = new mutable.HashMap[RDD[_], mutable.HashSet[RDD[_]]]()

  val reservedRDDs = new mutable.HashSet[RDD[_]]()
  val referenceCounts = new mutable.HashMap[RDD[_], Array[Int]]()
  val persistedBlocks = new mutable.HashMap[RDD[_], Array[Boolean]]()

  def onJobCreate(job: ActiveJob): Unit = {
    val finalRDD = job.finalStage.rdd

    val missingStages = getMissingStages(job.finalStage)
    val tasksToSubmit = getTasksToSubmit(missingStages)
    pendingTasks.clear()
    pendingTasks ++= tasksToSubmit
    activeTasks.clear()
    finishedTasks.clear()

    // update shuffleRDDs, rddIdToRDD, rddGraph
    traverse(finalRDD)
    //    logger.info("graph: " + tidy1(rddGraph))

    rddGraph.filter(_._2.size > 1).keys.foreach { rdd =>
      if (rdd.getStorageLevel == StorageLevel.NONE) {
        rdd.persist(StorageLevel.FAR)
      }
    }
    reservedRDDs.clear()
    reservedRDDs ++= rddIdToRDD.values.filter(_.getStorageLevel != StorageLevel.NONE)
    logger.info("Reserved RDDs: " + tidy3(reservedRDDs))

    // sync block persistence info
    persistedBlocks.clear()
    refreshPersistence()
    logger.info("Persistence State: " + tidy4(persistedBlocks))

    // clear and generate ReferenceCounts
    referenceCounts.clear()
    simulate1(pendingTasks)
    logger.info("Reference Counts: " + tidy5(referenceCounts))
  }

  def onJobComplete(): Unit = {
  }

  def onTaskSubmit(tasks: Seq[Task[_]], stage: Stage): Unit = {
    val rdd = stage.rdd
    tasks.foreach { task =>
      val taskBlock = RDDBlockId(rdd.id, task.partitionId)
      pendingTasks.remove(taskBlock)
      activeTasks.add(taskBlock)
    }
  }

  def onTaskComplete(task: Task[_], stage: Stage): Unit = synchronized {
    val pid = task.partitionId
    val rdd = stage.rdd
    val taskBlock = RDDBlockId(rdd.id, pid)

    val usage = simulate2(taskBlock)
    for ((blkId, count) <- usage) {
      val rdd = rddIdToRDD(blkId.rddId)
      referenceCounts.get(rdd).foreach(cnt => cnt(blkId.splitIndex) -= count)
      if (rdd.getStorageLevel == StorageLevel.FAR && referenceCounts.contains(rdd)
        && referenceCounts(rdd)(blkId.splitIndex) == 0) {
        sc.unpersistBlock(blkId)
      }
    }

    activeTasks.remove(taskBlock)
    finishedTasks.add(taskBlock)
  }

  def onBlockGenerate(blockId: RDDBlockId): Unit = synchronized {
    val rdd = rddIdToRDD(blockId.rddId)
    val split = blockId.splitIndex

    val usage = simulate2(blockId)
    for ((blkId, count) <- usage) {
      val rdd = rddIdToRDD(blkId.rddId)
      referenceCounts.get(rdd).foreach(cnt => cnt(blkId.splitIndex) -= count)
      if (rdd.getStorageLevel == StorageLevel.FAR && referenceCounts.contains(rdd) &&
        referenceCounts(rdd)(blkId.splitIndex) == 0) {
        sc.unpersistBlock(blkId)
      }
    }

    persistedBlocks(rdd)(split) = true
  }

  def onBlockEvict(blockId: RDDBlockId): Unit = synchronized {
    val rdd = rddIdToRDD(blockId.rddId)
    val split = blockId.splitIndex

    persistedBlocks(rdd)(split) = false
    if (referenceCounts.contains(rdd) && referenceCounts(rdd)(split) > 0) {
      val usage = simulate2(blockId)
      for ((blkId, count) <- usage) {
        val rdd = rddIdToRDD(blkId.rddId)
        referenceCounts.getOrElseUpdate(rdd, Array.fill(rdd.getNumPartitions)(0))(blkId.splitIndex) += count
      }
    }
  }

  def onFailedStageResubmit(failedStages: Array[Stage]): Unit = {
    val missingStages = new mutable.HashSet[Stage]()
    failedStages.foreach(missingStages ++= getMissingStages(_))

    val taskToResubmit = getTasksToSubmit(missingStages)
    activeTasks --= taskToResubmit
    finishedTasks --= taskToResubmit
    pendingTasks ++= taskToResubmit

    // sync block persistence info
    persistedBlocks.clear()
    refreshPersistence()
    logger.info("Failure Recovery: Persistence State: " + tidy4(persistedBlocks))

    // clear and generate ReferenceCounts
    referenceCounts.clear()
    simulate1(pendingTasks)
    logger.info("Failure Recovery: Reference Counts: " + tidy5(referenceCounts))
  }

  // ==========================================================

  private def getMissingStages(stage: Stage): mutable.HashSet[Stage] = {
    val missing = new HashSet[Stage]
    val visited = new HashSet[Stage]
    val waitingForVisit = new ArrayStack[Stage]

    missing += stage
    waitingForVisit.push(stage)

    while (waitingForVisit.nonEmpty) {
      val stage = waitingForVisit.pop()
      val missingParents = dagScheduler.getMissingParentStages(stage)
      missing ++= missingParents
      waitingForVisit ++= missingParents.filter(!visited(_))
      visited += stage
    }
    missing
  }

  private def getTasksToSubmit(stages: Set[Stage]): mutable.HashSet[RDDBlockId] = {
    val tasks = new mutable.HashSet[RDDBlockId]
    stages.foreach { stage =>
      stage.findMissingPartitions().foreach {
        split => tasks.add(RDDBlockId(stage.rdd.id, split))
      }
    }
    tasks
  }

  private def traverse(rdd: RDD[_]): Unit = {
    val visited = new HashSet[RDD[_]]
    val waiting = new Queue[RDD[_]]

    waiting.enqueue(rdd)
    while (waiting.nonEmpty) {
      val toVisit = waiting.dequeue()
      if (!visited(toVisit)) {
        rddIdToRDD.put(toVisit.id, toVisit)
        visited += toVisit
        for (dep <- toVisit.dependencies) {
          rddGraph.getOrElseUpdate(dep.rdd, new mutable.HashSet[RDD[_]]()).add(toVisit)
          waiting.enqueue(dep.rdd)
        }
      }
    }
  }

  private def refreshPersistence(): Unit = {
    dagScheduler.clearCacheLocs()
    for (rdd <- reservedRDDs) {
      val locs = dagScheduler.getCacheLocs(rdd)
      persistedBlocks(rdd) = locs.map(!_.isEmpty).toArray
    }
  }

  private def simulate1(tasks: mutable.HashSet[RDDBlockId]): Unit = {
    //  val referenceCounts = new mutable.HashMap[RDD[_], Array[Int]]()
    //  val persistedBlocks = new mutable.HashMap[RDD[_], Array[Boolean]]()

    val visited = new mutable.HashMap[RDDBlockId, Int]

    tasks.foreach { task =>
      val waiting = new Queue[RDDBlockId]()
      waiting.enqueue(task)
      while (waiting.nonEmpty) {
        val toVisit = waiting.dequeue()
        val rdd = rddIdToRDD(toVisit.rddId)
        val split = toVisit.splitIndex

        val persisted = persistedBlocks.contains(rdd) && persistedBlocks(rdd)(split)

        if (visited.contains(toVisit) || persisted) {
          visited(toVisit) = visited.getOrElse(toVisit, 0) + 1
        } else {
          visited(toVisit) = 1
          for (dep <- rdd.dependencies) {
            dep match {
              case narrow: NarrowDependency[_] =>
                narrow.getParents(split).foreach(dpid => waiting.enqueue(RDDBlockId(narrow.rdd.id, dpid)))
              case _: ShuffleDependency[_, _, _] =>
            }
          }
        }
      }
    }

    for ((blockId, count) <- visited) {
      val rdd = rddIdToRDD(blockId.rddId)
      val split = blockId.splitIndex
      if (reservedRDDs.contains(rdd)) {
        referenceCounts.getOrElseUpdate(rdd, Array.fill(rdd.getNumPartitions)(0))(split) = count
      }
    }
  }

  private def simulate2(blockId: RDDBlockId): mutable.HashMap[RDDBlockId, Int] = {
    //  val persistedBlocks = new mutable.HashMap[RDD[_], Array[Boolean]]()

    val usage = new mutable.HashMap[RDDBlockId, Int]()
    val waiting = new Queue[RDDBlockId]()

    waiting.enqueue(blockId)
    while (waiting.nonEmpty) {
      val toVisit = waiting.dequeue()
      val rdd = rddIdToRDD(toVisit.rddId)
      val split = toVisit.splitIndex

      if (persistedBlocks.contains(rdd)) {
        usage(toVisit) = usage.getOrElse(toVisit, 0) + 1
      }

      val persisted = persistedBlocks.contains(rdd) && persistedBlocks(rdd)(split)
      if (!persisted) {
        for (dep <- rdd.dependencies) {
          dep match {
            case narrow: NarrowDependency[_] =>
              narrow.getParents(split).foreach(dpid => waiting.enqueue(RDDBlockId(narrow.rdd.id, dpid)))
            case _: ShuffleDependency[_, _, _] =>
          }
        }
      }
    }
    usage.remove(blockId)
    usage
  }

  def tidy1(dict: mutable.HashMap[RDD[_], mutable.HashSet[RDD[_]]]): String = {
    dict.map(kv => "[" + kv._1.id + "] -> Set(" + kv._2.map(_.id).mkString(",") + ")").mkString("\n")
  }

  def tidy2(dict: mutable.HashMap[Int, RDD[_]]): String = {
    dict.map(kv => "[" + kv._1 + "] -> [" + kv._2.id + "]").mkString("\n")
  }

  def tidy3(sett: mutable.HashSet[RDD[_]]): String = {
    "[" + sett.map(_.id).mkString(",") + "]"
  }

  def tidy4(dict: mutable.HashMap[RDD[_], Array[Boolean]]): String = {
    dict.map(kv => "[" + kv._1.id + "] -> Array[" + kv._2.mkString(",") + "]").mkString("\n")
  }

  def tidy5(dict: mutable.HashMap[RDD[_], Array[Int]]): String = {
    dict.map(kv => "[" + kv._1.id + "] -> Array[" + kv._2.mkString(",") + "]").mkString("\n")
  }

}


// scalastyle:on
