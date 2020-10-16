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
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.{BlockId, RDDBlockId, StorageLevel}
import org.apache.spark.{NarrowDependency, ShuffleDependency, SparkContext, SparkEnv}

import scala.collection.{immutable, mutable}
import scala.collection.mutable.{ArrayStack, HashMap, HashSet, Queue, Set}
import scala.language.existentials


private[spark] class FARManager() extends Logging {
  private var sc: SparkContext = _
  private var dagScheduler: DAGScheduler = _
  private var state: FARState = _

  private val logger = LogManager.getLogger("FARManager")
  public var enabled = false

  def activeJobCreated(job: ActiveJob): Unit = {
    if (!enabled) {
      return
    }
    logger.info("ActiveJob created, id " + job.jobId)
    state.onJobCreate(job)
  }

  def taskSubmitting(tasks: Seq[Task[_]], stage: Stage): Unit = {
    if (!enabled) {
      return
    }
    logger.info("Tasks Submitting, id " + tasks)
    state.onTaskSubmit(tasks, stage)
  }

  def taskSucceed(task: Task[_], stage: Stage): Unit = {
    if (!enabled) {
      return
    }
    logger.info("Task Completed: " + task + ": rdd_" + stage.rdd.id + "_" + task.partitionId)
    state.onTaskComplete(task, stage)
  }

  def handleBlockGeneration(blockId: BlockId): Boolean = {
    if (!enabled) {
      return true
    }
    if (blockId.isInstanceOf[RDDBlockId]) {
      // logger.info("handleBlockGeneration " + blockId)
      state.onBlockGenerate(blockId.asRDDId.get)
    }
    true
  }

  def handleBlockEviction(blockId: BlockId): Boolean = {
    if (!enabled) {
      return true
    }
    if (blockId.isInstanceOf[RDDBlockId]) {
      // logger.info("handleBlockEviction " + blockId)
      state.onBlockEvict(blockId.asRDDId.get)
    }
    true
  }

  def failedStagesResubmitting(failedStages: Array[Stage]): Unit = {
    if (!enabled) {
      return
    }
    logger.info("Failed Stage Resubmitting , id " + failedStages.toSeq)
    state.onFailedStageResubmit(failedStages)
  }

  def stageFinished(stage: Stage): Unit = {
    if (!enabled) {
      return
    }
    // logger.info("Stage Finished, id " + stage.id)
    if (stage.isInstanceOf[ResultStage]) {
      state.onJobComplete();
    }
  }

  def RDDunpersisted(rddId: Int): Unit = {
    if (!enabled) {
      return
    }
    // logger.info("RDDunpersisted: " + rddId)
  }

  def stageSubmitting(stage: Stage): Unit = {
    if (!enabled) {
      return
    }
    // logger.info("Stage Submitting, id " + stage.id)
  }

  def shuffleMapStageCreated(rdd: RDD[_], sfStage: ShuffleMapStage): Unit = {
    if (!enabled) {
      return
    }
    // logger.info("ShuffleMapStage created, id " + sfStage.id)
  }

  def resultStageCreated(rdd: RDD[_], resStage: ResultStage, jobId: Int): Unit = {
    if (!enabled) {
      return
    }
    // logger.info("ResultStage created, id " + resStage.id)
  }

}

private[spark] object FARManager extends Logging {
  private val far: FARManager = new FARManager()

  def init(sc: SparkContext, dagScheduler: DAGScheduler) {
    far.sc = sc
    far.dagScheduler = dagScheduler
    far.enabled = sc.getConf.getBoolean("spark.far.enabled", false)
    far.state = new FARState(sc, dagScheduler)
  }

  def get: FARManager = {
    far
  }
}

// scalastyle:on
