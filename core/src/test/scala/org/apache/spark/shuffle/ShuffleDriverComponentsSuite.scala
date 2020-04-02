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

package org.apache.spark.shuffle

import java.util.{Map => JMap, Optional => JOptional}
import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.collect.ImmutableMap
import org.scalatest.Assertions._
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{LocalSparkContext, ShuffleDependency, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.internal.config.SHUFFLE_IO_PLUGIN_CLASS
import org.apache.spark.shuffle.api.{ShuffleDataIO, ShuffleDriverComponents, ShuffleExecutorComponents, ShuffleMapOutputWriter}
import org.apache.spark.shuffle.api.io.ShuffleBlockInputStream
import org.apache.spark.shuffle.api.metadata.{MapOutputMetadata, ShuffleBlockInfo, ShuffleMetadata, ShuffleOutputTracker, ShuffleUpdater}
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleDataIO

class ShuffleDriverComponentsSuite
    extends SparkFunSuite with LocalSparkContext with BeforeAndAfterEach {

  test("test serialization of shuffle initialization conf to executors") {
    val testConf = new SparkConf()
      .setAppName("testing")
      .set(ShuffleDataIOUtils.SHUFFLE_SPARK_CONF_PREFIX + "test-plugin-key", "user-set-value")
      .set(ShuffleDataIOUtils.SHUFFLE_SPARK_CONF_PREFIX + "test-user-key", "user-set-value")
      .setMaster("local-cluster[2,1,1024]")
      .set(SHUFFLE_IO_PLUGIN_CLASS, "org.apache.spark.shuffle.TestShuffleDataIO")

    sc = new SparkContext(testConf)

    val out = sc.parallelize(Seq((1, "one"), (2, "two"), (3, "three")), 3)
      .groupByKey()
      .foreach { _ =>
        if (!TestShuffleExecutorComponentsInitialized.initialized.get()) {
          throw new RuntimeException("TestShuffleExecutorComponents wasn't initialized")
        }
      }
  }
}

class TestShuffleDataIO(sparkConf: SparkConf) extends ShuffleDataIO {
  private val delegate = new LocalDiskShuffleDataIO(sparkConf)

  override def driver(): ShuffleDriverComponents = {
    new TestShuffleDriverComponents(delegate.driver())
  }

  override def executor(): ShuffleExecutorComponents =
    new TestShuffleExecutorComponentsInitialized(delegate.executor())
}

class TestShuffleDriverComponents(delegate: ShuffleDriverComponents)
  extends ShuffleDriverComponents {
  override def initializeApplication(): JMap[String, String] = {
    val confs = delegate.initializeApplication()
    ImmutableMap.builder[String, String]()
      .putAll(ImmutableMap.of("test-plugin-key", "plugin-set-value"))
      .putAll(confs)
      .build
  }

  override def cleanupApplication(): Unit = {}

  override def shuffleOutputTracker(): JOptional[ShuffleOutputTracker] = {
    delegate.shuffleOutputTracker()
  }
}

object TestShuffleExecutorComponentsInitialized {
  val initialized = new AtomicBoolean(false)
}

class TestShuffleExecutorComponentsInitialized(delegate: ShuffleExecutorComponents)
    extends ShuffleExecutorComponents {

  override def initializeExecutor(
      appId: String,
      execId: String,
      extraConfigs: JMap[String, String],
      updater: JOptional[ShuffleUpdater]): Unit = {
    delegate.initializeExecutor(appId, execId, extraConfigs, updater)
    assert(extraConfigs.get("test-plugin-key") == "plugin-set-value", extraConfigs)
    assert(extraConfigs.get("test-user-key") == "user-set-value")
    assert(updater.isPresent)
    TestShuffleExecutorComponentsInitialized.initialized.set(true)
  }

  override def createMapOutputWriter(
      shuffleId: Int,
      mapTaskId: Long,
      numPartitions: Int): ShuffleMapOutputWriter = {
    delegate.createMapOutputWriter(shuffleId, mapTaskId, numPartitions)
  }

  override def getPartitionReaders[K, V, C](
    blockInfos: java.lang.Iterable[ShuffleBlockInfo],
    dependency: ShuffleDependency[K, V, C],
    shuffleMetadata: JOptional[ShuffleMetadata]): java.lang.Iterable[ShuffleBlockInputStream] = {
    delegate.getPartitionReaders(blockInfos, dependency, shuffleMetadata)
  }

  override def getPartitionReadersForRange[K, V, C](
      blockInfos: java.lang.Iterable[ShuffleBlockInfo],
      startPartition: Int,
      endPartition: Int,
      dependency: ShuffleDependency[K, V, C],
      shuffleMetadata: JOptional[ShuffleMetadata]): java.lang.Iterable[ShuffleBlockInputStream] = {
    delegate.getPartitionReadersForRange(
      blockInfos, startPartition, endPartition, dependency, shuffleMetadata)
  }
}
