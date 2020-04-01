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

import java.lang.{Iterable => JIterable}
import java.util.{Iterator => JIterator}

import scala.collection.JavaConverters._

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.api.io.ShuffleBlockInputStream
import org.apache.spark.shuffle.api.metadata.ShuffleBlockInfo
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleBlockMetadata
import org.apache.spark.storage.{BlockId, BlockManager, ShuffleBlockFetcherIterator, ShuffleBlockId}

private[spark] class ShuffleBlockFetcherIterable(
    blocks: JIterable[ShuffleBlockInfo],
    context: TaskContext,
    blockManager: BlockManager,
    serializerManager: SerializerManager,
    conf: SparkConf,
    shouldBatchFetch: Boolean,
    dep: ShuffleDependency[_, _, _])
    extends JIterable[ShuffleBlockInputStream] with Logging {

  private def fetchContinuousBlocksInBatch: Boolean = {
    val serializerRelocatable = dep.serializer.supportsRelocationOfSerializedObjects
    val compressed = conf.get(SHUFFLE_COMPRESS)
    val codecConcatenation = if (compressed) {
      CompressionCodec.supportsConcatenationOfSerializedStreams(CompressionCodec.createCodec(conf))
    } else {
      true
    }
    val useOldFetchProtocol = conf.get(SHUFFLE_USE_OLD_FETCH_PROTOCOL)

    val doBatchFetch = this.shouldBatchFetch && serializerRelocatable &&
      (!compressed || codecConcatenation) && !useOldFetchProtocol
    if (shouldBatchFetch && !doBatchFetch) {
      logDebug("The feature tag of continuous shuffle block fetching is set to true, but " +
        "we can not enable the feature because other conditions are not satisfied. " +
        s"Shuffle compress: $compressed, serializer relocatable: $serializerRelocatable, " +
        s"codec concatenation: $codecConcatenation, use old shuffle fetch protocol: " +
        s"$useOldFetchProtocol.")
    }
    doBatchFetch
  }

  override def iterator(): JIterator[ShuffleBlockInputStream] = {
    new ShuffleBlockFetcherIterator(
      context,
      blockManager.blockStoreClient,
      blockManager,
      blocks.asScala
        .groupBy(shuffleBlockInfo => shuffleBlockInfo.getShuffleLocation())
        .map{ case (blockManagerId, shuffleBlockInfos) =>
          (blockManagerId,
            shuffleBlockInfos.map(info =>
              (ShuffleBlockId(
                info.getShuffleId,
                info.getMapTaskAttemptId,
                info.getReduceId).asInstanceOf[BlockId],
                info.getBlockLength,
                info.getMapIndex)).toSeq)
        }
        .iterator,
      serializerManager.wrapStream,
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      conf.get(REDUCER_MAX_SIZE_IN_FLIGHT) * 1024 * 1024,
      conf.get(REDUCER_MAX_REQS_IN_FLIGHT),
      conf.get(REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),
      conf.get(MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM),
      conf.get(SHUFFLE_DETECT_CORRUPT),
      conf.get(SHUFFLE_DETECT_CORRUPT_MEMORY),
      context.taskMetrics().createTempShuffleReadMetrics(),
      fetchContinuousBlocksInBatch)
      .toCompletionIterator
      .map {
        case (blockId, stream) =>
          ShuffleBlockInputStream.of(stream, new LocalDiskShuffleBlockMetadata(blockId))
      }
      .asJava
  }
}
