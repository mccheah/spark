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

package org.apache.spark.shuffle.sort.io;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;

import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.shuffle.ShuffleBlockFetcherIterable;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.api.SingleSpillShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.io.ShuffleBlockInputStream;
import org.apache.spark.shuffle.api.metadata.ShuffleBlockInfo;
import org.apache.spark.shuffle.api.metadata.ShuffleMetadata;
import org.apache.spark.shuffle.sort.SortShuffleManager$;
import org.apache.spark.storage.BlockManager;

public class LocalDiskShuffleExecutorComponents implements ShuffleExecutorComponents {

  private final SparkConf sparkConf;
  private BlockManager blockManager;
  private IndexShuffleBlockResolver blockResolver;
  private SerializerManager serializerManager;

  public LocalDiskShuffleExecutorComponents(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
  }

  @VisibleForTesting
  public LocalDiskShuffleExecutorComponents(
      SparkConf sparkConf,
      BlockManager blockManager,
      SerializerManager serializerManager,
      IndexShuffleBlockResolver blockResolver) {
    this.sparkConf = sparkConf;
    this.blockManager = blockManager;
    this.serializerManager = serializerManager;
    this.blockResolver = blockResolver;
  }

  @Override
  public void initializeExecutor(String appId, String execId, Map<String, String> extraConfigs) {
    blockManager = SparkEnv.get().blockManager();
    if (blockManager == null) {
      throw new IllegalStateException("No blockManager available from the SparkEnv.");
    }
    blockResolver = new IndexShuffleBlockResolver(sparkConf, blockManager);
    serializerManager = SparkEnv.get().serializerManager();
    if (serializerManager == null) {
      throw new IllegalStateException("No serializer manager available from the SparkEnv.");
    }
  }

  @Override
  public ShuffleMapOutputWriter createMapOutputWriter(
      int shuffleId,
      long mapTaskId,
      int numPartitions) {
    if (blockResolver == null) {
      throw new IllegalStateException(
          "Executor components must be initialized before getting writers.");
    }
    return new LocalDiskShuffleMapOutputWriter(
        shuffleId, mapTaskId, numPartitions, blockResolver, sparkConf);
  }

  @Override
  public Optional<SingleSpillShuffleMapOutputWriter> createSingleFileMapOutputWriter(
      int shuffleId,
      long mapId) {
    if (blockResolver == null) {
      throw new IllegalStateException(
          "Executor components must be initialized before getting writers.");
    }
    return Optional.of(new LocalDiskSingleSpillMapOutputWriter(shuffleId, mapId, blockResolver));
  }

  @Override
  public <K, V, C> Iterable<ShuffleBlockInputStream> getPartitionReaders(
      Iterable<ShuffleBlockInfo> blockInfos,
      ShuffleDependency<K, V, C> dependency,
      Optional<ShuffleMetadata> shuffleMetadata) throws IOException {
    if (blockResolver == null) {
      throw new IllegalStateException(
          "Executor components must be initialized before getting readers.");
    }
    TaskContext context = TaskContext.get();
    return new ShuffleBlockFetcherIterable(
        blockInfos,
        context,
        blockManager,
        serializerManager,
        sparkConf,
        false,
        dependency);
  }

  @Override
  public <K, V, C> Iterable<ShuffleBlockInputStream> getPartitionReadersForRange(
      Iterable<ShuffleBlockInfo> blockInfos,
      int startPartition,
      int endPartition,
      ShuffleDependency<K, V, C> dependency,
      Optional<ShuffleMetadata> shuffleMetadata) throws IOException {
    if (blockResolver == null) {
      throw new IllegalStateException(
          "Executor components must be initialized before getting readers.");
    }
    TaskContext context = TaskContext.get();
    boolean batchFetchSet =
        Optional.ofNullable(context).map(ctx -> ctx.getLocalProperty(
            SortShuffleManager$.MODULE$.FETCH_SHUFFLE_BLOCKS_IN_BATCH_ENABLED_KEY())
            .equalsIgnoreCase("true")).orElse(false);
    boolean fetchMultiPartitions = false;
    if (batchFetchSet) {
      fetchMultiPartitions = endPartition - startPartition > 1;
    }
    return new ShuffleBlockFetcherIterable(
        blockInfos,
        context,
        blockManager,
        serializerManager,
        sparkConf,
        fetchMultiPartitions,
        dependency);
  }

  private static final class StartEndPartitions {
    private int startPartition = Integer.MAX_VALUE;
    private int endPartition = 0;
  }
}
