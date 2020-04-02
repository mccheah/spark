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

package org.apache.spark.shuffle.api.metadata;

import java.util.Optional;

/**
 * :: Private ::
 *
 * A plugin that can monitor the storage of shuffle data from map tasks, and can provide
 * metadata to shuffle readers to aid their reading of shuffle blocks in reduce tasks.
 * <p>
 * {@link MapOutputMetadata} instances provided from the plugin tree's implementation of
 * {@link org.apache.spark.shuffle.api.ShuffleMapOutputWriter} are sent to
 * <p>
 * Implementations MUST be thread-safe. Spark will invoke methods in this module in parallel.
 */
public interface ShuffleOutputTracker {

  /**
   * Called when a new shuffle stage is going to be run.
   */
  void registerShuffle(int shuffleId);

  /**
   * Called when the shuffle with the given id is unregistered because it will no longer
   * be used by Spark tasks.
   */
  void unregisterShuffle(int shuffleId, boolean blocking);

  /**
   * Called when a map task completes, and the map output writer has provided metadata to be
   * persisted by this shuffle output tracker.
   */
  void registerMapOutput(
      int shuffleId, int mapId, long mapTaskAttemptId, MapOutputMetadata mapOutputMetadata);

  /**
   * Called when the given map output is discarded, and will not longer be used in future Spark
   * shuffles.
   */
  void removeMapOutput(int shuffleId, int mapId, long mapTaskAttemptId);

  /**
   * Called when a shuffle reduce stage starts, and shuffle readers thus need extra metadata
   * to read shuffle blocks.
   * <p>
   * Implementations can return an empty value if no shuffle metadata is available for the given
   * shuffle id.
   */
  Optional<ShuffleMetadata> getShuffleMetadata(int shuffleId);

  /**
   * Called when a shuffle reader implementation throws a fetch failure exception for the given
   * shuffle block.
   * <p>
   * Implementations can choose to update internal state accordingly.
   */
  default void handleFetchFailure(
      int shuffleId,
      int mapId,
      int reduceId,
      long mapTaskAttemptId,
      Optional<ShuffleBlockMetadata> blockMetadata) {}

  /**
   * Called when an executor is lost.
   * <p>
   * Shuffle plugin implementations that store any state on the lost executor should update
   * internal state accordingly.
   */
  default void handleExecutorLost(String executorId) {}

  /**
   * Called when a host running one or more executors is lost.
   * <p>
   * Shuffle plugin implementations that store any state on the lost host should update
   * internal state accordingly.
   */
  default void handleHostLost(String host) {}

  /**
   * Inspect whether or not all partitions from the given map output are available in an external
   * system, i.e. offloaded from an executor.
   * <p>
   * If the executor that ran the map task is lost, all map outputs that were written by that
   * executor are checked against this method. For each map output that is not stored externally,
   * that map output has to be recomputed.
   */
  boolean isMapOutputAvailableExternally(
      int shuffleId, int mapId, long mapTaskAttemptId);
}
