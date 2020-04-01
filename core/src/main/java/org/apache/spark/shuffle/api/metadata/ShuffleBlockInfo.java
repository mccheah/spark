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

import org.apache.spark.storage.BlockManagerId;

public class ShuffleBlockInfo {
  private final int shuffleId;
  private final int mapIndex;
  private final int reduceId;
  private final long blockLength;
  private final long mapTaskAttemptId;
  private final BlockManagerId shuffleLocation;

  public ShuffleBlockInfo(
      int shuffleId,
      int mapIndex,
      int reduceId,
      long blockLength,
      long mapTaskAttemptId,
      BlockManagerId mapperLocation) {
    this.shuffleId = shuffleId;
    this.mapIndex = mapIndex;
    this.reduceId = reduceId;
    this.blockLength = blockLength;
    this.mapTaskAttemptId = mapTaskAttemptId;
    this.shuffleLocation = mapperLocation;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getMapIndex() {
    return mapIndex;
  }

  public int getReduceId() {
    return reduceId;
  }

  public long getBlockLength() {
    return blockLength;
  }

  public long getMapTaskAttemptId() {
    return mapTaskAttemptId;
  }

  public BlockManagerId getShuffleLocation() {
    return shuffleLocation;
  }
}
