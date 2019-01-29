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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import org.apache.spark.api.shuffle.ShuffleLocation;
import org.apache.spark.storage.BlockManagerId;

public class DefaultShuffleLocation implements ShuffleLocation {

  private final BlockManagerId blockManagerId;
  private ByteBuffer encoded;

  public DefaultShuffleLocation(BlockManagerId blockManagerId) {
    this.blockManagerId = blockManagerId;
  }

  @Override
  public long locationId() {
    return blockManagerId.hashCode();
  }

  @Override
  public ByteBuffer locationEncoded() {
    if (encoded == null) {
      synchronized(this) {
        if (encoded == null) {
          ByteArrayOutputStream encodingStream = new ByteArrayOutputStream();
          try {
            ObjectOutputStream objectOutput = new ObjectOutputStream(encodingStream);
            blockManagerId.writeExternal(objectOutput);
            objectOutput.flush();
            encodingStream.flush();
          } catch (IOException e) {
            throw new IllegalArgumentException("Failed to encode block manager id.", e);
          }
          encoded = ByteBuffer.wrap(encodingStream.toByteArray()).asReadOnlyBuffer();
        }
      }
    }
    return encoded;
  }
}
