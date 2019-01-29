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

import java.io.Closeable

import org.apache.spark.api.shuffle.ShuffleBlockOutputStream
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.serializer.{SerializerInstance, SerializerManager}
import org.apache.spark.storage.BlockId
import org.apache.spark.util.collection.PairsWriter

class ShuffleMapOutputObjectWriter(
    blockId: BlockId,
    serializerManager: SerializerManager,
    serializerInstance: SerializerInstance,
    writeMetrics: ShuffleWriteMetrics,
    shuffleBlockStream: ShuffleBlockOutputStream) extends PairsWriter with Closeable {

  private val wrapped = serializerManager.wrapStream(blockId, shuffleBlockStream)
  private val objOut = serializerInstance.serializeStream(wrapped)

  override def write(key: Any, value: Any): Unit = {
    objOut.writeKey(key)
    objOut.writeValue(value)
    writeMetrics.incRecordsWritten(1)
  }

  override def close(): Unit = {
    objOut.flush()
    objOut.close()
    wrapped.flush()
    wrapped.close()
    shuffleBlockStream.flush()
    shuffleBlockStream.close()
    writeMetrics.incBytesWritten(shuffleBlockStream.getCount)
  }
}
