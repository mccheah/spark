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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Optional;

import org.apache.spark.SparkConf;
import org.apache.spark.api.shuffle.ShuffleBlockByteChannel;
import org.apache.spark.api.shuffle.ShuffleBlockOutputStream;
import org.apache.spark.api.shuffle.ShuffleLocation;
import org.apache.spark.api.shuffle.ShuffleMapOutputChannelWriter;
import org.apache.spark.internal.config.package$;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.util.Utils;

public class DefaultMapOutputWriter implements ShuffleMapOutputChannelWriter {

  private static final Logger log = LoggerFactory.getLogger(DefaultMapOutputWriter.class);

  private final int shuffleId;
  private final int mapId;
  private final ShuffleLocation location;
  private final IndexShuffleBlockResolver blockResolver;
  private final long[] partitionLengths;
  private final File outputFile;
  private final File outputTempFile;
  private final int outputBufferSizeInBytes;
  private FileOutputStream outputFileStream;
  private BufferedOutputStream outputBufferedFileStream;
  private FileChannel outputFileChannel;
  private DefaultShuffleBlockOutputStream currentPartitionOutputStream;
  private DefaultShuffleBlockByteChannel currentPartitionByteChannel;
  private boolean bytesWritten = false;
  private int nextPartitionId = 0;

  public DefaultMapOutputWriter(
      int shuffleId,
      int mapId,
      int numPartitions,
      BlockManagerId blockManagerId,
      IndexShuffleBlockResolver blockResolver,
      SparkConf sparkConf) {
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.location = new DefaultShuffleLocation(blockManagerId);
    this.blockResolver = blockResolver;
    this.outputBufferSizeInBytes =
        // TODO but now the buffer size conf is used for both safe and unsafe write - thus is this
        // still correct?
        (int) (long) sparkConf.get(
            package$.MODULE$.SHUFFLE_UNSAFE_FILE_OUTPUT_BUFFER_SIZE()) * 1024;
    this.partitionLengths = new long[numPartitions];
    this.outputFile = blockResolver.getDataFile(shuffleId, mapId);
    this.outputTempFile = Utils.tempFileWith(outputFile);
    for (int i = 0; i < partitionLengths.length; i++) {
      partitionLengths[i] = 0L;
    }
  }

  @Override
  public ShuffleBlockByteChannel getNextPartitionByteChannel() throws IOException {
    initializeChannelIfNecessary();
    if (currentPartitionByteChannel != null && !currentPartitionByteChannel.isClosed) {
      throw new IllegalStateException("Must close the previous byte channel before requesting the" +
          " next one.");
    }
    currentPartitionByteChannel = new DefaultShuffleBlockByteChannel(nextPartitionId++);
    return currentPartitionByteChannel;
  }

  @Override
  public ShuffleBlockOutputStream getNextPartitionOutputStream() throws IOException {
    initializeStreamIfNecessary();
    if (currentPartitionOutputStream != null && !currentPartitionOutputStream.isClosed) {
      throw new IllegalStateException("Must close the previous output stream before requesting" +
          " the next one.");
    }
    currentPartitionOutputStream = new DefaultShuffleBlockOutputStream(nextPartitionId++);
    return currentPartitionOutputStream;
  }

  @Override
  public void commit() throws IOException {
    closeResources();
    blockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, outputTempFile);
    if (!bytesWritten) {
      if (!outputFile.getParentFile().isDirectory() && !outputFile.getParentFile().mkdirs()) {
        throw new IOException(
            String.format(
                "Failed to create shuffle file directory at %s.",
                outputFile.getParentFile().getAbsolutePath()));
      }
      if (!outputFile.isFile() && !outputFile.createNewFile()) {
        throw new IOException(
            String.format(
                "Failed to create empty shuffle file at %s.", outputFile.getAbsolutePath()));
      }
    }
  }

  @Override
  public void abort(Throwable error) throws IOException {
    closeResources();
    if (!outputTempFile.delete()) {
      log.warn("Failed to delete temporary shuffle file at {}", outputTempFile.getAbsolutePath());
    }
    if (!outputFile.delete()) {
      log.warn("Failed to delete outputshuffle file at {}", outputTempFile.getAbsolutePath());
    }
  }

  private void closeResources() throws IOException {
    if (outputFileChannel != null) {
      outputFileChannel.close();
    }

    if (outputBufferedFileStream != null) {
      outputBufferedFileStream.flush();
      outputBufferedFileStream.close();
    }

    if (outputFileStream != null) {
      outputFileStream.flush();
      outputFileStream.close();
    }
  }

  private void initializeStreamIfNecessary() throws IOException {
    if (outputFileStream == null) {
      outputFileStream = new FileOutputStream(outputTempFile, true);
    }

    if (outputBufferedFileStream == null) {
      outputBufferedFileStream =new BufferedOutputStream(
          outputFileStream, outputBufferSizeInBytes);
    }
  }

  private void initializeChannelIfNecessary() throws IOException {
    if (outputFileStream == null) {
      outputFileStream = new FileOutputStream(outputTempFile, true);
    }

    if (outputFileChannel == null) {
      outputFileChannel = outputFileStream.getChannel();
    }
  }

  private class DefaultShuffleBlockOutputStream extends ShuffleBlockOutputStream {

    private final int partitionId;
    private int count = 0;
    private boolean isClosed = false;

    private DefaultShuffleBlockOutputStream(int partitionId) {
      this.partitionId = partitionId;
    }

    @Override
    public Optional<ShuffleLocation> getBlockLocation() {
      return Optional.of(location);
    }

    @Override
    public int getCount() {
      return count;
    }

    @Override
    public void write(int b) throws IOException {
      if (isClosed) {
        throw new IllegalStateException("Attempting to write to a closed block byte channel.");
      }
      bytesWritten = true;
      outputBufferedFileStream.write(b);
      count++;
    }

    @Override
    public void close() throws IOException {
      flush();
      partitionLengths[partitionId] = count;
      isClosed = true;
    }

    @Override
    public void flush() throws IOException {
      outputBufferedFileStream.flush();
    }
  }

  private class DefaultShuffleBlockByteChannel implements ShuffleBlockByteChannel {

    private final int partitionId;
    private int count = 0;
    private boolean isClosed = false;

    private DefaultShuffleBlockByteChannel(int partitionId) {
      this.partitionId = partitionId;
    }

    @Override
    public Optional<ShuffleLocation> getBlockLocation() {
      return Optional.of(location);
    }

    @Override
    public int getCount() {
      return count;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
      if (isClosed) {
        throw new IllegalStateException("Attempting to write to a closed block output stream.");
      }
      bytesWritten = true;
      int written = outputFileChannel.write(src);
      count += written;
      return written;
    }

    @Override
    public boolean isOpen() {
      return !isClosed;
    }

    @Override
    public void close() {
      // Don't actually close the underlying channel here, only do so in commit or abort
      isClosed = true;
      partitionLengths[partitionId] = count;
    }
  }
}
