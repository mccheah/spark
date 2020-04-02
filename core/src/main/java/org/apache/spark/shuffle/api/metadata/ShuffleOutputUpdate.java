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

import java.io.Serializable;

/**
 * Represents an opaque message to dynamically update information about the storage of shuffle
 * data.
 * <p>
 * Executors are expected to send appropriate implementations of these to the driver via
 * {@link ShuffleUpdater#updateShuffleOutput(ShuffleOutputUpdate)}, and the driver receives
 * these updates in {@link ShuffleOutputTracker#updateShuffleOutput(ShuffleOutputUpdate)}.
 */
public interface ShuffleOutputUpdate extends Serializable {}
