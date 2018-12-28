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

package org.apache.spark.deploy.k8s

import java.io.{File, FileInputStream}
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.function.BiFunction

import org.apache.commons.io.IOUtils
import scala.collection.JavaConverters._

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.ExternalShuffleService
import org.apache.spark.deploy.k8s.Config.KUBERNETES_REMOTE_SHUFFLE_SERVICE_CLEANUP_INTERVAL
import org.apache.spark.internal.Logging
import org.apache.spark.network.client.{RpcResponseCallback, StreamCallbackWithID, TransportClient}
import org.apache.spark.network.shuffle._
import org.apache.spark.network.shuffle.ExternalShuffleBlockResolver._
import org.apache.spark.network.shuffle.protocol._
import org.apache.spark.network.util.{JavaUtils, TransportConf}
import org.apache.spark.util.ThreadUtils

/**
 * An RPC endpoint that receives registration requests from Spark drivers running on Kubernetes.
 * It detects driver termination and calls the cleanup callback to [[ExternalShuffleService]].
 */
private[spark] class KubernetesExternalShuffleBlockHandler(
    transportConf: TransportConf,
    cleanerIntervalS: Long)
  extends ExternalShuffleBlockHandler(transportConf, null) with Logging {

  ThreadUtils.newDaemonSingleThreadScheduledExecutor("shuffle-cleaner-watcher")
    .scheduleAtFixedRate(new CleanerThread(), 0, cleanerIntervalS, TimeUnit.SECONDS)

  // Stores a map of app id to app state (timeout value and last heartbeat)
  private val connectedApps = new ConcurrentHashMap[String, AppState]()
  private val registeredExecutors =
    new ConcurrentHashMap[String, Map[String, ExecutorShuffleInfo]]()

  private val knownManagers = Array(
    "org.apache.spark.shuffle.sort.SortShuffleManager",
    "org.apache.spark.shuffle.unsafe.UnsafeShuffleManager")
  private final val shuffleDir = Files.createTempDirectory("spark-shuffle-dir").toFile()

  protected override def handleMessage(
      message: BlockTransferMessage,
      client: TransportClient,
      callback: RpcResponseCallback): Unit = {
    message match {
      case RegisterExecutorParam(appId, execId, shuffleManager) =>
        val fullId = new AppExecId(appId, execId)
        if (registeredExecutors.containsKey(fullId)) {
          throw new UnsupportedOperationException(s"Executor $fullId cannot be registered twice")
        }
        val executorDir = Paths.get(shuffleDir.getAbsolutePath, appId, execId).toFile
        if (!executorDir.mkdir()) {
          throw new RuntimeException(s"Failed to create dir ${executorDir.getAbsolutePath}")
        }
        if (!knownManagers.contains(shuffleManager)) {
          throw new UnsupportedOperationException(s"Unsupported shuffle manager of exec: ${fullId}")
        }
        val executorShuffleInfo = new ExecutorShuffleInfo(
          Array(executorDir.getAbsolutePath), 1, shuffleManager)
        val execMap = Map(execId -> executorShuffleInfo)
        registeredExecutors.merge(appId, execMap,
          new BiFunction[
            Map[String, ExecutorShuffleInfo],
            Map[String, ExecutorShuffleInfo],
            Map[String, ExecutorShuffleInfo]]() {
            override def apply(
                t: Map[String, ExecutorShuffleInfo], u: Map[String, ExecutorShuffleInfo]):
            Map[String, ExecutorShuffleInfo] = {
              t ++ u
            }
          })
        logInfo(s"Registering executor ${fullId} with ${executorShuffleInfo}")

      case RegisterDriverParam(appId, appState) =>
        val address = client.getSocketAddress
        val timeout = appState.heartbeatTimeout
        logInfo(s"Received registration request from app $appId (remote address $address, " +
          s"heartbeat timeout $timeout ms).")
        if (connectedApps.containsKey(appId)) {
          logWarning(s"Received a registration request from app $appId, but it was already " +
            s"registered")
        }
        val driverDir = Paths.get(shuffleDir.getAbsolutePath, appId).toFile
        if (!driverDir.mkdir()) {
          throw new RuntimeException(s"Failed to create dir ${driverDir.getAbsolutePath}")
        }
        connectedApps.put(appId, appState)
        registeredExecutors.put(appId, Map[String, ExecutorShuffleInfo]())
        callback.onSuccess(ByteBuffer.allocate(0))

      case Heartbeat(appId) =>
        val address = client.getSocketAddress
        Option(connectedApps.get(appId)) match {
          case Some(existingAppState) =>
            logTrace(s"Received ShuffleServiceHeartbeat from app '$appId' (remote " +
              s"address $address).")
            existingAppState.lastHeartbeat = System.nanoTime()
          case None =>
            logWarning(s"Received ShuffleServiceHeartbeat from an unknown app (remote " +
              s"address $address, appId '$appId').")
          case OpenParam(appId, execId, shuffleId, mapId, partitionId) =>
            logInfo(s"Received open param from app $appId from $execId")
            val file = getFile(
              appId, execId, shuffleId, mapId, "data", FileWriterStreamCallback.FileType.DATA)
            val fileInputStream = new FileInputStream(file)
            val bytes = IOUtils.toByteArray(fileInputStream)
            callback.onSuccess(ByteBuffer.wrap(bytes))
        }
      case _ => super.handleMessage(message, client, callback)
    }
  }

  protected override def handleStream(
    header: BlockTransferMessage,
    client: TransportClient,
    callback: RpcResponseCallback): StreamCallbackWithID = {
    header match {
      case UploadParam(
          appId, execId, shuffleId, mapId, partitionId) =>
        logInfo(s"Received upload param from app $appId from $execId")
        getFileWriterStreamCallback(
          appId, execId, shuffleId, mapId, "data", FileWriterStreamCallback.FileType.DATA)
      case _ =>
        super.handleStream(header, client, callback)
    }
  }

  private def getFileWriterStreamCallback(
      appId: String,
      execId: String,
      shuffleId: Int,
      mapId: Int,
      extension: String,
      fileType: FileWriterStreamCallback.FileType): StreamCallbackWithID = {
    val file = getFile(appId, execId, shuffleId, mapId, extension, fileType)
    val streamCallback =
      new FileWriterStreamCallback(new AppExecId(appId, execId), shuffleId, mapId, file, fileType)
    streamCallback.open()
    streamCallback
  }

  private def getFile(
      appId: String,
      execId: String,
      shuffleId: Int,
      mapId: Int,
      extension: String,
      fileType: FileWriterStreamCallback.FileType): File = {
    val execMap = registeredExecutors.get(appId)
    if (execMap == null) {
    throw new RuntimeException(
    s"appId=$appId is not registered for remote shuffle")
  }
    val executor = execMap(execId)
    if (executor == null) {
    throw new RuntimeException(
    s"App is not registered for remote shuffle (appId=$appId, execId=$execId)")
  }
    ExternalShuffleBlockResolver.getFile(executor.localDirs, executor.subDirsPerLocalDir,
    "shuffle_" + shuffleId + "_" + mapId + "_0." + extension)
  }

  /** An extractor object for matching BlockTransferMessages. */
  private object RegisterDriverParam {
    def unapply(r: RegisterDriver): Option[(String, AppState)] =
      Some((r.getAppId, new AppState(r.getHeartbeatTimeoutMs, System.nanoTime())))
  }

  private object Heartbeat {
    def unapply(h: ShuffleServiceHeartbeat): Option[String] = Some(h.getAppId)
  }

  private object UploadParam {
    def unapply(u: UploadShufflePartitionStream): Option[(String, String, Int, Int, Int)] =
      Some((u.appId, u.execId, u.shuffleId, u.mapId, u.partitionId))
  }

  private object RegisterExecutorParam {
    def unapply(e: RegisterExecutorWithExternal): Option[(String, String, String)] =
      Some((e.appId, e.execId, e.shuffleManager))
  }

  private object OpenParam {
    def unapply(o: OpenShufflePartition): Option[(String, String, Int, Int, Int)] =
      Some((o.appId, o.execId, o.shuffleId, o.mapId, o.partitionId))
  }

  private class AppState(val heartbeatTimeout: Long, @volatile var lastHeartbeat: Long)

  private class CleanerThread extends Runnable {
    override def run(): Unit = {
      val now = System.nanoTime()
      connectedApps.asScala.foreach { case (appId, appState) =>
        if (now - appState.lastHeartbeat > appState.heartbeatTimeout * 1000 * 1000) {
          logInfo(s"Application $appId timed out. Removing shuffle files.")
          connectedApps.remove(appId)
          applicationRemoved(appId, false)
          registeredExecutors.remove(appId)
          // TODO: Write removal logic
        }
      }
    }
  }
}

/**
 * A wrapper of [[ExternalShuffleService]] that provides an additional endpoint for drivers
 * to associate with. This allows the shuffle service to detect when a driver is terminated
 * and can clean up the associated shuffle files.
 */
private[spark] class KubernetesExternalShuffleService(
    conf: SparkConf, securityManager: SecurityManager)
  extends ExternalShuffleService(conf, securityManager) {

  protected override def newShuffleBlockHandler(
      conf: TransportConf): ExternalShuffleBlockHandler = {
    val cleanerIntervalS = this.conf.get(KUBERNETES_REMOTE_SHUFFLE_SERVICE_CLEANUP_INTERVAL)
    new KubernetesExternalShuffleBlockHandler(conf, cleanerIntervalS)
  }
}

private[spark] object KubernetesExternalShuffleService extends Logging {

  def main(args: Array[String]): Unit = {
    ExternalShuffleService.main(args,
      (conf: SparkConf, sm: SecurityManager) => new KubernetesExternalShuffleService(conf, sm))
  }
}


