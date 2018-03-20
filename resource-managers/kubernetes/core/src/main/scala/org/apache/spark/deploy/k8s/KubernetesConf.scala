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

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit.JavaMainAppResource
import org.apache.spark.deploy.k8s.submit.MainAppResource
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.launcher.SparkLauncher

private[k8s] sealed trait KubernetesRoleSpecificConf
private[k8s] case class KubernetesDriverSpecificConf(
  private val sparkConf: SparkConf,
  mainAppResource: Option[MainAppResource],
  mainClass: String,
  appName: String,
  appArgs: Seq[String]) extends KubernetesRoleSpecificConf {
  def driverAnnotations(): Map[String, String] = {
    val driverCustomAnnotations = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf, KUBERNETES_DRIVER_ANNOTATION_PREFIX)
    require(!driverCustomAnnotations.contains(SPARK_APP_NAME_ANNOTATION),
      s"Annotation with key $SPARK_APP_NAME_ANNOTATION is not allowed as it is reserved for" +
        " Spark bookkeeping operations.")
    driverCustomAnnotations ++ Map(SPARK_APP_NAME_ANNOTATION -> appName)
  }
}

private[k8s] case object KubernetesExecutorSpecificConf extends KubernetesRoleSpecificConf

private[k8s] class KubernetesConf[T <: KubernetesRoleSpecificConf](
  private val sparkConf: SparkConf,
  val roleSpecificConf: T,
  val appResourceNamePrefix: String,
  val appId: String,
  val mainAppResource: Option[MainAppResource]) {

  private val additionalMainAppJar = if (mainAppResource.nonEmpty) {
    val mayBeResource = mainAppResource.get match {
      case JavaMainAppResource(resource) if resource != SparkLauncher.NO_RESOURCE =>
        Some(resource)
      case _ => None
    }
    mayBeResource
  } else {
    None
  }

  def kubernetesAppId(): String = sparkConf
    .getOption("spark.app.id")
    .getOrElse(throw new SparkException("Invalid config state; an app id should always" +
      " be present."))

  def namespace(): String = sparkConf.get(KUBERNETES_NAMESPACE)

  def sparkJars(): Seq[String] = sparkConf
    .getOption("spark.jars")
    .map(str => str.split(",").toSeq)
    .getOrElse(Seq.empty[String])

  def sparkFiles(): Seq[String] =
    sparkConf.getOption("spark.files")
    .map(str => str.split(",").toSeq)
    .getOrElse(Seq.empty[String]) ++ additionalMainAppJar.toSeq

  def driverLabels(): Map[String, String] = {
    val driverCustomLabels = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf,
      KUBERNETES_DRIVER_LABEL_PREFIX)
    require(!driverCustomLabels.contains(SPARK_APP_ID_LABEL), "Label with key " +
      s"$SPARK_APP_ID_LABEL is not allowed as it is reserved for Spark bookkeeping " +
      "operations.")
    require(!driverCustomLabels.contains(SPARK_ROLE_LABEL), "Label with key " +
      s"$SPARK_ROLE_LABEL is not allowed as it is reserved for Spark bookkeeping " +
      "operations.")
    driverCustomLabels ++ Map(
      SPARK_APP_ID_LABEL -> kubernetesAppId,
      SPARK_ROLE_LABEL -> SPARK_POD_DRIVER_ROLE)
  }

  def executorLabels(executorId: String): Map[String, String] = {
    val executorLabels = KubernetesUtils.parsePrefixedKeyValuePairs(
      sparkConf,
      KUBERNETES_EXECUTOR_LABEL_PREFIX)
    require(
      !executorLabels.contains(SPARK_APP_ID_LABEL),
      s"Custom executor labels cannot contain $SPARK_APP_ID_LABEL as it is reserved for Spark.")
    require(
      !executorLabels.contains(SPARK_EXECUTOR_ID_LABEL),
      s"Custom executor labels cannot contain $SPARK_EXECUTOR_ID_LABEL as it is reserved for" +
        " Spark.")
    require(
      !executorLabels.contains(SPARK_ROLE_LABEL),
      s"Custom executor labels cannot contain $SPARK_ROLE_LABEL as it is reserved for Spark.")
    Map(
      SPARK_EXECUTOR_ID_LABEL -> executorId,
      SPARK_APP_ID_LABEL -> kubernetesAppId(),
      SPARK_ROLE_LABEL -> SPARK_POD_EXECUTOR_ROLE) ++
      executorLabels
  }

  def driverCustomEnvs(): Seq[(String, String)] =
    sparkConf.getAllWithPrefix(KUBERNETES_DRIVER_ENV_KEY).toSeq

  def imagePullPolicy(): String = sparkConf.get(CONTAINER_IMAGE_PULL_POLICY)

  def driverSecretNamesToMountPaths(): Map[String, String] =
    KubernetesUtils.parsePrefixedKeyValuePairs(sparkConf, KUBERNETES_DRIVER_SECRETS_PREFIX)

  def executorSecretNamesToMountPaths(): Map[String, String] =
    KubernetesUtils.parsePrefixedKeyValuePairs(sparkConf, KUBERNETES_EXECUTOR_SECRETS_PREFIX)

  def nodeSelector(): Map[String, String] =
    KubernetesUtils.parsePrefixedKeyValuePairs(sparkConf, KUBERNETES_NODE_SELECTOR_PREFIX)

  def getSparkConf(): SparkConf = sparkConf.clone()

  def get[T](config: ConfigEntry[T]): T = sparkConf.get(config)

  def getOption(key: String): Option[String] = sparkConf.getOption(key)

}

private[k8s] object KubernetesConf {
  def createDriverConf(
    sparkConf: SparkConf,
    appName: String,
    appResourceNamePrefix: String,
    appId: String,
    mainAppResource: Option[MainAppResource],
    mainClass: String,
    appArgs: Array[String]): KubernetesConf[KubernetesDriverSpecificConf] = {
    new KubernetesConf(
      sparkConf,
      KubernetesDriverSpecificConf(
        sparkConf, mainAppResource, appName, mainClass, appArgs),
      appResourceNamePrefix,
      appId,
      mainAppResource)
  }
}
