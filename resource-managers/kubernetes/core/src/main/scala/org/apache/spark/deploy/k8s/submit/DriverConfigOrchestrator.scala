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
package org.apache.spark.deploy.k8s.submit

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.{KubernetesConf, KuberneteSpec}
import org.apache.spark.deploy.k8s.features._
import org.apache.spark.launcher.SparkLauncher

/**
 * Figures out and returns the complete ordered list of needed KubernetesFeatureConfigSteps to
 * configure the Spark driver pod. The returned steps will be applied one by one in the given
 * order to produce a final KuberneteSpec that is used in KubernetesClientApplication
 * to construct and create the driver pod.
 */
private[spark] class DriverConfigOrchestrator(
    kubernetesAppId: String,
    kubernetesResourceNamePrefix: String,
    mainAppResource: Option[MainAppResource],
    appName: String,
    mainClass: String,
    appArgs: Array[String],
    sparkConf: SparkConf) {

  def getConfigSteps: Seq[Function1[KuberneteSpec, KuberneteSpec]] = {
    getFeatures.map(feature => {
      spec: KuberneteSpec => {
        val resolvedPod = feature.configurePod(spec.pod)
        val resolvedAdditionalResources =
          spec.additionalDriverKubernetesResources ++ feature.getAdditionalKubernetesResources()
        val resolvedPodSystemProps =
          spec.podJavaSystemProperties ++ feature.getAdditionalPodSystemProperties()
        KuberneteSpec(resolvedPod, resolvedAdditionalResources, resolvedPodSystemProps)
      }
    })
  }

  def getFeatures: Seq[KubernetesFeatureConfigStep] = {
    val kubernetesConf = KubernetesConf.createDriverConf(
      sparkConf,
      appName,
      kubernetesResourceNamePrefix,
      kubernetesAppId,
      mainAppResource,
      mainClass,
      appArgs)

    val baseDriverStep = new BasicDriverFeatureStep(kubernetesConf)

    val driverKubeCredStep =
      new DriverKubernetesCredentialsFeatureStep(kubernetesConf)

    val driverServiceStep =
      new DriverServiceFeatureStep(kubernetesConf)

    val additionalMainAppJar = if (mainAppResource.nonEmpty) {
       val mayBeResource = mainAppResource.get match {
        case JavaMainAppResource(resource) if resource != SparkLauncher.NO_RESOURCE =>
          Some(resource)
        case _ => None
      }
      mayBeResource
    } else {
      None
    }

    Seq(
      baseDriverStep,
      driverKubeCredStep,
      driverServiceStep)
  }
}
