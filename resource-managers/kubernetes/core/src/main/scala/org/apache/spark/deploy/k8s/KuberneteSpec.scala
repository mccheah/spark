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

import io.fabric8.kubernetes.api.model.ContainerBuilder
import io.fabric8.kubernetes.api.model.HasMetadata
import io.fabric8.kubernetes.api.model.PodBuilder
import org.apache.spark.SparkConf

import org.apache.spark.deploy.k8s.submit.KubernetesDriverSpec

private[k8s] case class KuberneteSpec(
  pod: SparkPod,
  additionalDriverKubernetesResources: Seq[HasMetadata],
  podJavaSystemProperties: Map[String, String])

private[k8s] object KuberneteSpec {
  def initialSpec(): KuberneteSpec = {
    KuberneteSpec(
      SparkPod(
        // Set new metadata and a new spec so that submission steps can use
        // PodBuilder#editMetadata() and/or PodBuilder#editSpec() safely.
        new PodBuilder().withNewMetadata().endMetadata().withNewSpec().endSpec().build(),
        new ContainerBuilder().build()
      ),
      Seq.empty[HasMetadata],
      Map.empty[String,String]
  }
}
