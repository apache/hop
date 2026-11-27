/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.spark.engines.check;

import io.fabric8.kubernetes.api.model.authorization.v1.SelfSubjectAccessReview;
import io.fabric8.kubernetes.api.model.authorization.v1.SelfSubjectAccessReviewBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.spark.engines.SparkClusterType;

/**
 * Kubernetes: the namespace exists, and the credentials of Hop may do in it what Spark and Hop do:
 * create, follow and delete pods, and create services and configmaps.
 */
public class KubernetesPermissionsCheck extends BaseSparkConfigurationCheck {

  public static final String ID = "KubernetesPermissions";

  /** Verb and resource pairs, as kubectl auth can-i asks them. */
  static final List<String[]> NEEDED =
      List.of(
          new String[] {"create", "pods"},
          new String[] {"get", "pods"},
          new String[] {"watch", "pods"},
          new String[] {"delete", "pods"},
          new String[] {"create", "services"},
          new String[] {"create", "configmaps"});

  public KubernetesPermissionsCheck() {
    super(ID);
  }

  @Override
  public boolean appliesTo(SparkCheckContext context) {
    return context.getClusterType() == SparkClusterType.KUBERNETES;
  }

  @Override
  public List<String> getRequiredChecks() {
    return List.of(KubernetesApiCheck.ID);
  }

  @Override
  public ICheckResult run(SparkCheckContext context) {
    KubernetesClient client = context.get(KubernetesApiCheck.CLIENT);
    String namespace = context.get(KubernetesApiCheck.NAMESPACE);
    try {
      if (client.namespaces().withName(namespace).get() == null) {
        return error(
            "The namespace '" + namespace + "' does not exist.",
            "Create it (kubectl create namespace "
                + namespace
                + "), or set spark.kubernetes.namespace"
                + " in the Spark config to an existing one.");
      }
    } catch (KubernetesClientException e) {
      // Reading namespaces is a cluster-wide right that many users don't have: the reviews below
      // say enough
    }
    List<String> missing = new ArrayList<>();
    for (String[] need : NEEDED) {
      SelfSubjectAccessReview review =
          new SelfSubjectAccessReviewBuilder()
              .withNewSpec()
              .withNewResourceAttributes()
              .withNamespace(namespace)
              .withVerb(need[0])
              .withResource(need[1])
              .endResourceAttributes()
              .endSpec()
              .build();
      SelfSubjectAccessReview answer =
          client.authorization().v1().selfSubjectAccessReview().create(review);
      if (answer.getStatus() == null || !Boolean.TRUE.equals(answer.getStatus().getAllowed())) {
        missing.add(need[0] + " " + need[1]);
      }
    }
    if (!missing.isEmpty()) {
      return error(
          "In the namespace '"
              + namespace
              + "' these credentials may not: "
              + String.join(", ", missing)
              + ".",
          "Ask the cluster administrators for a RoleBinding with these rights in '"
              + namespace
              + "' (the ClusterRole edit has them); kubectl auth can-i create pods -n "
              + namespace
              + " shows what you may do.");
    }
    return ok(
        "In the namespace '"
            + namespace
            + "' these credentials may create, follow and delete pods,"
            + " and create services and configmaps.");
  }
}
