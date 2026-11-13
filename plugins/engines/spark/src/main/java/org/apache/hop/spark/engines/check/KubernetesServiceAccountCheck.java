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

import io.fabric8.kubernetes.api.model.authorization.v1.SubjectAccessReview;
import io.fabric8.kubernetes.api.model.authorization.v1.SubjectAccessReviewBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.spark.engines.SparkClusterType;

/**
 * Kubernetes, cluster mode: the service account of the driver pod exists, and may create the
 * executor pods with their services and configmaps.
 */
public class KubernetesServiceAccountCheck extends BaseSparkConfigurationCheck {

  public static final String ID = "KubernetesServiceAccount";

  public KubernetesServiceAccountCheck() {
    super(ID);
  }

  @Override
  public boolean appliesTo(SparkCheckContext context) {
    return context.getClusterType() == SparkClusterType.KUBERNETES && context.isClusterMode();
  }

  @Override
  public List<String> getRequiredChecks() {
    return List.of(KubernetesApiCheck.ID);
  }

  @Override
  public ICheckResult run(SparkCheckContext context) {
    KubernetesClient client = context.get(KubernetesApiCheck.CLIENT);
    String namespace = context.get(KubernetesApiCheck.NAMESPACE);
    String account =
        context
            .getSparkConfigs()
            .getOrDefault("spark.kubernetes.authenticate.driver.serviceAccountName", "default");
    String user = "system:serviceaccount:" + namespace + ":" + account;
    String canI = "kubectl auth can-i create pods -n " + namespace + " --as=" + user;
    try {
      if (client.serviceAccounts().inNamespace(namespace).withName(account).get() == null) {
        return error(
            "The driver's service account '" + account + "' does not exist in '" + namespace + "'.",
            "Create it with a RoleBinding to the ClusterRole edit, or set"
                + " spark.kubernetes.authenticate.driver.serviceAccountName to an existing one.");
      }
      List<String> missing = new ArrayList<>();
      for (String[] need :
          List.of(
              new String[] {"create", "pods"},
              new String[] {"create", "services"},
              new String[] {"create", "configmaps"})) {
        SubjectAccessReview answer =
            client
                .authorization()
                .v1()
                .subjectAccessReview()
                .create(
                    new SubjectAccessReviewBuilder()
                        .withNewSpec()
                        .withUser(user)
                        .withNewResourceAttributes()
                        .withNamespace(namespace)
                        .withVerb(need[0])
                        .withResource(need[1])
                        .endResourceAttributes()
                        .endSpec()
                        .build());
        if (answer.getStatus() == null || !Boolean.TRUE.equals(answer.getStatus().getAllowed())) {
          missing.add(need[0] + " " + need[1]);
        }
      }
      if (!missing.isEmpty()) {
        return error(
            "The driver's service account '"
                + account
                + "' may not: "
                + String.join(", ", missing)
                + ".",
            "Bind it to the ClusterRole edit in '"
                + namespace
                + "' (a RoleBinding); "
                + canI
                + " shows"
                + " what it may do.");
      }
      return ok("The driver runs as '" + account + "', which may create the executor pods.");
    } catch (KubernetesClientException e) {
      if (e.getCode() == 403) {
        return warning(
            "These credentials may not check the rights of the service account '" + account + "'.",
            "Ask the cluster administrators to run: " + canI);
      }
      throw e;
    }
  }
}
