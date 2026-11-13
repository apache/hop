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

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import java.net.URI;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.spark.engines.SparkClusterType;

/**
 * Kubernetes: the API server answers with the credentials of the kubeconfig (or of the pod Hop runs
 * in), as the Spark client finds them.
 */
public class KubernetesApiCheck extends BaseSparkConfigurationCheck {

  public static final String ID = "KubernetesApi";

  static final String CLIENT = "kubernetes.client";
  static final String NAMESPACE = "kubernetes.namespace";

  public KubernetesApiCheck() {
    super(ID);
  }

  @Override
  public boolean appliesTo(SparkCheckContext context) {
    return context.getClusterType() == SparkClusterType.KUBERNETES;
  }

  @Override
  public List<String> getRequiredChecks() {
    return List.of(ClusterTypeCheck.ID);
  }

  @Override
  public ICheckResult run(SparkCheckContext context) {
    String apiServer = apiServer(context.getMaster());
    URI uri = URI.create(apiServer);
    int port = uri.getPort() > 0 ? uri.getPort() : ("http".equals(uri.getScheme()) ? 80 : 443);
    String unreachable = YarnResourceManagerCheck.unreachable(uri.getHost() + ":" + port);
    if (unreachable != null) {
      return error(
          "The Kubernetes API server " + apiServer + " doesn't answer: " + unreachable + ".",
          "Check the host and port in the Spark master (k8s://https://<api server>:6443), and that"
              + " the port is open to this machine.");
    }
    Config config =
        new ConfigBuilder(Config.autoConfigure(null))
            .withMasterUrl(apiServer)
            .withConnectionTimeout(StandaloneMasterCheck.TIMEOUT_MS)
            .withRequestTimeout(StandaloneMasterCheck.TIMEOUT_MS)
            .withRequestRetryBackoffLimit(0)
            .build();
    KubernetesClient client = new KubernetesClientBuilder().withConfig(config).build();
    context.put(CLIENT, client);
    context.put(
        NAMESPACE, context.getSparkConfigs().getOrDefault("spark.kubernetes.namespace", "default"));
    try {
      String version = client.getKubernetesVersion().getGitVersion();
      String user =
          config.getCurrentContext() == null ? null : config.getCurrentContext().getName();
      return ok(
          "Kubernetes "
              + version
              + " answers at "
              + apiServer
              + (user == null
                  ? "."
                  : ", with the credentials of the kubeconfig context '" + user + "'."));
    } catch (KubernetesClientException e) {
      if (e.getCode() == 401 || e.getCode() == 403) {
        return error(
            "The API server "
                + apiServer
                + " refuses the credentials: "
                + e.getStatus().getMessage()
                + ".",
            "Point KUBECONFIG (or ~/.kube/config) at a context with credentials for this cluster;"
                + " its token or certificate may have expired.");
      }
      return error(
          "The API server " + apiServer + " doesn't answer: " + e.getMessage(),
          "Check the Spark master and the kubeconfig: the API server's certificate must be trusted"
              + " (certificate-authority-data in the kubeconfig).");
    }
  }

  /** {@code k8s://https://host:port} gives {@code https://host:port}; no scheme means https. */
  static String apiServer(String master) {
    String url = master.trim().substring("k8s://".length());
    return url.contains("://") ? url : "https://" + url;
  }
}
