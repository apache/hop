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

package org.apache.hop.spark.submit;

import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.cluster.ISparkClusterSubmitter;
import org.apache.hop.core.cluster.SparkDriverStatus;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;

/**
 * Submits a driver to Kubernetes in cluster deploy mode, and follows and stops its driver pod.
 *
 * <p>Submission runs Spark's own Kubernetes client in a short-lived JVM of its own (see {@link
 * #runSubmitProcess}); Hop names the driver pod itself ({@code hop-<id>}) and follows the pod's
 * phase through the Kubernetes API. Stop deletes the driver pod: Kubernetes then removes the
 * executor pods, which the driver pod owns. The id of a submission is {@code <namespace>:<driver
 * pod>}, the form {@code spark-submit --status} uses.
 */
public class SparkKubernetesSubmitter implements ISparkClusterSubmitter {

  static final String IMAGE = "spark.kubernetes.container.image";
  static final String NAMESPACE = "spark.kubernetes.namespace";
  static final String DRIVER_POD_NAME = "spark.kubernetes.driver.pod.name";

  /** How long Kubernetes may take to create the driver pod. */
  static final long SUBMIT_TIMEOUT_MILLIS = 5 * 60 * 1000L;

  static final String CANCELLED = "The Kubernetes submission was cancelled";

  private final ILogChannel log;
  private final String master;
  private KubernetesClient client;

  /** Set by {@link #cancelSubmission()}: Stop came while spark-submit was still running. */
  private volatile boolean cancelled;

  private volatile Process submitProcess;

  /**
   * @param master {@code k8s://https://host:port}
   */
  public SparkKubernetesSubmitter(ILogChannel log, String master) {
    this.log = log;
    this.master = master;
  }

  @Override
  public String submit(
      String appResource,
      String mainClass,
      List<String> appArgs,
      Map<String, String> sparkProperties,
      Map<String, String> environmentVariables)
      throws HopException {
    if (StringUtils.isBlank(sparkProperties.get(IMAGE))) {
      throw new HopException(
          "Cluster mode on Kubernetes needs the container image of the driver and the executors:"
              + " set "
              + IMAGE
              + " in the Spark config of the run configuration, an image with Spark "
              + org.apache.spark.package$.MODULE$.SPARK_VERSION()
              + " and Java 21.");
    }
    String namespace = StringUtils.defaultIfBlank(sparkProperties.get(NAMESPACE), "default");
    String podName =
        StringUtils.defaultIfBlank(
            sparkProperties.get(DRIVER_POD_NAME),
            "hop-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12));

    Map<String, String> conf = new java.util.LinkedHashMap<>();
    for (Map.Entry<String, String> property : sparkProperties.entrySet()) {
      if (!"spark.master".equals(property.getKey())
          && !"spark.submit.deployMode".equals(property.getKey())) {
        conf.put(property.getKey(), property.getValue());
      }
    }
    for (Map.Entry<String, String> variable : environmentVariables.entrySet()) {
      conf.put("spark.kubernetes.driverEnv." + variable.getKey(), variable.getValue());
    }
    conf.put(NAMESPACE, namespace);
    conf.put(DRIVER_POD_NAME, podName);
    // Hand back once the driver pod exists; following happens through the Kubernetes API
    conf.put("spark.kubernetes.submission.waitAppCompletion", "false");

    List<String> command = new java.util.ArrayList<>();
    command.add(java.nio.file.Paths.get(System.getProperty("java.home"), "bin", "java").toString());
    command.add("-cp");
    command.add(submitClassPath());
    command.add("org.apache.spark.deploy.SparkSubmit");
    command.addAll(List.of("--master", master, "--deploy-mode", "cluster", "--class", mainClass));
    for (Map.Entry<String, String> entry : conf.entrySet()) {
      command.add("--conf");
      command.add(entry.getKey() + "=" + entry.getValue());
    }
    command.add(appResource);
    command.addAll(appArgs);

    ClassLoader previous = usePluginClassLoader();
    try {
      runSubmitProcess(command, SUBMIT_TIMEOUT_MILLIS);
      long deadline = System.currentTimeMillis() + SUBMIT_TIMEOUT_MILLIS;
      while (pod(namespace, podName) == null) {
        if (cancelled) {
          throw new HopException(CANCELLED);
        }
        if (System.currentTimeMillis() > deadline) {
          throw new HopException(
              "Kubernetes did not create driver pod "
                  + namespace
                  + "/"
                  + podName
                  + " within "
                  + SUBMIT_TIMEOUT_MILLIS / 60000
                  + " minutes");
        }
        Thread.sleep(500L);
      }
    } catch (HopException e) {
      if (cancelled) {
        // spark-submit may have created the driver pod before it was stopped
        deletePodQuietly(namespace, podName);
      }
      throw e;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HopException("Interrupted while submitting to Kubernetes", e);
    } catch (Exception e) {
      throw new HopException("Unable to submit to Kubernetes at " + master, e);
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
    return namespace + ":" + podName;
  }

  /** Stop the spark-submit process of a submission that is still running. */
  @Override
  public void cancelSubmission() {
    cancelled = true;
    Process process = submitProcess;
    if (process != null) {
      process.destroyForcibly();
    }
  }

  private void deletePodQuietly(String namespace, String podName) {
    try {
      kubernetes().pods().inNamespace(namespace).withName(podName).delete();
      log.logBasic("Deleted driver pod " + namespace + "/" + podName + " of the stopped run");
    } catch (Exception e) {
      log.logError("Unable to delete driver pod " + namespace + "/" + podName, e);
    }
  }

  /**
   * Run Spark's submission client in a JVM of its own. Spark names the driver's config map once per
   * JVM ({@code spark-drv-<id>-conf-map}, a static field), and the kubelet caches an immutable
   * config map by name: a second submission from the same Hop process would start its driver with
   * the settings of the first. A short-lived process per submission gets a name of its own. It
   * exits once Kubernetes created the driver pod.
   */
  void runSubmitProcess(List<String> command, long timeoutMillis) throws HopException {
    ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true);
    // No Spark installation: no spark-defaults.conf to read
    builder.environment().remove("SPARK_HOME");
    builder.environment().remove("SPARK_CONF_DIR");
    StringBuffer output = new StringBuffer();
    Process process;
    try {
      process = builder.start();
    } catch (Exception e) {
      throw new HopException("Unable to start the Kubernetes submission", e);
    }
    submitProcess = process;
    // Read the output on a thread of its own: a spark-submit that hangs keeps it open, and the
    // timeout and a cancel must still end it
    Thread reader =
        new Thread(() -> readOutput(process, output), "hop-spark-kubernetes-submit-output");
    reader.setDaemon(true);
    reader.start();
    try {
      long deadline = System.currentTimeMillis() + timeoutMillis;
      while (!process.waitFor(250L, java.util.concurrent.TimeUnit.MILLISECONDS)) {
        if (cancelled || System.currentTimeMillis() > deadline) {
          break;
        }
      }
      if (cancelled) {
        process.destroyForcibly();
        throw new HopException(CANCELLED);
      }
      if (process.isAlive()) {
        process.destroyForcibly();
        throw new HopException(
            "The Kubernetes submission did not finish within "
                + timeoutMillis / 1000
                + " s:\n"
                + StringUtils.right(output.toString(), 4000));
      }
      reader.join(5000L);
      if (process.exitValue() != 0) {
        throw new HopException(
            "The Kubernetes submission failed (exit code "
                + process.exitValue()
                + "):\n"
                + StringUtils.right(output.toString(), 4000));
      }
    } catch (InterruptedException e) {
      process.destroyForcibly();
      Thread.currentThread().interrupt();
      throw new HopException("Interrupted while submitting to Kubernetes", e);
    } finally {
      submitProcess = null;
    }
  }

  private void readOutput(Process process, StringBuffer output) {
    try (java.io.BufferedReader reader =
        new java.io.BufferedReader(
            new java.io.InputStreamReader(
                process.getInputStream(), java.nio.charset.StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        output.append(line).append('\n');
        log.logDetailed("spark-submit: " + line);
      }
    } catch (java.io.IOException e) {
      // The process was destroyed: what it wrote so far is in the output
    }
  }

  /**
   * The class path of the engine plugin (Spark, its Kubernetes client) followed by Hop's own: what
   * Spark runs on inside this JVM.
   */
  static String submitClassPath() {
    java.util.LinkedHashSet<String> entries = new java.util.LinkedHashSet<>();
    ClassLoader loader = SparkKubernetesSubmitter.class.getClassLoader();
    if (loader instanceof java.net.URLClassLoader urlClassLoader) {
      for (java.net.URL url : urlClassLoader.getURLs()) {
        try {
          entries.add(java.nio.file.Paths.get(url.toURI()).toString());
        } catch (Exception e) {
          // not a file
        }
      }
    }
    for (String entry :
        System.getProperty("java.class.path", "").split(java.io.File.pathSeparator)) {
      if (!entry.isEmpty()) {
        entries.add(entry);
      }
    }
    return String.join(java.io.File.pathSeparator, entries);
  }

  @Override
  public SparkDriverStatus status(String id) throws HopException {
    String[] parts = split(id);
    ClassLoader previous = usePluginClassLoader();
    try {
      return toStatus(pod(parts[0], parts[1]), parts[0] + "/" + parts[1]);
    } catch (Exception e) {
      throw new HopException("Unable to read driver pod " + id, e);
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }

  @Override
  public String kill(String id) throws HopException {
    String[] parts = split(id);
    ClassLoader previous = usePluginClassLoader();
    try {
      kubernetes().pods().inNamespace(parts[0]).withName(parts[1]).delete();
      return "driver pod deleted";
    } catch (Exception e) {
      throw new HopException("Unable to delete driver pod " + id, e);
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }

  @Override
  public String describe() {
    return "Kubernetes (" + master + ")";
  }

  @Override
  public synchronized void close() {
    if (client != null) {
      client.close();
      client = null;
    }
  }

  /**
   * The driver of a Kubernetes cluster-mode run is the driver pod: it ended well when it Succeeded.
   * A pod that is gone (deleted, evicted) is a failure.
   */
  static SparkDriverStatus toStatus(Pod pod, String location) {
    if (pod == null) {
      return new SparkDriverStatus(
          "GONE", location, "the driver pod no longer exists", true, false);
    }
    String phase =
        pod.getStatus() == null || pod.getStatus().getPhase() == null
            ? "Unknown"
            : pod.getStatus().getPhase();
    boolean succeeded = "Succeeded".equals(phase);
    boolean failed = "Failed".equals(phase);
    return new SparkDriverStatus(
        phase.toUpperCase(Locale.ROOT),
        location,
        failed ? terminationMessage(pod) : null,
        succeeded || failed,
        succeeded);
  }

  private static String terminationMessage(Pod pod) {
    List<ContainerStatus> statuses = pod.getStatus().getContainerStatuses();
    if (statuses != null) {
      for (ContainerStatus status : statuses) {
        ContainerStateTerminated terminated =
            status.getState() == null ? null : status.getState().getTerminated();
        if (terminated != null) {
          return "container "
              + status.getName()
              + " ended with exit code "
              + terminated.getExitCode()
              + (StringUtils.isNotEmpty(terminated.getReason())
                  ? " (" + terminated.getReason() + ")"
                  : "")
              + (StringUtils.isNotEmpty(terminated.getMessage())
                  ? ": " + terminated.getMessage()
                  : "");
        }
      }
    }
    return pod.getStatus().getMessage();
  }

  private Pod pod(String namespace, String name) {
    return kubernetes().pods().inNamespace(namespace).withName(name).get();
  }

  private synchronized KubernetesClient kubernetes() {
    if (client == null) {
      // kubeconfig, the service account of a pod, or the KUBERNETES_* environment, as kubectl
      Config config =
          new ConfigBuilder(Config.autoConfigure(null)).withMasterUrl(apiServer()).build();
      client = new KubernetesClientBuilder().withConfig(config).build();
      log.logDetailed("Connected to the Kubernetes API server " + apiServer());
    }
    return client;
  }

  /** {@code k8s://https://host:port} → {@code https://host:port}; no scheme means https. */
  String apiServer() {
    String url = master.trim().substring("k8s://".length());
    return url.contains("://") ? url : "https://" + url;
  }

  private static String[] split(String id) throws HopException {
    int colon = id.indexOf(':');
    if (colon <= 0) {
      throw new HopException("Not a Kubernetes submission id (namespace:pod): " + id);
    }
    return new String[] {id.substring(0, colon), id.substring(colon + 1)};
  }

  private static ClassLoader usePluginClassLoader() {
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(SparkKubernetesSubmitter.class.getClassLoader());
    return previous;
  }
}
