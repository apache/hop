/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.airflow.launcher;

import io.ktor.network.sockets.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.airflow.sdk.Bundle;
import org.apache.airflow.sdk.CoordinatorServer;
import org.apache.airflow.sdk.DagDef;
import org.apache.airflow.sdk.Server;

/**
 * What the Airflow Java coordinator runs for a Hop DAG bundle: to list the DAGs (DAG processor) and
 * to run a task (worker). Every workflow of the project becomes a DAG with one task that runs it.
 */
public final class HopBundleMain {

  /** The name of the one task of a workflow DAG. */
  public static final String TASK_ID = "run_workflow";

  /** The queue the DAGs use; the Airflow configuration maps it to the Java coordinator. */
  public static final String QUEUE = "java";

  private static final Map<String, HopWorkflowFile> WORKFLOWS = new ConcurrentHashMap<>();
  private static volatile BundleManifest bundle;

  private HopBundleMain() {}

  public static void main(String[] args) throws Exception {
    long start = System.currentTimeMillis();
    BundleManifest manifest = BundleManifest.locate();
    manifest.checkSchemaVersion(BundleManifest.sdkSchemaVersion());
    bundle = manifest;

    List<HopWorkflowFile> workflows =
        HopWorkflowFile.scan(
            manifest.folder(), manifest.workflowPatterns(), manifest.dagIdPrefix());
    Bundle dags = new Bundle();
    for (HopWorkflowFile workflow : workflows) {
      WORKFLOWS.put(workflow.dagId(), workflow);
      DagDef dag =
          new DagDef(workflow.dagId())
              .config("queue", QUEUE)
              .config("description", workflow.description())
              .config("tags", List.of("hop", manifest.projectName()));
      if (workflow.schedule() != null) {
        dag.config("schedule", workflow.schedule());
      }
      dag.task(TASK_ID, RunWorkflowTask.class);
      dags.register(dag);
    }
    System.err.println(
        "Hop project "
            + manifest.projectName()
            + ": "
            + workflows.size()
            + " workflows in "
            + manifest.folder()
            + " listed in "
            + (System.currentTimeMillis() - start)
            + " ms");
    server(args).serve(dags);
  }

  static BundleManifest bundle() {
    return bundle;
  }

  static HopWorkflowFile workflow(String dagId) {
    return WORKFLOWS.get(dagId);
  }

  /**
   * The coordinator appends {@code --comm=host:port} and {@code --logs=host:port}. {@code
   * Server.create(args)} would parse them with an LGPL library that Hop cannot ship, so parse them
   * here.
   */
  static Server server(String[] args) {
    String comm = null;
    String logs = null;
    for (String arg : args) {
      if (arg.startsWith("--comm=")) {
        comm = arg.substring("--comm=".length());
      } else if (arg.startsWith("--logs=")) {
        logs = arg.substring("--logs=".length());
      }
    }
    if (comm == null || logs == null) {
      throw new IllegalArgumentException(
          "Expected --comm=host:port and --logs=host:port from the Airflow coordinator, got: "
              + String.join(" ", args));
    }
    return new CoordinatorServer(address(comm), address(logs));
  }

  static InetSocketAddress address(String hostAndPort) {
    int colon = hostAndPort.lastIndexOf(':');
    if (colon <= 0 || colon == hostAndPort.length() - 1) {
      throw new IllegalArgumentException("Not a host:port address: " + hostAndPort);
    }
    String host = hostAndPort.substring(0, colon);
    if (host.startsWith("[") && host.endsWith("]")) {
      host = host.substring(1, host.length() - 1);
    }
    return new InetSocketAddress(host, Integer.parseInt(hostAndPort.substring(colon + 1)));
  }
}
