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

/**
 * The layout of a Hop DAG bundle: a Hop project folder with a stub jar ({@value #STUB_FILE_NAME})
 * at its root. The stub holds only a manifest. Airflow reads the main class and the supervisor
 * schema version from it; the JVM follows its Class-Path to the launcher in the Hop installation of
 * the worker; the launcher reads the Hop attributes below.
 *
 * <p>The Airflow plugin writes the stub with these constants. They are compile-time constants, so
 * the plugin does not need this jar at runtime.
 */
public final class HopAirflowBundle {

  /** The stub jar at the root of the project folder. */
  public static final String STUB_FILE_NAME = "hop-airflow.jar";

  /** The launcher jar, without a version so that a stub keeps working after a Hop upgrade. */
  public static final String LAUNCHER_FILE_NAME = "hop-airflow-launcher.jar";

  /** The folder of the launcher jar, relative to the Hop installation. */
  public static final String LAUNCHER_FOLDER = "plugins/tech/airflow/launcher";

  /** The main class of the stub. */
  public static final String MAIN_CLASS = "org.apache.hop.airflow.launcher.HopBundleMain";

  /** The wire schema between the Airflow supervisor and the SDK, set by the SDK jar too. */
  public static final String ATTRIBUTE_SCHEMA_VERSION = "Airflow-Supervisor-Schema-Version";

  /** The Hop version that wrote the stub. */
  public static final String ATTRIBUTE_HOP_VERSION = "Hop-Version";

  /** The name the project gets when the launcher registers it. Required. */
  public static final String ATTRIBUTE_PROJECT_NAME = "Hop-Project-Name";

  /** The workflow run configuration. Default: {@value #DEFAULT_RUN_CONFIGURATION}. */
  public static final String ATTRIBUTE_RUN_CONFIGURATION = "Hop-Run-Configuration";

  /** The log level of the runs. Default: {@value #DEFAULT_LOG_LEVEL}. */
  public static final String ATTRIBUTE_LOG_LEVEL = "Hop-Log-Level";

  /** Comma separated glob patterns of the workflows that become DAGs. Default: every workflow. */
  public static final String ATTRIBUTE_WORKFLOWS = "Hop-Workflows";

  /** Semicolon separated environment configuration files, relative to the project. Optional. */
  public static final String ATTRIBUTE_ENVIRONMENT_FILES = "Hop-Environment-Files";

  /** The start of every DAG id. Default: the project name and two underscores. */
  public static final String ATTRIBUTE_DAG_ID_PREFIX = "Hop-Dag-Id-Prefix";

  public static final String DEFAULT_RUN_CONFIGURATION = "local";
  public static final String DEFAULT_LOG_LEVEL = "Basic";

  /**
   * Starts the line of a DAG's documentation that names the version of its workflow: the first 12
   * hex digits of the SHA-256 of the .hwf file. The Airflow run configuration waits until Airflow
   * shows the version it published.
   */
  public static final String DAG_DOC_VERSION = "Workflow version: ";

  /** How many hex digits of the SHA-256 make the version of a workflow. */
  public static final int VERSION_LENGTH = 12;

  /** The name of the environment the launcher registers for the environment files. */
  public static final String ENVIRONMENT_NAME = "airflow";

  private HopAirflowBundle() {}
}
