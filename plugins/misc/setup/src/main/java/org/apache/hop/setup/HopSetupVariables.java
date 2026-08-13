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

package org.apache.hop.setup;

import java.util.List;

/** Environment variables managed by {@code hop setup} / the environment dialog. */
public final class HopSetupVariables {

  public static final String CONFIG_FOLDER = "HOP_CONFIG_FOLDER";
  public static final String AUDIT_FOLDER = "HOP_AUDIT_FOLDER";
  public static final String JAVA_HOME = "HOP_JAVA_HOME";
  public static final String OPTIONS = "HOP_OPTIONS";
  public static final String JDBC_FOLDERS = "HOP_SHARED_JDBC_FOLDERS";

  public static final List<String> ALL =
      List.of(CONFIG_FOLDER, AUDIT_FOLDER, JAVA_HOME, OPTIONS, JDBC_FOLDERS);

  public static final String DEFAULT_OPTIONS = "-Xmx2048m";

  public static final String HOP_CONFIG_JSON = "hop-config.json";

  public static final String NO_SETUP_DIALOG = "HOP_NO_SETUP_DIALOG";

  public static final String CONFIG_OPTION_DO_NOT_SHOW = "doNotShowSetupDialog";

  private HopSetupVariables() {}
}
