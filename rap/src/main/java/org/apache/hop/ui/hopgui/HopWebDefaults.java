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

package org.apache.hop.ui.hopgui;

import jakarta.servlet.ServletContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

/**
 * Default folders for Hop Web deployed as a war in a servlet container, so the hop-web war runs
 * without any HOP_* settings:
 *
 * <ul>
 *   <li>HOP_CONFIG_FOLDER: the web app's {@code config} folder
 *   <li>HOP_PLUGIN_BASE_FOLDERS: {@code CATALINA_BASE/plugins}
 *   <li>HOP_SHARED_JDBC_FOLDERS: {@code CATALINA_BASE/jdbc-drivers}
 * </ul>
 *
 * <p>A folder is only used when it exists. A system property always wins; an environment variable
 * of the same name is copied to the system property, because Hop only reads system properties here.
 *
 * <p>This runs before Hop is initialized, so it must not touch {@code Const}: its {@code
 * HOP_CONFIG_FOLDER} is computed once, when the class loads. For the same reason it uses plain file
 * system paths rather than Hop VFS.
 */
public final class HopWebDefaults {

  static final String HOP_CONFIG_FOLDER = "HOP_CONFIG_FOLDER";
  static final String HOP_PLUGIN_BASE_FOLDERS = "HOP_PLUGIN_BASE_FOLDERS";
  static final String HOP_SHARED_JDBC_FOLDERS = "HOP_SHARED_JDBC_FOLDERS";

  private HopWebDefaults() {}

  /**
   * Applies the defaults for the given web app.
   *
   * @return one line per setting that was set here, to be logged
   */
  public static List<String> apply(ServletContext servletContext) {
    return apply(
        servletContext.getRealPath("/"), System.getProperty("catalina.base"), System::getenv);
  }

  static List<String> apply(
      String webAppFolder, String catalinaBase, UnaryOperator<String> environment) {
    List<String> applied = new ArrayList<>();
    setDefault(applied, HOP_CONFIG_FOLDER, environment, folder(webAppFolder, "config"));
    setDefault(applied, HOP_PLUGIN_BASE_FOLDERS, environment, folder(catalinaBase, "plugins"));
    setDefault(applied, HOP_SHARED_JDBC_FOLDERS, environment, folder(catalinaBase, "jdbc-drivers"));
    return applied;
  }

  private static Path folder(String parent, String name) {
    if (isEmpty(parent)) {
      return null;
    }
    Path folder = Path.of(parent, name);
    return Files.isDirectory(folder) ? folder : null;
  }

  private static void setDefault(
      List<String> applied, String name, UnaryOperator<String> environment, Path fallback) {
    if (!isEmpty(System.getProperty(name))) {
      return;
    }
    String fromEnvironment = environment.apply(name);
    if (!isEmpty(fromEnvironment)) {
      System.setProperty(name, fromEnvironment);
      applied.add(name + "=" + fromEnvironment + " (environment variable)");
    } else if (fallback != null) {
      System.setProperty(name, fallback.toString());
      applied.add(name + "=" + fallback + " (default)");
    }
  }

  private static boolean isEmpty(String value) {
    return value == null || value.isBlank();
  }
}
