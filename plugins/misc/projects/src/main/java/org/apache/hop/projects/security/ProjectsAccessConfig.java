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

package org.apache.hop.projects.security;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.security.HopSecurityConfig;
import org.apache.hop.core.vfs.HopVfs;

/**
 * Project access control for authenticated Hop Web sessions. Stored under {@code
 * HOP_CONFIG_FOLDER/security/projects-access.json}.
 *
 * <p>When {@link #enabled} is false (default), all projects remain available. When enabled, rules
 * grant projects to users, Hop roles, or container/LDAP groups. Admins ({@code security.manage})
 * always keep full access.
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProjectsAccessConfig {

  public static final String FILENAME = "projects-access.json";

  /** Master switch. False = no project filtering (desktop-friendly default). */
  private boolean enabled;

  /**
   * When no rule matches the current user: if true, allow all projects; if false, allow none
   * (except admins).
   */
  private boolean defaultAllowAll = true;

  private List<ProjectsAccessRule> rules = new ArrayList<>();

  private static volatile ProjectsAccessConfig cached;

  public static String getConfigFilePath() {
    return Const.HOP_CONFIG_FOLDER
        + Const.FILE_SEPARATOR
        + HopSecurityConfig.SECURITY_FOLDER
        + Const.FILE_SEPARATOR
        + FILENAME;
  }

  public static ProjectsAccessConfig load() {
    ProjectsAccessConfig local = cached;
    if (local != null) {
      return local;
    }
    synchronized (ProjectsAccessConfig.class) {
      if (cached != null) {
        return cached;
      }
      cached = readFromFile();
      return cached;
    }
  }

  public static void clearCache() {
    cached = null;
  }

  public static void save(ProjectsAccessConfig config) {
    if (config == null) {
      return;
    }
    writeToFile(config);
    cached = config;
  }

  private static ProjectsAccessConfig readFromFile() {
    String path = getConfigFilePath();
    try {
      if (!HopVfs.fileExists(path)) {
        return new ProjectsAccessConfig();
      }
      try (InputStream in = HopVfs.getInputStream(path)) {
        ObjectMapper mapper = HopJson.newMapper();
        ProjectsAccessConfig config = mapper.readValue(in, ProjectsAccessConfig.class);
        if (config.getRules() == null) {
          config.setRules(new ArrayList<>());
        }
        return config;
      }
    } catch (Exception e) {
      LogChannel.GENERAL.logError(
          "Unable to read projects access config from '" + path + "', using defaults", e);
      return new ProjectsAccessConfig();
    }
  }

  private static void writeToFile(ProjectsAccessConfig config) {
    String path = getConfigFilePath();
    try {
      String folder =
          Const.HOP_CONFIG_FOLDER + Const.FILE_SEPARATOR + HopSecurityConfig.SECURITY_FOLDER;
      var folderObject = HopVfs.getFileObject(folder);
      if (!folderObject.exists()) {
        folderObject.createFolder();
      }
      ObjectMapper mapper = HopJson.newMapper();
      mapper.enable(com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT);
      byte[] json = mapper.writeValueAsString(config).getBytes(StandardCharsets.UTF_8);
      try (OutputStream out = HopVfs.getOutputStream(path, false)) {
        out.write(json);
      }
      LogChannel.GENERAL.logBasic("Saved projects access config to '" + path + "'");
    } catch (Exception e) {
      LogChannel.GENERAL.logError("Unable to save projects access config to '" + path + "'", e);
      throw new IllegalStateException("Unable to save projects access config", e);
    }
  }
}
