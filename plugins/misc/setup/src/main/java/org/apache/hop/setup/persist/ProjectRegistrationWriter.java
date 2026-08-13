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

package org.apache.hop.setup.persist;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Path;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.setup.HopEnvironmentApplyResult;
import org.apache.hop.setup.HopEnvironmentDefaults;
import org.apache.hop.setup.HopEnvironmentSpec;
import org.apache.hop.setup.HopSetupException;
import org.apache.hop.setup.HopSetupVariables;
import org.apache.hop.setup.OsFamily;
import org.apache.hop.setup.UserPaths;

/**
 * Creates a user-owned default project and registers it (plus the install samples project) in the
 * target {@code hop-config.json}. Does not use the live {@code HopConfig} singleton, which still
 * points at the current process config folder.
 */
public class ProjectRegistrationWriter {

  static final String MINIMAL_PROJECT_CONFIG =
      """
      {
        "metadataBaseFolder": "${PROJECT_HOME}/metadata",
        "unitTestsBasePath": "${PROJECT_HOME}",
        "dataSetsCsvFolder": "${PROJECT_HOME}/datasets",
        "enforcingExecutionInHome": true,
        "parentProjectName": "",
        "config": {
          "variables": []
        }
      }
      """;

  private final ObjectMapper mapper;

  public ProjectRegistrationWriter() {
    mapper = HopJson.newMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
  }

  public void apply(
      HopEnvironmentSpec spec, OsFamily os, UserPaths paths, HopEnvironmentApplyResult result)
      throws HopSetupException {
    apply(spec, os, paths, result, HopInstallHome.resolveOrNull());
  }

  public void apply(
      HopEnvironmentSpec spec,
      OsFamily os,
      UserPaths paths,
      HopEnvironmentApplyResult result,
      Path hopHome)
      throws HopSetupException {
    if (!spec.isCreateDefaultProject() && !spec.isRegisterSamples()) {
      return;
    }
    if (StringUtils.isBlank(spec.getConfigFolder())) {
      result.addMessage("Skipped project registration: HOP_CONFIG_FOLDER is not set");
      return;
    }

    String defaultHome = spec.getDefaultProjectHome();
    if (spec.isCreateDefaultProject() && StringUtils.isBlank(defaultHome)) {
      defaultHome = HopEnvironmentDefaults.recommendedDefaultProjectHome(os, paths);
      spec.setDefaultProjectHome(defaultHome);
    }

    if (spec.isCreateDefaultProject()) {
      seedDefaultProject(spec, hopHome, defaultHome, result);
    }

    String samplesHome = null;
    if (spec.isRegisterSamples()) {
      samplesHome = resolveSamplesHome(hopHome, result);
    }

    writeHopConfig(spec, defaultHome, samplesHome, result);
  }

  private void seedDefaultProject(
      HopEnvironmentSpec spec, Path hopHome, String defaultHome, HopEnvironmentApplyResult result)
      throws HopSetupException {
    String projectConfig = defaultHome + "/" + HopSetupVariables.PROJECT_CONFIG_FILENAME;
    if (HopVfsFiles.exists(projectConfig)) {
      result.addMessage("Default project already has " + HopSetupVariables.PROJECT_CONFIG_FILENAME);
      return;
    }
    String template =
        hopHome == null ? null : hopHome.resolve("config/projects/default").toString();
    boolean hasTemplate =
        template != null
            && HopVfsFiles.exists(template + "/" + HopSetupVariables.PROJECT_CONFIG_FILENAME);
    if (spec.isDryRun()) {
      result.addMessage(
          hasTemplate
              ? "Would copy default project template to " + defaultHome
              : "Would create default project at " + defaultHome);
      return;
    }
    HopVfsFiles.createFolder(defaultHome);
    if (hasTemplate) {
      HopVfsFiles.copyTree(template, defaultHome);
      result.addMessage("Created default project from install template at " + defaultHome);
    } else {
      HopVfsFiles.writeUtf8(projectConfig, MINIMAL_PROJECT_CONFIG);
      result.addMessage("Created default project at " + defaultHome);
    }
  }

  private String resolveSamplesHome(Path hopHome, HopEnvironmentApplyResult result)
      throws HopSetupException {
    if (hopHome == null) {
      result.addMessage("Skipped samples project: Hop install directory not found");
      return null;
    }
    String samples = hopHome.resolve("config/projects/samples").toString();
    if (!HopVfsFiles.exists(samples + "/" + HopSetupVariables.PROJECT_CONFIG_FILENAME)) {
      result.addMessage("Skipped samples project: " + samples + " not found");
      return null;
    }
    return samples;
  }

  private void writeHopConfig(
      HopEnvironmentSpec spec,
      String defaultHome,
      String samplesHome,
      HopEnvironmentApplyResult result)
      throws HopSetupException {
    String hopConfigPath = spec.getConfigFolder() + "/" + HopSetupVariables.HOP_CONFIG_JSON;
    try {
      ObjectNode root;
      if (HopVfsFiles.exists(hopConfigPath)) {
        JsonNode parsed = mapper.readTree(HopVfsFiles.readUtf8(hopConfigPath));
        root = parsed.isObject() ? (ObjectNode) parsed : mapper.createObjectNode();
      } else {
        root = mapper.createObjectNode();
      }
      ObjectNode projects =
          root.has("projectsConfig") && root.get("projectsConfig").isObject()
              ? (ObjectNode) root.get("projectsConfig")
              : root.putObject("projectsConfig");
      projects.put("enabled", true);
      projects.put("projectMandatory", true);
      if (spec.isCreateDefaultProject() && StringUtils.isNotBlank(defaultHome)) {
        projects.put("defaultProject", HopSetupVariables.DEFAULT_PROJECT_NAME);
        projects.put("standardParentProject", HopSetupVariables.DEFAULT_PROJECT_NAME);
        Path parent = Path.of(defaultHome).getParent();
        if (parent != null) {
          projects.put("standardProjectsFolder", parent.toString());
        }
      }
      projects.put("defaultProjectConfigFile", HopSetupVariables.PROJECT_CONFIG_FILENAME);

      ArrayNode configs =
          projects.has("projectConfigurations") && projects.get("projectConfigurations").isArray()
              ? (ArrayNode) projects.get("projectConfigurations")
              : projects.putArray("projectConfigurations");
      if (spec.isCreateDefaultProject() && StringUtils.isNotBlank(defaultHome)) {
        upsertProject(
            configs,
            HopSetupVariables.DEFAULT_PROJECT_NAME,
            defaultHome,
            HopSetupVariables.PROJECT_CONFIG_FILENAME);
      }
      if (StringUtils.isNotBlank(samplesHome)) {
        upsertProject(
            configs,
            HopSetupVariables.SAMPLES_PROJECT_NAME,
            samplesHome,
            HopSetupVariables.PROJECT_CONFIG_FILENAME);
      }

      String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root) + "\n";
      result.getPlannedFiles().put(hopConfigPath, json);
      if (spec.isDryRun()) {
        result.addMessage("Would update projects in " + hopConfigPath);
        return;
      }
      HopVfsFiles.writeUtf8(hopConfigPath, json);
      result.addMessage("Updated projects in " + hopConfigPath);
    } catch (HopSetupException e) {
      throw e;
    } catch (Exception e) {
      throw new HopSetupException("Unable to update projects in '" + hopConfigPath + "'", e);
    }
  }

  static void upsertProject(ArrayNode configs, String name, String home, String configFilename) {
    for (JsonNode node : configs) {
      if (node.isObject() && name.equalsIgnoreCase(text(node, "projectName"))) {
        ObjectNode object = (ObjectNode) node;
        object.put("projectHome", home);
        object.put("configFilename", configFilename);
        return;
      }
    }
    ObjectNode added = configs.addObject();
    added.put("projectName", name);
    added.put("projectHome", home);
    added.put("configFilename", configFilename);
  }

  private static String text(JsonNode node, String field) {
    JsonNode value = node.get(field);
    return value == null || value.isNull() ? "" : value.asText();
  }
}
