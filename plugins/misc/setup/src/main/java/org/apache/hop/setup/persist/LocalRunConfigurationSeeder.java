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

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.setup.HopEnvironmentApplyResult;
import org.apache.hop.setup.HopSetupException;

/**
 * Adds the standard {@code local} pipeline and workflow run configurations to a new project.
 *
 * <p>Files already present are left unchanged. A file that exists in the install default-project
 * template is copied from there. Otherwise the stock default-project content is written, including
 * the local execution info location and data profile those run configurations use.
 */
public class LocalRunConfigurationSeeder {

  static final String PIPELINE_RELATIVE = "metadata/pipeline-run-configuration/local.json";
  static final String WORKFLOW_RELATIVE = "metadata/workflow-run-configuration/local.json";
  static final String EXECUTION_INFO_RELATIVE = "metadata/execution-info-location/local-audit.json";
  static final String DATA_PROFILE_RELATIVE = "metadata/execution-data-profile/first-last.json";

  static final String PIPELINE_JSON =
      """
      {
        "engineRunConfiguration": {
          "Local": {
            "feedback_size": "50000",
            "sample_size": "100",
            "sample_type_in_gui": "Last",
            "rowset_size": "10000",
            "safe_mode": false,
            "show_feedback": false,
            "topo_sort": false,
            "gather_metrics": false
          }
        },
        "name": "local",
        "configurationVariables": [],
        "description": "",
        "executionInfoLocationName": "local-audit",
        "dataProfile": "first-last",
        "defaultSelection": true
      }
      """;

  static final String WORKFLOW_JSON =
      """
      {
        "engineRunConfiguration": {
          "Local": {
            "safe_mode": false
          }
        },
        "name": "local",
        "description": "",
        "executionInfoLocationName": "local-audit",
        "defaultSelection": true
      }
      """;

  static final String EXECUTION_INFO_JSON =
      """
      {
        "executionInfoLocation": {
          "local-folder": {
            "pluginName": "File location",
            "pluginId": "local-folder",
            "rootFolder": "${HOP_AUDIT_FOLDER}/executions/"
          }
        },
        "dataLoggingDelay": "2000",
        "name": "local-audit",
        "description": "",
        "dataLoggingInterval": "5000"
      }
      """;

  static final String DATA_PROFILE_JSON =
      """
      {
        "name": "first-last",
        "description": "capture the first and last 100 rows of every Hop transform",
        "sampler": [
          {
            "FirstRowsExecutionDataSampler": {
              "sampleSize": "100"
            }
          },
          {
            "LastRowsExecutionDataSampler": {
              "sampleSize": "100"
            }
          }
        ]
      }
      """;

  public boolean standardRunConfigurationsMissing(String templateHome) throws HopSetupException {
    return !templateContains(templateHome, PIPELINE_RELATIVE)
        || !templateContains(templateHome, WORKFLOW_RELATIVE);
  }

  public void ensure(String projectHome, String templateHome, HopEnvironmentApplyResult result)
      throws HopSetupException {
    boolean synthesized =
        ensureRunConfiguration(
            projectHome,
            templateHome,
            PIPELINE_RELATIVE,
            PIPELINE_JSON,
            "local pipeline run configuration",
            result);
    synthesized =
        ensureRunConfiguration(
                projectHome,
                templateHome,
                WORKFLOW_RELATIVE,
                WORKFLOW_JSON,
                "local workflow run configuration",
                result)
            || synthesized;
    if (synthesized) {
      ensureFile(
          projectHome,
          templateHome,
          EXECUTION_INFO_RELATIVE,
          EXECUTION_INFO_JSON,
          "local execution info location",
          result);
      ensureFile(
          projectHome,
          templateHome,
          DATA_PROFILE_RELATIVE,
          DATA_PROFILE_JSON,
          "execution data profile",
          result);
    } else {
      copyFromTemplateIfMissing(
          projectHome,
          templateHome,
          EXECUTION_INFO_RELATIVE,
          "local execution info location",
          result);
      copyFromTemplateIfMissing(
          projectHome, templateHome, DATA_PROFILE_RELATIVE, "execution data profile", result);
    }
  }

  private boolean ensureRunConfiguration(
      String projectHome,
      String templateHome,
      String relative,
      String fallback,
      String label,
      HopEnvironmentApplyResult result)
      throws HopSetupException {
    String destination = projectHome + "/" + relative;
    if (HopVfsFiles.exists(destination)) {
      return false;
    }
    if (templateContains(templateHome, relative)) {
      write(destination, HopVfsFiles.readUtf8(templateHome + "/" + relative), label, result);
      return false;
    }
    write(destination, fallback, label, result);
    return true;
  }

  private void ensureFile(
      String projectHome,
      String templateHome,
      String relative,
      String fallback,
      String label,
      HopEnvironmentApplyResult result)
      throws HopSetupException {
    String destination = projectHome + "/" + relative;
    if (HopVfsFiles.exists(destination)) {
      return;
    }
    String content =
        templateContains(templateHome, relative)
            ? HopVfsFiles.readUtf8(templateHome + "/" + relative)
            : fallback;
    write(destination, content, label, result);
  }

  private void copyFromTemplateIfMissing(
      String projectHome,
      String templateHome,
      String relative,
      String label,
      HopEnvironmentApplyResult result)
      throws HopSetupException {
    String destination = projectHome + "/" + relative;
    if (HopVfsFiles.exists(destination) || !templateContains(templateHome, relative)) {
      return;
    }
    write(destination, HopVfsFiles.readUtf8(templateHome + "/" + relative), label, result);
  }

  private static boolean templateContains(String templateHome, String relative)
      throws HopSetupException {
    return StringUtils.isNotBlank(templateHome)
        && HopVfsFiles.exists(templateHome + "/" + relative);
  }

  private static void write(
      String destination, String content, String label, HopEnvironmentApplyResult result)
      throws HopSetupException {
    HopVfsFiles.writeUtf8(destination, content);
    result.addMessage("Created " + label + " at " + destination);
  }
}
