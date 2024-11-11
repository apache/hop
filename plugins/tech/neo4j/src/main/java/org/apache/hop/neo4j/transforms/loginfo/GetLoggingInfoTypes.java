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
 *
 */

package org.apache.hop.neo4j.transforms.loginfo;

import lombok.Getter;
import org.apache.hop.i18n.BaseMessages;

@Getter
public enum GetLoggingInfoTypes {
  TYPE_SYSTEM_INFO_NONE("", ""),

  TYPE_SYSTEM_INFO_PIPELINE_DATE_FROM(
      "Specified pipeline: Start of date range", "Types.Desc.PipelineStartDateRange"),
  TYPE_SYSTEM_INFO_PIPELINE_DATE_TO(
      "Specified pipeline: End of date range", "Types.Desc.PipelineEndDateRange"),
  TYPE_SYSTEM_INFO_PIPELINE_PREVIOUS_EXECUTION_DATE(
      "Specified pipeline : Previous execution date", "Types.Desc.PipelinePreviousExecutionDate"),
  TYPE_SYSTEM_INFO_PIPELINE_PREVIOUS_SUCCESS_DATE(
      "Specified pipeline : Previous success date", "Types.Desc.PipelinePreviousSuccessDate"),

  TYPE_SYSTEM_INFO_WORKFLOW_DATE_FROM(
      "Specified workflow: Start of date range", "Types.Desc.WorkflowStartDateRange"),
  TYPE_SYSTEM_INFO_WORKFLOW_DATE_TO(
      "Specified workflow: End of date range", "Types.Desc.WorkflowEndDateRange"),
  TYPE_SYSTEM_INFO_WORKFLOW_PREVIOUS_EXECUTION_DATE(
      "Specified workflow: Previous execution date", "Types.Desc.WorkflowPreviousExecutionDate"),
  TYPE_SYSTEM_INFO_WORKFLOW_PREVIOUS_SUCCESS_DATE(
      "Specified workflow: Previous success date", "Types.Desc.WorkflowPreviousSuccessDate"),
  ;

  private final String code;
  private final String description;

  private static Class<?> pkg = GetLoggingInfoMeta.class;

  public String lookupDescription() {
    return description;
  }

  public static GetLoggingInfoTypes getTypeFromString(String typeStr) {
    for (GetLoggingInfoTypes type : GetLoggingInfoTypes.values()) {
      // attempting to purge this typo from KTRs
      if ("previous result nr lines rejected".equalsIgnoreCase(typeStr)) {
        typeStr = "previous result nr lines rejected";
      }

      if (type.toString().equals(typeStr)
          || type.code.equalsIgnoreCase(typeStr)
          || type.description.equalsIgnoreCase(typeStr)) {
        return type;
      }
    }

    return TYPE_SYSTEM_INFO_NONE;
  }

  public static String lookupDescription(String i18nDescription) {
    if (pkg == null) {
      pkg = GetLoggingInfoMeta.class;
    }
    return BaseMessages.getString(pkg, i18nDescription);
  }

  GetLoggingInfoTypes(String code, String i18nDescription) {
    this.code = code;
    this.description = lookupDescription(i18nDescription);
  }
}
