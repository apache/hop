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

package org.apache.hop.pipeline.transforms.systemdata;

import java.util.Arrays;
import lombok.Getter;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

@Getter
public enum SystemDataType implements IEnumHasCodeAndDescription {
  NONE("", ""),
  SYSTEM_DATE("system date (variable)", "SystemDateVariable"),
  SYSTEM_START("system date (fixed)", "SystemDateFixed"),
  PIPELINE_DATE_FROM("start date range", "StartDateRange"),
  PIPELINE_DATE_TO("end date range", "EndDateRange"),
  WORKFLOW_DATE_FROM("workflow start date range", "JobStartDateRange"),
  WORKFLOW_DATE_TO("workflow end date range", "JobEndDateRange"),
  PREV_DAY_START("yesterday start", "YesterdayStart"),
  PREV_DAY_END("yesterday end", "YesterdayEnd"),
  THIS_DAY_START("today start", "TodayStart"),
  THIS_DAY_END("today end", "TodayEnd"),
  NEXT_DAY_START("tomorrow start", "TomorrowStart"),
  NEXT_DAY_END("tomorrow end", "TomorrowEnd"),
  PREV_MONTH_START("last month start", "LastMonthStart"),
  PREV_MONTH_END("last month end", "LastMonthEnd"),
  THIS_MONTH_START("this month start", "ThisMonthStart"),
  THIS_MONTH_END("this month end", "ThisMonthEnd"),
  NEXT_MONTH_START("next month start", "NextMonthStart"),
  NEXT_MONTH_END("next month end", "NextMonthEnd"),
  COPYNR("copy of transform", "CopyOfTransform"),
  PIPELINE_NAME(" pipeline name", "PipelineName"),
  FILENAME(" pipeline file name", "PipelineFileName"),
  MODIFIED_USER("User modified", "UserModified"),
  MODIFIED_DATE("Date modified", "DateModified"),
  HOSTNAME("Hostname", "HostnameNetworkSetup"),
  HOSTNAME_REAL("Hostname real", "Hostname"),
  IP_ADDRESS("IP address", "IPAddress"),
  CURRENT_PID("Current PID", "CurrentPID"),

  JVM_MAX_MEMORY("jvm max memory", "JVMMaxMemory"),
  JVM_TOTAL_MEMORY("jvm total memory", "JVMTotalMemory"),
  JVM_FREE_MEMORY("jvm free memory", "JVMFreeMemory"),
  JVM_AVAILABLE_MEMORY("jvm available memory", "JVMAvailableMemory"),
  AVAILABLE_PROCESSORS("available processors", "AvailableProcessors"),
  JVM_CPU_TIME("jvm cpu time", "JVMCPUTime"),
  TOTAL_PHYSICAL_MEMORY_SIZE("total physical memory size", "TotalPhysicalMemorySize"),
  TOTAL_SWAP_SPACE_SIZE("total swap space size", "TotalSwapSpaceSize"),
  COMMITTED_VIRTUAL_MEMORY_SIZE("committed virtual memory size", "CommittedVirtualMemorySize"),
  FREE_PHYSICAL_MEMORY_SIZE("free physical memory size", "FreePhysicalMemorySize"),
  FREE_SWAP_SPACE_SIZE("free swap space size", "FreeSwapSpaceSize"),

  PREV_WEEK_START("last week start", "LastWeekStart"),
  PREV_WEEK_END("last week end", "LastWeekEnd"),
  PREV_WEEK_OPEN_END("last week open end", "LastWeekOpenEnd"),

  PREV_WEEK_START_US("last week start us", "LastWeekStartUS"),
  PREV_WEEK_END_US("last week end us", "LastWeekEndUS"),

  THIS_WEEK_START("this week start", "ThisWeekStart"),
  THIS_WEEK_END("this week end", "ThisWeekEnd"),
  THIS_WEEK_OPEN_END("this week open end", "ThisWeekOpenEnd"),

  THIS_WEEK_START_US("this week start us", "ThisWeekStartUS"),
  THIS_WEEK_END_US("this week end us", "ThisWeekEndUS"),

  NEXT_WEEK_START("next week start", "NextWeekStart"),
  NEXT_WEEK_END("next week end", "NextWeekEnd"),
  NEXT_WEEK_OPEN_END("next week open end", "NextWeekOpenEnd"),

  NEXT_WEEK_START_US("next week start us", "NextWeekStartUS"),
  NEXT_WEEK_END_US("next week end us", "NextWeekEndUS"),

  PREV_QUARTER_START("prev quarter start", "PrevQuarterStart"),
  PREV_QUARTER_END("prev quarter end", "PrevQuarterEnd"),

  THIS_QUARTER_START("this quarter start", "ThisQuarterStart"),
  THIS_QUARTER_END("this quarter end", "ThisQuarterEnd"),

  NEXT_QUARTER_START("next quarter start", "NextQuarterStart"),
  NEXT_QUARTER_END("next quarter end", "NextQuarterEnd"),

  PREV_YEAR_START("prev year start", "PrevYearStart"),
  PREV_YEAR_END("prev year end", "PrevYearEnd"),

  THIS_YEAR_START("this year start", "ThisYearStart"),
  THIS_YEAR_END("this year end", "ThisYearEnd"),
  NEXT_YEAR_START("next year start", "NextYearStart"),
  NEXT_YEAR_END("next year end", "NextYearEnd"),

  PREVIOUS_RESULT_RESULT("previous result result", "PreviousResultResult"),
  PREVIOUS_RESULT_EXIT_STATUS("previous result exist status", "PreviousResultExitStatus"),
  PREVIOUS_RESULT_ENTRY_NR("previous result entry nr", "PreviousResultEntryNr"),
  PREVIOUS_RESULT_NR_ERRORS("previous result nr errors", "PreviousResultNrErrors"),
  PREVIOUS_RESULT_NR_LINES_INPUT("previous result nr lines input", "PreviousResultNrLinesInput"),
  PREVIOUS_RESULT_NR_LINES_OUTPUT("previous result nr lines output", "PreviousResultNrLinesOutput"),
  PREVIOUS_RESULT_NR_LINES_READ("previous result nr lines read", "PreviousResultNrLinesRead"),
  PREVIOUS_RESULT_NR_LINES_UPDATED(
      "previous result nr lines updated", "PreviousResultNrLinesUpdated"),
  PREVIOUS_RESULT_NR_LINES_WRITTEN(
      "previous result nr lines written", "PreviousResultNrLinesWritten"),
  PREVIOUS_RESULT_NR_LINES_DELETED(
      "previous result nr lines deleted", "PreviousResultNrLinesDeleted"),
  PREVIOUS_RESULT_NR_LINES_REJECTED(
      "previous result nr lines rejected", "PreviousResultNrLinesRejected"),
  PREVIOUS_RESULT_NR_ROWS("previous result nr rows", "PreviousResultNrLinesNrRows"),
  PREVIOUS_RESULT_IS_STOPPED("previous result is stopped", "PreviousResultIsStopped"),
  PREVIOUS_RESULT_NR_FILES("previous result nr files", "PreviousResultNrFiles"),
  PREVIOUS_RESULT_NR_FILES_RETRIEVED(
      "previous result nr files retrieved", "PreviousResultNrFilesRetrieved"),
  PREVIOUS_RESULT_LOG_TEXT("previous result log text", "PreviousResultLogText");

  private final String code;
  private final String description;

  public static SystemDataType lookupDescription(String description) {
    return IEnumHasCodeAndDescription.lookupDescription(SystemDataType.class, description, NONE);
  }

  public static String[] getDescriptions() {
    return Arrays.stream(SystemDataType.values())
        .filter(t -> t != SystemDataType.NONE)
        .map(SystemDataType::getDescription)
        .toArray(String[]::new);
  }

  SystemDataType(String code, String descriptionName) {
    this.code = code;
    this.description =
        BaseMessages.getString(SystemDataType.class, "SystemDataMeta.TypeDesc." + descriptionName);
  }
}
