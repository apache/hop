/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License" );
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.pipeline.transforms.systemdata;

import org.apache.hop.i18n.BaseMessages;

public enum SystemDataTypes {
  TYPE_SYSTEM_INFO_NONE( "", "" ),
  TYPE_SYSTEM_INFO_SYSTEM_DATE( "system date (variable)", "SystemDateVariable" ),
  TYPE_SYSTEM_INFO_SYSTEM_START( "system date (fixed)", "SystemDateFixed" ),
  TYPE_SYSTEM_INFO_PIPELINE_DATE_FROM( "start date range", "StartDateRange" ),
  TYPE_SYSTEM_INFO_PIPELINE_DATE_TO( "end date range", "EndDateRange" ),
  TYPE_SYSTEM_INFO_JOB_DATE_FROM( "workflow start date range", "JobStartDateRange" ),
  TYPE_SYSTEM_INFO_JOB_DATE_TO( "workflow end date range", "JobEndDateRange" ),
  TYPE_SYSTEM_INFO_PREV_DAY_START( "yesterday start", "YesterdayStart" ),
  TYPE_SYSTEM_INFO_PREV_DAY_END( "yesterday end", "YesterdayEnd" ),
  TYPE_SYSTEM_INFO_THIS_DAY_START( "today start", "TodayStart" ),
  TYPE_SYSTEM_INFO_THIS_DAY_END( "today end", "TodayEnd" ),
  TYPE_SYSTEM_INFO_NEXT_DAY_START( "tomorrow start", "TomorrowStart" ),
  TYPE_SYSTEM_INFO_NEXT_DAY_END( "tomorrow end", "TomorrowEnd" ),
  TYPE_SYSTEM_INFO_PREV_MONTH_START( "last month start", "LastMonthStart" ),
  TYPE_SYSTEM_INFO_PREV_MONTH_END( "last month end", "LastMonthEnd" ),
  TYPE_SYSTEM_INFO_THIS_MONTH_START( "this month start", "ThisMonthStart" ),
  TYPE_SYSTEM_INFO_THIS_MONTH_END( "this month end", "ThisMonthEnd" ),
  TYPE_SYSTEM_INFO_NEXT_MONTH_START( "next month start", "NextMonthStart" ),
  TYPE_SYSTEM_INFO_NEXT_MONTH_END( "next month end", "NextMonthEnd" ),
  TYPE_SYSTEM_INFO_COPYNR( "copy of transform", "CopyOfTransform" ),
  TYPE_SYSTEM_INFO_PIPELINE_NAME( " pipeline name", "PipelineName" ),
  TYPE_SYSTEM_INFO_FILENAME( " pipeline file name", "PipelineFileName" ),
  TYPE_SYSTEM_INFO_MODIFIED_USER( "User modified", "UserModified" ),
  TYPE_SYSTEM_INFO_MODIFIED_DATE( "Date modified", "DateModified" ),
  TYPE_SYSTEM_INFO_HOSTNAME( "Hostname", "HostnameNetworkSetup" ),
  TYPE_SYSTEM_INFO_HOSTNAME_REAL( "Hostname real", "Hostname" ),
  TYPE_SYSTEM_INFO_IP_ADDRESS( "IP address", "IPAddress" ),
  TYPE_SYSTEM_INFO_HOP_VERSION( "hop version", "HopVersion" ),
  TYPE_SYSTEM_INFO_HOP_BUILD_VERSION( "hop build version", "HopBuildVersion" ),
  TYPE_SYSTEM_INFO_HOP_BUILD_DATE( "hop build date", "HopBuildDate" ),
  TYPE_SYSTEM_INFO_CURRENT_PID( "Current PID", "CurrentPID" ),

  TYPE_SYSTEM_INFO_JVM_MAX_MEMORY( "jvm max memory", "JVMMaxMemory" ),
  TYPE_SYSTEM_INFO_JVM_TOTAL_MEMORY( "jvm total memory", "JVMTotalMemory" ),
  TYPE_SYSTEM_INFO_JVM_FREE_MEMORY( "jvm free memory", "JVMFreeMemory" ),
  TYPE_SYSTEM_INFO_JVM_AVAILABLE_MEMORY( "jvm available memory", "JVMAvailableMemory" ),
  TYPE_SYSTEM_INFO_AVAILABLE_PROCESSORS( "available processors", "AvailableProcessors" ),
  TYPE_SYSTEM_INFO_JVM_CPU_TIME( "jvm cpu time", "JVMCPUTime" ),
  TYPE_SYSTEM_INFO_TOTAL_PHYSICAL_MEMORY_SIZE( "total physical memory size", "TotalPhysicalMemorySize" ),
  TYPE_SYSTEM_INFO_TOTAL_SWAP_SPACE_SIZE( "total swap space size", "TotalSwapSpaceSize" ),
  TYPE_SYSTEM_INFO_COMMITTED_VIRTUAL_MEMORY_SIZE( "committed virtual memory size", "CommittedVirtualMemorySize" ),
  TYPE_SYSTEM_INFO_FREE_PHYSICAL_MEMORY_SIZE( "free physical memory size", "FreePhysicalMemorySize" ),
  TYPE_SYSTEM_INFO_FREE_SWAP_SPACE_SIZE( "free swap space size", "FreeSwapSpaceSize" ),

  TYPE_SYSTEM_INFO_PREV_WEEK_START( "last week start", "LastWeekStart" ),
  TYPE_SYSTEM_INFO_PREV_WEEK_END( "last week end", "LastWeekEnd" ),
  TYPE_SYSTEM_INFO_PREV_WEEK_OPEN_END( "last week open end", "LastWeekOpenEnd" ),

  TYPE_SYSTEM_INFO_PREV_WEEK_START_US( "last week start us", "LastWeekStartUS" ),
  TYPE_SYSTEM_INFO_PREV_WEEK_END_US( "last week end us", "LastWeekEndUS" ),

  TYPE_SYSTEM_INFO_THIS_WEEK_START( "this week start", "ThisWeekStart" ),
  TYPE_SYSTEM_INFO_THIS_WEEK_END( "this week end", "ThisWeekEnd" ),
  TYPE_SYSTEM_INFO_THIS_WEEK_OPEN_END( "this week open end", "ThisWeekOpenEnd" ),

  TYPE_SYSTEM_INFO_THIS_WEEK_START_US( "this week start us", "ThisWeekStartUS" ),
  TYPE_SYSTEM_INFO_THIS_WEEK_END_US( "this week end us", "ThisWeekEndUS" ),

  TYPE_SYSTEM_INFO_NEXT_WEEK_START( "next week start", "NextWeekStart" ),
  TYPE_SYSTEM_INFO_NEXT_WEEK_END( "next week end", "NextWeekEnd" ),
  TYPE_SYSTEM_INFO_NEXT_WEEK_OPEN_END( "next week open end", "NextWeekOpenEnd" ),

  TYPE_SYSTEM_INFO_NEXT_WEEK_START_US( "next week start us", "NextWeekStartUS" ),
  TYPE_SYSTEM_INFO_NEXT_WEEK_END_US( "next week end us", "NextWeekEndUS" ),

  TYPE_SYSTEM_INFO_PREV_QUARTER_START( "prev quarter start", "PrevQuarterStart" ),
  TYPE_SYSTEM_INFO_PREV_QUARTER_END( "prev quarter end", "PrevQuarterEnd" ),

  TYPE_SYSTEM_INFO_THIS_QUARTER_START( "this quarter start", "ThisQuarterStart" ),
  TYPE_SYSTEM_INFO_THIS_QUARTER_END( "this quarter end", "ThisQuarterEnd" ),

  TYPE_SYSTEM_INFO_NEXT_QUARTER_START( "next quarter start", "NextQuarterStart" ),
  TYPE_SYSTEM_INFO_NEXT_QUARTER_END( "next quarter end", "NextQuarterEnd" ),

  TYPE_SYSTEM_INFO_PREV_YEAR_START( "prev year start", "PrevYearStart" ),
  TYPE_SYSTEM_INFO_PREV_YEAR_END( "prev year end", "PrevYearEnd" ),

  TYPE_SYSTEM_INFO_THIS_YEAR_START( "this year start", "ThisYearStart" ),
  TYPE_SYSTEM_INFO_THIS_YEAR_END( "this year end", "ThisYearEnd" ),
  TYPE_SYSTEM_INFO_NEXT_YEAR_START( "next year start", "NextYearStart" ),
  TYPE_SYSTEM_INFO_NEXT_YEAR_END( "next year end", "NextYearEnd" ),

  TYPE_SYSTEM_INFO_PREVIOUS_RESULT_RESULT( "previous result result", "PreviousResultResult" ),
  TYPE_SYSTEM_INFO_PREVIOUS_RESULT_EXIT_STATUS( "previous result exist status", "PreviousResultExitStatus" ),
  TYPE_SYSTEM_INFO_PREVIOUS_RESULT_ENTRY_NR( "previous result entry nr", "PreviousResultEntryNr" ),
  TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_ERRORS( "previous result nr errors", "PreviousResultNrErrors" ),
  TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_LINES_INPUT( "previous result nr lines input", "PreviousResultNrLinesInput" ),
  TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_LINES_OUTPUT( "previous result nr lines output",
    "PreviousResultNrLinesOutput" ),
  TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_LINES_READ( "previous result nr lines read",
    "PreviousResultNrLinesRead" ),
  TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_LINES_UPDATED( "previous result nr lines updated",
    "PreviousResultNrLinesUpdated" ),
  TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_LINES_WRITTEN( "previous result nr lines written",
    "PreviousResultNrLinesWritten" ),
  TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_LINES_DELETED( "previous result nr lines deleted",
    "PreviousResultNrLinesDeleted" ),
  TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_LINES_REJECTED( "previous result nr lines rejected",
    "PreviousResultNrLinesRejected" ),
  TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_ROWS( "previous result nr rows", "PreviousResultNrLinesNrRows" ),
  TYPE_SYSTEM_INFO_PREVIOUS_RESULT_IS_STOPPED( "previous result is stopped", "PreviousResultIsStopped" ),
  TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_FILES( "previous result nr files", "PreviousResultNrFiles" ),
  TYPE_SYSTEM_INFO_PREVIOUS_RESULT_NR_FILES_RETRIEVED( "previous result nr files retrieved",
    "PreviousResultNrFilesRetrieved" ),
  TYPE_SYSTEM_INFO_PREVIOUS_RESULT_LOG_TEXT( "previous result log text", "PreviousResultLogText" );

  private String code;
  private String description;
  private static Class<?> PKG = SystemDataMeta.class; // for i18n purposes, needed by Translator!!

  public String getCode() {
    return code;
  }

  public String getDescription() {
    return description;
  }

  public static SystemDataTypes getTypeFromString( String typeStr ) {
    for ( SystemDataTypes type : SystemDataTypes.values() ) {
      // attempting to purge this typo from KTRs
      if ( "previous result nr lines rejeted".equalsIgnoreCase( typeStr ) ) {
        typeStr = "previous result nr lines rejected";
      }

      if ( type.toString().equals( typeStr )
        || type.code.equalsIgnoreCase( typeStr )
        || type.description.equalsIgnoreCase( typeStr ) ) {
        return type;
      }
    }

    return TYPE_SYSTEM_INFO_NONE;
  }

  private static String getDescription( String name ) {
    if ( PKG == null ) {
      PKG = SystemDataMeta.class;
    }
    return BaseMessages.getString( PKG, "SystemDataMeta.TypeDesc." + name );
  }

  SystemDataTypes( String code, String descriptionName ) {
    this.code = code;
    this.description = getDescription( descriptionName );
  }
}
