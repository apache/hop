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
package org.apache.hop.workflow.actions.copymoveresultfilenames;

import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class WorkflowActionCopyMoveResultFilenamesTest extends WorkflowActionLoadSaveTestSupport<ActionCopyMoveResultFilenames> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<ActionCopyMoveResultFilenames> getActionClass() {
    return ActionCopyMoveResultFilenames.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList(
      "foldername",
      "specifywildcard",
      "wildcard",
      "wildcardexclude",
      "destination_folder",
      "nr_errors_less_than",
      "success_condition",
      "add_date",
      "add_time",
      "SpecifyFormat",
      "date_time_format",
      "action",
      "AddDateBeforeExtension",
      "OverwriteFile",
      "CreateDestinationFolder",
      "RemovedSourceFilename",
      "AddDestinationFilename" );
  }

  @Override
  protected Map<String, String> createGettersMap() {
    return toMap(
      "foldername", "getFoldername",
      "specifywildcard", "isSpecifyWildcard",
      "wildcard", "getWildcard",
      "wildcardexclude", "getWildcardExclude",
      "destination_folder", "getDestinationFolder",
      "nr_errors_less_than", "getNrErrorsLessThan",
      "success_condition", "getSuccessCondition",
      "add_date", "isAddDate",
      "add_time", "isAddTime",
      "SpecifyFormat", "isSpecifyFormat",
      "date_time_format", "getDateTimeFormat",
      "action", "getAction",
      "AddDateBeforeExtension", "isAddDateBeforeExtension",
      "OverwriteFile", "isOverwriteFile",
      "CreateDestinationFolder", "isCreateDestinationFolder",
      "RemovedSourceFilename", "isRemovedSourceFilename",
      "AddDestinationFilename", "isAddDestinationFilename" );
  }

  @Override
  protected Map<String, String> createSettersMap() {
    return toMap(
      "foldername", "setFoldername",
      "specifywildcard", "setSpecifyWildcard",
      "wildcard", "setWildcard",
      "wildcardexclude", "setWildcardExclude",
      "destination_folder", "setDestinationFolder",
      "nr_errors_less_than", "setNrErrorsLessThan",
      "success_condition", "setSuccessCondition",
      "add_date", "setAddDate",
      "add_time", "setAddTime",
      "SpecifyFormat", "setSpecifyFormat",
      "date_time_format", "setDateTimeFormat",
      "action", "setAction",
      "AddDateBeforeExtension", "setAddDateBeforeExtension",
      "OverwriteFile", "setOverwriteFile",
      "CreateDestinationFolder", "setCreateDestinationFolder",
      "RemovedSourceFilename", "setRemovedSourceFilename",
      "AddDestinationFilename", "setAddDestinationFilename" );
  }

}
