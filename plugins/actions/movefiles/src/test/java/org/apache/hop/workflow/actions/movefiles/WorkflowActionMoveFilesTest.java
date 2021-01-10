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
package org.apache.hop.workflow.actions.movefiles;

import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class WorkflowActionMoveFilesTest extends WorkflowActionLoadSaveTestSupport<ActionMoveFiles> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<ActionMoveFiles> getActionClass() {
    return ActionMoveFiles.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList(
      "addDate",
      "addTime",
      "specifyFormat",
      "dateTimeFormat",
      "addDateBeforeExtension",
      "doNotKeepFolderStructure",
      "ifFileExists",
      "destinationFolder",
      "ifMovedFileExists",
      "movedDateTimeFormat",
      "addMovedDateBeforeExtension",
      "addMovedDate",
      "addMovedTime",
      "specifyMoveFormat",
      "createMoveToFolder",
      "simulate"
    );
  }

  @Override
  protected Map<String, String> createGettersMap() {
    return toMap(
      "add_date", "isAddDate",
      "add_time", "isAddTime",
      "SpecifyFormat", "isSpecifyFormat",
      "date_time_format", "getDateTimeFormat",
      "AddDateBeforeExtension", "isAddDateBeforeExtension",
      "DoNotKeepFolderStructure", "isDoNotKeepFolderStructure",
      "iffileexists", "getIfFileExists",
      "destinationFolder", "getDestinationFolder",
      "ifmovedfileexists", "getIfMovedFileExists",
      "moved_date_time_format", "getMovedDateTimeFormat",
      "AddMovedDateBeforeExtension", "isAddMovedDateBeforeExtension",
      "add_moved_date", "isAddMovedDate",
      "add_moved_time", "isAddMovedTime",
      "SpecifyMoveFormat", "isSpecifyMoveFormat" );
  }

  @Override
  protected Map<String, String> createSettersMap() {
    return toMap(
      "add_date", "setAddDate",
      "add_time", "setAddTime",
      "SpecifyFormat", "setSpecifyFormat",
      "date_time_format", "setDateTimeFormat",
      "AddDateBeforeExtension", "setAddDateBeforeExtension",
      "DoNotKeepFolderStructure", "setDoNotKeepFolderStructure",
      "iffileexists", "setIfFileExists",
      "destinationFolder", "setDestinationFolder",
      "ifmovedfileexists", "setIfMovedFileExists",
      "moved_date_time_format", "setMovedDateTimeFormat",
      "AddMovedDateBeforeExtension", "setAddMovedDateBeforeExtension",
      "add_moved_date", "setAddMovedDate",
      "add_moved_time", "setAddMovedTime",
      "SpecifyMoveFormat", "setSpecifyMoveFormat",
      "simulate", "setSimulate" );
  }
}
