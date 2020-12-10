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
package org.apache.hop.workflow.actions.copyfiles;

import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class WorkflowActionCopyFilesLoadSaveTest extends WorkflowActionLoadSaveTestSupport<ActionCopyFiles> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<ActionCopyFiles> getActionClass() {
    return ActionCopyFiles.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList( "copyEmptyFolders", "argFromPrevious", "overwriteFiles", "includeSubFolders",
      "removeSourceFiles", "addResultFilenames", "destinationIsAFile", "createDestinationFolder",
      "sourceFileFolder", "destinationFileFolder", "wildcard" );
  }

  @Override
  protected Map<String, String> createGettersMap() {
    return toMap(
      "copyEmptyFolders", "isCopyEmptyFolders",
      "argFromPrevious", "isArgFromPrevious",
      "overwriteFiles", "isOverwriteFiles",
      "includeSubFolders", "isIncludeSubFolders",
      "removeSourceFiles", "isRemoveSourceFiles",
      "addResultFilenames", "isAddResultFilenames",
      "destinationIsAFile", "isDestinationIsAFile",
      "createDestinationFolder", "isCreateDestinationFolder"
    );
  }

  @Override
  protected Map<String, String> createSettersMap() {
    return toMap(
      "copyEmptyFolders", "setCopyEmptyFolders",
      "argFromPrevious", "setArgFromPrevious",
      "overwriteFiles", "setOverwriteFiles",
      "includeSubFolders", "setIncludeSubFolders",
      "removeSourceFiles", "setRemoveSourceFiles",
      "addResultFilenames", "setAddResultFilenames",
      "destinationIsAFile", "setDestinationIsAFile",
      "createDestinationFolder", "setCreateDestinationFolder"
    );
  }

  @Override
  protected Map<String, IFieldLoadSaveValidator<?>> createAttributeValidatorsMap() {
    int fileArraySize = new Random().nextInt( 5 ) + 1;
    Map<String, IFieldLoadSaveValidator<?>> attrMap = new HashMap<>();
    attrMap.put( "sourceFileFolder",
      new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), fileArraySize ) );
    attrMap.put( "destinationFileFolder",
      new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), fileArraySize ) );
    attrMap.put( "wildcard", new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), fileArraySize ) );
    return attrMap;
  }

}
