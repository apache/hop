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

package org.apache.hop.workflow.actions.mail;

import org.apache.hop.core.ResultFile;
import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IntLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.PrimitiveIntArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class WorkflowActionMailLoadSaveTest extends WorkflowActionLoadSaveTestSupport<ActionMail> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<ActionMail> getActionClass() {
    return ActionMail.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList( new String[] { "server", "port", "destination", "destinationCc", "destinationBCc",
      "replyAddress", "replyName", "subject", "includeDate", "contactPerson", "contactPhone", "comment",
      "includingFiles", "zipFiles", "zipFilename", "usingAuthentication", "usingSecureAuthentication",
      "authenticationUser", "authenticationPassword", "onlySendComment", "useHTML", "usePriority",
      "encoding", "priority", "importance", "sensitivity", "secureConnectionType", "replyToAddresses",
      "fileType", "embeddedimages", "contentids" } );
  }

  @Override
  protected Map<String, IFieldLoadSaveValidator<?>> createAttributeValidatorsMap() {
    Map<String, IFieldLoadSaveValidator<?>> validators = new HashMap<>();
    validators.put( "fileType", new PrimitiveIntArrayLoadSaveValidator(
      new IntLoadSaveValidator( ResultFile.fileTypeCode.length ) ) );

    int entries = new Random().nextInt( 20 ) + 1;
    validators.put( "embeddedimages", new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), entries ) );
    validators.put( "contentids", new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), entries ) );
    return validators;
  }

}
