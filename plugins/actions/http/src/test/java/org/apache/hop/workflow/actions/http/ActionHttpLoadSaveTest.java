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

package org.apache.hop.workflow.actions.http;

import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.junit.ClassRule;

import java.util.*;

public class ActionHttpLoadSaveTest extends WorkflowActionLoadSaveTestSupport<ActionHttp> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<ActionHttp> getActionClass() {
    return ActionHttp.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList(
        new String[] {
          "url",
          "targetFilename",
          "fileAppended",
          "dateTimeAdded",
          "targetFilenameExtension",
          "uploadFilename",
          "runForEveryRow",
          "urlFieldname",
          "uploadFieldname",
          "destinationFieldname",
          "username",
          "password",
          "proxyHostname",
          "proxyPort",
          "nonProxyHosts",
          "addFilenameToResult",
          "headerName",
          "headerValue"
        });
  }

  @Override
  protected Map<String, IFieldLoadSaveValidator<?>> createAttributeValidatorsMap() {
    Map<String, IFieldLoadSaveValidator<?>> validators = new HashMap<>();
    int entries = new Random().nextInt(20) + 1;
    validators.put(
        "headerName", new ArrayLoadSaveValidator<>(new StringLoadSaveValidator(), entries));
    validators.put(
        "headerValue", new ArrayLoadSaveValidator<>(new StringLoadSaveValidator(), entries));
    return validators;
  }
}
