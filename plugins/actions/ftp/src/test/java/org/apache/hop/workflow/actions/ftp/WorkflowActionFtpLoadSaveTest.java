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

package org.apache.hop.workflow.actions.ftp;

import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.List;

public class WorkflowActionFtpLoadSaveTest extends WorkflowActionLoadSaveTestSupport<ActionFtp> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<ActionFtp> getActionClass() {
    return ActionFtp.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList( new String[] {
      "serverPort",
      "serverName",
      "userName",
      "password",
      "remoteDirectory",
      "targetDirectory",
      "wildcard",
      "binaryMode",
      "timeout",
      "remove",
      "onlyGettingNewFiles",
      "activeConnection",
      "controlEncoding",
      "moveFiles",
      "moveToDirectory",
      "addDate",
      "addTime",
      "specifyFormat",
      "dateTimeFormat",
      "addDateBeforeExtension",
      "addResult",
      "createMoveFolder",
      "proxyHost",
      "proxyPort",
      "proxyUsername",
      "proxyPassword",
      "socksProxyHost",
      "socksProxyPort",
      "socksProxyUsername",
      "socksProxyPassword",
      "stringIfFileExists",
      "nrLimit",
      "successCondition" } );
  }

}
