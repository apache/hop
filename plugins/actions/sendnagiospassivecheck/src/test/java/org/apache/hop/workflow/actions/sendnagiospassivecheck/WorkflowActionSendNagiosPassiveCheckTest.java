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
package org.apache.hop.workflow.actions.sendnagiospassivecheck;

import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class WorkflowActionSendNagiosPassiveCheckTest extends WorkflowActionLoadSaveTestSupport<ActionSendNagiosPassiveCheck> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<ActionSendNagiosPassiveCheck> getActionClass() {
    return ActionSendNagiosPassiveCheck.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList(
      "port",
      "serverName",
      "password",
      "responseTimeOut",
      "connectionTimeOut",
      "senderServerName",
      "senderServiceName",
      "message" );
  }

  @Override
  protected Map<String, String> createGettersMap() {
    return toMap(
      "port", "getPort",
      "serverName", "getServerName",
      "password", "getPassword",
      "responseTimeOut", "getResponseTimeOut",
      "connectionTimeOut", "getConnectionTimeOut",
      "senderServerName", "getSenderServerName",
      "senderServiceName", "getSenderServiceName",
      "message", "getMessage" );
  }

  @Override
  protected Map<String, String> createSettersMap() {
    return toMap(
      "port", "setPort",
      "serverName", "setServerName",
      "password", "setPassword",
      "responseTimeOut", "setResponseTimeOut",
      "connectionTimeOut", "setConnectionTimeOut",
      "senderServerName", "setSenderServerName",
      "senderServiceName", "setSenderServiceName",
      "message", "setMessage" );
  }

}
