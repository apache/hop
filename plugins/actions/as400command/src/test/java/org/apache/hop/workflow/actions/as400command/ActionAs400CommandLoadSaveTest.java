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
package org.apache.hop.workflow.actions.as400command;

import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.apache.hop.workflow.actions.as400command.ActionAs400Command;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.ClassRule;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ActionAs400CommandLoadSaveTest
    extends WorkflowActionLoadSaveTestSupport<ActionAs400Command> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<ActionAs400Command> getActionClass() {
    return ActionAs400Command.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList( new String[] {"serverName", "userName", "password", "proxyHost", "proxyPort", "command"});
  }

  @Override
  protected Map<String, String> createGettersMap() {
    return toMap(
        "serverName", "getServerName",
        "userName", "getUserName",
        "password", "getPassword",
        "proxyHost", "getProxyHost",
        "proxyPort", "getProxyPort",
        "command", "getCommand");
  }

  @Override
  protected Map<String, String> createSettersMap() {
    return toMap(
        "serverName", "setServerName",
        "userName", "setUserName",
        "password", "setPassword",
        "proxyHost", "setProxyHost",
        "proxyPort", "setProxyPort",
        "command", "setCommand");
  }
}
