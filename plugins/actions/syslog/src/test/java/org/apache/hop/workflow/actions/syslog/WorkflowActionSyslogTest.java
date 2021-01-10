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
package org.apache.hop.workflow.actions.syslog;

import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class WorkflowActionSyslogTest extends WorkflowActionLoadSaveTestSupport<ActionSyslog> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<ActionSyslog> getActionClass() {
    return ActionSyslog.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList(
      "serverName",
      "port",
      "message",
      "facility",
      "priority",
      "datePattern",
      "addTimestamp",
      "addHostname" );
  }

  @Override
  protected Map<String, String> createGettersMap() {
    return toMap(
      "serverName", "getServerName",
      "port", "getPort",
      "message", "getMessage",
      "facility", "getFacility",
      "priority", "getPriority",
      "datePattern", "getDatePattern",
      "addTimestamp", "isAddTimestamp",
      "addHostname", "isAddHostName" );
  }

  @Override
  protected Map<String, String> createSettersMap() {
    return toMap(
      "serverName", "setServerName",
      "port", "setPort",
      "message", "setMessage",
      "facility", "setFacility",
      "priority", "setPriority",
      "datePattern", "setDatePattern",
      "addTimestamp", "addTimestamp",
      "addHostname", "addHostName" );
  }

}
