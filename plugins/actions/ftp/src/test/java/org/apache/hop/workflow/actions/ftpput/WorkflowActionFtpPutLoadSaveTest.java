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

package org.apache.hop.workflow.actions.ftpput;

import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class WorkflowActionFtpPutLoadSaveTest
    extends WorkflowActionLoadSaveTestSupport<ActionFtpPut> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<ActionFtpPut> getActionClass() {
    return ActionFtpPut.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList(
        "servername",
        "serverPort",
        "username",
        "password",
        "remoteDirectory",
        "localDirectory",
        "wildcard",
        "binary",
        "timeout",
        "remove",
        "only_new",
        "active",
        "control_encoding",
        "proxy_host",
        "proxy_port",
        "proxy_username",
        "proxy_password",
        "socksproxy_host",
        "socksproxy_port",
        "socksproxy_username",
        "socksproxy_password");
  }

  @Override
  protected Map<String, String> createGettersMap() {
    return toMap(
        "servername", "getServerName",
        "serverport", "getServerPort",
        "username", "getUserName",
        "password", "getPassword",
        "remoteDirectory", "getRemoteDirectory",
        "localDirectory", "getLocalDirectory",
        "wildcard", "getWildcard",
        "binary", "isBinaryMode",
        "timeout", "getTimeout",
        "remove", "getRemove",
        "only_new", "isOnlyPuttingNewFiles",
        "active", "isActiveConnection",
        "control_encoding", "getControlEncoding",
        "proxy_host", "getProxyHost",
        "proxy_port", "getProxyPort",
        "proxy_username", "getProxyUsername",
        "proxy_password", "getProxyPassword",
        "socksproxy_host", "getSocksProxyHost",
        "socksproxy_port", "getSocksProxyPort",
        "socksproxy_username", "getSocksProxyUsername",
        "socksproxy_password", "getSocksProxyPassword");
  }

  @Override
  protected Map<String, String> createSettersMap() {
    return toMap(
        "servername", "setServerName",
        "serverport", "setServerPort",
        "username", "setUserName",
        "password", "setPassword",
        "remoteDirectory", "setRemoteDirectory",
        "localDirectory", "setLocalDirectory",
        "wildcard", "setWildcard",
        "binary", "setBinaryMode",
        "timeout", "setTimeout",
        "remove", "setRemove",
        "only_new", "setOnlyPuttingNewFiles",
        "active", "setActiveConnection",
        "control_encoding", "setControlEncoding",
        "proxy_host", "setProxyHost",
        "proxy_port", "setProxyPort",
        "proxy_username", "setProxyUsername",
        "proxy_password", "setProxyPassword",
        "socksproxy_host", "setSocksProxyHost",
        "socksproxy_port", "setSocksProxyPort",
        "socksproxy_username", "setSocksProxyUsername",
        "socksproxy_password", "setSocksProxyPassword");
  }
}
