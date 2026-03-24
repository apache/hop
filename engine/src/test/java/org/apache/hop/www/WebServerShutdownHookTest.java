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

package org.apache.hop.www;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;

class WebServerShutdownHookTest {

  @Test
  void runInvokesStopServerWhenNotShuttingDown() throws Exception {
    WebServer webServer = mock(WebServer.class);
    WebServerShutdownHook hook = new WebServerShutdownHook(webServer);
    hook.run();
    verify(webServer).stopServer();
  }

  @Test
  void runSwallowsStopServerException() throws Exception {
    WebServer webServer = mock(WebServer.class);
    doThrow(new RuntimeException("stop failed")).when(webServer).stopServer();
    WebServerShutdownHook hook = new WebServerShutdownHook(webServer);
    hook.run();
    verify(webServer).stopServer();
  }

  @Test
  void runSkipsStopServerWhenAlreadyShuttingDown() throws Exception {
    WebServer webServer = mock(WebServer.class);
    WebServerShutdownHook hook = new WebServerShutdownHook(webServer);
    hook.setShuttingDown(true);
    hook.run();
    verify(webServer, never()).stopServer();
  }

  @Test
  void shuttingDownFlag() {
    WebServerShutdownHook hook = new WebServerShutdownHook(mock(WebServer.class));
    hook.setShuttingDown(true);
    assertTrue(hook.isShuttingDown());
    hook.setShuttingDown(false);
    assertFalse(hook.isShuttingDown());
  }
}
