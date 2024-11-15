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

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class ShutdownServlet extends BaseHttpServlet {
  private static final long serialVersionUID = -1L;

  public static final String CONTEXT_PATH = "/shutdown";

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    logBasic(
        "Received server shutdown from user '"
            + request.getRemoteUser()
            + "' at address "
            + request.getRemoteAddr());

    TimerTask task =
        new TimerTask() {
          public void run() {
            //  Gracefully shutdown the web server
            HopServer server = HopServerSingleton.getHopServer();
            if (server != null) {
              server.shutdown();
            }
          }
        };

    // Delay shutdown to give servlet time to respond
    new Timer("Server shutdown timer").schedule(task, 2000L);
    response.setStatus(HttpServletResponse.SC_OK);
  }
}
