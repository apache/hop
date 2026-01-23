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

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.Serial;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.hop.core.annotations.HopServerServlet;

@HopServerServlet(id = "shutdown", name = "Shut down the server")
public class ShutdownServlet extends BaseHttpServlet implements IHopServerPlugin {

  @Serial private static final long serialVersionUID = -911533231051351L;

  public static final String CONTEXT_PATH = "/shutdown";

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
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

  public String toString() {
    return "Shut down the server";
  }

  @Override
  public String getService() {
    return CONTEXT_PATH + " (" + toString() + ")";
  }

  @Override
  public String getContextPath() {
    return CONTEXT_PATH;
  }
}
