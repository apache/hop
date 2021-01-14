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

public class WebServerShutdownHook extends Thread {

  WebServer webServer;
  boolean shuttingDown; //Set when shutting down so we only stop the server once.

  WebServerShutdownHook( WebServer webServer ) {
    this.webServer = webServer;
  }

  @Override
  public void run() {
    if ( !shuttingDown ) {
      try {
        webServer.stopServer();
      } catch ( Exception e ) {
        e.printStackTrace();
      }
    }
  }

  public boolean isShuttingDown() {
    return shuttingDown;
  }

  public void setShuttingDown( boolean shuttingDown ) {
    this.shuttingDown = shuttingDown;
  }

}
