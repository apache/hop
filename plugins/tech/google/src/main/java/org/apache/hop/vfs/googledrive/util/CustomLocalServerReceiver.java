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

package org.apache.hop.vfs.googledrive.util;

import com.google.api.client.extensions.java6.auth.oauth2.VerificationCodeReceiver;
import com.google.api.client.util.Throwables;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import org.eclipse.jetty.ee11.webapp.WebAppContext;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.Fields;
import org.eclipse.jetty.util.resource.ResourceFactory;

public class CustomLocalServerReceiver implements VerificationCodeReceiver {

  private Server server;
  String code;
  String error;
  private int port;
  private final String host;
  private String url;

  public CustomLocalServerReceiver() {
    this("localhost", -1);
  }

  CustomLocalServerReceiver(String host, int port) {
    this.host = host;
    this.port = port;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  @Override
  public String getRedirectUri() throws IOException {
    if (this.port == -1) {
      this.port = getUnusedPort();
    }

    this.server = new Server(this.port);

    InetSocketAddress socketAddress = new InetSocketAddress(this.host, this.port);
    this.server = new Server(socketAddress);
    this.server.setHandler(new CallbackHandler());

    try {
      this.server.start();
    } catch (Exception var5) {
      Throwables.propagateIfPossible(var5);
      throw new IOException(var5);
    }

    return "http://" + this.host + ":" + this.port + "/Callback/success.html";
  }

  @Override
  public String waitForCode() throws IOException {
    return this.code;
  }

  @Override
  public void stop() throws IOException {
    if (this.server != null) {
      try {
        this.server.stop();
      } catch (Exception var2) {
        Throwables.propagateIfPossible(var2);
        throw new IOException(var2);
      }
      this.server = null;
    }
  }

  public String getHost() {
    return this.host;
  }

  public int getPort() {
    return this.port;
  }

  private static int getUnusedPort() throws IOException {
    Socket s = new Socket();
    s.bind((InetSocketAddress) null);

    int var1;
    try {
      var1 = s.getLocalPort();
    } finally {
      s.close();
    }
    return var1;
  }

  class CallbackHandler extends WebAppContext {

    CallbackHandler() {
      URL warUrl = this.getClass().getClassLoader().getResource("success_page");
      String warUrlString = warUrl.toExternalForm();
      setBaseResource(ResourceFactory.of(this).newResource(warUrlString));
      setContextPath("/Callback");
    }

    @Override
    public boolean handle(Request request, Response response, Callback callback) throws Exception {
      String pathInContext = request.getHttpURI().getPath();

      if (pathInContext.contains("/Callback")) {

        Fields params = Request.extractQueryParameters(request);

        CustomLocalServerReceiver.this.error = params.getValue("error");
        if (CustomLocalServerReceiver.this.code == null) {
          CustomLocalServerReceiver.this.code = params.getValue("code");
        }
        if (CustomLocalServerReceiver.this.url != null
            && CustomLocalServerReceiver.this.error != null
            && CustomLocalServerReceiver.this.error.equals("access_denied")) {
          Response.sendRedirect(request, response, callback, CustomLocalServerReceiver.this.url);
        } else {
          return super.handle(request, response, callback);
        }
      }

      callback.succeeded();
      return true;
    }
  }

  public static final class Builder {
    private String host = "localhost";
    private int port = -1;

    public Builder() {
      // Do nothing
    }

    public CustomLocalServerReceiver build() {
      return new CustomLocalServerReceiver(this.host, this.port);
    }

    public String getHost() {
      return this.host;
    }

    public Builder setHost(String host) {
      this.host = host;
      return this;
    }

    public int getPort() {
      return this.port;
    }

    public Builder setPort(int port) {
      this.port = port;
      return this;
    }
  }
}
