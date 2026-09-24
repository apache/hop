/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.workflow.actions.http;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * The proxy and the target server each keep their own credentials.
 *
 * <p>The action used to configure itself through {@code System.setProperty("http.proxyHost", ...)}
 * and a JVM-wide {@link java.net.Authenticator}, which is consulted for a 401 from the server and a
 * 407 from the proxy alike. Whichever of the two asked first was handed the same user name and
 * password, so the server's credentials went to the proxy operator (issues #2962 and #3440).
 */
class ActionHttpProxyTest {

  private static final String PROXY_USER = "proxyuser";
  private static final String PROXY_PASSWORD = "proxypassword";
  private static final String SERVER_USER = "serveruser";
  private static final String SERVER_PASSWORD = "serverpassword";
  private static final String PROXIED_PAYLOAD = "through the proxy";
  private static final String ORIGIN_PAYLOAD = "straight from the origin";

  /** A host the test must never resolve: everything for it has to go through the proxy. */
  private static final String UNRESOLVABLE_TARGET = "http://target.invalid/data";

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private HttpServer proxy;
  private HttpServer origin;
  private final AtomicReference<String> proxyAuthorization = new AtomicReference<>();
  private final AtomicReference<String> serverAuthorization = new AtomicReference<>();
  private final AtomicBoolean originWasCalled = new AtomicBoolean();
  private final AtomicBoolean proxyWasCalled = new AtomicBoolean();

  @BeforeAll
  static void setupBeforeClass() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void startProxy() throws IOException {
    proxy = HttpServer.create(new InetSocketAddress("localhost", 0), 10);
    proxy.createContext(
        "/",
        exchange -> {
          proxyWasCalled.set(true);
          List<String> proxyAuth = exchange.getRequestHeaders().get("Proxy-Authorization");
          List<String> serverAuth = exchange.getRequestHeaders().get("Authorization");
          proxyAuthorization.set(proxyAuth == null ? null : proxyAuth.get(0));
          serverAuthorization.set(serverAuth == null ? null : serverAuth.get(0));

          if (proxyAuth == null) {
            exchange.getResponseHeaders().add("Proxy-Authenticate", "Basic realm=\"hop-test\"");
            exchange.sendResponseHeaders(407, -1);
            exchange.close();
            return;
          }

          byte[] body = PROXIED_PAYLOAD.getBytes(UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
          exchange.close();
        });
    proxy.start();

    // A plain origin server, used to show that a bypassed host is reached without the proxy.
    origin = HttpServer.create(new InetSocketAddress("localhost", 0), 10);
    origin.createContext(
        "/",
        exchange -> {
          originWasCalled.set(true);
          byte[] body = ORIGIN_PAYLOAD.getBytes(UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
          exchange.close();
        });
    origin.start();
  }

  @AfterEach
  void stopProxy() {
    proxy.stop(0);
    origin.stop(0);
    proxyAuthorization.set(null);
    serverAuthorization.set(null);
    originWasCalled.set(false);
    proxyWasCalled.set(false);
  }

  @Test
  void proxyCredentialsAnswerTheProxyChallenge() throws Exception {
    File target = File.createTempFile("proxied", ".tmp");
    target.deleteOnExit();

    ActionHttp action = action(target);
    action.setProxyUsername(PROXY_USER);
    action.setProxyPassword(PROXY_PASSWORD);

    Result result = action.execute(new Result(), 0);

    assertTrue(result.getResult());
    assertEquals(0, result.getNrErrors());
    assertEquals(PROXIED_PAYLOAD, FileUtils.readFileToString(target, UTF_8));
    assertEquals(basic(PROXY_USER, PROXY_PASSWORD), proxyAuthorization.get());
  }

  @Test
  void serverCredentialsAreNeverOfferedToTheProxy() throws Exception {
    File target = File.createTempFile("notproxied", ".tmp");
    target.deleteOnExit();

    // Credentials for the target server only: the proxy asks for its own and gets nothing.
    ActionHttp action = action(target);
    action.setUsername(SERVER_USER);
    action.setPassword(SERVER_PASSWORD);

    Result result = action.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
    assertNull(proxyAuthorization.get(), "the server's credentials were sent to the proxy");
    assertNull(serverAuthorization.get(), "the server's credentials were sent to the proxy");
  }

  @Test
  void proxyCredentialsAreNeverOfferedToTheServer() throws Exception {
    File target = File.createTempFile("proxyonly", ".tmp");
    target.deleteOnExit();

    ActionHttp action = action(target);
    action.setProxyUsername(PROXY_USER);
    action.setProxyPassword(PROXY_PASSWORD);
    action.setUsername(SERVER_USER);
    action.setPassword(SERVER_PASSWORD);

    Result result = action.execute(new Result(), 0);

    assertTrue(result.getResult());
    assertEquals(basic(PROXY_USER, PROXY_PASSWORD), proxyAuthorization.get());
    // The server never challenged, so its credentials stayed at home.
    assertNull(serverAuthorization.get());
  }

  @Test
  void bypassedHostIsReachedWithoutTheProxy() throws Exception {
    // The proxy is configured, and it demands credentials that this action does not have. The
    // request still succeeds, because the bypass list sends it straight to the origin instead.
    File target = File.createTempFile("bypassed", ".tmp");
    target.deleteOnExit();

    ActionHttp action = new ActionHttp();
    action.setParentWorkflow(new LocalWorkflowEngine());
    action.setAddFilenameToResult(false);
    action.setUrl("http://localhost:" + origin.getAddress().getPort() + "/data");
    action.setTargetFilename(target.getCanonicalPath());
    action.setProxyHostname("localhost");
    action.setProxyPort(String.valueOf(proxy.getAddress().getPort()));
    action.setNonProxyHosts("localhost|127.*");

    Result result = action.execute(new Result(), 0);

    assertTrue(result.getResult());
    assertTrue(originWasCalled.get());
    assertEquals(ORIGIN_PAYLOAD, FileUtils.readFileToString(target, UTF_8));
    assertNull(proxyAuthorization.get(), "the request went through the proxy after all");
  }

  @Test
  void hostOutsideTheBypassListStillGoesThroughTheProxy() throws Exception {
    File target = File.createTempFile("notbypassed", ".tmp");
    target.deleteOnExit();

    ActionHttp action = new ActionHttp();
    action.setParentWorkflow(new LocalWorkflowEngine());
    action.setAddFilenameToResult(false);
    action.setUrl(UNRESOLVABLE_TARGET);
    action.setTargetFilename(target.getCanonicalPath());
    action.setProxyHostname("localhost");
    action.setProxyPort(String.valueOf(proxy.getAddress().getPort()));
    action.setProxyUsername(PROXY_USER);
    action.setProxyPassword(PROXY_PASSWORD);
    action.setNonProxyHosts("localhost|127.*");

    Result result = action.execute(new Result(), 0);

    assertTrue(result.getResult());
    assertEquals(PROXIED_PAYLOAD, FileUtils.readFileToString(target, UTF_8));
    assertEquals(basic(PROXY_USER, PROXY_PASSWORD), proxyAuthorization.get());
  }

  @Test
  void withoutAProxyOfItsOwnTheJvmProxySettingsApply() throws Exception {
    // A workflow that leaves the proxy blank and relies on -Dhttp.proxyHost or
    // java.net.useSystemProxies kept using that proxy under URLConnection; it still has to.
    File target = File.createTempFile("systemproxy", ".tmp");
    target.deleteOnExit();

    InetSocketAddress proxyAddress = proxy.getAddress();
    ProxySelector previous = ProxySelector.getDefault();
    ProxySelector.setDefault(
        new ProxySelector() {
          @Override
          public List<Proxy> select(URI uri) {
            return List.of(new Proxy(Proxy.Type.HTTP, proxyAddress));
          }

          @Override
          public void connectFailed(URI uri, SocketAddress sa, IOException ioe) {
            // Nothing to do in a test.
          }
        });
    try {
      ActionHttp action = new ActionHttp();
      action.setParentWorkflow(new LocalWorkflowEngine());
      action.setAddFilenameToResult(false);
      action.setUrl(UNRESOLVABLE_TARGET);
      action.setTargetFilename(target.getCanonicalPath());

      action.execute(new Result(), 0);

      assertTrue(proxyWasCalled.get(), "the JVM proxy settings were ignored");
    } finally {
      ProxySelector.setDefault(previous);
    }
  }

  @Test
  void credentialsInTheUrlNeverReachTheLog() throws Exception {
    File target = File.createTempFile("urlcredentials", ".tmp");
    target.deleteOnExit();

    ActionHttp action = new ActionHttp();
    action.setParentWorkflow(new LocalWorkflowEngine());
    action.setAddFilenameToResult(false);
    action.setUrl(
        "http://urluser:topsecret@localhost:"
            + origin.getAddress().getPort()
            + "/data?api_key=k3y&q=1");
    action.setTargetFilename(target.getCanonicalPath());

    Result result = action.execute(new Result(), 0);
    String log =
        HopLogStore.getAppender()
            .getBuffer(action.getLogChannel().getLogChannelId(), false)
            .toString();

    assertTrue(result.getResult());
    assertTrue(originWasCalled.get());
    assertTrue(log.contains("localhost"), "the URL should still be logged: " + log);
    assertFalse(log.contains("topsecret"), "the password in the URL was logged: " + log);
    assertFalse(log.contains("k3y"), "the API key in the URL was logged: " + log);
  }

  private ActionHttp action(File target) throws IOException {
    ActionHttp action = new ActionHttp();
    action.setParentWorkflow(new LocalWorkflowEngine());
    action.setAddFilenameToResult(false);
    action.setUrl(UNRESOLVABLE_TARGET);
    action.setTargetFilename(target.getCanonicalPath());
    action.setProxyHostname("localhost");
    action.setProxyPort(String.valueOf(proxy.getAddress().getPort()));
    return action;
  }

  private static String basic(String user, String password) {
    return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(UTF_8));
  }
}
