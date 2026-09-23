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

package org.apache.hop.pipeline.transforms.webservices.wsdl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Loading a WSDL from the places a user can point the Web services lookup transform at: an http(s)
 * URL, a plain path as the file dialog hands it out, a file: URL, or any other Hop VFS location.
 */
class WsdlTest {

  private static final String RESOURCES = "/org/apache/hop/pipeline/transforms/webservices/wsdl/";

  @TempDir Path tempDir;

  /** A folder with a space in its name: a plain path like that is not a valid URI. */
  private Path wsdlFolder;

  @BeforeEach
  void copyWsdlFiles() throws IOException {
    wsdlFolder = tempDir.resolve("my wsdl folder");
    Files.createDirectories(wsdlFolder.resolve("abstract"));
    copyResource("hello.wsdl");
    copyResource("hello-service.wsdl");
    copyResource("abstract/hello-abstract.wsdl");
  }

  private void copyResource(String name) throws IOException {
    try (InputStream in = WsdlTest.class.getResourceAsStream(RESOURCES + name)) {
      assertNotNull(in, "Missing test resource " + name);
      Files.copy(in, wsdlFolder.resolve(name));
    }
  }

  private static Wsdl load(String location) {
    return new Wsdl(location, null, null, null, null, null);
  }

  private static void assertHelloService(Wsdl wsdl) throws Exception {
    assertEquals("Hello_Service", wsdl.getServiceName());
    assertEquals("Hello_Port", wsdl.getPortName());
    assertEquals("http://localhost:8080/hello", wsdl.getServiceEndpoint());
    assertEquals(1, wsdl.getOperations().size());
    assertNotNull(wsdl.getOperation("sayHello"));
  }

  @Test
  void loadsFromPlainPath() throws Exception {
    // What the file dialog hands out, once a variable like ${PROJECT_HOME} is resolved (#4813)
    assertHelloService(load(wsdlFolder.resolve("hello.wsdl").toString()));
  }

  @Test
  void ignoresSurroundingWhitespace() throws Exception {
    assertHelloService(load("  " + wsdlFolder.resolve("hello.wsdl") + " "));
  }

  @Test
  void loadsFromFileUrl() throws Exception {
    assertHelloService(load(wsdlFolder.resolve("hello.wsdl").toUri().toString()));
  }

  @Test
  void loadsFromFileUri() throws Exception {
    Wsdl wsdl = new Wsdl(wsdlFolder.resolve("hello.wsdl").toUri(), null, null);
    assertHelloService(wsdl);
  }

  @Test
  void loadsWithVariables() throws Exception {
    Wsdl wsdl =
        new Wsdl(
            wsdlFolder.resolve("hello.wsdl").toString(), new Variables(), null, null, null, null);
    assertHelloService(wsdl);
  }

  @Test
  void resolvesImportsRelativeToTheFile() throws Exception {
    // The messages and port type live in abstract/hello-abstract.wsdl, next to this file
    assertHelloService(load(wsdlFolder.resolve("hello-service.wsdl").toString()));
  }

  @Test
  void missingFileNamesTheLocation() {
    String location = wsdlFolder.resolve("does-not-exist.wsdl").toString();
    HopRuntimeException e = assertThrows(HopRuntimeException.class, () -> load(location));
    assertTrue(e.getMessage().startsWith(Wsdl.CONST_COULD_NOT_LOAD_WSDL_FILE + location));
    assertTrue(e.getMessage().contains("does not exist"), e.getMessage());
  }

  @Test
  void blankLocationIsRejected() {
    HopRuntimeException e = assertThrows(HopRuntimeException.class, () -> load(" "));
    assertTrue(e.getMessage().contains("No WSDL location"), e.getMessage());
  }

  @Test
  void recognizesHttpLocations() {
    assertTrue(Wsdl.isHttpLocation("http://host/service?wsdl"));
    assertTrue(Wsdl.isHttpLocation("HTTPS://host/service?wsdl"));
    assertFalse(Wsdl.isHttpLocation("/work/wsdl/service.wsdl"));
    assertFalse(Wsdl.isHttpLocation("file:///work/wsdl/service.wsdl"));
    assertFalse(Wsdl.isHttpLocation("C:\\work\\wsdl\\service.wsdl"));
    assertFalse(Wsdl.isHttpLocation("s3://bucket/service.wsdl"));
  }

  @Test
  void loadsOverHttpWithBasicAuthentication() throws Exception {
    AtomicReference<String> authorization = new AtomicReference<>();
    HttpServer server = serveWsdl(authorization, 200);
    try {
      Wsdl wsdl = new Wsdl(urlOf(server), null, null, null, "user", "secret");
      assertHelloService(wsdl);
      String expected =
          Base64.getEncoder().encodeToString("user:secret".getBytes(StandardCharsets.UTF_8));
      assertEquals("Basic " + expected, authorization.get());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void loadsOverHttpWithoutAuthentication() throws Exception {
    AtomicReference<String> authorization = new AtomicReference<>();
    HttpServer server = serveWsdl(authorization, 200);
    try {
      assertHelloService(new Wsdl(new URI(urlOf(server)), null, null));
      assertNull(authorization.get());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void loadsOverHttpWithSurroundingWhitespace() throws Exception {
    HttpServer server = serveWsdl(new AtomicReference<>(), 200);
    try {
      assertHelloService(load(" " + urlOf(server) + " "));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void httpErrorNamesTheUrl() throws Exception {
    HttpServer server = serveWsdl(new AtomicReference<>(), 404);
    try {
      String url = urlOf(server);
      HopRuntimeException e = assertThrows(HopRuntimeException.class, () -> load(url));
      assertTrue(e.getMessage().contains("Unable to read WSDL from " + url), e.getMessage());
    } finally {
      server.stop(0);
    }
  }

  private HttpServer serveWsdl(AtomicReference<String> authorization, int status)
      throws IOException {
    byte[] body = Files.readAllBytes(wsdlFolder.resolve("hello.wsdl"));
    HttpServer server =
        HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext(
        "/hello",
        exchange -> {
          authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
          exchange.getResponseHeaders().add("Content-Type", "text/xml");
          if (status == 200) {
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
              out.write(body);
            }
          } else {
            exchange.sendResponseHeaders(status, -1);
            exchange.close();
          }
        });
    server.start();
    return server;
  }

  private static String urlOf(HttpServer server) {
    return "http://"
        + InetAddress.getLoopbackAddress().getHostAddress()
        + ":"
        + server.getAddress().getPort()
        + "/hello?wsdl";
  }
}
