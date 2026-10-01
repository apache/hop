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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import javax.wsdl.extensions.schema.Schema;
import javax.wsdl.extensions.schema.SchemaImport;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
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
    Files.createDirectories(wsdlFolder.resolve("schema"));
    copyResource("hello-typed.wsdl");
    copyResource("schema/hello-types.xsd");
  }

  /**
   * Copies the WSDL files to an in-memory Hop VFS file system: one Java's URL class cannot open,
   * like s3:// or azure://, so every import has to go through Hop VFS to be found.
   */
  private String copyToRam() throws Exception {
    String folder = "ram:///wsdl-" + UUID.randomUUID();
    for (String name :
        List.of(
            "hello-service.wsdl",
            "abstract/hello-abstract.wsdl",
            "hello-typed.wsdl",
            "schema/hello-types.xsd")) {
      HopVfs.getFileObject(folder + "/" + name).getParent().createFolder();
      try (OutputStream out = HopVfs.getOutputStream(folder + "/" + name, false)) {
        Files.copy(wsdlFolder.resolve(name), out);
      }
    }
    return folder;
  }

  /** The XML schema the typed WSDL imports was found and read, not just named. */
  private static void assertTypesSchemaWasImported(Wsdl wsdl) {
    Schema schema = (Schema) wsdl.getWsdlTypes().getSchemas().get(0);
    List<?> imports = (List<?>) schema.getImports().get("http://example.com/hello/types");
    assertNotNull(imports, "the schema import is missing");
    SchemaImport schemaImport = (SchemaImport) imports.get(0);
    assertNotNull(schemaImport.getReferencedSchema(), "the imported schema was not read");
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
  void resolvesSchemaImportsRelativeToTheFile() throws Exception {
    Wsdl wsdl = load(wsdlFolder.resolve("hello-typed.wsdl").toString());
    assertEquals("Hello_Service", wsdl.getServiceName());
    assertTypesSchemaWasImported(wsdl);
  }

  @Test
  void resolvesImportsOnAnyHopVfsFileSystem() throws Exception {
    assertHelloService(load(copyToRam() + "/hello-service.wsdl"));
  }

  @Test
  void resolvesSchemaImportsOnAnyHopVfsFileSystem() throws Exception {
    Wsdl wsdl = load(copyToRam() + "/hello-typed.wsdl");
    assertEquals("Hello_Service", wsdl.getServiceName());
    assertTypesSchemaWasImported(wsdl);
  }

  @Test
  void aMissingImportNamesTheImport() throws Exception {
    String folder = copyToRam();
    HopVfs.getFileObject(folder + "/abstract/hello-abstract.wsdl").delete();
    HopRuntimeException e =
        assertThrows(HopRuntimeException.class, () -> load(folder + "/hello-service.wsdl"));
    assertTrue(e.getMessage().contains("abstract/hello-abstract.wsdl"), e.getMessage());
  }

  @Test
  void relativeImportsOverHttpAreAuthenticatedWithTheWsdlHost() throws Exception {
    Map<String, String> authorizations = new ConcurrentHashMap<>();
    HttpServer server = serveFolder(authorizations, true);
    try {
      String url = baseUrlOf(server) + "/hello-service.wsdl";
      assertHelloService(new Wsdl(url, null, null, null, "user", "secret"));
      String expected =
          "Basic "
              + Base64.getEncoder().encodeToString("user:secret".getBytes(StandardCharsets.UTF_8));
      assertEquals(expected, authorizations.get("/hello-service.wsdl"));
      assertEquals(expected, authorizations.get("/abstract/hello-abstract.wsdl"));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void importsFromAnotherHostGetNoCredentials() throws Exception {
    Map<String, String> wsdlHost = new ConcurrentHashMap<>();
    Map<String, String> otherHost = new ConcurrentHashMap<>();
    HttpServer other = serveFolder(otherHost, false);
    // The WSDL names its import with an absolute URL on another server
    String service =
        Files.readString(wsdlFolder.resolve("hello-service.wsdl"))
            .replace(
                "location=\"abstract/hello-abstract.wsdl\"",
                "location=\"" + baseUrlOf(other) + "/abstract/hello-abstract.wsdl\"");
    Files.writeString(wsdlFolder.resolve("hello-service.wsdl"), service);
    HttpServer server = serveFolder(wsdlHost, false);
    try {
      assertHelloService(
          new Wsdl(baseUrlOf(server) + "/hello-service.wsdl", null, null, null, "user", "secret"));
      assertNotNull(wsdlHost.get("/hello-service.wsdl"));
      assertTrue(otherHost.containsKey("/abstract/hello-abstract.wsdl"));
      assertEquals(
          "", otherHost.get("/abstract/hello-abstract.wsdl"), "credentials went to another host");
    } finally {
      server.stop(0);
      other.stop(0);
    }
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

  /**
   * Serves the WSDL folder, recording the Authorization header of each request by path ("" when
   * there was none). With requireAuthentication a request without one gets a 401.
   */
  private HttpServer serveFolder(Map<String, String> authorizations, boolean requireAuthentication)
      throws IOException {
    HttpServer server =
        HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext(
        "/",
        exchange -> {
          String path = exchange.getRequestURI().getPath();
          String authorization = exchange.getRequestHeaders().getFirst("Authorization");
          authorizations.put(path, authorization == null ? "" : authorization);
          Path file = wsdlFolder.resolve(path.substring(1));
          if ((requireAuthentication && authorization == null) || !Files.isRegularFile(file)) {
            exchange.sendResponseHeaders(authorization == null ? 401 : 404, -1);
            exchange.close();
            return;
          }
          byte[] body = Files.readAllBytes(file);
          exchange.getResponseHeaders().add("Content-Type", "text/xml");
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
          }
        });
    server.start();
    return server;
  }

  private static String baseUrlOf(HttpServer server) {
    return "http://"
        + InetAddress.getLoopbackAddress().getHostAddress()
        + ":"
        + server.getAddress().getPort();
  }

  private static String urlOf(HttpServer server) {
    return "http://"
        + InetAddress.getLoopbackAddress().getHostAddress()
        + ":"
        + server.getAddress().getPort()
        + "/hello?wsdl";
  }
}
