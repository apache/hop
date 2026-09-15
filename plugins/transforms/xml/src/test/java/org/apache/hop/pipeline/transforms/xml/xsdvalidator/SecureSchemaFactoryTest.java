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

package org.apache.hop.pipeline.transforms.xml.xsdvalidator;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.File;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.xml.XMLConstants;
import javax.xml.validation.SchemaFactory;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXParseException;

/**
 * The XSD Validator transform and action hand external entity resolution to {@link
 * XmlParserFactoryProducer#createSecureSchemaFactory(String)}. This module puts Xerces on the
 * classpath the way the Hop runtime does, and Xerces' {@code XMLSchemaFactory} does not recognize
 * the JAXP external access properties -- so the restrictions have to be verified here, not only
 * against the JDK implementation used in hop-core's own tests.
 */
class SecureSchemaFactoryTest {

  private static final String REMOTE_SCHEMA =
      "<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">"
          + "<xs:element name=\"remote\" type=\"xs:string\"/>"
          + "</xs:schema>";

  @Test
  void secureSchemaFactoryIsTheXercesImplementation() throws Exception {
    SchemaFactory schemaFactory =
        XmlParserFactoryProducer.createSecureSchemaFactory(XMLConstants.W3C_XML_SCHEMA_NS_URI);

    assertEquals(
        "org.apache.xerces.jaxp.validation.XMLSchemaFactory", schemaFactory.getClass().getName());
    assertTrue(schemaFactory.getFeature(XMLConstants.FEATURE_SECURE_PROCESSING));
  }

  @Test
  void secureSchemaFactoryDoesNotFetchRemoteSchemaReference(@TempDir Path tempDir)
      throws Exception {
    AtomicInteger requests = new AtomicInteger();
    HttpServer server = startSchemaServer(requests);
    try {
      File including = writeIncludingSchema(tempDir, "including-secure.xsd", server);

      SchemaFactory schemaFactory =
          XmlParserFactoryProducer.createSecureSchemaFactory(XMLConstants.W3C_XML_SCHEMA_NS_URI);
      // Xerces reports an unresolvable xs:include as a non-fatal error, so collect it rather than
      // expecting newSchema() to throw.
      List<SAXParseException> problems = new ArrayList<>();
      schemaFactory.setErrorHandler(collectingErrorHandler(problems));

      schemaFactory.newSchema(including);

      assertEquals(0, requests.get(), "the secure schema factory contacted the remote host");
      assertTrue(!problems.isEmpty(), "expected the blocked xs:include to be reported");
    } finally {
      server.stop(0);
    }
  }

  @Test
  void plainSchemaFactoryDoesFetchRemoteSchemaReference(@TempDir Path tempDir) throws Exception {
    // Guards the test above: without the hardening the very same xs:include is fetched, so a zero
    // request count there really is the restriction at work and not an unreachable server.
    AtomicInteger requests = new AtomicInteger();
    HttpServer server = startSchemaServer(requests);
    try {
      File including = writeIncludingSchema(tempDir, "including-plain.xsd", server);

      SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI).newSchema(including);

      assertEquals(1, requests.get());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void secureSchemaFactoryStillResolvesLocalSchemaReference(@TempDir Path tempDir)
      throws Exception {
    // Restricting external access must not break multi-file schemas on the local file system.
    Files.writeString(
        tempDir.resolve("included.xsd"),
        "<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">"
            + "<xs:element name=\"included\" type=\"xs:string\"/>"
            + "</xs:schema>");
    File including = tempDir.resolve("including.xsd").toFile();
    Files.writeString(
        including.toPath(),
        "<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">"
            + "<xs:include schemaLocation=\"included.xsd\"/>"
            + "<xs:element name=\"root\" type=\"xs:string\"/>"
            + "</xs:schema>");

    SchemaFactory schemaFactory =
        XmlParserFactoryProducer.createSecureSchemaFactory(XMLConstants.W3C_XML_SCHEMA_NS_URI);

    assertDoesNotThrow(() -> schemaFactory.newSchema(including));
  }

  private HttpServer startSchemaServer(AtomicInteger requests) throws Exception {
    HttpServer server =
        HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext(
        "/included.xsd",
        exchange -> {
          requests.incrementAndGet();
          byte[] body = REMOTE_SCHEMA.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "text/xml");
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
          }
        });
    server.start();
    return server;
  }

  private File writeIncludingSchema(Path tempDir, String filename, HttpServer server)
      throws Exception {
    File including = tempDir.resolve(filename).toFile();
    Files.writeString(
        including.toPath(),
        "<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">"
            + "<xs:include schemaLocation=\"http://"
            + server.getAddress().getHostString()
            + ":"
            + server.getAddress().getPort()
            + "/included.xsd\"/>"
            + "</xs:schema>");
    return including;
  }

  private ErrorHandler collectingErrorHandler(List<SAXParseException> problems) {
    return new ErrorHandler() {
      @Override
      public void warning(SAXParseException e) {
        problems.add(e);
      }

      @Override
      public void error(SAXParseException e) {
        problems.add(e);
      }

      @Override
      public void fatalError(SAXParseException e) throws SAXParseException {
        throw e;
      }
    };
  }
}
