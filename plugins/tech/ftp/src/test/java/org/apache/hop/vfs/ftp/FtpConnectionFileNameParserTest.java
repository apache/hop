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

package org.apache.hop.vfs.ftp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.apache.commons.vfs2.provider.VfsComponentContext;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPlugin;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.ftp.metadata.FtpConnection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The scheme of a URI behind a named connection is the name of that connection, and everything
 * after it is a path on the server. The server itself never appears in the URI.
 */
class FtpConnectionFileNameParserTest {

  private IVariables variables;
  private VfsComponentContext context;

  @BeforeEach
  void setUp() throws Exception {
    // The parser resolves the password of the connection, which needs the encoder.
    PluginRegistry.getInstance()
        .registerPluginClass(
            HopTwoWayPasswordEncoder.class.getName(),
            TwoWayPasswordEncoderPluginType.class,
            TwoWayPasswordEncoderPlugin.class);
    Encr.init("Hop");

    variables = new Variables();
    FileSystemManager manager = mock(FileSystemManager.class);
    when(manager.getSchemes()).thenReturn(new String[] {"prod", "file", "ftp"});
    context = mock(VfsComponentContext.class);
    when(context.getFileSystemManager()).thenReturn(manager);
  }

  @Test
  @DisplayName("Any number of slashes after the scheme means the same absolute path")
  void slashesAfterTheSchemeAreEquivalent() throws Exception {
    for (String uri :
        new String[] {"prod://tmp/x.csv", "prod:///tmp/x.csv", "prod:////tmp/x.csv"}) {
      assertEquals("/tmp/x.csv", parse(uri).getPath(), uri);
    }
  }

  @Test
  @DisplayName("The server, port and credentials come from the connection, not from the URI")
  void serverComesFromTheConnection() throws Exception {
    GenericFileName name = parse("prod:///inbox/customers.csv");

    assertEquals("ftp.example.com", name.getHostName());
    assertEquals(2121, name.getPort());
    assertEquals("hop", name.getUserName());
    assertEquals("secret", name.getPassword());
  }

  @Test
  @DisplayName("The URI hands back what the user typed, without the server or the credentials")
  void uriKeepsTheConnectionName() throws Exception {
    assertEquals("prod:///inbox/customers.csv", parse("prod:///inbox/customers.csv").getURI());
  }

  @Test
  @DisplayName("An FTPS connection defaults to the implicit port when none is given")
  void implicitFtpsDefaultsToPort990() throws Exception {
    FtpConnection connection = connection();
    connection.setSecurityMode(FtpSecurityMode.FTPS_IMPLICIT);
    connection.setServerPort("");

    assertEquals(990, parse(connection, "prod:///x").getPort());
  }

  @Test
  @DisplayName("A URI naming another connection is refused rather than quietly rerouted")
  void otherSchemeIsRefused() {
    FileSystemException e =
        assertThrows(FileSystemException.class, () -> parse("ftp://elsewhere/x.csv"));
    assertEquals(true, e.getMessage().contains("prod"));
  }

  @Test
  @DisplayName("A connection without a server name is refused with a message naming it")
  void missingServerNameIsRefused() {
    FtpConnection connection = connection();
    connection.setServerName("");

    FileSystemException e =
        assertThrows(FileSystemException.class, () -> parse(connection, "prod:///x"));
    assertEquals(true, e.getMessage().contains("prod"));
  }

  @Test
  @DisplayName("With an FTP proxy the name points at the proxy and carries the server along")
  void proxyBecomesTheHost() throws Exception {
    FtpConnection connection = connection();
    connection.setProxyHost("proxy.example.com");
    connection.setProxyPort("8021");

    GenericFileName name = parse(connection, "prod:///x");

    assertEquals("proxy.example.com", name.getHostName());
    assertEquals(8021, name.getPort());
    assertEquals("hop@ftp.example.com", name.getUserName());
  }

  @Test
  @DisplayName("A name relative to a base file stays on the same connection")
  void aRelativeNameStaysOnTheConnection() throws Exception {
    FtpConnection connection = connection();
    GenericFileName base =
        (GenericFileName)
            new FtpConnectionFileNameParser(variables, connection)
                .parseUri(context, null, "prod:///inbox");

    GenericFileName child =
        (GenericFileName)
            new FtpConnectionFileNameParser(variables, connection)
                .parseUri(context, base, "customers.csv");

    assertEquals("/inbox/customers.csv", child.getPath());
    assertEquals("prod", child.getScheme());
  }

  @Test
  @DisplayName("A relative name without a base has nothing to be relative to")
  void aRelativeNameNeedsABase() {
    assertThrows(FileSystemException.class, () -> parse("customers.csv"));
  }

  @Test
  @DisplayName("A connection without a name is refused: the name is the scheme")
  void aNamelessConnectionIsRefused() {
    FtpConnection connection = connection();
    connection.setName("");

    assertThrows(FileSystemException.class, () -> parse(connection, "prod:///x"));
  }

  @Test
  @DisplayName("A path with dot segments in it is canonicalised")
  void dotSegmentsAreResolved() throws Exception {
    assertEquals("/inbox/x.csv", parse("prod:///inbox/sub/../x.csv").getPath());
  }

  private GenericFileName parse(String uri) throws FileSystemException {
    return parse(connection(), uri);
  }

  private GenericFileName parse(FtpConnection connection, String uri) throws FileSystemException {
    return (GenericFileName)
        new FtpConnectionFileNameParser(variables, connection).parseUri(context, null, uri);
  }

  private FtpConnection connection() {
    FtpConnection connection = new FtpConnection();
    connection.setName("prod");
    connection.setServerName("ftp.example.com");
    connection.setServerPort("2121");
    connection.setUserName("hop");
    connection.setPassword("secret");
    return connection;
  }
}
