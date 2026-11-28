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

package org.apache.hop.core.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.json.JsonMetadataParser;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The SSH jump hosts of a connection: stored in the connection JSON, copied with the connection.
 */
class DatabaseMetaSshJumpHostTest {

  private JsonMetadataParser<DatabaseMeta> parser;

  @BeforeAll
  static void setUpClass() throws HopException {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().searchPlugins();
  }

  @BeforeEach
  void setUp() {
    JsonMetadataProvider provider =
        new JsonMetadataProvider(
            new HopTwoWayPasswordEncoder(),
            "/tmp/test-metadata",
            Variables.getADefaultVariableSpace());
    parser = new JsonMetadataParser<>(DatabaseMeta.class, provider);
  }

  @Test
  void jumpHostsSurviveAJsonRoundTripWithEncryptedSecrets() throws Exception {
    DatabaseMeta databaseMeta = connectionWithTwoJumpHosts();

    JSONObject json = parser.getJsonObject(databaseMeta);
    JSONObject rdbms = (JSONObject) ((JSONObject) json.get("rdbms")).get("NONE");
    JSONArray jumpHosts = (JSONArray) rdbms.get("sshTunnelJumpHosts");
    assertNotNull(jumpHosts, json.toJSONString());
    assertEquals(2, jumpHosts.size());

    JSONObject first = (JSONObject) jumpHosts.get(0);
    assertEquals("bastion1.example.com", first.get("host"));
    assertEquals("PRIVATE_KEY", first.get("authenticationMethod"));
    assertTrue(((String) first.get("passphrase")).startsWith("Encrypted "), first.toJSONString());
    JSONObject second = (JSONObject) jumpHosts.get(1);
    assertEquals("PASSWORD", second.get("authenticationMethod"));
    assertTrue(((String) second.get("password")).startsWith("Encrypted "), second.toJSONString());
    assertFalse(json.toJSONString().contains("bastion-password"), "password stored in clear text");

    DatabaseMeta loaded = load(json.toJSONString());
    List<SshJumpHost> loadedJumpHosts = loaded.getSshTunnelJumpHosts();
    assertEquals(2, loadedJumpHosts.size());
    assertJumpHost(
        loadedJumpHosts.get(0),
        "bastion1.example.com",
        "2222",
        "alice",
        SshAuthenticationMethod.PRIVATE_KEY,
        null,
        "${PROJECT_HOME}/keys/bastion1",
        "key-passphrase");
    assertJumpHost(
        loadedJumpHosts.get(1),
        "${BASTION2_HOST}",
        "22",
        "bob",
        SshAuthenticationMethod.PASSWORD,
        "bastion-password",
        null,
        null);
    assertEquals("ssh.internal", loaded.getSshTunnelHost());
  }

  @Test
  void connectionSavedBeforeJumpHostsLoadsWithoutAny() throws Exception {
    String json =
        "{ \"name\": \"old\", \"rdbms\": { \"NONE\": {"
            + " \"pluginId\": \"NONE\", \"hostname\": \"db\","
            + " \"sshTunnelEnabled\": true, \"sshTunnelHost\": \"ssh\","
            + " \"sshTunnelPort\": \"22\", \"sshTunnelUsername\": \"hop\" } } }";

    DatabaseMeta loaded = load(json);

    assertTrue(loaded.isSshTunnelEnabled());
    assertEquals("ssh", loaded.getSshTunnelHost());
    assertNotNull(loaded.getSshTunnelJumpHosts());
    assertTrue(loaded.getSshTunnelJumpHosts().isEmpty());
  }

  @Test
  void cloneCopiesTheJumpHosts() {
    DatabaseMeta databaseMeta = connectionWithTwoJumpHosts();

    DatabaseMeta copy = (DatabaseMeta) databaseMeta.clone();
    copy.getSshTunnelJumpHosts().get(0).setHost("changed");
    copy.getSshTunnelJumpHosts().remove(1);

    assertEquals(2, databaseMeta.getSshTunnelJumpHosts().size());
    assertEquals("bastion1.example.com", databaseMeta.getSshTunnelJumpHosts().get(0).getHost());
    assertNotSame(databaseMeta.getSshTunnelJumpHosts().get(0), copy.getSshTunnelJumpHosts().get(0));
  }

  private DatabaseMeta connectionWithTwoJumpHosts() {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setName("behind-bastions");
    databaseMeta.setSshTunnelEnabled(true);
    databaseMeta.setSshTunnelHost("ssh.internal");
    databaseMeta.setSshTunnelPort("22");
    databaseMeta.setSshTunnelUsername("hop");
    databaseMeta
        .getSshTunnelJumpHosts()
        .add(
            new SshJumpHost(
                "bastion1.example.com",
                "2222",
                "alice",
                SshAuthenticationMethod.PRIVATE_KEY,
                null,
                "${PROJECT_HOME}/keys/bastion1",
                "key-passphrase"));
    databaseMeta
        .getSshTunnelJumpHosts()
        .add(
            new SshJumpHost(
                "${BASTION2_HOST}",
                "22",
                "bob",
                SshAuthenticationMethod.PASSWORD,
                "bastion-password",
                null,
                null));
    return databaseMeta;
  }

  private DatabaseMeta load(String json) throws Exception {
    try (JsonParser jsonParser = new JsonFactory().createParser(json)) {
      jsonParser.nextToken();
      return parser.loadJsonObject(DatabaseMeta.class, jsonParser);
    }
  }

  private static void assertJumpHost(
      SshJumpHost jumpHost,
      String host,
      String port,
      String username,
      SshAuthenticationMethod method,
      String password,
      String privateKeyFile,
      String passphrase) {
    assertEquals(host, jumpHost.getHost());
    assertEquals(port, jumpHost.getPort());
    assertEquals(username, jumpHost.getUsername());
    assertEquals(method, jumpHost.getAuthenticationMethod());
    assertEquals(password, jumpHost.getPassword());
    assertEquals(privateKeyFile, jumpHost.getPrivateKeyFile());
    assertEquals(passphrase, jumpHost.getPassphrase());
  }
}
