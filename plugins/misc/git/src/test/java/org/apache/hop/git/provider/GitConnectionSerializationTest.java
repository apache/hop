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

package org.apache.hop.git.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GitConnectionSerializationTest {

  private JsonMetadataProvider metadataProvider;

  @BeforeEach
  void setUp() {
    String baseFolder =
        System.getProperty("java.io.tmpdir")
            + Const.FILE_SEPARATOR
            + "gitconnection-serialization-test";
    metadataProvider =
        new JsonMetadataProvider(
            new HopTwoWayPasswordEncoder(), baseFolder, Variables.getADefaultVariableSpace());
  }

  @Test
  void roundTripGithubConnection() throws HopException {
    GitConnection connection = new GitConnection();
    connection.setName("github");
    connection.setGitProvider(GitProvider.GITHUB_CLOUD);
    connection.setToken("${GIT_TOKEN}");

    IHopMetadataSerializer<GitConnection> serializer =
        metadataProvider.getSerializer(GitConnection.class);
    serializer.save(connection);

    assertTrue(serializer.exists("github"));

    GitConnection loaded = serializer.load("github");
    assertEquals("github", loaded.getName());
    assertEquals(GitProvider.GITHUB_CLOUD, loaded.getGitProvider());
    assertEquals("${GIT_TOKEN}", loaded.getToken());
    assertNull(loaded.getPassword());
    assertNull(loaded.getUsername());
  }

  @Test
  void roundTripBitbucketConnection() throws HopException {
    GitConnection connection = new GitConnection();
    connection.setName("bitbucket");
    connection.setGitProvider(GitProvider.BITBUCKET);
    connection.setUsername("user");
    connection.setPassword("secret");

    IHopMetadataSerializer<GitConnection> serializer =
        metadataProvider.getSerializer(GitConnection.class);
    serializer.save(connection);

    GitConnection loaded = serializer.load("bitbucket");
    assertEquals(GitProvider.BITBUCKET, loaded.getGitProvider());
    assertEquals("user", loaded.getUsername());
    assertEquals("secret", loaded.getPassword());
    assertNull(loaded.getToken());
  }
}
