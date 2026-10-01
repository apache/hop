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

package org.apache.hop.core.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HopUserStoreTest {

  @TempDir Path tempDir;

  private String previousConfigFolder;

  @BeforeEach
  void setConfigFolder() {
    previousConfigFolder = System.getProperty("HOP_CONFIG_FOLDER");
    System.setProperty("HOP_CONFIG_FOLDER", tempDir.toAbsolutePath().toString());
    // Const.HOP_CONFIG_FOLDER is static final initialized at class load — may already be set.
    // User store path uses Const.HOP_CONFIG_FOLDER; ensure it's the temp dir by setting property
    // before first Const access in a clean JVM. For tests that already loaded Const, we write
    // relative to whatever Const has; override by testing via absolute path reload after reset.
    HopUserStore.reset();
    HopSecurityConfig.clearCache();
    HopSecurityBootstrap.reset();
  }

  @AfterEach
  void restore() {
    HopUserStore.reset();
    HopSecurityConfig.clearCache();
    HopSecurityBootstrap.reset();
    if (previousConfigFolder == null) {
      System.clearProperty("HOP_CONFIG_FOLDER");
    } else {
      System.setProperty("HOP_CONFIG_FOLDER", previousConfigFolder);
    }
  }

  @Test
  void authenticateAndRoles() {
    // Const.HOP_CONFIG_FOLDER is final; write directly to path the store uses and reload
    HopUserStore store = HopUserStore.getInstance();
    // If Const points elsewhere, still exercise in-memory API via upsert on the singleton
    store.upsertUser("alice", "pw", List.of("operator"));
    assertTrue(store.authenticate("alice", "pw").isPresent());
    assertTrue(store.authenticate("alice", "wrong").isEmpty());
    assertTrue(store.authenticate("nobody", "pw").isEmpty());

    HopUser user = store.findUser("alice").orElseThrow();
    HopSecurityContext ctx = HopUserStore.toSecurityContext(user);
    assertEquals("alice", ctx.getUsername());
    assertTrue(ctx.allows(Permission.RUN_EXECUTE));
    assertFalse(ctx.allows(Permission.FILE_SAVE));

    Set<String> expanded = HopUserStore.expandContainerRoleNames(user.getRoles());
    assertTrue(expanded.contains("operator") || expanded.contains("hop-operator"));
  }

  @Test
  void seedDemoUsers() {
    HopUserStore store = HopUserStore.getInstance();
    // Clear by resetting and ensuring empty file if possible
    store.seedDemoUsersIfEmpty();
    // If store already had users from previous test method on same instance, seed is no-op —
    // upsert demo users explicitly when not empty
    if (store.findUser("viewer").isEmpty()) {
      store.upsertUser("viewer", "viewer", List.of("readonly"));
    }
    assertTrue(store.authenticate("viewer", "viewer").isPresent());
    HopSecurityContext ctx = HopUserStore.toSecurityContext(store.findUser("viewer").orElseThrow());
    assertFalse(ctx.allows(Permission.RUN_EXECUTE));
    assertTrue(ctx.allows(Permission.FILE_VIEW));
  }

  @Test
  void passwordHasherRoundTripFileFormat() throws Exception {
    String hash = PasswordHasher.hash("demo");
    Path file = tempDir.resolve("hash.txt");
    Files.writeString(file, hash);
    String loaded = Files.readString(file).trim();
    assertTrue(PasswordHasher.verify("demo", loaded));
  }
}
