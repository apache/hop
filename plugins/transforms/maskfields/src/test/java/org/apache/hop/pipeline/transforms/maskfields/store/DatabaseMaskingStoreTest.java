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

package org.apache.hop.pipeline.transforms.maskfields.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transforms.maskfields.MaskingKey;
import org.apache.hop.pipeline.transforms.maskfields.MaskingRuntime;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DatabaseMaskingStoreTest {

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void aSecondStoreSeesTheMappingAndContinuesTheSequence() throws Exception {
    String databaseName =
        "mem:mask" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
    DatabaseMeta databaseMeta =
        new DatabaseMeta("mask-h2", "H2", "Native", "", databaseName, "", "sa", "");
    Variables variables = new Variables();

    DatabaseMaskingStore first =
        new DatabaseMaskingStore(
            (ILoggingObject) null, variables, databaseMeta, (String) null, "mask_map");
    first.open();
    try {
      assertEquals("first-name-1", allocate(first, "Matt"));
      assertEquals("first-name-1", allocate(first, "Matt"));
      assertEquals("first-name-2", allocate(first, "Ann"));
    } finally {
      first.close();
    }

    DatabaseMaskingStore second =
        new DatabaseMaskingStore(
            (ILoggingObject) null, variables, databaseMeta, (String) null, "mask_map");
    second.open();
    try {
      assertEquals("first-name-1", allocate(second, "Matt"));
      assertEquals("first-name-3", allocate(second, "Jo"));
    } finally {
      second.close();
    }
  }

  @Test
  void sequenceStartingAtZeroForList() throws Exception {
    String databaseName =
        "mem:mask" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
    DatabaseMeta databaseMeta =
        new DatabaseMeta("mask-h2", "H2", "Native", "", databaseName, "", "sa", "");
    Variables variables = new Variables();

    DatabaseMaskingStore store =
        new DatabaseMaskingStore(
            (ILoggingObject) null, variables, databaseMeta, (String) null, "mask_list_map");
    store.open();
    try {
      assertEquals(0L, store.allocateSequence("Country", 0));
      assertEquals(1L, store.allocateSequence("Country", 0));
      assertEquals(2L, store.allocateSequence("Country", 0));
    } finally {
      store.close();
    }
  }

  @Test
  void runtimeSharesOneDatabaseStoreAcrossCopies() throws Exception {
    String id = UUID.randomUUID().toString().replace("-", "");
    String databaseName = "mem:mask" + id + ";DB_CLOSE_DELAY=-1";
    DatabaseMeta databaseMeta =
        new DatabaseMeta("mask-" + id, "H2", "Native", "", databaseName, "", "sa", "");
    Variables variables = new Variables();
    MaskingRuntime runtime = MaskingRuntime.getInstance();
    MaskingRuntime.Lease first = runtime.acquire("db-" + id);
    MaskingRuntime.Lease second = runtime.acquire("db-" + id);
    try {
      DatabaseMaskingStore firstStore =
          first.database((ILoggingObject) null, variables, databaseMeta, null, "mask_map");
      DatabaseMaskingStore secondStore =
          second.database((ILoggingObject) null, variables, databaseMeta, null, "mask_map");
      assertSame(firstStore, secondStore);
      assertEquals("first-name-1", allocate(firstStore, "Matt"));
      assertEquals("first-name-1", allocate(secondStore, "Matt"));
      assertEquals("first-name-2", allocate(secondStore, "Ann"));
    } finally {
      first.release();
      second.release();
    }

    MaskingRuntime.Lease again = runtime.acquire("db-" + id + "-again");
    try {
      DatabaseMaskingStore store =
          again.database((ILoggingObject) null, variables, databaseMeta, null, "mask_map");
      assertEquals("first-name-1", allocate(store, "Matt"));
      assertEquals("first-name-3", allocate(store, "Jo"));
    } finally {
      again.release();
    }
  }

  @Test
  void twoProcessesNeverHandOutTheSameValue() throws Exception {
    DatabaseMeta databaseMeta = h2();
    Variables variables = new Variables();
    // Two stores on one table stand in for two Hop processes: no shared lock, two connections.
    DatabaseMaskingStore first =
        new DatabaseMaskingStore((ILoggingObject) null, variables, databaseMeta, null, "mask_map");
    DatabaseMaskingStore second =
        new DatabaseMaskingStore((ILoggingObject) null, variables, databaseMeta, null, "mask_map");
    first.open();
    second.open();
    int perStore = 200;
    // The first value creates the sequence row. Both processes then race on updating it.
    Set<String> masked = new HashSet<>(List.of(allocate(first, "seed")));
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<List<String>> a = executor.submit(() -> allocateAll(first, "a", perStore));
      Future<List<String>> b = executor.submit(() -> allocateAll(second, "b", perStore));
      masked.addAll(a.get(60, TimeUnit.SECONDS));
      masked.addAll(b.get(60, TimeUnit.SECONDS));
      assertEquals(2 * perStore + 1, masked.size());
    } finally {
      executor.shutdownNow();
      first.close();
      second.close();
    }
  }

  @Test
  void aHashSecretKeepsOriginalValuesOutOfTheTable() throws Exception {
    DatabaseMeta databaseMeta = h2();
    Variables variables = new Variables();
    MaskingKey key = new MaskingKey(false, false, "s3cret");
    DatabaseMaskingStore store =
        new DatabaseMaskingStore((ILoggingObject) null, variables, databaseMeta, null, "mask_map");
    store.open();
    try {
      String hashed = key.storeKey("Matt");
      assertTrue(hashed.startsWith("hmac-sha256:"));
      assertEquals("first-name-1", allocate(store, hashed, "Matt"));
      assertEquals("first-name-1", allocate(store, hashed, "Matt"));
      assertEquals(List.of(hashed), sourceKeys(databaseMeta, variables));
    } finally {
      store.close();
    }
  }

  @Test
  void aPlainTextRowMovesToItsHashedKey() throws Exception {
    DatabaseMeta databaseMeta = h2();
    Variables variables = new Variables();
    DatabaseMaskingStore store =
        new DatabaseMaskingStore((ILoggingObject) null, variables, databaseMeta, null, "mask_map");
    store.open();
    try {
      // A row written before the pattern had a hash secret.
      assertEquals("first-name-1", allocate(store, "Matt"));
    } finally {
      store.close();
    }

    String hashed = new MaskingKey(false, false, "s3cret").storeKey("Matt");
    DatabaseMaskingStore later =
        new DatabaseMaskingStore((ILoggingObject) null, variables, databaseMeta, null, "mask_map");
    later.open();
    try {
      assertEquals("first-name-1", allocate(later, hashed, "Matt"));
      assertEquals("first-name-2", allocate(later, hashed + "x", "Ann"));
    } finally {
      later.close();
    }
    List<String> keys = sourceKeys(databaseMeta, variables);
    assertTrue(keys.contains(hashed));
    assertFalse(keys.contains("Matt"));
  }

  @Test
  void connectionsWithTheSameNameOnDifferentDatabasesGetTheirOwnStore() throws Exception {
    Variables variables = new Variables();
    DatabaseMeta one = h2();
    DatabaseMeta two = h2();
    two.setName(one.getName());
    MaskingRuntime.Lease lease = MaskingRuntime.getInstance().acquire("db-" + UUID.randomUUID());
    try {
      DatabaseMaskingStore first = lease.database(null, variables, one, null, "mask_map");
      DatabaseMaskingStore second = lease.database(null, variables, two, null, "mask_map");
      assertNotSame(first, second);
      assertEquals("first-name-1", allocate(first, "Matt"));
      assertEquals("first-name-1", allocate(second, "Ann"));
    } finally {
      lease.release();
    }
  }

  @Test
  void anExhaustedRetryKeepsTheSourceKeyOutOfTheError() throws Exception {
    DatabaseMaskingStore store =
        new DatabaseMaskingStore((ILoggingObject) null, new Variables(), h2(), null, "mask_map");
    store.open();
    try {
      // Every new key gets the same replacement, so the unique key on it rejects every attempt.
      assertEquals("same", store.findOrCreate("First name", "Matt", current -> "same"));
      HopException e =
          assertThrows(
              HopException.class, () -> store.findOrCreate("First name", "Ann", current -> "same"));
      assertNull(e.getCause());
      assertFalse(e.getMessage().contains("Ann"), e.getMessage());
      assertTrue(e.getMessage().contains("First name"), e.getMessage());
    } finally {
      store.close();
    }
  }

  private static List<String> sourceKeys(DatabaseMeta databaseMeta, Variables variables)
      throws Exception {
    Database database = new Database((ILoggingObject) null, variables, databaseMeta);
    database.connect();
    try {
      List<String> keys = new ArrayList<>();
      for (Object[] row : database.getRows("SELECT source_key FROM mask_map", 1000)) {
        keys.add((String) row[0]);
      }
      return keys;
    } finally {
      database.disconnect();
    }
  }

  private static List<String> allocateAll(DatabaseMaskingStore store, String prefix, int count)
      throws Exception {
    List<String> masked = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      masked.add(allocate(store, prefix + i));
    }
    return masked;
  }

  private static DatabaseMeta h2() {
    String id = UUID.randomUUID().toString().replace("-", "");
    return new DatabaseMeta(
        "mask-" + id, "H2", "Native", "", "mem:mask" + id + ";DB_CLOSE_DELAY=-1", "", "sa", "");
  }

  private static String allocate(DatabaseMaskingStore store, String source) throws Exception {
    return allocate(store, source, null);
  }

  private static String allocate(DatabaseMaskingStore store, String source, String legacy)
      throws Exception {
    return store.findOrCreate(
        "First name",
        source,
        legacy == null ? null : () -> legacy,
        current -> "first-name-" + current.allocateSequence("First name", 1));
  }
}
