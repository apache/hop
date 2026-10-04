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

import java.util.UUID;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.variables.Variables;
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

  private static String allocate(DatabaseMaskingStore store, String source) throws Exception {
    return store.findOrCreate(
        "First name", source, current -> "first-name-" + current.allocateSequence("First name", 1));
  }
}
