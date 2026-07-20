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

package org.apache.hop.pipeline.transforms.databaselookup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.row.RowMeta;
import org.junit.jupiter.api.Test;

class DatabaseLookupDataTest {

  @Test
  void defaultConstructor_InitializesNullRuntimeState() {
    DatabaseLookupData data = new DatabaseLookupData();

    assertNull(data.db);
    assertNull(data.cache);
    assertNull(data.nullif);
    assertNull(data.keynrs);
    assertNull(data.keynrs2);
    assertNull(data.keytypes);
    assertNull(data.outputRowMeta);
    assertNull(data.lookupMeta);
    assertNull(data.returnMeta);
    assertNull(data.dbRowMeta);
    assertNull(data.conditions);
    assertNull(data.returnValueTypes);
    assertNull(data.returnTrimTypes);
    assertNull(data.trimIndexes);
    assertFalse(data.isCanceled);
    assertFalse(data.allEquals);
    assertFalse(data.hasDBCondition);
  }

  @Test
  void fields_CanBeAssignedAndRead() {
    DatabaseLookupData data = new DatabaseLookupData();
    data.dbRowMeta = new RowMeta();
    data.lookupMeta = new RowMeta();
    data.returnMeta = new RowMeta();
    data.outputRowMeta = new RowMeta();
    data.keynrs = new int[] {0};
    data.keynrs2 = new int[] {-1};
    data.keytypes = new int[] {1};
    data.conditions = new int[] {DatabaseLookupMeta.CONDITION_EQ};
    data.returnValueTypes = new int[] {2};
    data.returnTrimTypes = new String[] {"none"};
    data.nullif = new Object[] {"N/A"};
    data.cache = DefaultCache.newCache(data, 8);
    data.allEquals = true;
    data.hasDBCondition = true;
    data.isCanceled = true;

    assertTrue(data.allEquals);
    assertTrue(data.hasDBCondition);
    assertTrue(data.isCanceled);
    assertEquals(0, data.keynrs[0]);
    assertEquals(DatabaseLookupMeta.CONDITION_EQ, data.conditions[0]);
    assertInstanceOf(DefaultCache.class, data.cache);
  }
}
