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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.Test;

class LookupTest {

  @Test
  void defaultConstructor_StartsEmpty() {
    Lookup lookup = new Lookup();
    assertTrue(lookup.getKeyFields().isEmpty());
    assertTrue(lookup.getReturnValues().isEmpty());
    assertFalse(lookup.isFailingOnMultipleResults());
    assertFalse(lookup.isEatingRowOnLookupFailure());
  }

  @Test
  void settersAndGetters() {
    Lookup lookup = new Lookup();
    lookup.setSchemaName("public");
    lookup.setTableName("users");
    lookup.setOrderByClause("id DESC");
    lookup.setFailingOnMultipleResults(true);
    lookup.setEatingRowOnLookupFailure(true);

    assertEquals("public", lookup.getSchemaName());
    assertEquals("users", lookup.getTableName());
    assertEquals("id DESC", lookup.getOrderByClause());
    assertTrue(lookup.isFailingOnMultipleResults());
    assertTrue(lookup.isEatingRowOnLookupFailure());
  }

  @Test
  void clone_DeepCopiesKeysAndReturns() {
    Lookup original = new Lookup();
    original.setSchemaName("public");
    original.setTableName("users");
    original.setOrderByClause("id DESC");
    original.setFailingOnMultipleResults(true);
    original.setEatingRowOnLookupFailure(true);
    original.getKeyFields().add(new KeyField("in_id", "", "=", "id"));
    original
        .getReturnValues()
        .add(
            new ReturnValue(
                "name",
                "user_name",
                "",
                "String",
                ValueMetaString.getTrimTypeCode(IValueMeta.TRIM_TYPE_NONE)));

    Lookup clone = original.clone();

    assertNotSame(original, clone);
    assertNotSame(original.getKeyFields(), clone.getKeyFields());
    assertNotSame(original.getReturnValues(), clone.getReturnValues());
    assertEquals("public", clone.getSchemaName());
    assertEquals("users", clone.getTableName());
    assertEquals("id DESC", clone.getOrderByClause());
    assertTrue(clone.isFailingOnMultipleResults());
    assertTrue(clone.isEatingRowOnLookupFailure());
    assertEquals(1, clone.getKeyFields().size());
    assertEquals("in_id", clone.getKeyFields().get(0).getStreamField1());
    assertEquals(1, clone.getReturnValues().size());
    assertEquals("user_name", clone.getReturnValues().get(0).getNewName());

    clone.getKeyFields().get(0).setStreamField1("changed");
    clone.getReturnValues().get(0).setNewName("changed");
    assertEquals("in_id", original.getKeyFields().get(0).getStreamField1());
    assertEquals("user_name", original.getReturnValues().get(0).getNewName());
  }
}
