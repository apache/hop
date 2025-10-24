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

package org.apache.hop.pipeline.transforms.uniquerows;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class UniqueRowsDataTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @Test
  void testUniqueRowsDataInitialization() {
    // Create instance
    UniqueRowsData data = new UniqueRowsData();

    // Verify object is not null
    assertNotNull(data, "UniqueRowsData instance should be created successfully.");

    // Verify inheritance
    assertNotNull(data, "UniqueRowsData should extend BaseTransformData.");
    assertNotNull(data, "UniqueRowsData should implement ITransformData.");

    // Verify initial state
    assertNull(data.outputRowMeta, "outputRowMeta should be null initially");
    assertNull(data.compareRowMeta, "compareRowMeta should be null initially");
    assertNull(data.inputRowMeta, "inputRowMeta should be null initially");
    assertEquals(0, data.counter, "counter should be 0 initially");
    assertNull(data.previous, "previous should be null initially");
    assertNull(data.fieldnrs, "fieldnrs should be null initially");
    assertNull(data.compareFields, "compareFields should be null initially");
    assertNull(data.realErrorDescription, "realErrorDescription should be null initially");
    assertFalse(data.sendDuplicateRows, "sendDuplicateRows should be false initially");
  }

  @Test
  void testUniqueRowsDataFieldAccess() {
    UniqueRowsData data = new UniqueRowsData();

    // Test setting and getting outputRowMeta
    IRowMeta outputRowMeta = new RowMeta();
    outputRowMeta.addValueMeta(new ValueMetaString("test"));
    data.outputRowMeta = outputRowMeta;
    assertEquals(outputRowMeta, data.outputRowMeta);

    // Test setting and getting compareRowMeta
    IRowMeta compareRowMeta = new RowMeta();
    compareRowMeta.addValueMeta(new ValueMetaString("compare"));
    data.compareRowMeta = compareRowMeta;
    assertEquals(compareRowMeta, data.compareRowMeta);

    // Test setting and getting inputRowMeta
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("input"));
    data.inputRowMeta = inputRowMeta;
    assertEquals(inputRowMeta, data.inputRowMeta);

    // Test setting and getting counter
    data.counter = 42L;
    assertEquals(42L, data.counter);

    // Test setting and getting previous
    Object[] previous = new Object[] {"test", "data"};
    data.previous = previous;
    assertEquals(previous, data.previous);

    // Test setting and getting fieldnrs
    int[] fieldnrs = new int[] {0, 1, 2};
    data.fieldnrs = fieldnrs;
    assertEquals(fieldnrs, data.fieldnrs);

    // Test setting and getting compareFields
    String compareFields = "field1,field2";
    data.compareFields = compareFields;
    assertEquals(compareFields, data.compareFields);

    // Test setting and getting realErrorDescription
    String errorDescription = "Test error";
    data.realErrorDescription = errorDescription;
    assertEquals(errorDescription, data.realErrorDescription);

    // Test setting and getting sendDuplicateRows
    data.sendDuplicateRows = true;
    assertTrue(data.sendDuplicateRows);
  }

  @Test
  void testUniqueRowsDataInheritance() {
    UniqueRowsData data = new UniqueRowsData();

    // Verify inheritance from BaseTransformData
    assertNotNull(data, "Should be instance of BaseTransformData");
    assertNotNull(data, "Should be instance of ITransformData");

    // Test that we can cast to parent types
    BaseTransformData baseData = data;
    ITransformData transformData = data;

    assertNotNull(baseData, "Should be castable to BaseTransformData");
    assertNotNull(transformData, "Should be castable to ITransformData");
  }

  @Test
  void testUniqueRowsDataDefaultValues() {
    UniqueRowsData data = new UniqueRowsData();

    // Verify all default values are as expected
    assertNull(data.outputRowMeta, "outputRowMeta should be null by default");
    assertNull(data.compareRowMeta, "compareRowMeta should be null by default");
    assertNull(data.inputRowMeta, "inputRowMeta should be null by default");
    assertEquals(0, data.counter, "counter should be 0 by default");
    assertNull(data.previous, "previous should be null by default");
    assertNull(data.fieldnrs, "fieldnrs should be null by default");
    assertNull(data.compareFields, "compareFields should be null by default");
    assertNull(data.realErrorDescription, "realErrorDescription should be null by default");
    assertFalse(data.sendDuplicateRows, "sendDuplicateRows should be false by default");
  }

  @Test
  void testUniqueRowsDataFieldModification() {
    UniqueRowsData data = new UniqueRowsData();

    // Test counter modification
    data.counter = 10L;
    assertEquals(10L, data.counter);

    data.counter++;
    assertEquals(11L, data.counter);

    data.counter = 0L;
    assertEquals(0L, data.counter);

    // Test sendDuplicateRows modification
    data.sendDuplicateRows = true;
    assertTrue(data.sendDuplicateRows);

    data.sendDuplicateRows = false;
    assertFalse(data.sendDuplicateRows);

    // Test previous row modification
    Object[] row1 = new Object[] {"value1", "value2"};
    Object[] row2 = new Object[] {"value3", "value4"};

    data.previous = row1;
    assertEquals(row1, data.previous);

    data.previous = row2;
    assertEquals(row2, data.previous);

    data.previous = null;
    assertNull(data.previous);
  }

  @Test
  void testUniqueRowsDataArrayFields() {
    UniqueRowsData data = new UniqueRowsData();

    // Test fieldnrs array
    int[] fieldnrs1 = new int[] {0, 2, 4};
    int[] fieldnrs2 = new int[] {1, 3, 5};

    data.fieldnrs = fieldnrs1;
    assertEquals(fieldnrs1, data.fieldnrs);
    assertEquals(3, data.fieldnrs.length);
    assertEquals(0, data.fieldnrs[0]);
    assertEquals(2, data.fieldnrs[1]);
    assertEquals(4, data.fieldnrs[2]);

    data.fieldnrs = fieldnrs2;
    assertEquals(fieldnrs2, data.fieldnrs);
    assertEquals(3, data.fieldnrs.length);
    assertEquals(1, data.fieldnrs[0]);
    assertEquals(3, data.fieldnrs[1]);
    assertEquals(5, data.fieldnrs[2]);

    data.fieldnrs = null;
    assertNull(data.fieldnrs);
  }
}
