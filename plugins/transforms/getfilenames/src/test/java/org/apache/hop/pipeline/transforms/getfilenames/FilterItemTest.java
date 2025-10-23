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

package org.apache.hop.pipeline.transforms.getfilenames;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Test class for FilterItem */
class FilterItemTest {

  @Test
  void testDefaultConstructor() {
    FilterItem filterItem = new FilterItem();

    assertNotNull(filterItem);
    assertNull(filterItem.getFileTypeFilterSelection());
  }

  @Test
  void testParameterizedConstructor() {
    String fileTypeFilterSelection = "FILES_ONLY";

    FilterItem filterItem = new FilterItem(fileTypeFilterSelection);

    assertEquals(fileTypeFilterSelection, filterItem.getFileTypeFilterSelection());
  }

  @Test
  void testParameterizedConstructorWithNull() {
    FilterItem filterItem = new FilterItem(null);

    assertNull(filterItem.getFileTypeFilterSelection());
  }

  @Test
  void testSettersAndGetters() {
    FilterItem filterItem = new FilterItem();

    filterItem.setFileTypeFilterSelection("FILES_AND_FOLDERS");

    assertEquals("FILES_AND_FOLDERS", filterItem.getFileTypeFilterSelection());
  }

  @Test
  void testEquals() {
    FilterItem filterItem1 = new FilterItem("FILES_ONLY");
    FilterItem filterItem2 = new FilterItem("FILES_ONLY");
    FilterItem filterItem3 = new FilterItem("FILES_AND_FOLDERS");
    FilterItem filterItem4 = new FilterItem(null);

    assertEquals(filterItem1, filterItem2);
    assertNotEquals(filterItem1, filterItem3);
    assertNotEquals(filterItem1, filterItem4);
    assertNotEquals(null, filterItem1);
    assertNotEquals("not a FilterItem", filterItem1);
  }

  @Test
  void testEqualsWithNullValues() {
    FilterItem filterItem1 = new FilterItem(null);
    FilterItem filterItem2 = new FilterItem(null);
    FilterItem filterItem3 = new FilterItem("FILES_ONLY");

    assertEquals(filterItem1, filterItem2);
    assertNotEquals(filterItem1, filterItem3);
  }

  @Test
  void testHashCode() {
    FilterItem filterItem1 = new FilterItem("FILES_ONLY");
    FilterItem filterItem2 = new FilterItem("FILES_ONLY");
    FilterItem filterItem3 = new FilterItem("FILES_AND_FOLDERS");

    assertEquals(filterItem1.hashCode(), filterItem2.hashCode());
    assertNotEquals(filterItem1.hashCode(), filterItem3.hashCode());
  }

  @Test
  void testHashCodeWithNullValues() {
    FilterItem filterItem1 = new FilterItem(null);
    FilterItem filterItem2 = new FilterItem(null);

    assertEquals(filterItem1.hashCode(), filterItem2.hashCode());
  }

  @Test
  void testSelfEquality() {
    FilterItem filterItem = new FilterItem("FILES_ONLY");
    assertEquals(filterItem, filterItem);
  }

  @Test
  void testToString() {
    FilterItem filterItem = new FilterItem("FILES_ONLY");
    String result = filterItem.toString();
    assertNotNull(result);
    assertTrue(result.contains("FilterItem"));
  }

  @Test
  void testEmptyString() {
    FilterItem filterItem = new FilterItem("");

    assertEquals("", filterItem.getFileTypeFilterSelection());
    assertEquals(new FilterItem(""), filterItem);
    assertEquals(filterItem.hashCode(), new FilterItem("").hashCode());
  }
}
