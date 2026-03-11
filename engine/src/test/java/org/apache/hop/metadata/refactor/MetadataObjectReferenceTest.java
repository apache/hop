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

package org.apache.hop.metadata.refactor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

class MetadataObjectReferenceTest {

  @Test
  void testGetters() {
    MetadataObjectReference ref =
        new MetadataObjectReference("pipeline-run-configuration", "Local");
    assertEquals("pipeline-run-configuration", ref.getContainerMetadataKey());
    assertEquals("Local", ref.getContainerObjectName());
  }

  @Test
  void testEqualsSameValues() {
    MetadataObjectReference a = new MetadataObjectReference("rdbms", "MyDb");
    MetadataObjectReference b = new MetadataObjectReference("rdbms", "MyDb");
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  void testEqualsDifferentKey() {
    MetadataObjectReference a = new MetadataObjectReference("rdbms", "MyDb");
    MetadataObjectReference b = new MetadataObjectReference("other", "MyDb");
    assertNotEquals(a, b);
  }

  @Test
  void testEqualsDifferentName() {
    MetadataObjectReference a = new MetadataObjectReference("rdbms", "MyDb");
    MetadataObjectReference b = new MetadataObjectReference("rdbms", "OtherDb");
    assertNotEquals(a, b);
  }

  @Test
  void testEqualsNull() {
    MetadataObjectReference a = new MetadataObjectReference("rdbms", "MyDb");
    assertNotEquals(a, null);
  }

  @Test
  void testEqualsSelf() {
    MetadataObjectReference a = new MetadataObjectReference("rdbms", "MyDb");
    assertEquals(a, a);
  }
}
