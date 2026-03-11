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

class MetadataReferenceResultTest {

  @Test
  void testGetters() {
    MetadataReferenceResult result = new MetadataReferenceResult("/path/to/pipeline.hpl", 3);
    assertEquals("/path/to/pipeline.hpl", result.getFilePath());
    assertEquals(3, result.getReferenceCount());
  }

  @Test
  void testEqualsSameValues() {
    MetadataReferenceResult a = new MetadataReferenceResult("/a.hpl", 2);
    MetadataReferenceResult b = new MetadataReferenceResult("/a.hpl", 2);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  void testEqualsDifferentPath() {
    MetadataReferenceResult a = new MetadataReferenceResult("/a.hpl", 2);
    MetadataReferenceResult b = new MetadataReferenceResult("/b.hpl", 2);
    assertNotEquals(a, b);
  }

  @Test
  void testEqualsDifferentCount() {
    MetadataReferenceResult a = new MetadataReferenceResult("/a.hpl", 1);
    MetadataReferenceResult b = new MetadataReferenceResult("/a.hpl", 2);
    assertNotEquals(a, b);
  }

  @Test
  void testEqualsNull() {
    MetadataReferenceResult a = new MetadataReferenceResult("/a.hpl", 1);
    assertNotEquals(a, null);
  }

  @Test
  void testEqualsSelf() {
    MetadataReferenceResult a = new MetadataReferenceResult("/a.hpl", 1);
    assertEquals(a, a);
  }
}
