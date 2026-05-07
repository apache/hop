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

package org.apache.hop.mongo.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class MongoDbConnectionTypeTest {

  @Test
  void testGetNames() {
    String[] names = MongoDbConnectionType.getNames();
    assertNotNull(names);
    assertEquals(2, names.length);
    assertEquals("STANDARD", names[0]);
    assertEquals("SRV", names[1]);
  }

  @Test
  void testGetConnectionTypeValid() {
    // getConnectionType is an instance method, use any enum value to call it
    MongoDbConnectionType type = MongoDbConnectionType.STANDARD;
    assertEquals(MongoDbConnectionType.STANDARD, type.getConnectionType("STANDARD"));
    assertEquals(MongoDbConnectionType.SRV, type.getConnectionType("SRV"));
  }

  @Test
  void testGetConnectionTypeInvalid() {
    // Should default to STANDARD for invalid values
    MongoDbConnectionType type = MongoDbConnectionType.STANDARD;
    assertEquals(MongoDbConnectionType.STANDARD, type.getConnectionType("INVALID"));
    // null should return STANDARD
    assertEquals(MongoDbConnectionType.STANDARD, type.getConnectionType(null));
  }

  @Test
  void testValues() {
    MongoDbConnectionType[] values = MongoDbConnectionType.values();
    assertEquals(2, values.length);
    assertEquals(MongoDbConnectionType.STANDARD, values[0]);
    assertEquals(MongoDbConnectionType.SRV, values[1]);
  }
}
