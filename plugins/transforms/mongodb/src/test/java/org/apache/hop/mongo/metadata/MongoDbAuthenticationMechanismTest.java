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

class MongoDbAuthenticationMechanismTest {

  @Test
  void testGetNames() {
    String[] names = MongoDbAuthenticationMechanism.getNames();
    assertNotNull(names);
    assertEquals(4, names.length);
    assertEquals("SCRAM_SHA_1", names[0]);
    assertEquals("SCRAM_SHA_256", names[1]);
    assertEquals("MONGODB_CR", names[2]);
    assertEquals("PLAIN", names[3]);
  }

  @Test
  void testGetMechanismValid() {
    // getMechanism is an instance method, use any enum value to call it
    MongoDbAuthenticationMechanism mechanism = MongoDbAuthenticationMechanism.PLAIN;
    assertEquals(MongoDbAuthenticationMechanism.SCRAM_SHA_1, mechanism.getMechanism("SCRAM_SHA_1"));
    assertEquals(
        MongoDbAuthenticationMechanism.SCRAM_SHA_256, mechanism.getMechanism("SCRAM_SHA_256"));
    assertEquals(MongoDbAuthenticationMechanism.MONGODB_CR, mechanism.getMechanism("MONGODB_CR"));
    assertEquals(MongoDbAuthenticationMechanism.PLAIN, mechanism.getMechanism("PLAIN"));
  }

  @Test
  void testGetMechanismInvalid() {
    // Should default to PLAIN for invalid values
    MongoDbAuthenticationMechanism mechanism = MongoDbAuthenticationMechanism.PLAIN;
    assertEquals(MongoDbAuthenticationMechanism.PLAIN, mechanism.getMechanism("INVALID"));
    // null should return PLAIN
    assertEquals(MongoDbAuthenticationMechanism.PLAIN, mechanism.getMechanism(null));
  }

  @Test
  void testValues() {
    MongoDbAuthenticationMechanism[] values = MongoDbAuthenticationMechanism.values();
    assertEquals(4, values.length);
    assertEquals(MongoDbAuthenticationMechanism.SCRAM_SHA_1, values[0]);
    assertEquals(MongoDbAuthenticationMechanism.SCRAM_SHA_256, values[1]);
    assertEquals(MongoDbAuthenticationMechanism.MONGODB_CR, values[2]);
    assertEquals(MongoDbAuthenticationMechanism.PLAIN, values[3]);
  }
}
