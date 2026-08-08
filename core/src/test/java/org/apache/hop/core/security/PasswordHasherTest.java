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

package org.apache.hop.core.security;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PasswordHasherTest {

  @Test
  void hashAndVerify() {
    String hash = PasswordHasher.hash("secret");
    assertTrue(PasswordHasher.isHashedFormat(hash));
    assertTrue(PasswordHasher.verify("secret", hash));
    assertFalse(PasswordHasher.verify("wrong", hash));
    assertFalse(PasswordHasher.verify("secret", "not-a-hash"));
  }

  @Test
  void differentSaltsProduceDifferentHashes() {
    String a = PasswordHasher.hash("same");
    String b = PasswordHasher.hash("same");
    assertNotEquals(a, b);
    assertTrue(PasswordHasher.verify("same", a));
    assertTrue(PasswordHasher.verify("same", b));
  }
}
