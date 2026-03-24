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

package org.apache.hop.www.jaxrs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class NVPairTest {

  @Test
  void defaultConstructor() {
    NVPair p = new NVPair();
    assertNull(p.getName());
    assertNull(p.getValue());
  }

  @Test
  void valueConstructorAndSetters() {
    NVPair p = new NVPair("k", "v");
    assertEquals("k", p.getName());
    assertEquals("v", p.getValue());
    p.setName("k2");
    p.setValue("v2");
    assertEquals("k2", p.getName());
    assertEquals("v2", p.getValue());
  }
}
