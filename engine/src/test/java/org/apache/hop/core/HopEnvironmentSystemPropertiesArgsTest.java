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

package org.apache.hop.core;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class HopEnvironmentSystemPropertiesArgsTest {

  @Test
  void argumentsWithoutSystemProperties() {
    assertFalse(HopEnvironment.maySetSystemProperties(new String[0]));
    assertFalse(
        HopEnvironment.maySetSystemProperties(
            new String[] {"-j", "project", "-r", "local", "-f", "/a/sales/pipeline.hpl"}));
    assertFalse(
        HopEnvironment.maySetSystemProperties(new String[] {"--level=Basic", "--file=x.hpl"}));
  }

  @Test
  void argumentsThatMaySetSystemProperties() {
    assertTrue(HopEnvironment.maySetSystemProperties(new String[] {"-s", "A=1"}));
    assertTrue(HopEnvironment.maySetSystemProperties(new String[] {"-sA=1,B=2"}));
    assertTrue(HopEnvironment.maySetSystemProperties(new String[] {"--system-properties=A=1"}));
    assertTrue(HopEnvironment.maySetSystemProperties(new String[] {"-f", "x.hpl", "@options.txt"}));
  }
}
