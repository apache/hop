/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.marketplace.gui;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class HopInstallSpecFileTypeTest {

  @Test
  void claimsWellKnownBasenamesOnly() throws Exception {
    HopInstallSpecFileType type = new HopInstallSpecFileType();
    assertTrue(type.isHandledBy("/opt/hop/hop-env.yaml", false));
    assertTrue(type.isHandledBy("hop-env.yml", false));
    assertTrue(type.isHandledBy("hop-env.json", false));
    assertTrue(type.isHandledBy("full-client-env.yaml", false));
    assertFalse(type.isHandledBy("config.yaml", false));
    assertFalse(type.isHandledBy("other.json", false));
  }

  @Test
  void yamlOrJsonHelper() {
    assertTrue(HopInstallSpecFileType.isYamlOrJson("deploy/plugins.yaml"));
    assertTrue(HopInstallSpecFileType.isYamlOrJson("x.yml"));
    assertTrue(HopInstallSpecFileType.isYamlOrJson("x.json"));
    assertFalse(HopInstallSpecFileType.isYamlOrJson("readme.txt"));
  }
}
