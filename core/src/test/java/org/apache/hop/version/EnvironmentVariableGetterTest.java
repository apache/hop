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

package org.apache.hop.version;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.exception.HopRuntimeException;
import org.junit.jupiter.api.Test;

class EnvironmentVariableGetterTest {

  @Test
  void getEnvVariable_returnsPathOnTypicalSystems() throws Exception {
    EnvironmentVariableGetter getter = new EnvironmentVariableGetter();
    String path = getter.getEnvVarible("PATH");
    assertNotNull(path);
    assertFalse(path.isEmpty());
  }

  @Test
  void getEnvVariable_throwsWhenUndefined() {
    EnvironmentVariableGetter getter = new EnvironmentVariableGetter();
    HopRuntimeException ex =
        assertThrows(
            HopRuntimeException.class,
            () -> getter.getEnvVarible("HOP_TEST_ENV_VAR_SHOULD_NOT_EXIST_9f3a2c1b"));
    assertTrue(ex.getMessage().contains("undefined"));
    assertTrue(ex.getMessage().contains("HOP_TEST_ENV_VAR_SHOULD_NOT_EXIST_9f3a2c1b"));
  }
}
