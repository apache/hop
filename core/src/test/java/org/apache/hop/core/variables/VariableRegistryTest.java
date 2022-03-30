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

package org.apache.hop.core.variables;

import org.apache.hop.core.Const;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.*;

public class VariableRegistryTest {

  @ClassRule public static RestoreHopEnvironment env = new RestoreHopEnvironment();
  
  @Test
  public void testInit() throws Exception {

    VariableRegistry.init();
    
    VariableRegistry registry = VariableRegistry.getInstance();
    DescribedVariable describedVariable =  registry.findDescribedVariable(Const.HOP_PASSWORD_ENCODER_PLUGIN);
    assertNotNull(describedVariable);

    boolean actual = Boolean.valueOf(describedVariable.getValue());
    assertEquals(false, actual);

    assertEquals(
        "Specifies the password encoder plugin to use by ID (Hop is the default).",
        describedVariable.getDescription());
  }
}
