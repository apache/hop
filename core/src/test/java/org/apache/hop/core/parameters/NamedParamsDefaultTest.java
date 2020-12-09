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
package org.apache.hop.core.parameters;

import org.apache.hop.core.variables.Variables;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.spy;

public class NamedParamsDefaultTest {
  INamedParameters namedParams;

  @Before
  public void setUp() throws Exception {
    namedParams = spy(new NamedParameters());
  }

  @Test
  public void testParameters() throws Exception {
    assertNull(namedParams.getParameterValue("var1"));
    assertNull(namedParams.getParameterDefault("var1"));
    assertNull(namedParams.getParameterDescription("var1"));

    namedParams.setParameterValue("var1", "y");
    // Values for new parameters must be added by addParameterDefinition
    assertNull(namedParams.getParameterValue("var1"));
    assertNull(namedParams.getParameterDefault("var1"));
    assertNull(namedParams.getParameterDescription("var1"));

    namedParams.addParameterDefinition("var2", "z", "My Description");
    assertEquals("", namedParams.getParameterValue("var2"));
    assertEquals("z", namedParams.getParameterDefault("var2"));
    assertEquals("My Description", namedParams.getParameterDescription("var2"));
    namedParams.setParameterValue("var2", "y");
    assertEquals("y", namedParams.getParameterValue("var2"));
    assertEquals("z", namedParams.getParameterDefault("var2"));

    // clearParameters() just clears their values, not their presence
    namedParams.clearParameterValues();
    assertEquals("", namedParams.getParameterValue("var2"));

    // eraseParameters() clears the list of parameters
    namedParams.removeAllParameters();
    assertNull(namedParams.getParameterValue("var1"));

    // test activateParameters()
    Variables variables = new Variables();

    namedParams.addParameterDefinition("var1", "A", "Variable 1");
    namedParams.setParameterValue( "var1", "AAA" );
    namedParams.addParameterDefinition("var2", null, "Variable 2");
    namedParams.setParameterValue( "var2", "BBB" );
    namedParams.addParameterDefinition("var3", "CCC", "Variable 2");
    namedParams.setParameterValue( "var3", null );

    namedParams.activateParameters(variables);
    assertEquals( "AAA", variables.getVariable( "var1" ) );
    assertEquals( "BBB", variables.getVariable( "var2" ) );
    assertEquals( "CCC", variables.getVariable( "var3" ) );
  }

  @Test(expected = DuplicateParamException.class)
  public void testAddParameterDefinitionWithException() throws DuplicateParamException {
    namedParams.addParameterDefinition("key", "value", "description");
    namedParams.addParameterDefinition("key", "value", "description");
  }
}
