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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The contract every action and transform that runs a child pipeline or workflow now shares. See
 * {@link SubExecutionParameters}.
 */
class SubExecutionParametersTest {

  private IVariables parent;
  private IVariables childVariables;
  private NamedParameters childParameters;

  @BeforeEach
  void setUp() throws Exception {
    parent = new Variables();
    childVariables = new Variables();
    childParameters = new NamedParameters();
    childParameters.addParameterDefinition("MY_PARAM", "child-default", "");
  }

  private void activate(String[] names, String[] values, boolean passingParentValues) {
    SubExecutionParameters.activate(
        childVariables,
        childParameters,
        parent,
        childParameters.listParameters(),
        names,
        values,
        passingParentValues,
        false);
  }

  @Test
  void tabValueWinsOverParentValueAndDefault() {
    parent.setVariable("MY_PARAM", "parent-value");

    activate(new String[] {"MY_PARAM"}, new String[] {"from-tab"}, true);

    assertEquals("from-tab", childVariables.getVariable("MY_PARAM"));
  }

  @Test
  void parentValuePassedDownWhenOptionIsOn() {
    parent.setVariable("MY_PARAM", "parent-value");

    activate(new String[0], new String[0], true);

    assertEquals("parent-value", childVariables.getVariable("MY_PARAM"));
  }

  /** Issue #8084: an unrelated same-named value in the caller's scope must not leak in. */
  @Test
  void ownDefaultKeptWhenOptionIsOff() {
    parent.setVariable("MY_PARAM", "parent-value");

    activate(new String[0], new String[0], false);

    assertEquals("child-default", childVariables.getVariable("MY_PARAM"));
  }

  /**
   * An empty cell on the Parameters tab means nothing was passed, so the child's own default
   * applies. It must not pick up the value the caller happens to hold, which is how a value from a
   * previous executor row used to stick.
   */
  @Test
  void emptyTabValueFallsBackToDefaultNotToParentValue() {
    parent.setVariable("MY_PARAM", "sticky-from-previous-row");

    activate(new String[] {"MY_PARAM"}, new String[] {""}, true);

    assertEquals("child-default", childVariables.getVariable("MY_PARAM"));
  }

  /** A name the child does not declare is declared on it, so the child can use it too. */
  @Test
  void nameNotDeclaredByChildIsAddedToIt() throws Exception {
    activate(new String[] {"EXTRA"}, new String[] {"extra-value"}, false);

    assertEquals("extra-value", childVariables.getVariable("EXTRA"));
    assertEquals("extra-value", childParameters.getParameterValue("EXTRA"));
  }

  /** Passing values down must never write into the caller's own scope. */
  @Test
  void callerScopeIsLeftAlone() {
    parent.setVariable("MY_PARAM", "parent-value");

    activate(new String[] {"MY_PARAM"}, new String[] {"from-tab"}, true);

    assertEquals("parent-value", parent.getVariable("MY_PARAM"));
  }
}
