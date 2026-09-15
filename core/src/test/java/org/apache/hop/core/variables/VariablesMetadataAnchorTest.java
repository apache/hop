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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Hop GUI keeps its project's metadata reachable from its variables by putting it behind them,
 * rather than in them, so that handing the GUI a fresh set of variables - which loading a project
 * does - cannot quietly lose it. This is the property that makes that safe.
 */
class VariablesMetadataAnchorTest {

  @Test
  @DisplayName("An anchor behind a variable space is found, and does not touch variable lookup")
  void anchorIsReachableAndInert() {
    IHopMetadataProvider provider = mock(IHopMetadataProvider.class);
    IVariables anchor =
        new Variables() {
          @Override
          public IHopMetadataProvider getMetadataProvider() {
            return provider;
          }
        };

    IVariables freshSpace = Variables.getADefaultVariableSpace();
    freshSpace.setParentVariables(anchor);
    freshSpace.setVariable("SOME_VARIABLE", "its own value");

    assertSame(
        provider,
        freshSpace.findExecutionMetadataProvider(),
        "The metadata behind the space has to be reachable from it");
    assertEquals(
        "its own value",
        freshSpace.getVariable("SOME_VARIABLE"),
        "The anchor must not interfere with looking a variable up");
  }
}
