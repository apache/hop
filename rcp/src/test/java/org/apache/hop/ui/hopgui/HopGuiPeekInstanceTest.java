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

package org.apache.hop.ui.hopgui;

import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.hop.ui.hopgui.perspective.configuration.ConfigurationPerspective;
import org.apache.hop.ui.hopgui.perspective.execution.ExecutionPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.metadata.MetadataPerspective;
import org.junit.jupiter.api.Test;

class HopGuiPeekInstanceTest {

  @Test
  void perspectiveGetInstanceDoesNotConstructHopGui() {
    HopGui before = HopGui.peekInstance();
    ExplorerPerspective.getInstance();
    MetadataPerspective.getInstance();
    ExecutionPerspective.getInstance();
    ConfigurationPerspective.getInstance();
    assertSame(
        before,
        HopGui.peekInstance(),
        "Perspective getInstance() must not start a HopGui (that floods SWT tests and the display)");
  }
}
