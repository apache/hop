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

package org.apache.hop.neo4j.shared;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.apache.hop.core.variables.Variables;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.neo4j.execution.NeoExecutionInfoLocation;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The connection of the Neo4j execution information location uses its own metadata selection line
 * through {@code @GuiWidgetElement(metadataSelectionLine = ...)}, so it lists graph database
 * connections next to Neo4j connections.
 */
@Tag("uitest")
class NeoConnectionSelectionLineTest extends SwtBotTestBase {

  @BeforeAll
  static void registerGuiPluginElements() throws Exception {
    HopGuiEnvironment.init();
  }

  @Test
  void testExecutionInfoLocationUsesNeoConnectionSelectionLine() {
    ensureDisplay();
    Shell shell = new Shell(display);
    shell.setLayout(new FormLayout());
    try {
      NeoExecutionInfoLocation location = new NeoExecutionInfoLocation();
      location.setConnectionName("my-graph");
      GuiCompositeWidgets widgets = new GuiCompositeWidgets(new Variables());
      widgets.createCompositeWidgets(
          location, null, shell, ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID, null);
      widgets.setWidgetsContents(
          location, shell, ExecutionInfoLocation.GUI_PLUGIN_ELEMENT_PARENT_ID);

      Control control = widgets.getWidgetsMap().get("connectionName");
      NeoConnectionSelectionLine line = assertInstanceOf(NeoConnectionSelectionLine.class, control);
      assertEquals("my-graph", line.getText());
    } finally {
      shell.dispose();
    }
  }
}
