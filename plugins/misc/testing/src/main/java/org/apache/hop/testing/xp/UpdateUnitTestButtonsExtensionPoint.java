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

package org.apache.hop.testing.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.testing.gui.TestingGuiPlugin;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.eclipse.swt.widgets.Display;

@ExtensionPoint(
    extensionPointId = "HopGuiPipelineGraphUpdateGui",
    id = "UpdateUnitTestButtonsUpdateGuiExtensionPoint",
    description = "Enable/disable unit test toolbar buttons based on whether a test is selected")
/**
 * This extension point is called whenever the pipeline graph GUI is updated. It ensures the unit
 * test toolbar buttons (Edit, Detach, Delete) are enabled or disabled based on whether a unit test
 * is currently selected.
 */
public class UpdateUnitTestButtonsExtensionPoint implements IExtensionPoint<HopGuiPipelineGraph> {

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, HopGuiPipelineGraph pipelineGraph)
      throws HopException {

    // Update the unit test button states
    // Use asyncExec to ensure this runs after any async combo population
    Display display = HopGui.getInstance().getDisplay();
    if (display != null && !display.isDisposed()) {
      display.asyncExec(() -> TestingGuiPlugin.getInstance().enableUnitTestButtons());
    }
  }
}
