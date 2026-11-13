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

package org.apache.hop.ui.pipeline.config;

import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.engines.local.LocalPipelineRunConfiguration;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;

/** The local engine does not use the wait time for buffer checks with lock-free row sets. */
public class LocalPipelineRunConfigurationWidgetsListener extends GuiCompositeWidgetsAdapter {

  public LocalPipelineRunConfigurationWidgetsListener(
      IPipelineEngineRunConfiguration runConfiguration) {
    // The widgets say what is set; nothing to keep from the configuration itself
  }

  @Override
  public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {
    enableWaitTime(compositeWidgets);
  }

  @Override
  public void widgetModified(
      GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
    if (LocalPipelineRunConfiguration.WIDGET_ID_LOCK_FREE_ROWSETS.equals(widgetId)) {
      enableWaitTime(compositeWidgets);
    }
  }

  private static void enableWaitTime(GuiCompositeWidgets compositeWidgets) {
    Control lockFree =
        compositeWidgets
            .getWidgetsMap()
            .get(LocalPipelineRunConfiguration.WIDGET_ID_LOCK_FREE_ROWSETS);
    if (!(lockFree instanceof Button button) || button.isDisposed()) {
      return;
    }
    boolean enabled = !button.getSelection();
    Control waitTime =
        compositeWidgets.getWidgetsMap().get(LocalPipelineRunConfiguration.WIDGET_ID_WAIT_TIME);
    if (waitTime != null && !waitTime.isDisposed()) {
      waitTime.setEnabled(enabled);
    }
    Control waitTimeLabel =
        compositeWidgets.getLabelsMap().get(LocalPipelineRunConfiguration.WIDGET_ID_WAIT_TIME);
    if (waitTimeLabel != null && !waitTimeLabel.isDisposed()) {
      waitTimeLabel.setEnabled(enabled);
    }
  }
}
