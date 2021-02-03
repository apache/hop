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

package org.apache.hop.ui.core.gui;

import org.eclipse.swt.widgets.Control;

public interface IGuiPluginCompositeWidgetsListener {
  /**
   * This method is called when all the widgets are created
   *
   * @param compositeWidgets
   */
  void widgetsCreated(GuiCompositeWidgets compositeWidgets);

  /**
   * This method is called when all the widgets have received data, right before they're shown
   *
   * @param compositeWidgets
   */
  void widgetsPopulated(GuiCompositeWidgets compositeWidgets);

  /**
   * This method is called when a widget was modified
   *  @param compositeWidgets All the widgets to reference
   * @param changedWidget The widget that got changed
   * @param widgetId The ID of the widget changed.
   */
  void widgetModified( GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId );

  /**
   * Will be called when the changed data needs to be persisted
   *
   * @param compositeWidgets The widgets
   */
  void persistContents(GuiCompositeWidgets compositeWidgets);
}
