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

/**
 * The callbacks of {@link IGuiPluginCompositeWidgetsListener} without SWT types, for the model
 * classes whose widgets Hop builds from their annotations and that Hop also uses without a GUI:
 * metadata types, configuration options, variable resolvers. Hop's serializers and command line
 * options look up the methods of those classes by reflection, which needs every type in every
 * method signature: one SWT type there, the Control of widgetModified for example, and without SWT
 * on the class path (hop-run, hop-server, a Spark executor) the class can't be used at all.
 *
 * <p>Dialogs and other GUI classes keep implementing {@link IGuiPluginCompositeWidgetsListener}.
 */
public interface IGuiPluginModelWidgetsListener {

  /** All the widgets are created. */
  default void widgetsCreated(GuiCompositeWidgets compositeWidgets) {}

  /** All the widgets have their values, right before they're shown. */
  default void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {}

  /**
   * A widget was modified.
   *
   * @param compositeWidgets all the widgets, the changed one among them under its id
   * @param widgetId the id of the changed widget
   */
  default void widgetModified(GuiCompositeWidgets compositeWidgets, String widgetId) {}

  /** The changed values need to be persisted. */
  default void persistContents(GuiCompositeWidgets compositeWidgets) {}
}
