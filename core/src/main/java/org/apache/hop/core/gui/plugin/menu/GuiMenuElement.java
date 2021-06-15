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

package org.apache.hop.core.gui.plugin.menu;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation allows a method in a GuiPlugin to be identified as a contributor to the Hop UI
 */
@Documented
@Retention( RetentionPolicy.RUNTIME )
@Target( { ElementType.FIELD, ElementType.METHOD } )
public @interface GuiMenuElement {

  /**
   * @return The root to which this menu item belongs. For example see HopGui.ID_MAIN_MENU
   */
  String root();

  /**
   * Every GUI Element has a unique ID so it can be replaced by other plugins at any given time.
   *
   * @return The unique ID of the GUI Element
   */
  String id();

  /**
   * The type of GUI Element this method covers for
   *
   * @return
   */
  GuiMenuElementType type() default GuiMenuElementType.MENU_ITEM;

  /**
   * The label of the GUI element: the menu item text and so on.
   *
   * @return The GUI Element Label
   */
  String label() default "";

  /**
   * The tooltip of the GUI element (when applicable)
   *
   * @return The GUI Element tooltip for the widget and the label
   */
  String toolTip() default "";



  /**
   * The image filename of the GUI Element, usually an SVG icon.
   *
   * @return The
   */
  String image() default "";

  /**
   * The ID of the parent GUI element. This is usually the parent menu or toolbar, ...
   *
   * @return The ID of the parent GUI element
   */
  String parentId() default "";

  /**
   * If we want to reference a menu element in another class than the GuiPlugin.
   * For example, a plugin wants to add a menu item in the HopGui menu.
   * In that case we would reference HopGui.class.getName() here.
   * @return The parent class name or an empty String if nothing was specified.
   */
  String parentClass() default "";

  /**
   * Set this flag to true if you want to ignore the field as a GUI Element.
   * You can use this to override a GUI element from a base class.
   *
   * @return True if you want this element to be ignored
   */
  boolean ignored() default false;

  /**
   * Set to true if you want the menu-item of toolbar icon to be preceded by a separator or variables.
   *
   * @return True if you want a separator before this element
   */
  boolean separator() default false;
}
