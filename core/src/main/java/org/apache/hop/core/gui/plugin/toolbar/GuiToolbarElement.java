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

package org.apache.hop.core.gui.plugin.toolbar;

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
public @interface GuiToolbarElement {

  /**
   * @return The ID of the toolbar to which this element belongs
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
  GuiToolbarElementType type() default GuiToolbarElementType.BUTTON;

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
   * @return The image for the toolbar icon
   */
  String image() default "";

  /**
   * The disabled image filename of the GUI Element, usually an SVG icon.
   *
   * @return The disabled image
   */
  String disabledImage() default "";

  /**
   * @return True if the text element you define is a password with an asterisk mask
   */
  boolean password() default false;

  /**
   * @return true if the widget supports variables
   */
  boolean variables() default true;

  /**
   * @return The getter method of the property if it's non-standard
   */
  String getterMethod() default "";

  /**
   * @return The setter method of the property if it's non-standard
   */
  String setterMethod() default "";

  /**
   * @return The method which returns a String[] to populate a combo box widget GUI element
   */
  String comboValuesMethod() default "";

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

  /**
   * If you want extra width on a difficult item like a label or a combo
   * @return The extra width to give the item
   */
  int extraWidth() default 0;

  /**
   * The text alignment of label or combo widgets
   * @return
   */
  boolean alignRight() default false;
}
