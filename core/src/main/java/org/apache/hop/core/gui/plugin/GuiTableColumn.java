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

package org.apache.hop.core.gui.plugin;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Column of a {@link GuiElementType#TABLE} widget. Put this on a field of the row class named by
 * {@code List<Row>} on the widget. It is not read by the metadata serializers;
 * {@code @HopMetadataProperty} still stores the field.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface GuiTableColumn {

  /** Column id. Empty uses the field name. */
  String id() default "";

  /** Alphabetical sort key among the columns of this row class, same rule as {@code order()}. */
  String order() default "";

  /** Header text. Use {@code i18n::} the same way as {@link GuiWidgetElement#label()}. */
  String label() default "";

  /** Header tooltip. Use {@code i18n::} the same way as {@link GuiWidgetElement#toolTip()}. */
  String toolTip() default "";

  /** Cell editor. Must agree with the field type, or the column is left out of the grid. */
  GuiTableColumnType type();

  /**
   * @return true if a text or string-combo cell offers variables
   */
  boolean variables() default true;

  /**
   * @return true if a text cell masks its value
   */
  boolean password() default false;

  /** Column width in pixels. Negative means the table sizes the column. */
  int width() default -1;

  /** Getter name when it is not the bean property name ({@code getX} / {@code isX}). */
  String getterMethod() default "";

  /** Setter name when it is not {@code setX}. */
  String setterMethod() default "";

  /**
   * Method on the parent object (the action or metadata, not the row) that returns the items of a
   * {@link GuiTableColumnType#COMBO} on a {@link String} field. Signature: {@code List<String>
   * method(ILogChannel log, IHopMetadataProvider metadataProvider)}. Ignored for an enum field,
   * which uses {@link Enum#name()}.
   */
  String comboValuesMethod() default "";
}
