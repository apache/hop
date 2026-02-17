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

package org.apache.hop.ui.core.widget;

import java.util.function.Supplier;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Image;

/** Used to define the behaviour and the content of a Table column in a TableView object. */
public class ColumnInfo {
  public static final int COLUMN_TYPE_NONE = 0;
  public static final int COLUMN_TYPE_TEXT = 1;
  public static final int COLUMN_TYPE_CCOMBO = 2;
  public static final int COLUMN_TYPE_BUTTON = 3;
  public static final int COLUMN_TYPE_ICON = 4;
  public static final int COLUMN_TYPE_FORMAT = 5;
  public static final int COLUMN_TYPE_TEXT_BUTTON = 6;

  @Getter private final int type;

  @Getter private final String name;

  @Setter private String[] comboValues;
  @Setter @Getter private Supplier<String[]> comboValueSupplier = () -> comboValues;
  @Getter @Setter private boolean numeric;
  @Getter @Setter private String tooltip;
  @Getter @Setter private Image image;
  @Setter @Getter private int alignment;
  private boolean readonly;
  @Setter @Getter private String buttonText;
  private boolean hidingNegativeValues;
  @Getter @Setter private int width = -1;

  @Getter @Setter private boolean autoResize = true;

  @Getter @Setter private IValueMeta valueMeta;

  private SelectionListener selButton;

  @Getter @Setter private SelectionListener textVarButtonSelectionListener;

  @Getter @Setter private ITextVarButtonRenderCallback renderTextVarButtonCallback;

  @Getter @Setter private IFieldDisabledListener disabledListener;

  @Getter @Setter private boolean usingVariables;

  @Getter @Setter private boolean passwordField;

  @Getter @Setter private IComboValuesSelectionListener comboValuesSelectionListener;
  @Getter @Setter private int fieldTypeColumn;

  /**
   * Creates a column info class for use with the TableView class.
   *
   * @param columnName The column name
   * @param columnType The column type (see: COLUMN_TYPE_...)
   */
  public ColumnInfo(String columnName, int columnType) {
    this.name = columnName;
    this.type = columnType;
    comboValues = null;
    numeric = false;
    tooltip = null;
    alignment = SWT.LEFT;
    readonly = false;
    hidingNegativeValues = false;
    valueMeta = new ValueMetaString(columnName);
  }

  /**
   * Creates a column info class for use with the TableView class. The type of column info to be
   * created is : COLUMN_TYPE_CCOMBO
   *
   * @param columnName The column name
   * @param columnType The column type (see: COLUMN_TYPE_...)
   * @param comboValues The choices in the comboValues box
   */
  public ColumnInfo(String columnName, int columnType, String... comboValues) {
    this(columnName, columnType);
    this.comboValues = comboValues;
    numeric = false;
    tooltip = null;
    alignment = SWT.LEFT;
    readonly = false;
    hidingNegativeValues = false;
    valueMeta = new ValueMetaString(columnName);
  }

  /**
   * Creates a column info class for use with the TableView class.
   *
   * @param columnName The column name
   * @param columnType The column type (see: COLUMN_TYPE_...)
   * @param numeric true if the column type is numeric. Use setValueType() to specify the type of
   *     numeric: IValueMeta.TYPE_INTEGER is the default.
   */
  public ColumnInfo(String columnName, int columnType, boolean numeric) {
    this(columnName, columnType);
    this.comboValues = null;
    this.numeric = numeric;
    this.tooltip = null;
    this.alignment = SWT.LEFT;
    this.readonly = false;
    this.hidingNegativeValues = false;
    if (numeric) {
      valueMeta = new ValueMetaInteger(columnName);
    } else {
      valueMeta = new ValueMetaString(columnName);
    }
  }

  /**
   * Creates a column info class for use with the TableView class. The type of column info to be
   * created is : COLUMN_TYPE_CCOMBO
   *
   * @param name The column name
   * @param type The column type (see: COLUMN_TYPE_...)
   * @param combo The choices in the combo box
   * @param readOnly true if the column is read-only (you can't type in the combo box, you CAN make
   *     a choice)
   */
  public ColumnInfo(String name, int type, String[] combo, boolean readOnly) {
    this(name, type, combo);
    readonly = readOnly;
  }

  /**
   * Creates a column info class for use with the TableView class.
   *
   * @param name The column name
   * @param type The column type (see: COLUMN_TYPE_...)
   * @param num true if the column type is numeric. Use setValueType() to specify the type of
   *     numeric: IValueMeta.TYPE_INTEGER is the default.
   * @param readOnly true if the column is read-only.
   */
  public ColumnInfo(String name, int type, boolean num, boolean readOnly) {
    this(name, type, num);
    readonly = readOnly;
  }

  /**
   * Creates a column info class for use with the TableView class.
   *
   * @param colname The column name
   * @param coltype The column type (see: COLUMN_TYPE_...)
   * @param num true if the column type is numeric. Use setValueType() to specify the type of
   *     numeric: IValueMeta.TYPE_INTEGER is the default.
   * @param ro true if the column is read-only.
   * @param width The column width
   */
  public ColumnInfo(String colname, int coltype, boolean num, boolean ro, int width) {
    this(colname, coltype, num);
    readonly = ro;
    this.width = width;
  }

  /**
   * Creates a column info class for use with the TableView class. The type of column info to be
   * created is : COLUMN_TYPE_FORMAT
   *
   * @param colname The column name
   * @param coltype The column type (see: COLUMN_TYPE_...)
   * @param fieldTypeColumn The column that contains the field type (for use when filtering the
   *     format combo dropdown)
   */
  public ColumnInfo(String colname, int coltype, int fieldTypeColumn) {
    this(colname, coltype);
    this.fieldTypeColumn = fieldTypeColumn;
  }

  @Override
  public String toString() {
    return name;
  }

  public void setToolTip(String tip) {
    tooltip = tip;
  }

  public void setReadOnly(boolean ro) {
    readonly = ro;
  }

  public String[] getComboValues() {
    return comboValueSupplier.get();
  }

  public String getToolTip() {
    return tooltip;
  }

  public boolean isReadOnly() {
    return readonly;
  }

  public void setSelectionAdapter(SelectionListener sb) {
    selButton = sb;
  }

  public SelectionListener getSelectionAdapter() {
    return selButton;
  }

  public void hideNegative() {
    hidingNegativeValues = true;
  }

  public void showNegative() {
    hidingNegativeValues = false;
  }

  public boolean isNegativeHidden() {
    return hidingNegativeValues;
  }

  public boolean shouldRenderTextVarButton() {
    return this.renderTextVarButtonCallback == null
        || this.renderTextVarButtonCallback.shouldRenderButton();
  }
}
