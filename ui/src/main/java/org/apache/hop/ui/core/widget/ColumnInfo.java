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

import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionListener;

import java.util.function.Supplier;

/**
 * Used to define the behaviour and the content of a Table column in a TableView object.
 *
 * @author Matt
 * @since 27-05-2003
 */
public class ColumnInfo {
  public static final int COLUMN_TYPE_NONE = 0;
  public static final int COLUMN_TYPE_TEXT = 1;
  public static final int COLUMN_TYPE_CCOMBO = 2;
  public static final int COLUMN_TYPE_BUTTON = 3;
  public static final int COLUMN_TYPE_ICON = 4;
  public static final int COLUMN_TYPE_FORMAT = 5;
  public static final int COLUMN_TYPE_TEXT_BUTTON = 6;

  private int type;
  private String name;

  private String[] comboValues;
  private Supplier<String[]> comboValueSupplier = () -> comboValues;
  private boolean numeric;
  private String tooltip;
  private int alignment;
  private boolean readonly;
  private String buttonText;
  private boolean hidingNegativeValues;
  private int width = -1;
  private boolean autoResize = true;

  private IValueMeta valueMeta;

  private SelectionListener selButton;
  private SelectionListener textVarButtonSelectionListener;

  private ITextVarButtonRenderCallback renderTextVarButtonCallback;

  private IFieldDisabledListener disabledListener;

  private boolean usingVariables;
  private boolean passwordField;

  private IComboValuesSelectionListener comboValuesSelectionListener;
  private int fieldTypeColumn;

  /**
   * Creates a column info class for use with the TableView class.
   *
   * @param colname The column name
   * @param coltype The column type (see: COLUMN_TYPE_...)
   */
  public ColumnInfo( String colname, int coltype ) {
    name = colname;
    type = coltype;
    comboValues = null;
    numeric = false;
    tooltip = null;
    alignment = SWT.LEFT;
    readonly = false;
    hidingNegativeValues = false;
    valueMeta = new ValueMetaString( colname );
  }

  /**
   * Creates a column info class for use with the TableView class. The type of column info to be created is :
   * COLUMN_TYPE_CCOMBO
   *
   * @param colname The column name
   * @param coltype The column type (see: COLUMN_TYPE_...)
   * @param combo   The choices in the combo box
   */
  public ColumnInfo( String colname, int coltype, String[] combo ) {
    this( colname, coltype );
    comboValues = combo;
    numeric = false;
    tooltip = null;
    alignment = SWT.LEFT;
    readonly = false;
    hidingNegativeValues = false;
    valueMeta = new ValueMetaString( colname );
  }

  /**
   * Creates a column info class for use with the TableView class.
   *
   * @param colname The column name
   * @param coltype The column type (see: COLUMN_TYPE_...)
   * @param numeric true if the column type is numeric. Use setValueType() to specify the type of numeric:
   *                IValueMeta.TYPE_INTEGER is the default.
   */
  public ColumnInfo( String colname, int coltype, boolean numeric ) {
    this( colname, coltype );
    this.comboValues = null;
    this.numeric = numeric;
    this.tooltip = null;
    this.alignment = SWT.LEFT;
    this.readonly = false;
    this.hidingNegativeValues = false;
    if ( numeric ) {
      valueMeta = new ValueMetaInteger( colname );
    } else {
      valueMeta = new ValueMetaString( colname );
    }
  }

  /**
   * Creates a column info class for use with the TableView class. The type of column info to be created is :
   * COLUMN_TYPE_CCOMBO
   *
   * @param colname The column name
   * @param coltype The column type (see: COLUMN_TYPE_...)
   * @param combo   The choices in the combo box
   * @param ro      true if the column is read-only (you can't type in the combo box, you CAN make a choice)
   */
  public ColumnInfo( String colname, int coltype, String[] combo, boolean ro ) {
    this( colname, coltype, combo );
    readonly = ro;
  }

  /**
   * Creates a column info class for use with the TableView class.
   *
   * @param colname The column name
   * @param coltype The column type (see: COLUMN_TYPE_...)
   * @param num     true if the column type is numeric. Use setValueType() to specify the type of numeric:
   *                IValueMeta.TYPE_INTEGER is the default.
   * @param ro      true if the column is read-only.
   */
  public ColumnInfo( String colname, int coltype, boolean num, boolean ro ) {
    this( colname, coltype, num );
    readonly = ro;
  }

  /**
   * Creates a column info class for use with the TableView class.
   *
   * @param colname The column name
   * @param coltype The column type (see: COLUMN_TYPE_...)
   * @param num     true if the column type is numeric. Use setValueType() to specify the type of numeric:
   *                IValueMeta.TYPE_INTEGER is the default.
   * @param ro      true if the column is read-only.
   * @param width   The column width
   */
  public ColumnInfo( String colname, int coltype, boolean num, boolean ro, int width ) {
    this( colname, coltype, num );
    readonly = ro;
    this.width = width;
  }

  /**
   * Creates a column info class for use with the TableView class. The type of column info to be created is :
   * COLUMN_TYPE_FORMAT
   *
   * @param colname         The column name
   * @param coltype         The column type (see: COLUMN_TYPE_...)
   * @param fieldTypeColumn The column that contains the field type (for use when filtering the format combo dropdown)
   */
  public ColumnInfo( String colname, int coltype, int fieldTypeColumn ) {
    this( colname, coltype );
    this.fieldTypeColumn = fieldTypeColumn;
  }

  @Override
  public String toString() {
    return name;
  }

  public void setToolTip( String tip ) {
    tooltip = tip;
  }

  public void setReadOnly( boolean ro ) {
    readonly = ro;
  }

  public void setAlignment( int allign ) {
    alignment = allign;
  }

  public void setComboValues( String[] cv ) {
    comboValues = cv;
  }

  public void setComboValueSupplier( Supplier<String[]> comboValueSupplier ) {
    this.comboValueSupplier = comboValueSupplier;
  }

  public void setButtonText( String bt ) {
    buttonText = bt;
  }

  public String getName() {
    return name;
  }

  public int getType() {
    return type;
  }

  public String[] getComboValues() {
    String[] retval = comboValueSupplier.get();
    return retval;
  }

  /**
   * @return the numeric
   */
  public boolean isNumeric() {
    return numeric;
  }

  /**
   * @param numeric the numeric to set
   */
  public void setNumeric( boolean numeric ) {
    this.numeric = numeric;
  }

  public String getToolTip() {
    return tooltip;
  }

  public int getAlignment() {
    return alignment;
  }

  public boolean isReadOnly() {
    return readonly;
  }

  public String getButtonText() {
    return buttonText;
  }

  public void setSelectionAdapter( SelectionListener sb ) {
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

  /**
   * @return the valueMeta
   */
  public IValueMeta getValueMeta() {
    return valueMeta;
  }

  /**
   * @param valueMeta the valueMeta to set
   */
  public void setValueMeta( IValueMeta valueMeta ) {
    this.valueMeta = valueMeta;
  }

  /**
   * @return the usingVariables
   */
  public boolean isUsingVariables() {
    return usingVariables;
  }

  /**
   * @param usingVariables the usingVariables to set
   */
  public void setUsingVariables( boolean usingVariables ) {
    this.usingVariables = usingVariables;
  }

  /**
   * @return the password
   */
  public boolean isPasswordField() {
    return passwordField;
  }

  /**
   * @param password the password to set
   */
  public void setPasswordField( boolean password ) {
    this.passwordField = password;
  }

  public int getFieldTypeColumn() {
    return fieldTypeColumn;
  }

  public void setFieldTypeColumn( int fieldTypeColumn ) {
    this.fieldTypeColumn = fieldTypeColumn;
  }

  /**
   * @return the comboValuesSelectionListener
   */
  public IComboValuesSelectionListener getComboValuesSelectionListener() {
    return comboValuesSelectionListener;
  }

  /**
   * @param comboValuesSelectionListener the comboValuesSelectionListener to set
   */
  public void setComboValuesSelectionListener( IComboValuesSelectionListener comboValuesSelectionListener ) {
    this.comboValuesSelectionListener = comboValuesSelectionListener;
  }

  /**
   * @return the disabledListener
   */
  public IFieldDisabledListener getDisabledListener() {
    return disabledListener;
  }

  /**
   * @param disabledListener the disabledListener to set
   */
  public void setDisabledListener( IFieldDisabledListener disabledListener ) {
    this.disabledListener = disabledListener;
  }

  public SelectionListener getTextVarButtonSelectionListener() {
    return textVarButtonSelectionListener;
  }

  public void setTextVarButtonSelectionListener( SelectionListener textVarButtonSelectionListener ) {
    this.textVarButtonSelectionListener = textVarButtonSelectionListener;
  }

  public void setRenderTextVarButtonCallback( ITextVarButtonRenderCallback callback ) {
    this.renderTextVarButtonCallback = callback;
  }

  public boolean shouldRenderTextVarButton() {
    return this.renderTextVarButtonCallback == null || this.renderTextVarButtonCallback.shouldRenderButton();
  }

  public int getWidth() {
    return this.width;
  }

  /**
   * @return if should be resized to accommodate contents
   */
  public boolean isAutoResize() {
    return autoResize;
  }

  /**
   * If should be resized to accommodate contents. Default is <code>true</code>.
   */
  public void setAutoResize( boolean resize ) {
    this.autoResize = resize;
  }


}
