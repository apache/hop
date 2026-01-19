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
package org.apache.hop.staticschema.metadata;

import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class SchemaDefinitionEditor extends MetadataEditor<SchemaDefinition> {

  private static final Class<?> PKG = SchemaDefinitionEditor.class;

  // Connection properties
  //
  private Text wName;
  private Text wDescription;
  private Text wSeparator;
  private Text wEnclosure;
  private TableView wFields;

  public SchemaDefinitionEditor(
      HopGui hopGui, MetadataManager<SchemaDefinition> manager, SchemaDefinition metadata) {
    super(hopGui, manager, metadata);
  }

  @Override
  public void createControl(Composite parent) {

    PropsUi props = PropsUi.getInstance();
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin() + 2;

    Control lastControl;

    // The name
    //
    Label wlName = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "SchemaDefinitionDialog.Name.Label"));
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(0, margin);
    fdlName.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlName.right = new FormAttachment(middle, -margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.left = new FormAttachment(middle, 0); // To the right of the label
    fdName.right = new FormAttachment(95, 0);
    wName.setLayoutData(fdName);
    lastControl = wName;

    // The description
    //
    Label wlDescription = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlDescription);
    wlDescription.setText(BaseMessages.getString(PKG, "SchemaDefinitionDialog.Description.Label"));
    FormData fdlDescription = new FormData();
    fdlDescription.top = new FormAttachment(lastControl, margin);
    fdlDescription.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlDescription.right = new FormAttachment(middle, -margin);
    wlDescription.setLayoutData(fdlDescription);
    wDescription = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDescription);
    FormData fdDescription = new FormData();
    fdDescription.top = new FormAttachment(wlDescription, 0, SWT.CENTER);
    fdDescription.left = new FormAttachment(middle, 0); // To the right of the label
    fdDescription.right = new FormAttachment(95, 0);
    wDescription.setLayoutData(fdDescription);
    lastControl = wDescription;

    // Separator
    //
    Label wlSeparator = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlSeparator);
    wlSeparator.setText(BaseMessages.getString(PKG, "SchemaDefinitionDialog.Separator.Label"));
    FormData fdlSeparator = new FormData();
    fdlSeparator.top = new FormAttachment(lastControl, margin);
    fdlSeparator.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlSeparator.right = new FormAttachment(middle, -margin);
    wlSeparator.setLayoutData(fdlSeparator);
    wSeparator = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSeparator);
    FormData fdSeparator = new FormData();
    fdSeparator.top = new FormAttachment(wlSeparator, 0, SWT.CENTER);
    fdSeparator.left = new FormAttachment(middle, 0); // To the right of the label
    fdSeparator.right = new FormAttachment(95, 0);
    wSeparator.setLayoutData(fdSeparator);
    lastControl = wSeparator;

    // Enclosure
    //
    Label wlEnclosure = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlEnclosure);
    wlEnclosure.setText(BaseMessages.getString(PKG, "SchemaDefinitionDialog.Enclosure.Label"));
    FormData fdlEnclosure = new FormData();
    fdlEnclosure.top = new FormAttachment(lastControl, margin);
    fdlEnclosure.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlEnclosure.right = new FormAttachment(middle, -margin);
    wlEnclosure.setLayoutData(fdlEnclosure);
    wEnclosure = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wEnclosure);
    FormData fdEnclosure = new FormData();
    fdEnclosure.top = new FormAttachment(wlEnclosure, 0, SWT.CENTER);
    fdEnclosure.left = new FormAttachment(middle, 0); // To the right of the label
    fdEnclosure.right = new FormAttachment(95, 0);
    wEnclosure.setLayoutData(fdEnclosure);
    lastControl = wEnclosure;

    // Fields...
    //
    Label wlFields = new Label(parent, SWT.LEFT);
    PropsUi.setLook(wlFields);
    wlFields.setText(BaseMessages.getString(PKG, "SchemaDefinitionDialog.Fields.Label"));
    FormData fdlFields = new FormData();
    fdlFields.top = new FormAttachment(lastControl, margin);
    fdlFields.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlFields.right = new FormAttachment(100, 0);
    wlFields.setLayoutData(fdlFields);

    ColumnInfo[] columnInfos =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "SchemaDefinitionDialog.Fields.Column.FieldName"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SchemaDefinitionDialog.Fields.Column.FieldType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SchemaDefinitionDialog.Fields.Column.FieldFormat"),
              ColumnInfo.COLUMN_TYPE_FORMAT,
              2),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SchemaDefinitionDialog.Fields.Column.FieldLength"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SchemaDefinitionDialog.Fields.Column.FieldPrecision"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SchemaDefinitionDialog.Fields.Column.FieldCurrency"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SchemaDefinitionDialog.Fields.Column.FieldDecimal"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SchemaDefinitionDialog.Fields.Column.FieldGroup"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SchemaDefinitionDialog.Fields.Column.FieldIfNull"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SchemaDefinitionDialog.Fields.Column.FieldTrimType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaString.trimTypeDesc,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SchemaDefinitionDialog.Fields.Column.FieldComment"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SchemaDefinitionDialog.Fields.Column.RoundingType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaString.roundingTypeDesc,
              true),
        };

    wFields =
        new TableView(
            new Variables(),
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columnInfos,
            this.getMetadata().getFieldDefinitions().size(),
            null,
            props);
    PropsUi.setLook(wFields);
    FormData fdFields = new FormData();
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.left = new FormAttachment(0, 0); // First one in the left top corner
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(100, -margin * 2);
    wFields.setLayoutData(fdFields);

    setWidgetsContent();

    // Add listener to detect change after loading data
    wName.addModifyListener(e -> setChanged());
    wDescription.addModifyListener(e -> setChanged());
    wSeparator.addModifyListener(e -> setChanged());
    wEnclosure.addModifyListener(e -> setChanged());
    wFields.addModifyListener(e -> setChanged());
  }

  @Override
  public void setWidgetsContent() {
    SchemaDefinition SchemaDefinition = this.getMetadata();
    wName.setText(Const.NVL(SchemaDefinition.getName(), ""));
    wDescription.setText(Const.NVL(SchemaDefinition.getDescription(), ""));
    wSeparator.setText(Const.NVL(SchemaDefinition.getSeparator(), ""));
    wEnclosure.setText(Const.NVL(SchemaDefinition.getEnclosure(), ""));

    List<SchemaFieldDefinition> fields = SchemaDefinition.getFieldDefinitions();
    for (int i = 0; i < fields.size(); i++) {
      SchemaFieldDefinition field = fields.get(i);
      TableItem item = wFields.table.getItem(i);
      item.setText(1, Const.NVL(field.getName(), ""));
      item.setText(2, Const.NVL(field.getHopType(), ""));
      item.setText(3, Const.NVL(field.getFormatMask(), ""));
      item.setText(4, field.getLength() < 0 ? "" : Integer.toString(field.getLength()));
      item.setText(5, field.getPrecision() < 0 ? "" : Integer.toString(field.getPrecision()));
      item.setText(6, Const.NVL(field.getCurrencySymbol(), ""));
      item.setText(7, Const.NVL(field.getDecimalSymbol(), ""));
      item.setText(8, Const.NVL(field.getGroupingSymbol(), ""));
      item.setText(9, Const.NVL(field.getIfNullValue(), ""));

      String trimType = Const.NVL(ValueMetaBase.getTrimTypeDesc(field.getTrimType()), "");
      item.setText(10, trimType);

      item.setText(11, Const.NVL(field.getComment(), ""));
      String roundingType = ValueMetaBase.getRoundingTypeDesc(field.getRoundingType());
      item.setText(12, roundingType);
    }
  }

  @Override
  public void getWidgetsContent(SchemaDefinition SchemaDefinition) {

    SchemaDefinition.setName(wName.getText());
    SchemaDefinition.setDescription(wDescription.getText());
    SchemaDefinition.setSeparator(wSeparator.getText());
    SchemaDefinition.setEnclosure(wEnclosure.getText());
    SchemaDefinition.getFieldDefinitions().clear();

    for (int i = 0; i < wFields.nrNonEmpty(); i++) {
      TableItem item = wFields.getNonEmpty(i);
      String name = item.getText(1);
      String hopType = item.getText(2);
      SchemaFieldDefinition sfd = new SchemaFieldDefinition(name, hopType);
      sfd.setFormatMask(item.getText(3));
      sfd.setLength(Const.toInt(item.getText(4), -1));
      sfd.setPrecision(Const.toInt(item.getText(5), -1));
      sfd.setCurrencySymbol(item.getText(6));
      sfd.setDecimalSymbol(item.getText(7));
      sfd.setGroupingSymbol(item.getText(8));
      sfd.setIfNullValue(item.getText(9));
      sfd.setTrimType(ValueMetaString.getTrimTypeByDesc(item.getText(10)));
      sfd.setComment(item.getText(11));
      sfd.setRoundingType(ValueMetaBase.getRoundingTypeCode(item.getText(12)));
      SchemaDefinition.getFieldDefinitions().add(sfd);
    }
  }

  @Override
  public boolean setFocus() {
    if (wName == null || wName.isDisposed()) {
      return false;
    }
    return wName.setFocus();
  }
}
