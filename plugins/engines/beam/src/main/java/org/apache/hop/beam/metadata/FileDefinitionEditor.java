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
package org.apache.hop.beam.metadata;

import java.util.List;

import org.apache.hop.core.Const;
import org.apache.hop.core.row.value.ValueMetaFactory;
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

public class FileDefinitionEditor extends MetadataEditor<FileDefinition> {

  private static final Class<?> PKG = FileDefinitionEditor.class; // For Translator

  // Connection properties
  //
  private Text wName;
  private Text wDescription;
  private Text wSeparator;
  private Text wEnclosure;
  private TableView wFields;

  public FileDefinitionEditor(HopGui hopGui, MetadataManager<FileDefinition> manager, FileDefinition metadata) {
    super(hopGui, manager, metadata);
  }

  @Override
  public void createControl(Composite parent) {

    PropsUi props = PropsUi.getInstance();
    int middle = props.getMiddlePct();
    int margin = Const.MARGIN + 2;

    Control lastControl;

    // The name
    //
    Label wlName = new Label(parent, SWT.RIGHT);
    props.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "FileDefinitionDialog.Name.Label"));
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(0, margin);
    fdlName.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlName.right = new FormAttachment(middle, -margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.left = new FormAttachment(middle, 0); // To the right of the label
    fdName.right = new FormAttachment(95, 0);
    wName.setLayoutData(fdName);
    lastControl = wName;

    // The description
    //
    Label wlDescription = new Label(parent, SWT.RIGHT);
    props.setLook(wlDescription);
    wlDescription.setText(BaseMessages.getString(PKG, "FileDefinitionDialog.Description.Label"));
    FormData fdlDescription = new FormData();
    fdlDescription.top = new FormAttachment(lastControl, margin);
    fdlDescription.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlDescription.right = new FormAttachment(middle, -margin);
    wlDescription.setLayoutData(fdlDescription);
    wDescription = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wDescription);
    FormData fdDescription = new FormData();
    fdDescription.top = new FormAttachment(wlDescription, 0, SWT.CENTER);
    fdDescription.left = new FormAttachment(middle, 0); // To the right of the label
    fdDescription.right = new FormAttachment(95, 0);
    wDescription.setLayoutData(fdDescription);
    lastControl = wDescription;

    // Separator
    //
    Label wlSeparator = new Label(parent, SWT.RIGHT);
    props.setLook(wlSeparator);
    wlSeparator.setText(BaseMessages.getString(PKG, "FileDefinitionDialog.Separator.Label"));
    FormData fdlSeparator = new FormData();
    fdlSeparator.top = new FormAttachment(lastControl, margin);
    fdlSeparator.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlSeparator.right = new FormAttachment(middle, -margin);
    wlSeparator.setLayoutData(fdlSeparator);
    wSeparator = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wSeparator);
    FormData fdSeparator = new FormData();
    fdSeparator.top = new FormAttachment(wlSeparator, 0, SWT.CENTER);
    fdSeparator.left = new FormAttachment(middle, 0); // To the right of the label
    fdSeparator.right = new FormAttachment(95, 0);
    wSeparator.setLayoutData(fdSeparator);
    lastControl = wSeparator;

    // Enclosure
    //
    Label wlEnclosure = new Label(parent, SWT.RIGHT);
    props.setLook(wlEnclosure);
    wlEnclosure.setText(BaseMessages.getString(PKG, "FileDefinitionDialog.Enclosure.Label"));
    FormData fdlEnclosure = new FormData();
    fdlEnclosure.top = new FormAttachment(lastControl, margin);
    fdlEnclosure.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlEnclosure.right = new FormAttachment(middle, -margin);
    wlEnclosure.setLayoutData(fdlEnclosure);
    wEnclosure = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wEnclosure);
    FormData fdEnclosure = new FormData();
    fdEnclosure.top = new FormAttachment(wlEnclosure, 0, SWT.CENTER);
    fdEnclosure.left = new FormAttachment(middle, 0); // To the right of the label
    fdEnclosure.right = new FormAttachment(95, 0);
    wEnclosure.setLayoutData(fdEnclosure);
    lastControl = wEnclosure;

    // Fields...
    //
    Label wlFields = new Label(parent, SWT.LEFT);
    props.setLook(wlFields);
    wlFields.setText(BaseMessages.getString(PKG, "FileDefinitionDialog.Fields.Label"));
    FormData fdlFields = new FormData();
    fdlFields.top = new FormAttachment(lastControl, margin);
    fdlFields.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlFields.right = new FormAttachment(100, 0);
    wlFields.setLayoutData(fdlFields);

    ColumnInfo[] columnInfos =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "FileDefinitionDialog.Fields.Column.FieldName"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "FileDefinitionDialog.Fields.Column.FieldType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames(),
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "FileDefinitionDialog.Fields.Column.FieldFormat"),
              ColumnInfo.COLUMN_TYPE_FORMAT,
              2),
          new ColumnInfo(
              BaseMessages.getString(PKG, "FileDefinitionDialog.Fields.Column.FieldLength"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "FileDefinitionDialog.Fields.Column.FieldPrecision"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
        };

    wFields =
        new TableView(
            new Variables(),
            parent,
            SWT.BORDER,
            columnInfos,
            this.getMetadata().getFieldDefinitions().size(),
            null,
            props);
    props.setLook(wFields);
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
    FileDefinition fileDefinition = this.getMetadata();
    wName.setText(Const.NVL(fileDefinition.getName(), ""));
    wDescription.setText(Const.NVL(fileDefinition.getDescription(), ""));
    wSeparator.setText(Const.NVL(fileDefinition.getSeparator(), ""));
    wEnclosure.setText(Const.NVL(fileDefinition.getEnclosure(), ""));

    List<FieldDefinition> fields = fileDefinition.getFieldDefinitions();
    for (int i = 0; i < fields.size(); i++) {
      FieldDefinition field = fields.get(i);
      TableItem item = wFields.table.getItem(i);
      item.setText(1, Const.NVL(field.getName(), ""));
      item.setText(2, Const.NVL(field.getHopType(), ""));
      item.setText(3, Const.NVL(field.getFormatMask(), ""));
      item.setText(4, field.getLength() < 0 ? "" : Integer.toString(field.getLength()));
      item.setText(5, field.getPrecision() < 0 ? "" : Integer.toString(field.getPrecision()));
    }
  }

  @Override
  public void getWidgetsContent(FileDefinition fileDefinition) {
    fileDefinition.setName(wName.getText());
    fileDefinition.setDescription(wDescription.getText());
    fileDefinition.setSeparator(wSeparator.getText());
    fileDefinition.setEnclosure(wEnclosure.getText());
    fileDefinition.getFieldDefinitions().clear();
    for (int i = 0; i < wFields.nrNonEmpty(); i++) {
      TableItem item = wFields.getNonEmpty(i);
      String name = item.getText(1);
      String hopType = item.getText(2);
      String formatMask = item.getText(3);
      int length = Const.toInt(item.getText(4), -1);
      int precision = Const.toInt(item.getText(5), -1);
      fileDefinition
          .getFieldDefinitions()
          .add(new FieldDefinition(name, hopType, length, precision, formatMask));
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
