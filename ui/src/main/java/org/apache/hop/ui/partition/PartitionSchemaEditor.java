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

package org.apache.hop.ui.partition;

import java.util.ArrayList;
import java.util.List;

import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/**
 * Dialog that allows you to edit the settings of the partition schema
 *
 * @author Matt
 * @see PartitionSchema
 * @since 17-11-2006
 */
public class PartitionSchemaEditor extends MetadataEditor<PartitionSchema> {
  private static final Class<?> PKG = PartitionSchemaEditor.class; // For Translator

  // Name
  private Text wName;

  // Dynamic definition?
  private Button wDynamic;
  private TextVar wNumber;

  // Partitions
  private Label wlPartitions;
  private TableView wPartitions;

  public PartitionSchemaEditor(
      HopGui hopGui, MetadataManager<PartitionSchema> manager, PartitionSchema metadata) {
    super(hopGui, manager, metadata);
  }

  @Override
  public void createControl(Composite parent) {

    PropsUi props = PropsUi.getInstance();

    int margin = props.getMargin();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    // The rest stays above the buttons, so we added those first...

    // What's the schema name??
    //
    Label wIcon = new Label(parent, SWT.RIGHT);
    wIcon.setImage(getImage());
    FormData fdlicon = new FormData();
    fdlicon.top = new FormAttachment(0, 0);
    fdlicon.right = new FormAttachment(100, 0);
    wIcon.setLayoutData(fdlicon);
    props.setLook(wIcon);

    // What's the name
    Label wlName = new Label(parent, SWT.RIGHT);
    props.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "PartitionSchemaDialog.PartitionName.Label"));
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(0, 0);
    fdlName.left = new FormAttachment(0, 0);
    wlName.setLayoutData(fdlName);

    wName = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(wlName, margin);
    fdName.left = new FormAttachment(0, 0);
    fdName.right = new FormAttachment(wIcon, -margin);
    wName.setLayoutData(fdName);

    Label spacer = new Label(parent, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdSpacer = new FormData();
    fdSpacer.left = new FormAttachment(0, 0);
    fdSpacer.top = new FormAttachment(wName, 15);
    fdSpacer.right = new FormAttachment(100, 0);
    spacer.setLayoutData(fdSpacer);

    // Is the schema defined dynamically using the number?
    //
    wDynamic = new Button(parent, SWT.CHECK);
    props.setLook(wDynamic);
    wDynamic.setText(BaseMessages.getString(PKG, "PartitionSchemaDialog.Dynamic.Label"));
    wDynamic.setToolTipText(BaseMessages.getString(PKG, "PartitionSchemaDialog.Dynamic.Tooltip"));
    FormData fdDynamic = new FormData();
    fdDynamic.top = new FormAttachment(spacer, margin);
    fdDynamic.left = new FormAttachment(0, 0);
    fdDynamic.right = new FormAttachment(100, 0);
    wDynamic.setLayoutData(fdDynamic);

    // If dynamic is chosen, disable to list of partition names
    //
    wDynamic.addListener(SWT.Selection, e -> enableFields());

    // The number of partitions
    //
    Label wlNumber = new Label(parent, SWT.LEFT);
    props.setLook(wlNumber);
    wlNumber.setText(BaseMessages.getString(PKG, "PartitionSchemaDialog.Number.Label"));
    FormData fdlNumber = new FormData();
    fdlNumber.top = new FormAttachment(wDynamic, 15);
    fdlNumber.left = new FormAttachment(0, 0);
    fdlNumber.right = new FormAttachment(100, 0);
    wlNumber.setLayoutData(fdlNumber);

    wNumber =
        new TextVar(
            this.getHopGui().getVariables(),
            parent,
            SWT.LEFT | SWT.BORDER | SWT.SINGLE,
            BaseMessages.getString(PKG, "PartitionSchemaDialog.Number.Tooltip"));
    props.setLook(wNumber);
    FormData fdNumber = new FormData();
    fdNumber.top = new FormAttachment(wlNumber, margin);
    fdNumber.left = new FormAttachment(0, 0);
    fdNumber.right = new FormAttachment(100, 0);
    wNumber.setLayoutData(fdNumber);



    // Schema list:
    wlPartitions = new Label(parent, SWT.LEFT);
    wlPartitions.setText(BaseMessages.getString(PKG, "PartitionSchemaDialog.Partitions.Label"));
    props.setLook(wlPartitions);
    FormData fdlPartitions = new FormData();
    fdlPartitions.left = new FormAttachment(0, 0);
    fdlPartitions.right = new FormAttachment(100, 0);
    fdlPartitions.top = new FormAttachment(wNumber, margin);
    wlPartitions.setLayoutData(fdlPartitions);

    ColumnInfo[] partitionColumns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "PartitionSchemaDialog.PartitionID.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
        };
    wPartitions =
        new TableView(
            manager.getVariables(),
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            partitionColumns,
            1,
            null,
            props);
    props.setLook(wPartitions);
    FormData fdPartitions = new FormData();
    fdPartitions.left = new FormAttachment(0, 0);
    fdPartitions.right = new FormAttachment(100, 0);
    fdPartitions.top = new FormAttachment(wlPartitions, margin);
    fdPartitions.bottom = new FormAttachment(100, -margin * 2);
    wPartitions.setLayoutData(fdPartitions);

    setWidgetsContent();

    // Add listener to detect change after loading data
    ModifyListener lsMod = e -> setChanged();
    wName.addModifyListener(lsMod);
    wNumber.addModifyListener(lsMod);
    wDynamic.addListener(SWT.Selection, e -> setChanged());
    wPartitions.addModifyListener(lsMod);
  }

  private void enableFields() {
    boolean tableEnabled = !wDynamic.getSelection();
    wlPartitions.setEnabled(tableEnabled);
    wPartitions.setEnabled(tableEnabled);
  }

  @Override
  public void setWidgetsContent() {
    PartitionSchema partitionSchema = getMetadata();
    
    wName.setText(Const.NVL(partitionSchema.getName(), ""));

    refreshPartitions();

    wDynamic.setSelection(partitionSchema.isDynamicallyDefined());
    wNumber.setText(Const.NVL(partitionSchema.getNumberOfPartitions(), ""));

    enableFields();
  }

  private void refreshPartitions() {
    wPartitions.clearAll(false);
    PartitionSchema partitionSchema = getMetadata();
    List<String> partitionIDs = partitionSchema.getPartitionIDs();
    for (int i = 0; i < partitionIDs.size(); i++) {
      TableItem item = new TableItem(wPartitions.table, SWT.NONE);
      item.setText(1, partitionIDs.get(i));
    }
    wPartitions.removeEmptyRows();
    wPartitions.setRowNums();
    wPartitions.optWidth(true);
  }

  @Override
  public void getWidgetsContent(PartitionSchema partitionSchema) {
    partitionSchema.setName(wName.getText());
    partitionSchema.setNumberOfPartitions( wNumber.getText() );
    partitionSchema.setDynamicallyDefined( wDynamic.getSelection() );

    List<String> parts = new ArrayList<>();

    int nrNonEmptyPartitions = wPartitions.nrNonEmpty();
    for (int i = 0; i < nrNonEmptyPartitions; i++) {
      parts.add(wPartitions.getNonEmpty(i).getText(1));
    }
    partitionSchema.setPartitionIDs(parts);

  }

  @Override
  public boolean setFocus() {
    if (wName == null || wName.isDisposed()) {
      return false;
    }
    return wName.setFocus();
  }
}
