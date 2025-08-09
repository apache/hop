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

package org.apache.hop.pipeline.transforms.schemamapping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.staticschema.metadata.SchemaDefinition;
import org.apache.hop.staticschema.metadata.SchemaFieldDefinition;
import org.apache.hop.staticschema.util.SchemaDefinitionUtil;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterMappingDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class SchemaMappingDialog extends BaseTransformDialog {
  private static final Class<?> PKG = SchemaMappingDialog.class;

  private final SchemaMappingMeta input;

  private MetaSelectionLine<SchemaDefinition> wSchemaDefinition;

  /* Columns definition for the mapping table */
  private ColumnInfo[] ciFields;

  private TableView wMappingFields;

  private Button wGetFields;

  private Button wDoMapping;

  /** List of ColumnInfo that should have the field names of the selected schema definition */
  private final List<ColumnInfo> schemaFieldColumns = new ArrayList<>();

  /* Stream input fields */
  private final List<String> inputFields = new ArrayList<>();

  public SchemaMappingDialog(
      Shell parent,
      IVariables variables,
      SchemaMappingMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();
    SelectionListener lsSelection =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            fillStaticSchemaFieldset();
          }
        };

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "SchemaMappingDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "SchemaMappingDialog.TransformName.Label"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    wSchemaDefinition =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            SchemaDefinition.class,
            shell,
            SWT.NONE,
            BaseMessages.getString(PKG, "SchemaMappingDialog.SchemaDefinition.Label"),
            BaseMessages.getString(PKG, "SchemaMappingDialog.SchemaDefinition.Tooltip"));

    PropsUi.setLook(wSchemaDefinition);
    FormData fdSchemaDefinition = new FormData();
    fdSchemaDefinition.left = new FormAttachment(0, 0);
    fdSchemaDefinition.top = new FormAttachment(wTransformName, margin);
    fdSchemaDefinition.right = new FormAttachment(100, 0);
    wSchemaDefinition.setLayoutData(fdSchemaDefinition);

    try {
      wSchemaDefinition.fillItems();
    } catch (Exception e) {
      log.logError("Error getting schema definition items", e);
    }

    wSchemaDefinition.addSelectionListener(lsSelection);
    // Some buttons at the bottom
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    buildMappingTable(lsMod, margin);

    //
    // Search the fields in the background
    //

    final Runnable runnable =
        () -> {
          TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
          if (transformMeta != null) {
            try {
              IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);

              // Remember these fields...
              for (int i = 0; i < row.size(); i++) {
                inputFields.add(row.getValueMeta(i).getName());
              }

              setComboBoxes();
            } catch (HopException e) {
              logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
            }
          }
        };
    new Thread(runnable).start();

    setSchemaFieldCombo();
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel.addListener(SWT.Selection, e -> cancel());
    wGetFields.addListener(SWT.Selection, e -> get());

    getData();
    input.setChanged(changed);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void fillStaticSchemaFieldset() {
    String schemaName = wSchemaDefinition.getText();
    if (!Utils.isEmpty(schemaName)) {
      try {
        SchemaDefinition schemaDefinition =
            (new SchemaDefinitionUtil()).loadSchemaDefinition(metadataProvider, schemaName);
        if (schemaDefinition != null) {
          List<SchemaFieldDefinition> fieldDefinitions = schemaDefinition.getFieldDefinitions();

          wMappingFields.table.removeAll();
          wMappingFields.table.setItemCount(fieldDefinitions.size());
          for (int i = 0; i < fieldDefinitions.size(); i++) {
            SchemaFieldDefinition sfd = fieldDefinitions.get(i);
            TableItem item = wMappingFields.table.getItem(i);
            if (sfd.getName() != null) {
              item.setText(1, sfd.getName());
            }
          }
        }
      } catch (HopTransformException e) {
        // ignore any errors here. drop downs will not be
        // filled, but no problem for the user
      }
    }
  }

  private void buildMappingTable(ModifyListener lsMod, int margin) {

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "SchemaMappingDialog.MappingFields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlUpIns = new FormData();
    fdlUpIns.left = new FormAttachment(0, 0);
    fdlUpIns.top = new FormAttachment(wSchemaDefinition, margin);
    wlFields.setLayoutData(fdlUpIns);

    int tableCols = 2;
    int upInsRows =
        (input.getMappingFieldset() != null
                && !input.getMappingFieldset().equals(Collections.emptyList())
            ? input.getMappingFieldset().size()
            : 1);

    ciFields = new ColumnInfo[tableCols];
    ciFields[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "SchemaMappingDialog.ColumnInfo.SchemaField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciFields[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "SchemaMappingDialog.ColumnInfo.StreamField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);

    schemaFieldColumns.add(ciFields[0]);

    wMappingFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciFields,
            upInsRows,
            lsMod,
            props);

    wGetFields = new Button(shell, SWT.PUSH);
    wGetFields.setText(BaseMessages.getString(PKG, "SchemaMappingDialog.GetFields.Button"));
    FormData fdGetFields = new FormData();
    fdGetFields.top = new FormAttachment(wlFields, margin);
    fdGetFields.right = new FormAttachment(100, 0);
    wGetFields.setLayoutData(fdGetFields);

    wDoMapping = new Button(shell, SWT.PUSH);
    wDoMapping.setText(BaseMessages.getString(PKG, "SchemaMappingDialog.DoMapping.Button"));
    FormData fdDoMapping = new FormData();
    fdDoMapping.top = new FormAttachment(wGetFields, margin);
    fdDoMapping.right = new FormAttachment(100, 0);
    wDoMapping.setLayoutData(fdDoMapping);

    wDoMapping.addListener(SWT.Selection, arg0 -> generateMappings());
    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(wDoMapping, -margin);
    fdFields.bottom = new FormAttachment(wOk, -2 * margin);
    wMappingFields.setLayoutData(fdFields);
  }

  /** Fill up the fields table with the incoming fields. */
  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null && !r.isEmpty()) {
        BaseTransformDialog.getFieldsFromPrevious(
            r, wMappingFields, 1, new int[] {1, 2}, new int[] {}, -1, -1, null);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "SchemaMappingDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "SchemaMappingDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    String[] fieldNames = ConstUi.sortFieldNames(inputFields);
    ciFields[1].setComboValues(fieldNames);
  }

  private void setSchemaFieldCombo() {
    Runnable fieldLoader =
        () -> {
          if (!wSchemaDefinition.isDisposed()) {
            final String schemaName = wSchemaDefinition.getText();

            // clear
            for (ColumnInfo colInfo : schemaFieldColumns) {
              colInfo.setComboValues(new String[] {});
            }
            if (!Utils.isEmpty(schemaName)) {
              try {
                SchemaDefinition schemaDefinition =
                    (new SchemaDefinitionUtil()).loadSchemaDefinition(metadataProvider, schemaName);
                if (schemaDefinition != null) {
                  IRowMeta r = schemaDefinition.getRowMeta();
                  if (null != r) {
                    String[] fieldNames = r.getFieldNames();
                    if (null != fieldNames) {
                      for (ColumnInfo colInfo : schemaFieldColumns) {
                        colInfo.setComboValues(fieldNames);
                      }
                    }
                  }
                }
              } catch (HopTransformException | HopPluginException e) {
                for (ColumnInfo colInfo : schemaFieldColumns) {
                  colInfo.setComboValues(new String[] {});
                }
                // ignore any errors here. drop downs will not be
                // filled, but no problem for the user
              }
            }
          }
        };
    shell.getDisplay().asyncExec(fieldLoader);
  }

  /**
   * Reads in the fields from the previous transforms and from the ONE next transform and opens an
   * EnterMappingDialog with this information. After the user did the mapping, those information is
   * put into the Select/Rename table.
   */
  private void generateMappings() {

    // Determine the source and target fields...
    //
    IRowMeta sourceFields;
    IRowMeta targetFields;

    try {
      sourceFields = pipelineMeta.getPrevTransformFields(variables, transformMeta);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(
              PKG, "SchemaMappingDialog.DoMapping.UnableToFindSourceFields.Title"),
          BaseMessages.getString(
              PKG, "SchemaMappingDialog.DoMapping.UnableToFindSourceFields.Message"),
          e);
      return;
    }

    String schemaName = variables.resolve(wSchemaDefinition.getText());
    if (!Utils.isEmpty(schemaName)) {
      try {
        SchemaDefinition schemaDefinition =
            (new SchemaDefinitionUtil()).loadSchemaDefinition(metadataProvider, schemaName);
        targetFields = schemaDefinition.getRowMeta();
      } catch (HopException e) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(
                PKG, "SchemaMappingDialog.DoMapping.UnableToFindSchemaFields.Title"),
            BaseMessages.getString(
                PKG, "SchemaMappingDialog.DoMapping.UnableToFindSchemaFields.Message"),
            e);
        return;
      }

      String[] inputNames = new String[sourceFields.size()];
      for (int i = 0; i < sourceFields.size(); i++) {
        IValueMeta value = sourceFields.getValueMeta(i);
        inputNames[i] = value.getName();
      }

      // Create the existing mapping list...
      //
      List<SourceToTargetMapping> mappings = new ArrayList<>();
      StringBuilder missingSourceFields = new StringBuilder();
      StringBuilder missingTargetFields = new StringBuilder();

      int nrFields = wMappingFields.nrNonEmpty();
      for (int i = 0; i < nrFields; i++) {
        TableItem item = wMappingFields.getNonEmpty(i);
        String source = item.getText(2);
        String target = item.getText(1);

        int sourceIndex = sourceFields.indexOfValue(source);
        if (sourceIndex < 0) {
          missingSourceFields
              .append(Const.CR)
              .append("   ")
              .append(source)
              .append(" --> ")
              .append(target);
        }
        int targetIndex = targetFields.indexOfValue(target);
        if (targetIndex < 0) {
          missingTargetFields
              .append(Const.CR)
              .append("   ")
              .append(source)
              .append(" --> ")
              .append(target);
        }
        if (sourceIndex < 0 || targetIndex < 0) {
          continue;
        }

        SourceToTargetMapping mapping = new SourceToTargetMapping(sourceIndex, targetIndex);
        mappings.add(mapping);
      }

      // show a confirm dialog if some missing field was found
      //
      if (!missingSourceFields.isEmpty() || !missingTargetFields.isEmpty()) {

        String message = "";
        if (!missingSourceFields.isEmpty()) {
          message +=
              BaseMessages.getString(
                      PKG,
                      "SchemaMappingDialog.DoMapping.SomeSourceFieldsNotFound",
                      missingSourceFields.toString())
                  + Const.CR;
        }
        if (!missingTargetFields.isEmpty()) {
          message +=
              BaseMessages.getString(
                      PKG,
                      "SchemaMappingDialog.DoMapping.SomeTargetFieldsNotFound",
                      missingSourceFields.toString())
                  + Const.CR;
        }
        message += Const.CR;
        message +=
            BaseMessages.getString(PKG, "SchemaMappingDialog.DoMapping.SomeFieldsNotFoundContinue")
                + Const.CR;
        int answer =
            BaseDialog.openMessageBox(
                shell,
                BaseMessages.getString(
                    PKG, "SchemaMappingDialog.DoMapping.SomeFieldsNotFoundTitle"),
                message,
                SWT.ICON_QUESTION | SWT.YES | SWT.NO);
        boolean goOn = (answer & SWT.YES) != 0;
        if (!goOn) {
          return;
        }
      }
      EnterMappingDialog d =
          new EnterMappingDialog(
              SchemaMappingDialog.this.shell,
              sourceFields.getFieldNames(),
              targetFields.getFieldNames(),
              mappings);
      mappings = d.open();

      // mappings == null if the user pressed cancel
      //
      if (mappings != null) {
        // Clear and re-populate!
        //
        wMappingFields.table.removeAll();
        wMappingFields.table.setItemCount(mappings.size());
        for (int i = 0; i < mappings.size(); i++) {
          SourceToTargetMapping mapping = mappings.get(i);
          TableItem item = wMappingFields.table.getItem(i);
          item.setText(2, sourceFields.getValueMeta(mapping.getSourcePosition()).getName());
          item.setText(1, targetFields.getValueMeta(mapping.getTargetPosition()).getName());
        }
        wMappingFields.setRowNums();
        wMappingFields.optWidth(true);
        input.setChanged(changed);
      }
    } else {
      MessageBox mb = new MessageBox(shell, SWT.ICON_ERROR | SWT.OK);
      mb.setMessage(
          BaseMessages.getString(
              PKG, "SchemaMappingDialog.DoMapping.SchemaNameNotProvided.Message"));
      mb.setText(
          BaseMessages.getString(PKG, "SchemaMappingDialog.DoMapping.SchemaNameNotProvided.Title"));
      mb.open();
    }
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }
    // Get the information for the dialog into the input structure.
    getInfo();
    dispose();
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (!Utils.isEmpty(input.getSchemaName())) wSchemaDefinition.setText(input.getSchemaName());

    if (input.getMappingFieldset() != null) {
      for (int i = 0; i < input.getMappingFieldset().size(); i++) {
        SchemaMappingField mf = input.getMappingFieldset().get(i);
        TableItem item = wMappingFields.table.getItem(i);
        if (mf.getFieldSchemaDefinition() != null) {
          item.setText(1, mf.getFieldSchemaDefinition());
        }
        if (mf.getFieldStream() != null) {
          item.setText(2, mf.getFieldStream());
        }
      }
    }
  }

  private void getInfo() {

    input.setSchemaName(wSchemaDefinition.getText());
    int nrRows = wMappingFields.nrNonEmpty();

    HashMap<String, SchemaMappingField> smfMap = new HashMap<>();
    for (int i = 0; i < nrRows; i++) {
      TableItem item = wMappingFields.getNonEmpty(i);
      SchemaMappingField sf =
          new SchemaMappingField(item.getText(1), Const.NVL(item.getText(2), ""));
      smfMap.put(item.getText(1), sf);
    }

    String schemaName = variables.resolve(wSchemaDefinition.getText());
    if (!Utils.isEmpty(schemaName)) {
      try {
        SchemaDefinition schemaDefinition =
            (new SchemaDefinitionUtil()).loadSchemaDefinition(metadataProvider, schemaName);

        if (input.getMappingFieldset() != null) input.getMappingFieldset().clear();
        ArrayList<SchemaFieldDefinition> staticFieldset =
            (ArrayList<SchemaFieldDefinition>) schemaDefinition.getFieldDefinitions();
        for (int i = 0; i < staticFieldset.size(); i++) {
          SchemaFieldDefinition sfItem = staticFieldset.get(i);
          if (smfMap.get(sfItem.getName()) != null)
            input.getMappingFieldset().add(smfMap.get(sfItem.getName()));
        }

      } catch (HopException e) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(
                PKG, "SchemaMappingDialog.DoMapping.UnableToFindSchemaFields.Title"),
            BaseMessages.getString(
                PKG, "SchemaMappingDialog.DoMapping.UnableToFindSchemaFields.Message"),
            e);
        return;
      }
    }

    transformName = wTransformName.getText();
  }
}
