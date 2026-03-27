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

package org.apache.hop.pipeline.transforms.streamschemamerge;

import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;

public class StreamSchemaDialog extends BaseTransformDialog {

  private static final Class<?> PKG = StreamSchemaMeta.class;

  // this is the object the stores the transform's settings
  // the dialog reads the settings from it when opening
  // the dialog writes the settings to it when confirmed
  private StreamSchemaMeta meta;

  private String[] previousTransforms; // transforms sending data in to this transform

  // text field holding the name of the field to add to the row stream
  private TableView wTransforms;

  public StreamSchemaDialog(
      Shell parent,
      IVariables variables,
      StreamSchemaMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    meta = transformMeta;
  }

  /**
   * The constructor should simply invoke super() and save the incoming meta object to a local
   * variable, so it can conveniently read and write settings from/to it.
   *
   * <p>or null if the user cancelled the dialog.
   */
  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "StreamSchemaTransform.Shell.Title"));

    buildButtonBar()
        .ok(e -> ok())
        .get(e -> getSourceTransformNames())
        .cancel(e -> cancel())
        .build();

    // Save the value of the changed flag on the meta object. If the user cancels
    // the dialog, it will be restored to this saved value.
    // The "changed" variable is inherited from BaseTransformDialog
    changed = meta.hasChanged();

    // The ModifyListener used on all controls. It will update the meta object to
    // indicate that changes are being made.
    ModifyListener lsMod = e -> meta.setChanged();

    // Table with fields for inputting transform names
    Label wlTransforms = new Label(shell, SWT.NONE);
    wlTransforms.setText(
        BaseMessages.getString(PKG, "StreamSchemaTransformDialog.Transforms.Label"));
    PropsUi.setLook(wlTransforms);
    FormData fdlTransforms = new FormData();
    fdlTransforms.left = new FormAttachment(0, 0);
    fdlTransforms.top = new FormAttachment(wSpacer, margin);
    wlTransforms.setLayoutData(fdlTransforms);

    previousTransforms = pipelineMeta.getPrevTransformNames(transformName);

    ColumnInfo[] columnInfos =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "StreamSchemaTransformDialog.TransformName.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              previousTransforms,
              false)
        };

    wTransforms =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columnInfos,
            1,
            lsMod,
            props);

    FormData fdTransforms = new FormData();
    fdTransforms.left = new FormAttachment(0, 0);
    fdTransforms.top = new FormAttachment(wlTransforms, margin);
    fdTransforms.right = new FormAttachment(100, 0);
    fdTransforms.bottom = new FormAttachment(wOk, -margin);
    wTransforms.setLayoutData(fdTransforms);

    // populate the dialog with the values from the meta object
    populateDialog();

    // restore the changed flag to original value, as the modify listeners fire during dialog
    // population
    meta.setChanged(changed);

    // open dialog and enter event loop
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    // at this point the dialog has closed, so either ok() or cancel() have been executed
    // The "TransformName" variable is inherited from BaseTransformDialog
    return transformName;
  }

  /**
   * This helper method puts the transform configuration stored in the meta object and puts it into
   * the dialog controls.
   */
  private void populateDialog() {
    for (StreamSchemaMeta.TransformToMerge transformToMerge : meta.getTransformsToMerge()) {
      TableItem ti = new TableItem(wTransforms.table, SWT.NONE);
      ti.setText(1, Const.NVL(transformToMerge.getName(), ""));
    }
    wTransforms.optimizeTableView();
  }

  /** Populates the table with a list of fields that have incoming hops */
  private void getSourceTransformNames() {
    wTransforms.removeAll();
    Table table = wTransforms.table;

    for (String previousTransform : previousTransforms) {
      TableItem ti = new TableItem(table, SWT.NONE);
      ti.setText(1, previousTransform);
    }
    wTransforms.optimizeTableView();
  }

  /** Called when the user cancels the dialog. */
  private void cancel() {
    // The "TransformName" variable will be the return value for the open() method.
    // Setting to null to indicate that dialog was cancelled.
    transformName = null;
    // Restoring original "changed" flag on the met aobject
    meta.setChanged(changed);
    // close the SWT dialog window
    dispose();
  }

  /** Called when the user confirms the dialog */
  private void ok() {
    // The "TransformName" variable will be the return value for the open() method.
    // Setting to transform name from the dialog control
    transformName = wTransformName.getText();
    // set output field name

    meta.getTransformsToMerge().clear();
    for (TableItem item : wTransforms.getNonEmptyItems()) {
      StreamSchemaMeta.TransformToMerge transformToMerge = new StreamSchemaMeta.TransformToMerge();
      transformToMerge.setName(item.getText(1));
      meta.getTransformsToMerge().add(transformToMerge);
    }
    meta.resetTransformIoMeta();

    // close the SWT dialog window
    dispose();
  }
}
