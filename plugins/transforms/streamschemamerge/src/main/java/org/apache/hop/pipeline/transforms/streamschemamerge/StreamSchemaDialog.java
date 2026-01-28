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

import java.util.List;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transform.stream.Stream;
import org.apache.hop.pipeline.transform.stream.StreamIcon;
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

    buildButtonBar().ok(e -> ok()).get(e -> get()).cancel(e -> cancel()).build();

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

    final int FieldsCols = 1;
    final int FieldsRows = meta.getNumberOfTransforms();

    previousTransforms = pipelineMeta.getPrevTransformNames(transformName);

    ColumnInfo[] colinf = new ColumnInfo[FieldsCols];
    colinf[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "StreamSchemaTransformDialog.TransformName.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            previousTransforms,
            false);

    wTransforms =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            FieldsRows,
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
    Table table = wTransforms.table;
    if (meta.getNumberOfTransforms() > 0) {
      table.removeAll();
    }
    String[] transformNames = meta.getTransformsToMerge();
    for (int i = 0; i < transformNames.length; i++) {
      TableItem ti = new TableItem(table, SWT.NONE);
      ti.setText(0, "" + (i + 1));
      if (transformNames[i] != null) {
        ti.setText(1, transformNames[i]);
      }
    }

    wTransforms.removeEmptyRows();
    wTransforms.setRowNums();
    wTransforms.optWidth(true);
  }

  /** Populates the table with a list of fields that have incoming hops */
  private void get() {
    wTransforms.removeAll();
    Table table = wTransforms.table;

    for (int i = 0; i < previousTransforms.length; i++) {
      TableItem ti = new TableItem(table, SWT.NONE);
      ti.setText(0, "" + (i + 1));
      ti.setText(1, previousTransforms[i]);
    }
    wTransforms.removeEmptyRows();
    wTransforms.setRowNums();
    wTransforms.optWidth(true);
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

  /**
   * Helping method to update meta information when ok is selected
   *
   * @param inputTransforms Names of the transforms that are being merged together
   */
  private void getMeta(String[] inputTransforms) {
    List<IStream> infoStreams = meta.getTransformIOMeta().getInfoStreams();

    if (infoStreams.isEmpty() || inputTransforms.length < infoStreams.size()) {
      if (inputTransforms.length != 0) {
        for (String inputTransform : inputTransforms) {
          meta.getTransformIOMeta()
              .addStream(new Stream(IStream.StreamType.INFO, null, "", StreamIcon.INFO, null));
        }
        infoStreams = meta.getTransformIOMeta().getInfoStreams();
      }
    } else if (infoStreams.size() < inputTransforms.length) {
      int requiredStreams = inputTransforms.length - infoStreams.size();

      for (int i = 0; i < requiredStreams; i++) {
        meta.getTransformIOMeta()
            .addStream(new Stream(IStream.StreamType.INFO, null, "", StreamIcon.INFO, null));
      }
      infoStreams = meta.getTransformIOMeta().getInfoStreams();
    }
    int streamCount = infoStreams.size();

    String[] transformsToMerge = meta.getTransformsToMerge();
    for (int i = 0; i < streamCount; i++) {
      String transform = transformsToMerge[i];
      IStream infoStream = infoStreams.get(i);
      infoStream.setTransformMeta(pipelineMeta.findTransform(transform));
      infoStream.setSubject(transform);
    }
  }

  /** Called when the user confirms the dialog */
  private void ok() {
    // The "TransformName" variable will be the return value for the open() method.
    // Setting to transform name from the dialog control
    transformName = wTransformName.getText();
    // set output field name

    int nrtransforms = wTransforms.nrNonEmpty();
    String[] transformNames = new String[nrtransforms];
    for (int i = 0; i < nrtransforms; i++) {
      TableItem ti = wTransforms.getNonEmpty(i);
      TransformMeta tm = pipelineMeta.findTransform(ti.getText(1));
      if (tm != null) {
        transformNames[i] = tm.getName();
      }
    }
    meta.setTransformsToMerge(transformNames);
    getMeta(transformNames);

    // close the SWT dialog window
    dispose();
  }
}
