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

package org.apache.hop.ui.hopgui.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.List;

public class PreviewSelectDialog extends Dialog {
  private static final Class<?> PKG = PreviewSelectDialog.class; // For Translator

  private TableView wFields;

  private Shell shell;
  private PipelineMeta pipelineMeta;

  public String[] previewTransforms;
  public int[] previewSizes;

  private PropsUi props;

  public PreviewSelectDialog(Shell parent, int style, PipelineMeta pipelineMeta) {
    super(parent, style);

    this.pipelineMeta = pipelineMeta;
    this.props = PropsUi.getInstance();

    previewTransforms = null;
    previewSizes = null;
  }

  public void open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageHopUi());

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(
        BaseMessages.getString(
            PKG, "PreviewSelectDialog.Dialog.PreviewSelection.Title")); // Preview
    // selection
    // screen
    shell.setImage(GuiResource.getInstance().getImageHopUi());

    int margin = props.getMargin();

    // Buttons at the bottom
    //
    Button wPreview = new Button(shell, SWT.PUSH);
    wPreview.setText(BaseMessages.getString(PKG, "System.Button.Show"));
    wPreview.addListener(SWT.Selection, e -> preview());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Close"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wPreview, wCancel}, margin, null);

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(
        BaseMessages.getString(PKG, "PreviewSelectDialog.Label.Transforms")); // Transforms:
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(0, margin);
    wlFields.setLayoutData(fdlFields);

    List<TransformMeta> usedTransforms = pipelineMeta.getUsedTransforms();
    final int nrRows = usedTransforms.size();

    ColumnInfo[] columns = {
      new ColumnInfo(
          BaseMessages.getString(PKG, "PreviewSelectDialog.Column.TransformName"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true), // TransformName
      new ColumnInfo(
          BaseMessages.getString(PKG, "PreviewSelectDialog.Column.PreviewSize"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          false), // Preview size
    };

    wFields =
        new TableView(
            HopGui.getInstance().getVariables(),
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            nrRows,
            true, // read-only
            null,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wPreview, -2 * margin);
    wFields.setLayoutData(fdFields);

    getData();

    BaseDialog.defaultShellHandling(shell, c -> preview(), c -> cancel());
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    String[] prTransforms = props.getLastPreview();
    int[] prSizes = props.getLastPreviewSize();
    String name;
    List<TransformMeta> selectedTransforms = pipelineMeta.getSelectedTransforms();
    List<TransformMeta> usedTransforms = pipelineMeta.getUsedTransforms();

    if (selectedTransforms.size() == 0) {

      int line = 0;
      for (TransformMeta transformMeta : usedTransforms) {

        TableItem item = wFields.table.getItem(line++);
        name = transformMeta.getName();
        item.setText(1, transformMeta.getName());
        item.setText(2, "0");

        // Remember the last time...?
        for (int x = 0; x < prTransforms.length; x++) {
          if (prTransforms[x].equalsIgnoreCase(name)) {
            item.setText(2, "" + prSizes[x]);
          }
        }
      }
    } else {
      // No previous selection: set the selected transforms to the default preview size
      //
      int line = 0;
      for (TransformMeta transformMeta : usedTransforms) {
        TableItem item = wFields.table.getItem(line++);
        name = transformMeta.getName();
        item.setText(1, transformMeta.getName());
        item.setText(2, "");

        // Is the transform selected?
        if (transformMeta.isSelected()) {
          item.setText(2, "" + props.getDefaultPreviewSize());
        }
      }
    }

    wFields.optWidth(true);
  }

  private void cancel() {
    dispose();
  }

  private void preview() {
    int sels = 0;
    for (int i = 0; i < wFields.table.getItemCount(); i++) {
      TableItem ti = wFields.table.getItem(i);
      int size = Const.toInt(ti.getText(2), 0);
      if (size > 0) {
        sels++;
      }
    }

    previewTransforms = new String[sels];
    previewSizes = new int[sels];

    sels = 0;
    for (int i = 0; i < wFields.table.getItemCount(); i++) {
      TableItem ti = wFields.table.getItem(i);
      int size = Const.toInt(ti.getText(2), 0);

      if (size > 0) {
        previewTransforms[sels] = ti.getText(1);
        previewSizes[sels] = size;

        sels++;
      }
    }

    props.setLastPreview(previewTransforms, previewSizes);

    dispose();
  }
}
