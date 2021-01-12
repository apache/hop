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

package org.apache.hop.ui.pipeline.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PartitionerPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.ModPartitioner;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

import java.util.Arrays;

public class ModPartitionerDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = PipelineDialog.class; // For Translator

  private TransformPartitioningMeta partitioningMeta;
  private ModPartitioner partitioner;
  private String fieldName;

  private Label wlFieldname;
  private CCombo wFieldname;
  private FormData fdlFieldname, fdFieldname;

  public ModPartitionerDialog(
      Shell parent,
      IVariables variables,
      TransformMeta transformMeta,
      TransformPartitioningMeta partitioningMeta,
      PipelineMeta pipelineMeta) {
    super(
        parent,
        variables,
        (BaseTransformMeta) transformMeta.getTransform(),
      pipelineMeta, partitioningMeta.getPartitioner().getDescription() );
    this.transformMeta = transformMeta;
    this.partitioningMeta = partitioningMeta;
    partitioner = (ModPartitioner) partitioningMeta.getPartitioner();
    fieldName = partitioner.getFieldName();
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    props.setLook(shell);
    setShellImage(shell);

    ModifyListener lsMod = e -> partitioningMeta.hasChanged(true);
    changed = partitioningMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(partitioner.getDescription());

    int margin = props.getMargin();

    int middle = props.getMiddlePct();

    wlFieldname = new Label(shell, SWT.RIGHT);
    wlFieldname.setText("Fieldname");
    props.setLook(wlFieldname);
    fdlFieldname = new FormData();
    fdlFieldname.left = new FormAttachment(0, 0);
    fdlFieldname.right = new FormAttachment(middle, -margin);
    fdlFieldname.top = new FormAttachment(0, margin);
    wlFieldname.setLayoutData(fdlFieldname);
    wFieldname = new CCombo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wFieldname.setText(fieldName == null ? "" : fieldName);
    props.setLook(wFieldname);
    wFieldname.addModifyListener(lsMod);
    fdFieldname = new FormData();
    fdFieldname.left = new FormAttachment(middle, 0);
    fdFieldname.top = new FormAttachment(0, margin);
    fdFieldname.right = new FormAttachment(100, 0);
    wFieldname.setLayoutData(fdFieldname);
    try {
      IRowMeta inputFields = pipelineMeta.getPrevTransformFields(variables, transformMeta);
      if (inputFields != null) {
        String[] fieldNames = inputFields.getFieldNames();
        Arrays.sort(fieldNames);
        wFieldname.setItems(fieldNames);
      }
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error obtaining list of input fields:", e);
    }

    // Some buttons
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    fdOk = new FormData();

    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    // Add listeners
    lsCancel = e -> cancel();
    lsOk = e -> ok();

    wCancel.addListener(SWT.Selection, lsCancel);
    wOk.addListener(SWT.Selection, lsOk);

    lsDef =
        new SelectionAdapter() {
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    getData();

    // Set the shell size, based upon previous time, current content......
    //
    setSize();

    partitioningMeta.hasChanged(changed);

    setSize();
    wOk.setEnabled(!StringUtil.isEmpty(wFieldname.getText()));
    ModifyListener modifyListener =
        modifyEvent -> wOk.setEnabled(!StringUtil.isEmpty(wFieldname.getText()));
    wFieldname.addModifyListener(modifyListener);

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wFieldname.setText(fieldName == null ? "" : fieldName);
  }

  private void cancel() {
    transformName = null;
    partitioningMeta.hasChanged(changed);
    dispose();
  }

  private void ok() {
    fieldName = wFieldname.getText();
    partitioner.setFieldName(fieldName);
    dispose();
  }

  private void setShellImage(Shell shell) {
    IPlugin plugin =
        PluginRegistry.getInstance().getPlugin(PartitionerPluginType.class, partitioner.getId());
    if (!Utils.isEmpty(plugin.getDocumentationUrl())) {
      createHelpButton(shell, transformMeta, plugin);
    }

    shell.setImage(GuiResource.getInstance().getImageHopUi());
  }
}
