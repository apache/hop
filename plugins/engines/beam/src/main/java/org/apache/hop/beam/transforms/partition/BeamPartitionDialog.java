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

package org.apache.hop.beam.transforms.partition;

import org.apache.hop.beam.core.BeamDefaults;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/** Issue #2040: dialog for the Beam partition transform. */
public class BeamPartitionDialog extends BaseTransformDialog {
  private static final Class<?> PKG = BeamPartitionDialog.class;
  private final BeamPartitionMeta input;

  private Label wlKeyField;
  private Label wlNumPartitions;
  private Combo wPartitionType;
  private TextVar wKeyField;
  private TextVar wNumPartitions;

  public BeamPartitionDialog(
      Shell parent,
      IVariables variables,
      BeamPartitionMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "BeamPartitionDialog.DialogTitle"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = PropsUi.getFormMargin();
    contentLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(contentLayout);

    changed = input.hasChanged();

    Control lastControl = null;

    // Partitioning type
    //
    Label wlPartitionType = new Label(shell, SWT.RIGHT);
    wlPartitionType.setText(BaseMessages.getString(PKG, "BeamPartitionDialog.PartitionType"));
    PropsUi.setLook(wlPartitionType);
    FormData fdlPartitionType = new FormData();
    fdlPartitionType.left = new FormAttachment(0, 0);
    fdlPartitionType.top = new FormAttachment(lastControl, PropsUi.getFormMargin());
    fdlPartitionType.right = new FormAttachment(middle, -PropsUi.getFormMargin());
    wlPartitionType.setLayoutData(fdlPartitionType);

    wPartitionType = new Combo(shell, SWT.READ_ONLY);
    PropsUi.setLook(wPartitionType);
    wPartitionType.setItems(
        new String[] {
          BaseMessages.getString(PKG, "BeamPartitionDialog.PartitionType.Single"),
          BaseMessages.getString(PKG, "BeamPartitionDialog.PartitionType.Key")
        });
    FormData fdPartitionType = new FormData();
    fdPartitionType.left = new FormAttachment(middle, 0);
    fdPartitionType.top = new FormAttachment(wlPartitionType, 0, SWT.CENTER);
    fdPartitionType.right = new FormAttachment(100, 0);
    wPartitionType.setLayoutData(fdPartitionType);
    lastControl = wPartitionType;

    // Key field, only meaningful for the keyed mode.
    //
    wlKeyField = new Label(shell, SWT.RIGHT);
    wlKeyField.setText(BaseMessages.getString(PKG, "BeamPartitionDialog.KeyField"));
    PropsUi.setLook(wlKeyField);
    FormData fdlKeyField = new FormData();
    fdlKeyField.left = new FormAttachment(0, 0);
    fdlKeyField.top = new FormAttachment(lastControl, PropsUi.getFormMargin());
    fdlKeyField.right = new FormAttachment(middle, -PropsUi.getFormMargin());
    wlKeyField.setLayoutData(fdlKeyField);

    wKeyField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wKeyField);
    wKeyField.setToolTipText(BaseMessages.getString(PKG, "BeamPartitionDialog.KeyField.ToolTip"));
    FormData fdKeyField = new FormData();
    fdKeyField.left = new FormAttachment(middle, 0);
    fdKeyField.top = new FormAttachment(wlKeyField, 0, SWT.CENTER);
    fdKeyField.right = new FormAttachment(100, 0);
    wKeyField.setLayoutData(fdKeyField);
    lastControl = wKeyField;

    // Number of partitions
    //
    wlNumPartitions = new Label(shell, SWT.RIGHT);
    wlNumPartitions.setText(BaseMessages.getString(PKG, "BeamPartitionDialog.NumPartitions"));
    PropsUi.setLook(wlNumPartitions);
    FormData fdlNumPartitions = new FormData();
    fdlNumPartitions.left = new FormAttachment(0, 0);
    fdlNumPartitions.top = new FormAttachment(lastControl, PropsUi.getFormMargin());
    fdlNumPartitions.right = new FormAttachment(middle, -PropsUi.getFormMargin());
    wlNumPartitions.setLayoutData(fdlNumPartitions);

    wNumPartitions = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wNumPartitions);
    wNumPartitions.setToolTipText(
        BaseMessages.getString(PKG, "BeamPartitionDialog.NumPartitions.ToolTip"));
    FormData fdNumPartitions = new FormData();
    fdNumPartitions.left = new FormAttachment(middle, 0);
    fdNumPartitions.top = new FormAttachment(wlNumPartitions, 0, SWT.CENTER);
    fdNumPartitions.right = new FormAttachment(100, 0);
    wNumPartitions.setLayoutData(fdNumPartitions);
    lastControl = wNumPartitions;

    // The two settings only matter for keyed partitioning, so grey them out otherwise.
    //
    wPartitionType.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent event) {
            setKeyedFieldsEnabled(wPartitionType.getSelectionIndex() == 1);
          }
        });

    shell.setSize(450, 240);

    getData();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Greys out the settings that only apply to keyed partitioning. */
  private void setKeyedFieldsEnabled(boolean enabled) {
    wlKeyField.setEnabled(enabled);
    wKeyField.setEnabled(enabled);
  }

  /** Populate the widgets from the transform. */
  public void getData() {
    if (BeamDefaults.PARTITION_TYPE_KEY.equals(input.getPartitionType())) {
      wPartitionType.select(1);
    } else {
      wPartitionType.select(0);
    }
    wKeyField.setText(Const.NVL(input.getKeyField(), ""));
    wNumPartitions.setText(Const.NVL(input.getNumPartitions(), "1"));

    setKeyedFieldsEnabled(wPartitionType.getSelectionIndex() == 1);
  }

  /** Cancel and restore the original values. */
  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    input.setPartitionType(
        wPartitionType.getSelectionIndex() == 1
            ? BeamDefaults.PARTITION_TYPE_KEY
            : BeamDefaults.PARTITION_TYPE_SINGLE);
    input.setKeyField(wKeyField.getText());
    input.setNumPartitions(wNumPartitions.getText());

    if (org.apache.hop.core.util.Utils.isEmpty(wTransformName.getText())) {
      return;
    }
    input.setParentTransformMeta(null);
    input.setChanged(true);
    dispose();
  }
}
