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

package org.apache.hop.ui.pipeline.dialog;

import java.util.Arrays;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.HashPartitioner;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;

/** The fields of the Hash partitioner: a table of the incoming fields to partition on. */
public class HashPartitionerDialog extends BaseTransformDialog {
  private static final Class<?> PKG = HashPartitioner.class;

  private final TransformPartitioningMeta partitioningMeta;
  private final HashPartitioner partitioner;
  private GuiCompositeWidgets widgets;

  public HashPartitionerDialog(
      Shell parent,
      IVariables variables,
      TransformMeta transformMeta,
      TransformPartitioningMeta partitioningMeta,
      PipelineMeta pipelineMeta) {
    super(
        parent,
        variables,
        (BaseTransformMeta) transformMeta.getTransform(),
        pipelineMeta,
        partitioningMeta.getPartitioner().getDescription());
    this.transformMeta = transformMeta;
    this.partitioningMeta = partitioningMeta;
    this.partitioner = (HashPartitioner) partitioningMeta.getPartitioner();
  }

  @Override
  public String open() {
    shell = new Shell(getParent(), SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    PropsUi.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "HashPartitioner.Dialog.Title"));
    margin = PropsUi.getMargin();
    middle = props.getMiddlePct();

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    changed = partitioningMeta.hasChanged();
    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell, variables, null, wOk, HashPartitioner.GUI_PLUGIN_ELEMENT_PARENT_ID, partitioner);
    offerInputFields();
    partitioningMeta.hasChanged(changed);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  /** The field column lists the fields that come into the transform. */
  private void offerInputFields() {
    Control control = widgets.getWidgetsMap().get(HashPartitioner.WIDGET_FIELDS);
    if (!(control instanceof TableView tableView)) {
      return;
    }
    try {
      IRowMeta inputFields = pipelineMeta.getPrevTransformFields(variables, transformMeta);
      if (inputFields != null) {
        String[] fieldNames = inputFields.getFieldNames();
        Arrays.sort(fieldNames);
        tableView.getColumns()[0].setComboValues(fieldNames);
      }
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error obtaining list of input fields:", e);
    }
  }

  private void cancel() {
    transformName = null;
    partitioningMeta.hasChanged(changed);
    dispose();
  }

  private void ok() {
    widgets.getWidgetsContents(partitioner, HashPartitioner.GUI_PLUGIN_ELEMENT_PARENT_ID);
    if (partitioner.getFieldNames().isEmpty()) {
      // Nothing to partition on: as a cancel, the partitioning is rolled back
      transformName = null;
    } else {
      partitioningMeta.hasChanged(true);
    }
    dispose();
  }
}
