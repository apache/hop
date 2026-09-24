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

package org.apache.hop.ui.pipeline.transform;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ComboItems;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;

/**
 * The fields that flow into a transform, as a dialog needs them to fill its field drop-downs.
 *
 * <p>The fields are fetched once and cached for the lifetime of the dialog. When the upstream
 * transforms cannot describe their output (a broken query, a missing file, ...) this falls back to
 * an empty row, shows the error to the user once, and never throws. Combined with {@link
 * #fillCombos(Control...)} that means a dialog keeps every value the user configured, whether or
 * not the incoming fields can be loaded.
 *
 * <p>Call it from the UI thread: the first failure opens an error dialog.
 */
public class PreviousFields {
  private static final Class<?> PKG = ITransform.class;

  private final Shell shell;
  private final IVariables variables;
  private final PipelineMeta pipelineMeta;
  private final String transformName;

  private IRowMeta rowMeta;
  private Exception failure;

  /**
   * @param shell the dialog shell, parent of the error dialog
   * @param variables the variables to resolve the upstream transforms with
   * @param pipelineMeta the pipeline the transform is part of
   * @param transformName the name of the transform whose input fields are wanted
   */
  public PreviousFields(
      Shell shell, IVariables variables, PipelineMeta pipelineMeta, String transformName) {
    this.shell = shell;
    this.variables = variables;
    this.pipelineMeta = pipelineMeta;
    this.transformName = transformName;
  }

  /**
   * @return the incoming fields, or an empty row when they could not be determined. Never null.
   */
  public IRowMeta getRowMeta() {
    if (rowMeta == null) {
      load();
    }
    return rowMeta;
  }

  /**
   * @return the names of the incoming fields, in stream order. Never null.
   */
  public String[] getFieldNames() {
    return getRowMeta().getFieldNames();
  }

  /**
   * @return true when the incoming fields were determined. False when there is no upstream
   *     transform description to work with, for example because an upstream transform failed.
   */
  public boolean isAvailable() {
    getRowMeta();
    return failure == null;
  }

  /**
   * Puts the incoming field names in the drop-down of each combo, keeping the value every combo
   * currently shows. See {@link ComboItems#setItemsKeepingText(Control, String[])}.
   *
   * @param combos the combos to fill
   */
  public void fillCombos(Control... combos) {
    String[] fieldNames = getFieldNames();
    for (Control combo : combos) {
      if (combo != null && !combo.isDisposed()) {
        ComboItems.setItemsKeepingText(combo, fieldNames);
      }
    }
  }

  /** Forgets the cached fields, so the next call fetches them again and may report a new error. */
  public void refresh() {
    rowMeta = null;
    failure = null;
  }

  private void load() {
    try {
      IRowMeta fields = pipelineMeta.getPrevTransformFields(variables, transformName);
      rowMeta = fields == null ? new RowMeta() : fields;
      failure = null;
    } catch (Exception e) {
      // Also catch runtime exceptions: a buggy upstream getFields() must not break this dialog.
      rowMeta = new RowMeta();
      failure = e;
      if (shell != null && !shell.isDisposed()) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(
                PKG, "BaseTransformDialog.FailedToGetFieldsPrevious.DialogTitle"),
            BaseMessages.getString(
                PKG, "BaseTransformDialog.FailedToGetFieldsPrevious.DialogMessage"),
            e);
      }
    }
  }
}
