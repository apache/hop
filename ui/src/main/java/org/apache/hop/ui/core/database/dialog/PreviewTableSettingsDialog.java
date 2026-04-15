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

package org.apache.hop.ui.core.database.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/** Asks for row limit and JDBC query timeout before a database table preview. */
public class PreviewTableSettingsDialog extends Dialog {
  private static final Class<?> PKG = PreviewTableSettingsDialog.class;

  /** Row limit and {@link java.sql.Statement#setQueryTimeout(int)} in seconds (0 = no limit). */
  public static final class Settings {
    public final int rowLimit;
    public final int queryTimeoutSeconds;

    public Settings(int rowLimit, int queryTimeoutSeconds) {
      this.rowLimit = rowLimit;
      this.queryTimeoutSeconds = queryTimeoutSeconds;
    }
  }

  private final Shell parentShell;
  private final int initialRowLimit;

  /** Hop variable space for default timeout in the dialog; may be {@code null}. */
  private final IVariables hopVariables;

  /**
   * When {@code true}, {@link #open()} on OK clones {@code hopVariables} and sets {@code
   * HOP_QUERY_PREVIEW_TIMEOUT} on the clone; use {@link #getPreviewExecutionVariables()} for
   * pipeline preview.
   */
  private final boolean forTableInputPipelinePreview;

  private Shell shell;
  private Text wRows;
  private Text wTimeout;
  private Settings result;
  private Variables previewExecutionVariables;
  private final PropsUi props = PropsUi.getInstance();

  /**
   * @param hopVariables context for resolving default timeout (may be null unless {@code
   *     forTableInputPipelinePreview})
   * @param forTableInputPipelinePreview when true, {@code hopVariables} is required; OK builds
   *     {@link #getPreviewExecutionVariables()}
   */
  public PreviewTableSettingsDialog(
      Shell parent,
      int initialRowLimit,
      IVariables hopVariables,
      boolean forTableInputPipelinePreview) {
    super(parent, SWT.NONE);
    this.parentShell = parent;
    this.initialRowLimit = initialRowLimit;
    this.hopVariables = hopVariables;
    this.forTableInputPipelinePreview = forTableInputPipelinePreview;
    if (forTableInputPipelinePreview && hopVariables == null) {
      throw new IllegalArgumentException(
          "hopVariables is required when forTableInputPipelinePreview is true");
    }
  }

  private static int defaultTimeoutFieldSeconds(IVariables hopVariables) {
    IVariables forResolve = hopVariables != null ? hopVariables : new Variables();
    String raw =
        forResolve.getVariable(
            Const.HOP_QUERY_PREVIEW_TIMEOUT,
            EnvUtil.getSystemProperty(Const.HOP_QUERY_PREVIEW_TIMEOUT, "20"));
    return Math.max(0, Const.toInt(forResolve.resolve(raw), 0));
  }

  /**
   * @return preview settings, or {@code null} if the user cancelled
   */
  public Settings open() {
    result = null;
    previewExecutionVariables = null;
    int initialQueryTimeoutSeconds = defaultTimeoutFieldSeconds(hopVariables);

    shell = new Shell(parentShell, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.SHEET);
    PropsUi.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    shell.setText(BaseMessages.getString(PKG, "PreviewTableSettingsDialog.Title"));

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = BaseDialog.MARGIN_SIZE;
    formLayout.marginHeight = BaseDialog.MARGIN_SIZE;
    shell.setLayout(formLayout);

    Label wlRows = new Label(shell, SWT.LEFT);
    wlRows.setText(BaseMessages.getString(PKG, "PreviewTableSettingsDialog.Rows.Label"));
    PropsUi.setLook(wlRows);
    FormData fdlRows = new FormData();
    fdlRows.left = new FormAttachment(0, 0);
    fdlRows.top = new FormAttachment(0, 0);
    wlRows.setLayoutData(fdlRows);

    wRows = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wRows);
    wRows.setText(Integer.toString(initialRowLimit));
    FormData fdRows = new FormData();
    fdRows.left = new FormAttachment(0, 0);
    fdRows.top = new FormAttachment(wlRows, BaseDialog.LABEL_SPACING);
    fdRows.right = new FormAttachment(100, 0);
    wRows.setLayoutData(fdRows);

    Label wlRowsHint = new Label(shell, SWT.LEFT);
    wlRowsHint.setText(BaseMessages.getString(PKG, "PreviewTableSettingsDialog.Rows.Hint"));
    PropsUi.setLook(wlRowsHint);
    FormData fdlRowsHint = new FormData();
    fdlRowsHint.left = new FormAttachment(0, 0);
    fdlRowsHint.top = new FormAttachment(wRows, BaseDialog.LABEL_SPACING);
    fdlRowsHint.right = new FormAttachment(100, 0);
    wlRowsHint.setLayoutData(fdlRowsHint);

    Label wlTimeout = new Label(shell, SWT.LEFT);
    wlTimeout.setText(BaseMessages.getString(PKG, "PreviewTableSettingsDialog.Timeout.Label"));
    PropsUi.setLook(wlTimeout);
    FormData fdlTimeout = new FormData();
    fdlTimeout.left = new FormAttachment(0, 0);
    fdlTimeout.top = new FormAttachment(wlRowsHint, BaseDialog.ELEMENT_SPACING);
    wlTimeout.setLayoutData(fdlTimeout);

    wTimeout = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTimeout);
    wTimeout.setText(Integer.toString(initialQueryTimeoutSeconds));
    FormData fdTimeout = new FormData();
    fdTimeout.left = new FormAttachment(0, 0);
    fdTimeout.top = new FormAttachment(wlTimeout, BaseDialog.LABEL_SPACING);
    fdTimeout.right = new FormAttachment(100, 0);
    wTimeout.setLayoutData(fdTimeout);

    Label wlTimeoutHint = new Label(shell, SWT.LEFT);
    wlTimeoutHint.setText(BaseMessages.getString(PKG, "PreviewTableSettingsDialog.Timeout.Hint"));
    PropsUi.setLook(wlTimeoutHint);
    FormData fdlTimeoutHint = new FormData();
    fdlTimeoutHint.left = new FormAttachment(0, 0);
    fdlTimeoutHint.top = new FormAttachment(wTimeout, BaseDialog.LABEL_SPACING);
    fdlTimeoutHint.right = new FormAttachment(100, 0);
    wlTimeoutHint.setLayoutData(fdlTimeoutHint);

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString("System.Button.OK"));
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString("System.Button.Cancel"));
    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wOk, wCancel}, PropsUi.getMargin(), wlTimeoutHint);

    wOk.addListener(SWT.Selection, e -> ok());
    wCancel.addListener(SWT.Selection, e -> cancel());
    shell.setDefaultButton(wOk);

    wRows.setFocus();
    wRows.selectAll();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel(), false);
    return result;
  }

  /**
   * After a successful {@link #open()} with {@code forTableInputPipelinePreview}, returns a clone
   * of the constructor {@code hopVariables} with {@code HOP_QUERY_PREVIEW_TIMEOUT} set from the
   * dialog. Otherwise {@code null}.
   */
  public Variables getPreviewExecutionVariables() {
    return previewExecutionVariables;
  }

  private void ok() {
    try {
      int rows = Integer.parseInt(wRows.getText().trim());
      int timeout = Integer.parseInt(wTimeout.getText().trim());
      result = new Settings(Math.max(0, rows), Math.max(0, timeout));
      if (forTableInputPipelinePreview) {
        previewExecutionVariables = new Variables();
        previewExecutionVariables.copyFrom(hopVariables);
        previewExecutionVariables.setVariable(
            Const.HOP_QUERY_PREVIEW_TIMEOUT, Integer.toString(result.queryTimeoutSeconds));
      }
      dispose();
    } catch (NumberFormatException e) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG, "PreviewTableSettingsDialog.Error.InvalidNumber"));
      mb.setText(BaseMessages.getString(PKG, "PreviewTableSettingsDialog.Error.Title"));
      mb.open();
    }
  }

  private void cancel() {
    result = null;
    previewExecutionVariables = null;
    dispose();
  }

  private void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }
}
