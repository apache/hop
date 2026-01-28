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

package org.apache.hop.pipeline.transforms.blockingtransform;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class BlockingTransformDialog extends BaseTransformDialog {
  private static final Class<?> PKG = BlockingTransformDialog.class;

  private final BlockingTransformMeta input;

  private Button wPassAllRows;

  private Label wlSpoolDir;
  private Button wbSpoolDir;
  private TextVar wSpoolDir;

  private Label wlPrefix;
  private Text wPrefix;

  private Label wlCacheSize;
  private Text wCacheSize;

  private Label wlCompress;
  private Button wCompress;

  public BlockingTransformDialog(
      Shell parent,
      IVariables variables,
      BlockingTransformMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "BlockingTransformDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ScrolledComposite scrolledComposite = new ScrolledComposite(shell, SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(scrolledComposite);
    FormData fdScrolledComposite = new FormData();
    fdScrolledComposite.left = new FormAttachment(0, 0);
    fdScrolledComposite.top = new FormAttachment(wSpacer, 0);
    fdScrolledComposite.right = new FormAttachment(100, 0);
    fdScrolledComposite.bottom = new FormAttachment(wOk, -margin);
    scrolledComposite.setLayoutData(fdScrolledComposite);
    scrolledComposite.setLayout(new FillLayout());

    Composite wContent = new Composite(scrolledComposite, SWT.NONE);
    PropsUi.setLook(wContent);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = PropsUi.getFormMargin();
    contentLayout.marginHeight = PropsUi.getFormMargin();
    wContent.setLayout(contentLayout);

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    // Update the dimension?
    Label wlPassAllRows = new Label(wContent, SWT.RIGHT);
    wlPassAllRows.setText(BaseMessages.getString(PKG, "BlockingTransformDialog.PassAllRows.Label"));
    PropsUi.setLook(wlPassAllRows);
    FormData fdlUpdate = new FormData();
    fdlUpdate.left = new FormAttachment(0, 0);
    fdlUpdate.right = new FormAttachment(middle, -margin);
    fdlUpdate.top = new FormAttachment(0, margin);
    wlPassAllRows.setLayoutData(fdlUpdate);
    wPassAllRows = new Button(wContent, SWT.CHECK);
    PropsUi.setLook(wPassAllRows);
    FormData fdUpdate = new FormData();
    fdUpdate.left = new FormAttachment(middle, 0);
    fdUpdate.top = new FormAttachment(wlPassAllRows, 0, SWT.CENTER);
    fdUpdate.right = new FormAttachment(100, 0);
    wPassAllRows.setLayoutData(fdUpdate);

    // Clicking on update changes the options in the update combo boxes!
    wPassAllRows.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            setEnableDialog();
          }
        });

    // Temp directory for sorting
    wlSpoolDir = new Label(wContent, SWT.RIGHT);
    wlSpoolDir.setText(BaseMessages.getString(PKG, "BlockingTransformDialog.SpoolDir.Label"));
    PropsUi.setLook(wlSpoolDir);
    FormData fdlSpoolDir = new FormData();
    fdlSpoolDir.left = new FormAttachment(0, 0);
    fdlSpoolDir.right = new FormAttachment(middle, -margin);
    fdlSpoolDir.top = new FormAttachment(wPassAllRows, margin);
    wlSpoolDir.setLayoutData(fdlSpoolDir);

    wbSpoolDir = new Button(wContent, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbSpoolDir);
    wbSpoolDir.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbSpoolDir = new FormData();
    fdbSpoolDir.right = new FormAttachment(100, 0);
    fdbSpoolDir.top = new FormAttachment(wPassAllRows, margin);
    wbSpoolDir.setLayoutData(fdbSpoolDir);

    wSpoolDir = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSpoolDir);
    wSpoolDir.addModifyListener(lsMod);
    FormData fdSpoolDir = new FormData();
    fdSpoolDir.left = new FormAttachment(middle, 0);
    fdSpoolDir.top = new FormAttachment(wPassAllRows, margin);
    fdSpoolDir.right = new FormAttachment(wbSpoolDir, -margin);
    wSpoolDir.setLayoutData(fdSpoolDir);

    // Whenever something changes, set the tooltip to the expanded version:
    wSpoolDir.addModifyListener(
        e -> wSpoolDir.setToolTipText(variables.resolve(wSpoolDir.getText())));

    wbSpoolDir.addListener(
        SWT.Selection, e -> BaseDialog.presentDirectoryDialog(shell, wSpoolDir, variables));

    // Prefix of temporary file
    wlPrefix = new Label(wContent, SWT.RIGHT);
    wlPrefix.setText(BaseMessages.getString(PKG, "BlockingTransformDialog.Prefix.Label"));
    PropsUi.setLook(wlPrefix);
    FormData fdlPrefix = new FormData();
    fdlPrefix.left = new FormAttachment(0, 0);
    fdlPrefix.right = new FormAttachment(middle, -margin);
    fdlPrefix.top = new FormAttachment(wbSpoolDir, margin);
    wlPrefix.setLayoutData(fdlPrefix);
    wPrefix = new Text(wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPrefix);
    wPrefix.addModifyListener(lsMod);
    FormData fdPrefix = new FormData();
    fdPrefix.left = new FormAttachment(middle, 0);
    fdPrefix.top = new FormAttachment(wbSpoolDir, margin);
    fdPrefix.right = new FormAttachment(100, 0);
    wPrefix.setLayoutData(fdPrefix);

    // Maximum number of lines to keep in memory before using temporary files
    wlCacheSize = new Label(wContent, SWT.RIGHT);
    wlCacheSize.setText(BaseMessages.getString(PKG, "BlockingTransformDialog.CacheSize.Label"));
    PropsUi.setLook(wlCacheSize);
    FormData fdlCacheSize = new FormData();
    fdlCacheSize.left = new FormAttachment(0, 0);
    fdlCacheSize.right = new FormAttachment(middle, -margin);
    fdlCacheSize.top = new FormAttachment(wPrefix, margin);
    wlCacheSize.setLayoutData(fdlCacheSize);
    wCacheSize = new Text(wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wCacheSize);
    wCacheSize.addModifyListener(lsMod);
    FormData fdCacheSize = new FormData();
    fdCacheSize.left = new FormAttachment(middle, 0);
    fdCacheSize.top = new FormAttachment(wPrefix, margin);
    fdCacheSize.right = new FormAttachment(100, 0);
    wCacheSize.setLayoutData(fdCacheSize);

    // Using compression for temporary files?
    wlCompress = new Label(wContent, SWT.RIGHT);
    wlCompress.setText(BaseMessages.getString(PKG, "BlockingTransformDialog.Compress.Label"));
    PropsUi.setLook(wlCompress);
    FormData fdlCompress = new FormData();
    fdlCompress.left = new FormAttachment(0, 0);
    fdlCompress.right = new FormAttachment(middle, -margin);
    fdlCompress.top = new FormAttachment(wCacheSize, margin);
    wlCompress.setLayoutData(fdlCompress);
    wCompress = new Button(wContent, SWT.CHECK);
    PropsUi.setLook(wCompress);
    FormData fdCompress = new FormData();
    fdCompress.left = new FormAttachment(middle, 0);
    fdCompress.top = new FormAttachment(wlCompress, 0, SWT.CENTER);
    fdCompress.right = new FormAttachment(100, 0);
    wCompress.setLayoutData(fdCompress);
    wCompress.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    wContent.pack();
    Rectangle bounds = wContent.getBounds();
    scrolledComposite.setContent(wContent);
    scrolledComposite.setExpandHorizontal(true);
    scrolledComposite.setExpandVertical(true);
    scrolledComposite.setMinWidth(bounds.width);
    scrolledComposite.setMinHeight(bounds.height);

    getData();

    // Set the enablement of the dialog widgets
    setEnableDialog();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wPassAllRows.setSelection(input.isPassAllRows());
    if (input.getPrefix() != null) {
      wPrefix.setText(input.getPrefix());
    }
    if (input.getDirectory() != null) {
      wSpoolDir.setText(input.getDirectory());
    }
    wCacheSize.setText("" + input.getCacheSize());
    wCompress.setSelection(input.isCompressFiles());
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

    transformName = wTransformName.getText(); // return value

    input.setPrefix(wPrefix.getText());
    input.setDirectory(wSpoolDir.getText());
    input.setCacheSize(Const.toInt(wCacheSize.getText(), BlockingTransformMeta.CACHE_SIZE));
    if (isDetailed()) {
      logDetailed("Compression is set to " + wCompress.getSelection());
    }
    input.setCompressFiles(wCompress.getSelection());
    input.setPassAllRows(wPassAllRows.getSelection());

    dispose();
  }

  /** Set the correct state "enabled or not" of the dialog widgets. */
  private void setEnableDialog() {
    wlSpoolDir.setEnabled(wPassAllRows.getSelection());
    wbSpoolDir.setEnabled(wPassAllRows.getSelection());
    wSpoolDir.setEnabled(wPassAllRows.getSelection());
    wlPrefix.setEnabled(wPassAllRows.getSelection());
    wPrefix.setEnabled(wPassAllRows.getSelection());
    wlCacheSize.setEnabled(wPassAllRows.getSelection());
    wCacheSize.setEnabled(wPassAllRows.getSelection());
    wlCompress.setEnabled(wPassAllRows.getSelection());
    wCompress.setEnabled(wPassAllRows.getSelection());
  }
}
