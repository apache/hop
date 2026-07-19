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

package org.apache.hop.spark.gui;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.spark.pkg.PackageExportFilter;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * Edit {@link PackageExportFilter#CONFIG_FILENAME} for the active project (Native Spark package
 * include/exclude rules).
 */
public class SparkPackageConfigDialog extends Dialog {
  private static final Class<?> PKG = HopSparkGuiPlugin.class;

  private final String projectHome;
  private Shell shell;
  private PropsUi props;

  private Text wProjectHome;
  private StyledText wExcludeDirs;
  private StyledText wExcludeGlobs;
  private StyledText wIncludePaths;
  private Button wReplaceDefaults;

  public SparkPackageConfigDialog(Shell parent, String projectHome) {
    super(parent, SWT.NONE);
    this.projectHome = projectHome;
    this.props = PropsUi.getInstance();
  }

  public void open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    shell.setText(BaseMessages.getString(PKG, "SparkPackageConfigDialog.Shell.Title"));

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel.addListener(SWT.Selection, e -> cancel());

    Label wlIntro = new Label(shell, SWT.LEFT | SWT.WRAP);
    props.setLook(wlIntro);
    wlIntro.setText(BaseMessages.getString(PKG, "SparkPackageConfigDialog.Intro"));
    FormData fdlIntro = new FormData();
    fdlIntro.left = new FormAttachment(0, 0);
    fdlIntro.top = new FormAttachment(0, margin);
    fdlIntro.right = new FormAttachment(100, 0);
    wlIntro.setLayoutData(fdlIntro);

    Label wlHome = new Label(shell, SWT.RIGHT);
    props.setLook(wlHome);
    wlHome.setText(BaseMessages.getString(PKG, "SparkPackageConfigDialog.ProjectHome.Label"));
    FormData fdlHome = new FormData();
    fdlHome.left = new FormAttachment(0, 0);
    fdlHome.top = new FormAttachment(wlIntro, margin * 2);
    fdlHome.right = new FormAttachment(middle, -margin);
    wlHome.setLayoutData(fdlHome);
    wProjectHome = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.READ_ONLY);
    props.setLook(wProjectHome);
    FormData fdHome = new FormData();
    fdHome.left = new FormAttachment(middle, 0);
    fdHome.top = new FormAttachment(wlIntro, margin * 2);
    fdHome.right = new FormAttachment(100, 0);
    wProjectHome.setLayoutData(fdHome);

    Label wlDefaults = new Label(shell, SWT.LEFT | SWT.WRAP);
    props.setLook(wlDefaults);
    wlDefaults.setText(
        BaseMessages.getString(
            PKG,
            "SparkPackageConfigDialog.Defaults.Label",
            String.join(", ", PackageExportFilter.DEFAULT_EXCLUDE_DIRS)));
    FormData fdlDefaults = new FormData();
    fdlDefaults.left = new FormAttachment(0, 0);
    fdlDefaults.top = new FormAttachment(wProjectHome, margin);
    fdlDefaults.right = new FormAttachment(100, 0);
    wlDefaults.setLayoutData(fdlDefaults);

    wReplaceDefaults = new Button(shell, SWT.CHECK);
    props.setLook(wReplaceDefaults);
    wReplaceDefaults.setText(
        BaseMessages.getString(PKG, "SparkPackageConfigDialog.ReplaceDefaults.Label"));
    FormData fdReplace = new FormData();
    fdReplace.left = new FormAttachment(middle, 0);
    fdReplace.top = new FormAttachment(wlDefaults, margin);
    fdReplace.right = new FormAttachment(100, 0);
    wReplaceDefaults.setLayoutData(fdReplace);

    wExcludeDirs =
        addMultiLine(
            shell,
            wReplaceDefaults,
            middle,
            margin,
            "SparkPackageConfigDialog.ExcludeDirs.Label",
            "SparkPackageConfigDialog.ExcludeDirs.Tooltip",
            60);
    wExcludeGlobs =
        addMultiLine(
            shell,
            wExcludeDirs,
            middle,
            margin,
            "SparkPackageConfigDialog.ExcludeGlobs.Label",
            "SparkPackageConfigDialog.ExcludeGlobs.Tooltip",
            60);
    wIncludePaths =
        addMultiLine(
            shell,
            wExcludeGlobs,
            middle,
            margin,
            "SparkPackageConfigDialog.IncludePaths.Label",
            "SparkPackageConfigDialog.IncludePaths.Tooltip",
            80);

    // stretch last field to bottom buttons
    FormData fdInc = (FormData) wIncludePaths.getLayoutData();
    fdInc.bottom = new FormAttachment(wOk, -margin);

    getData();
    // Blocks until the shell is disposed (OK / Cancel / Escape / window close).
    // Do not add listeners or run another event loop after this returns — the shell is gone.
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
  }

  private StyledText addMultiLine(
      Shell shell,
      org.eclipse.swt.widgets.Control above,
      int middle,
      int margin,
      String labelKey,
      String tooltipKey,
      int height) {
    Label wl = new Label(shell, SWT.RIGHT);
    props.setLook(wl);
    wl.setText(BaseMessages.getString(PKG, labelKey));
    FormData fdl = new FormData();
    fdl.left = new FormAttachment(0, 0);
    fdl.top = new FormAttachment(above, margin);
    fdl.right = new FormAttachment(middle, -margin);
    wl.setLayoutData(fdl);
    StyledText w =
        new StyledText(shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    props.setLook(w);
    w.setToolTipText(BaseMessages.getString(PKG, tooltipKey));
    FormData fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(above, margin);
    fd.right = new FormAttachment(100, 0);
    fd.height = height;
    w.setLayoutData(fd);
    return w;
  }

  private void getData() {
    wProjectHome.setText(Const.NVL(projectHome, ""));
    try {
      PackageExportFilter filter = PackageExportFilter.loadFromProjectHome(projectHome);
      wExcludeDirs.setText(String.join(Const.CR, filter.getExcludeDirs()));
      wExcludeGlobs.setText(String.join(Const.CR, filter.getExcludeGlobs()));
      wIncludePaths.setText(String.join(Const.CR, filter.getIncludePaths()));
      wReplaceDefaults.setSelection(filter.isReplaceDefaultExcludeDirs());
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "SparkPackageConfigDialog.Error.Load.Title"),
          BaseMessages.getString(PKG, "SparkPackageConfigDialog.Error.Load.Message"),
          e);
    }
  }

  private void ok() {
    try {
      PackageExportFilter filter =
          new PackageExportFilter(
              linesToList(wExcludeDirs.getText()),
              linesToList(wExcludeGlobs.getText()),
              linesToList(wIncludePaths.getText()),
              wReplaceDefaults.getSelection());
      PackageExportFilter.saveToProjectHome(projectHome, filter);
      dispose();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "SparkPackageConfigDialog.Error.Save.Title"),
          BaseMessages.getString(PKG, "SparkPackageConfigDialog.Error.Save.Message"),
          e);
    }
  }

  private static List<String> linesToList(String text) {
    if (StringUtils.isBlank(text)) {
      return List.of();
    }
    return Arrays.stream(text.split("\\R"))
        .map(String::trim)
        .filter(StringUtils::isNotBlank)
        .collect(Collectors.toList());
  }

  private void cancel() {
    dispose();
  }

  private void dispose() {
    if (shell == null || shell.isDisposed()) {
      return;
    }
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }
}
