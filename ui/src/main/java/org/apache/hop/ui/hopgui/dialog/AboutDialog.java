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

package org.apache.hop.ui.hopgui.dialog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopVersionProvider;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.VariableRegistry;
import org.apache.hop.core.variables.VariableScope;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.ExpandBar;
import org.eclipse.swt.widgets.ExpandItem;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Link;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/** A dialog to display version and system information. */
public class AboutDialog extends Dialog {
  private static final Class<?> PKG = AboutDialog.class;

  private static final String HOP_URL = "https://hop.apache.org";
  private static final long MB = 1024L * 1024L;

  /** Full set of Java properties shown, sorted, in the collapsible "Advanced" section. */
  private static final String[] JAVA_PROPERTIES =
      new String[] {
        "os.name",
        "os.version",
        "os.arch",
        "java.version",
        "java.vm.vendor",
        "java.specification.version",
        "java.class.path",
        "file.encoding"
      };

  private Shell shell;
  private ExpandBar expandBar;
  private ExpandItem advanced;
  private Button wOk;
  private int collapsedHeight;

  public AboutDialog(Shell parent) {
    super(parent, SWT.NONE);
  }

  public void open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();
    shell = new Shell(parent, BaseDialog.getDefaultDialogStyle());
    shell.setText(BaseMessages.getString(PKG, "AboutDialog.Title"));
    shell.setImage(GuiResource.getInstance().getImageHopUi());

    int margin = PropsUi.getMargin();
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);
    PropsUi.setLook(shell);

    // ----- Header: logo on the left, name / version / link stacked on the right -----
    Composite header = new Composite(shell, SWT.NONE);
    PropsUi.setLook(header);
    GridLayout headerLayout = new GridLayout(2, false);
    headerLayout.marginWidth = 0;
    headerLayout.marginHeight = 0;
    headerLayout.horizontalSpacing = 2 * margin;
    header.setLayout(headerLayout);
    FormData fdHeader = new FormData();
    fdHeader.top = new FormAttachment(0, 0);
    fdHeader.left = new FormAttachment(0, 0);
    fdHeader.right = new FormAttachment(100, 0);
    header.setLayoutData(fdHeader);

    Label wLogo = new Label(header, SWT.NONE);
    wLogo.setImage(
        SwtSvgImageUtil.getImageAsResource(display, "ui/images/logo_hop.svg")
            .getAsBitmapForSize(display, 90, 90));
    wLogo.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));

    Composite titleBox = new Composite(header, SWT.NONE);
    PropsUi.setLook(titleBox);
    GridLayout titleLayout = new GridLayout(1, false);
    titleLayout.marginWidth = 0;
    titleLayout.marginHeight = 0;
    titleBox.setLayout(titleLayout);
    titleBox.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));

    Label wName = new Label(titleBox, SWT.LEFT);
    wName.setText("Apache Hop");
    wName.setFont(GuiResource.getInstance().getFontBold());
    wName.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, true, false));
    PropsUi.setLook(wName);

    Label wVersion = new Label(titleBox, SWT.LEFT);
    wVersion.setText(getVersionLabel());
    wVersion.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, true, false));
    PropsUi.setLook(wVersion);

    Link wLink = new Link(titleBox, SWT.NONE);
    wLink.setText("<a href=\"" + HOP_URL + "\">hop.apache.org</a>");
    wLink.addListener(SWT.Selection, e -> openUrl());
    wLink.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, true, false));
    PropsUi.setLook(wLink);

    // ----- Separator -----
    Label separator = new Label(shell, SWT.SEPARATOR | SWT.HORIZONTAL);
    FormData fdSeparator = new FormData();
    fdSeparator.top = new FormAttachment(header, 2 * margin);
    fdSeparator.left = new FormAttachment(0, 0);
    fdSeparator.right = new FormAttachment(100, 0);
    separator.setLayoutData(fdSeparator);

    // ----- "System information" heading with a Copy button on the right -----
    Button wCopy = new Button(shell, SWT.PUSH);
    wCopy.setText(BaseMessages.getString(PKG, "AboutDialog.Button.Copy"));
    wCopy.setToolTipText(BaseMessages.getString(PKG, "AboutDialog.Button.Copy.Tooltip"));
    wCopy.addListener(SWT.Selection, e -> copyToClipboard(wCopy));
    FormData fdCopy = new FormData();
    fdCopy.top = new FormAttachment(separator, margin);
    fdCopy.right = new FormAttachment(100, 0);
    wCopy.setLayoutData(fdCopy);

    Label wSysInfo = new Label(shell, SWT.LEFT);
    wSysInfo.setText(BaseMessages.getString(PKG, "AboutDialog.SystemInformation"));
    wSysInfo.setFont(GuiResource.getInstance().getFontBold());
    PropsUi.setLook(wSysInfo);
    FormData fdSysInfo = new FormData();
    fdSysInfo.top = new FormAttachment(wCopy, 0, SWT.CENTER);
    fdSysInfo.left = new FormAttachment(0, 0);
    wSysInfo.setLayoutData(fdSysInfo);

    // ----- The essentials: a compact aligned key / value block -----
    Composite info = new Composite(shell, SWT.NONE);
    PropsUi.setLook(info);
    GridLayout infoLayout = new GridLayout(2, false);
    infoLayout.marginWidth = 0;
    infoLayout.marginHeight = 0;
    infoLayout.horizontalSpacing = 3 * margin;
    infoLayout.verticalSpacing = Math.max(2, margin / 2);
    info.setLayout(infoLayout);
    FormData fdInfo = new FormData();
    fdInfo.top = new FormAttachment(wCopy, margin);
    fdInfo.left = new FormAttachment(0, 0);
    fdInfo.right = new FormAttachment(100, 0);
    info.setLayoutData(fdInfo);

    for (String[] row : getEssentials()) {
      Label wKey = new Label(info, SWT.LEFT);
      wKey.setText(row[0]);
      wKey.setFont(GuiResource.getInstance().getFontBold());
      GridData gdKey = new GridData(SWT.LEFT, SWT.TOP, false, false);
      gdKey.widthHint = (int) (140 * PropsUi.getInstance().getZoomFactor());
      wKey.setLayoutData(gdKey);
      PropsUi.setLook(wKey);

      Label wValue = new Label(info, SWT.LEFT);
      wValue.setText(row[1]);
      wValue.setToolTipText(row[1]);
      wValue.setLayoutData(new GridData(SWT.FILL, SWT.TOP, true, false));
      PropsUi.setLook(wValue);
    }

    // ----- OK button, anchored at the bottom of the (resizable) dialog -----
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk}, margin, null);

    // ----- Collapsible "Advanced" section with the full property / classpath dump -----
    expandBar = new ExpandBar(shell, SWT.V_SCROLL);
    PropsUi.setLook(expandBar);
    FormData fdBar = new FormData();
    fdBar.top = new FormAttachment(info, 2 * margin);
    fdBar.left = new FormAttachment(0, 0);
    fdBar.right = new FormAttachment(100, 0);
    fdBar.bottom = new FormAttachment(wOk, -margin);
    expandBar.setLayoutData(fdBar);

    Text wProperties =
        new Text(
            expandBar,
            SWT.READ_ONLY | SWT.WRAP | SWT.MULTI | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    wProperties.setText(getProperties());
    PropsUi.setLook(wProperties);

    advanced = new ExpandItem(expandBar, SWT.NONE);
    advanced.setText(BaseMessages.getString(PKG, "AboutDialog.Advanced"));
    advanced.setControl(wProperties);
    advanced.setHeight((int) (240 * PropsUi.getInstance().getZoomFactor()));
    advanced.setExpanded(false);

    // Keep the properties view filling the available height and grow / shrink on toggle.
    shell.addListener(SWT.Resize, e -> fillAdvanced());
    expandBar.addListener(SWT.Expand, e -> display.asyncExec(this::onExpand));
    expandBar.addListener(SWT.Collapse, e -> display.asyncExec(this::onCollapse));

    // Detect [X] or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          @Override
          public void shellClosed(ShellEvent e) {
            ok();
          }
        });

    // Size the dialog: compact by default, with the Advanced section collapsed.
    double zoom = PropsUi.getInstance().getZoomFactor();
    int width =
        Math.clamp(
            shell.computeSize(SWT.DEFAULT, SWT.DEFAULT).x, (int) (520 * zoom), (int) (780 * zoom));
    shell.setSize(width, (int) (600 * zoom));
    shell.layout(true, true);
    collapsedHeight = computeCollapsedHeight(margin);
    shell.setSize(width, collapsedHeight);
    shell.setMinimumSize(width, collapsedHeight);

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
  }

  /** Compact height with the Advanced section collapsed, including the window trim. */
  private int computeCollapsedHeight(int margin) {
    int clientHeight =
        expandBar.getLocation().y
            + advanced.getHeaderHeight()
            + 2 * expandBar.getSpacing()
            + margin
            + wOk.getSize().y
            + PropsUi.getFormMargin();
    return clientHeight + (shell.getSize().y - shell.getClientArea().height);
  }

  private void onExpand() {
    if (shell.isDisposed()) {
      return;
    }
    int grow = (int) (260 * PropsUi.getInstance().getZoomFactor());
    shell.setSize(shell.getSize().x, Math.max(shell.getSize().y, collapsedHeight + grow));
    shell.layout(true, true);
    fillAdvanced();
  }

  private void onCollapse() {
    if (shell.isDisposed()) {
      return;
    }
    shell.setSize(shell.getSize().x, collapsedHeight);
    shell.layout(true, true);
  }

  /** When expanded, size the properties view to fill the space above the OK button. */
  private void fillAdvanced() {
    if (shell == null || shell.isDisposed() || advanced == null || !advanced.getExpanded()) {
      return;
    }
    int height =
        expandBar.getClientArea().height - advanced.getHeaderHeight() - 2 * expandBar.getSpacing();
    int minHeight = (int) (60 * PropsUi.getInstance().getZoomFactor());
    if (height < minHeight) {
      height = minHeight;
    }
    if (height != advanced.getHeight()) {
      advanced.setHeight(height);
    }
  }

  /** Copy the full system report to the clipboard and briefly acknowledge on the button. */
  private void copyToClipboard(Button button) {
    GuiResource.getInstance().toClipboard(getReport());
    String original = button.getText();
    button.setText(BaseMessages.getString(PKG, "AboutDialog.Button.Copied"));
    button
        .getDisplay()
        .timerExec(
            1500,
            () -> {
              if (!button.isDisposed()) {
                button.setText(original);
              }
            });
  }

  private void openUrl() {
    try {
      EnvironmentUtils.getInstance().openUrl(HOP_URL);
    } catch (Exception ex) {
      new ErrorDialog(shell, "Error", "Error opening URL", ex);
    }
  }

  protected String getVersion() {
    HopVersionProvider versionProvider = new HopVersionProvider();
    return versionProvider.getVersion()[0];
  }

  private String getVersionLabel() {
    String version = getVersion();
    if (version == null || version.isBlank()) {
      return BaseMessages.getString(PKG, "AboutDialog.DevelopmentBuild");
    }
    return version;
  }

  /** The handful of values most people open this dialog for, shown at the top. */
  private List<String[]> getEssentials() {
    IVariables variables = HopGui.getInstance().getVariables();
    List<String[]> rows = new ArrayList<>();
    rows.add(essential("HopVersion", getVersionLabel()));
    rows.add(
        essential(
            "OperatingSystem",
            prop(variables, "os.name")
                + " "
                + prop(variables, "os.version")
                + " ("
                + prop(variables, "os.arch")
                + ")"));
    rows.add(
        essential(
            "Java",
            prop(variables, "java.version") + " (" + prop(variables, "java.vm.vendor") + ")"));
    rows.add(essential("JavaSpecification", prop(variables, "java.specification.version")));
    rows.add(essential("FileEncoding", prop(variables, "file.encoding")));
    rows.add(essential("Memory", getMemory()));
    rows.add(essential("Processors", Integer.toString(Runtime.getRuntime().availableProcessors())));
    rows.add(essential("ConfigFolder", Const.HOP_CONFIG_FOLDER));
    return rows;
  }

  private String[] essential(String key, String value) {
    return new String[] {
      BaseMessages.getString(PKG, "AboutDialog.Label." + key), value == null ? "" : value
    };
  }

  private String getMemory() {
    Runtime runtime = Runtime.getRuntime();
    long usedMb = (runtime.totalMemory() - runtime.freeMemory()) / MB;
    long max = runtime.maxMemory();
    String maxLabel =
        max == Long.MAX_VALUE
            ? BaseMessages.getString(PKG, "AboutDialog.Memory.NoLimit")
            : (max / MB) + " MB";
    return usedMb + " MB / " + maxLabel;
  }

  private String prop(IVariables variables, String name) {
    String value = variables.getVariable(name);
    if (value == null || value.isEmpty()) {
      value = System.getProperty(name, "");
    }
    return value;
  }

  /** The complete, sorted list of system properties shown in the Advanced section. */
  private String getProperties() {
    Set<String> names = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    names.addAll(VariableRegistry.getInstance().getVariableNames(VariableScope.SYSTEM));
    Collections.addAll(names, JAVA_PROPERTIES);

    IVariables variables = HopGui.getInstance().getVariables();
    StringBuilder builder = new StringBuilder();
    for (String name : names) {
      builder.append(name).append('=').append(variables.getVariable(name, "")).append('\n');
    }
    return builder.toString();
  }

  /** Essentials followed by the full property dump — what the Copy button puts on the clipboard. */
  private String getReport() {
    StringBuilder builder = new StringBuilder();
    for (String[] row : getEssentials()) {
      builder.append(row[0]).append(": ").append(row[1]).append('\n');
    }
    builder.append('\n').append(getProperties());
    return builder.toString();
  }

  public void dispose() {
    shell.dispose();
  }

  private void ok() {
    dispose();
  }
}
