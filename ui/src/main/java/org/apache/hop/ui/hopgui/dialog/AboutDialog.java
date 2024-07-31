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

import java.util.Set;
import org.apache.hop.core.HopVersionProvider;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.VariableRegistry;
import org.apache.hop.core.variables.VariableScope;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
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
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Link;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/** A dialog to display version information. */
public class AboutDialog extends Dialog {
  private static final Class<?> PKG = AboutDialog.class;

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

  public AboutDialog(Shell parent) {
    super(parent, SWT.NONE);
  }

  public void open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.SHEET | SWT.RESIZE);
    shell.setText(BaseMessages.getString(PKG, "AboutDialog.Title"));
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    shell.setSize(700, 500);
    shell.setMinimumSize(450, 300);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);
    PropsUi.setLook(shell);

    // Composite for logo, app name & version, and centering link
    Composite composite = new Composite(shell, SWT.NONE);
    FormData fdLink = new FormData();
    fdLink.top = new FormAttachment(0, 0);
    fdLink.left = new FormAttachment(0, 0);
    fdLink.right = new FormAttachment(100, 0);
    composite.setLayoutData(fdLink);
    GridLayout gridLayout = new GridLayout(2, false);
    gridLayout.marginWidth = 0;
    composite.setLayout(gridLayout);
    PropsUi.setLook(composite);

    // Widget application logo
    Label wLogo = new Label(composite, SWT.CENTER);
    wLogo.setImage(
        SwtSvgImageUtil.getImageAsResource(display, "ui/images/logo_hop.svg")
            .getAsBitmapForSize(display, 100, 100));
    wLogo.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false, 1, 3));

    // Widget application name
    Label wName = new Label(composite, SWT.CENTER);
    wName.setText("Apache Hop");
    wName.setFont(GuiResource.getInstance().getFontBold());
    wName.setLayoutData(new GridData(SWT.CENTER, SWT.CENTER, true, false));
    PropsUi.setLook(wName);

    // Widget application version
    Text wVersion = new Text(composite, SWT.READ_ONLY | SWT.CENTER);
    wVersion.setText(this.getVersion());
    wVersion.setLayoutData(new GridData(SWT.CENTER, SWT.CENTER, true, false));
    PropsUi.setLook(wVersion);

    Link wLink = new Link(composite, SWT.WRAP | SWT.MULTI);
    wLink.setText("<a href=\"https://hop.apache.org\">hop.apache.org</a>");
    wLink.addListener(
        SWT.Selection,
        e -> {
          try {
            EnvironmentUtils.getInstance().openUrl("https://hop.apache.org");
          } catch (Exception ex) {
            new ErrorDialog(shell, "Error", "Error opening URL", ex);
          }
        });
    wLink.setLayoutData(new GridData(SWT.CENTER, SWT.CENTER, true, false));
    PropsUi.setLook(wLink);

    // Buttons
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk}, PropsUi.getMargin(), null);

    // Widget system properties
    Text wText =
        new Text(
            shell, SWT.READ_ONLY | SWT.WRAP | SWT.MULTI | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    wText.setText(getProperties());
    FormData fdText = new FormData();
    fdText.top = new FormAttachment(composite, PropsUi.getMargin());
    fdText.left = new FormAttachment(0, 0);
    fdText.right = new FormAttachment(100, 0);
    fdText.bottom = new FormAttachment(wOk, -PropsUi.getMargin());
    wText.setLayoutData(fdText);
    PropsUi.setLook(wText);

    // Detect [X] or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          @Override
          public void shellClosed(ShellEvent e) {
            ok();
          }
        });

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
  }

  protected String getVersion() {
    HopVersionProvider versionProvider = new HopVersionProvider();
    return versionProvider.getVersion()[0];
  }

  private String getProperties() {
    Set<String> names = VariableRegistry.getInstance().getVariableNames(VariableScope.SYSTEM);
    for (String name : JAVA_PROPERTIES) {
      names.add(name);
    }

    IVariables variables = HopGui.getInstance().getVariables();
    StringBuilder builder = new StringBuilder();
    for (String name : names) {
      builder.append(name);
      builder.append('=');
      builder.append(variables.getVariable(name, ""));
      builder.append('\n');
    }

    return builder.toString();
  }

  public void dispose() {
    shell.dispose();
  }

  private void ok() {
    dispose();
  }
}
