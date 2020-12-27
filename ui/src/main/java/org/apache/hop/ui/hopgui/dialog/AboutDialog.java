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

import java.util.Arrays;
import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.program.Program;
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
  private static final Class<?> PKG = AboutDialog.class; // Needed by Translator

  private final String[] PROPERTIES =  new String[] { "os.name","os.version","os.arch", "java.version", "java.vm.vendor", "java.specification.version","java.class.path","file.encoding","HOP_PLATFORM_RUNTIME","HOP_CONFIG_FOLDER"}; 

  private Shell shell;

  public AboutDialog(Shell parent) {
    super(parent, SWT.NONE);
  }

  public void open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    PropsUi props = PropsUi.getInstance();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.SHEET | SWT.RESIZE);
    shell.setText(BaseMessages.getString(PKG, "AboutDialog.Title"));   
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    shell.setSize(400, 400);
    shell.setMinimumSize(350, 250);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;
    shell.setLayout(formLayout);
    props.setLook(shell);

    // Widget application logo
    Label wLogo = new Label(shell, SWT.CENTER);
    wLogo.setImage(
        SwtSvgImageUtil.getImageAsResource(display, "ui/images/logo_hop.svg")
            .getAsBitmapForSize(display, 100, 100));
    FormData fdLogo = new FormData();
    fdLogo.top = new FormAttachment(0, 0);
    fdLogo.left = new FormAttachment(0, 0);
    wLogo.setLayoutData(fdLogo);

    // Widget application name
    Label wName = new Label(shell, SWT.CENTER);
    wName.setText("Apache Hop\n(Incubating)");
    wName.setFont(GuiResource.getInstance().getFontLarge());
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(0, 0);
    fdName.left = new FormAttachment(wLogo, 0);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);
    props.setLook(wName);

    // Widget application version
    Label wVersion = new Label(shell, SWT.CENTER);
    wVersion.setText(HopGui.class.getPackage().getImplementationVersion());
    FormData fdAppVersion = new FormData();
    fdAppVersion.top = new FormAttachment(wName, Const.MARGIN);
    fdAppVersion.left = new FormAttachment(wLogo, 0);
    fdAppVersion.right = new FormAttachment(100, 0);
    wVersion.setLayoutData(fdAppVersion);
    props.setLook(wVersion);

    // Widget link, use composite for centering
    Composite composite = new Composite(shell, SWT.NONE);
    FormData fdLink = new FormData();
    fdLink.top = new FormAttachment(wVersion, Const.MARGIN);
    fdLink.left = new FormAttachment(wLogo, 0);
    fdLink.right = new FormAttachment(100, 0);
    composite.setLayoutData(fdLink);
    composite.setLayout(new GridLayout(1, false));
    props.setLook(composite);

    Link wLink = new Link(composite, SWT.WRAP | SWT.MULTI);
    wLink.setText("<a href=\"https://hop.apache.org\">hop.apache.org</a>");
    wLink.addListener(SWT.Selection, e -> Program.launch("https://hop.apache.org"));
    wLink.setLayoutData(new GridData(SWT.CENTER, SWT.FILL, true, true));
    props.setLook(wLink);

    // Buttons
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk}, Const.MARGIN, null);

    // Widget system properties
    Text wText = new Text(shell, SWT.READ_ONLY | SWT.MULTI | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    wText.setText(getProperties());
    FormData fdText = new FormData();
    fdText.top = new FormAttachment(wLogo, Const.MARGIN);
    fdText.left = new FormAttachment(0, 0);
    fdText.right = new FormAttachment(100, 0);
    fdText.bottom = new FormAttachment(wOk, -Const.MARGIN);
    wText.setLayoutData(fdText);
    props.setLook(wText);

    // Detect [X] or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
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

  private String getProperties() {

    StringBuilder builder = new StringBuilder();
    Arrays.sort(PROPERTIES);      
    for (String name : PROPERTIES) {
      builder.append(name);
      builder.append('=');      
      builder.append(System.getProperty(name,""));
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
