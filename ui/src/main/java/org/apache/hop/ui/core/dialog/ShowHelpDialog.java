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

package org.apache.hop.ui.core.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.laf.BasePropertyHandler;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.browser.Browser;
import org.eclipse.swt.browser.LocationEvent;
import org.eclipse.swt.browser.LocationListener;
import org.eclipse.swt.browser.ProgressEvent;
import org.eclipse.swt.browser.ProgressListener;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.program.Program;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

import java.net.MalformedURLException;
import java.net.URL;

public class ShowHelpDialog extends Dialog {
  private static final Class<?> PKG = HopGui.class;

  private static final String DOC_URL =
      Const.getDocUrl(BasePropertyHandler.getProperty("documentationUrl"));
  private static final String PREFIX = "https://help";
  private static final String PRINT_PREFIX = "https://f1.help";
  private static final String PRINT_SCRIPT = "javascript:window.print();";
  private static final int MARGIN = 5;

  private boolean fromPrint;

  private String dialogTitle;
  private String url;
  private String homeURL;

  private Browser wBrowser;

  private ToolItem tltmBack;
  private ToolItem tltmForward;
  private ToolItem tltmRefresh;
  private ToolItem tltmHome;
  private ToolItem tltmPrint;

  private Text textURL;

  private Shell shell;

  public ShowHelpDialog(Shell parent, String dialogTitle, String url, String header) {
    super(parent, SWT.NONE);    
    this.dialogTitle = BaseMessages.getString(PKG, "HopGui.Documentation.Hop.Title");
    this.url = url;
    try {
      this.homeURL = new URL(DOC_URL).toString();
    } catch (MalformedURLException e) {
    }
  }

  public ShowHelpDialog(Shell parent, String dialogTitle, String url) {
    this(parent, dialogTitle, url, "");
  }

  protected Shell createShell(Shell parent) {
    return new Shell(parent, SWT.RESIZE | SWT.MAX | SWT.MIN | SWT.DIALOG_TRIM);
  }

  public void open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();
    PropsUi props = PropsUi.getInstance();
    
    shell = createShell(parent);
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    shell.setLayout(new FormLayout());
    shell.setText(dialogTitle);
    props.setLook(shell);

    Cursor cursorHand = new Cursor(display, SWT.CURSOR_HAND);

    ToolBar navigateToolBar = new ToolBar(shell, SWT.FLAT);
    FormData fdtoolBarBack = new FormData();
    fdtoolBarBack.top = new FormAttachment(0, MARGIN);
    fdtoolBarBack.left = new FormAttachment(0, 0);
    navigateToolBar.setLayoutData(fdtoolBarBack);
    navigateToolBar.setCursor(cursorHand);
    navigateToolBar.setBackground(navigateToolBar.getParent().getBackground());
    
    tltmHome = new ToolItem(navigateToolBar, SWT.NONE);
    tltmHome.setImage(GuiResource.getInstance().getImageHome());
    tltmHome.setToolTipText(BaseMessages.getString(PKG, "HopGui.Documentation.Tooltip.Home"));
    tltmHome.setEnabled(true);
    tltmHome.addListener(SWT.Selection, e -> home());

    // Browser in RAP does not implement back() and forward()
    if (!EnvironmentUtils.getInstance().isWeb()) {
      tltmBack = new ToolItem(navigateToolBar, SWT.NONE);
      tltmBack.setImage(GuiResource.getInstance().getImageNavigateBack());
      tltmBack.setToolTipText(BaseMessages.getString(PKG, "HopGui.Documentation.Tooltip.Back"));
      tltmBack.setEnabled(false);
      tltmBack.addListener(SWT.Selection, e -> back());

      tltmForward = new ToolItem(navigateToolBar, SWT.NONE);
      tltmForward.setImage(GuiResource.getInstance().getImageNavigateForward());
      tltmForward.setToolTipText(BaseMessages.getString(PKG, "HopGui.Documentation.Tooltip.Forward"));
      tltmForward.setEnabled(false);
      tltmForward.addListener(SWT.Selection, e -> forward());
    }

    tltmRefresh = new ToolItem(navigateToolBar, SWT.NONE);
    tltmRefresh.setImage(GuiResource.getInstance().getImageRefresh());
    tltmRefresh.setToolTipText(BaseMessages.getString(PKG, "HopGui.Documentation.Tooltip.Refresh"));
    tltmRefresh.addListener(SWT.Selection, e -> refresh());

    ToolBar printToolBar = new ToolBar(shell, SWT.FLAT);
    FormData fdtoolBarPrint = new FormData();
    fdtoolBarPrint.top = new FormAttachment(0, MARGIN);
    fdtoolBarPrint.right = new FormAttachment(100, -MARGIN);
    printToolBar.setLayoutData(fdtoolBarPrint);
    printToolBar.setCursor(cursorHand);
    printToolBar.setBackground(printToolBar.getParent().getBackground());

    tltmPrint = new ToolItem(printToolBar, SWT.NONE);
    tltmPrint.setImage(GuiResource.getInstance().getImagePrint());
    tltmPrint.setToolTipText(BaseMessages.getString(PKG, "HopGui.Documentation.Tooltip.Print"));
    tltmPrint.setEnabled(true);    
    tltmPrint.addListener(SWT.Selection, e -> print());
    
    textURL = new Text(shell, SWT.BORDER);
    FormData fdtext = new FormData();
    fdtext.top = new FormAttachment(0, MARGIN);
    fdtext.right = new FormAttachment(printToolBar, -MARGIN);
    fdtext.left = new FormAttachment(navigateToolBar, MARGIN);
    textURL.setLayoutData(fdtext);
    textURL.setForeground(new Color(display, props.contrastColor( 101, 101, 101)));

    // Browser
    wBrowser = new Browser(shell, SWT.NONE);
    FormData fdBrowser = new FormData();
    fdBrowser.top = new FormAttachment(textURL, MARGIN);
    fdBrowser.right = new FormAttachment(100, 0);
    fdBrowser.bottom = new FormAttachment(100, 0);
    fdBrowser.left = new FormAttachment(0, 0);
    wBrowser.setLayoutData(fdBrowser);
    wBrowser.setUrl(url);
    props.setLook(wBrowser);

    addProgressAndLocationListener();   

    // Specs are 760/530, but due to rendering differences, we need to adjust the actual hgt/wdt
    // used
    BaseTransformDialog.setSize(shell, 755, 538, true);
    shell.setMinimumSize(515, 408);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addListener(SWT.Close, e -> ok());
    
    textURL.setFocus();
    
    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
  }

  private void addProgressAndLocationListener() {
    ProgressListener progressListener =
        new ProgressListener() {
          @Override
          public void changed(ProgressEvent event) {}

          @Override
          public void completed(ProgressEvent event) {
            if (fromPrint) {
              wBrowser.execute(PRINT_SCRIPT);
              fromPrint = false;
            }
            if (!EnvironmentUtils.getInstance().isWeb()) {
              // Browser in RAP does not implement back() and forward()
              setBackEnable(wBrowser.isBackEnabled());
              setForwardEnable(wBrowser.isForwardEnabled());
            }
          }
        };

    LocationListener listener =
        new LocationListener() {
          @Override
          public void changing(LocationEvent event) {
            if (event.location.endsWith(".pdf")) {
              Program.launch(event.location);
              event.doit = false;
            }
          }

          @Override
          public void changed(LocationEvent event) {
            textURL.setText(event.location);
          }
        };
    wBrowser.addProgressListener(progressListener);
    wBrowser.addLocationListener(listener);
  }

  private void back() {
    wBrowser.back();
  }

  private void forward() {
    wBrowser.forward();
  }

  private void refresh() {
    wBrowser.refresh();
  }

  private void home() {
    wBrowser.setUrl(homeURL != null ? homeURL : url);
  }

  private void print() {
    String printURL = wBrowser.getUrl();
    if (printURL.startsWith(PREFIX)) {
      printURL = printURL.replace(PREFIX, PRINT_PREFIX);
      fromPrint = true;
      wBrowser.setUrl(printURL);
    } else {
      wBrowser.execute(PRINT_SCRIPT);
    }
  }

  private void setBackEnable(boolean enable) {
    tltmBack.setEnabled(enable);
  }

  private void setForwardEnable(boolean enable) {
    tltmForward.setEnabled(enable);
  }

  public void dispose() {
    shell.dispose();
  }

  private void ok() {
    dispose();
  }
}
