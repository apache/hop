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

package org.apache.hop.ui.core.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
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
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

/**
 * Modeless documentation viewer parented to the current context shell (transform/action dialog or
 * the main Hop GUI window).
 */
public class ShowHelpDialog extends Dialog {
  public static final String SHELL_DATA_KEY = "hop.help.dialog";

  private static final Class<?> PKG = HopGui.class;

  private static final String PRINT_SCRIPT = "javascript:window.print();";
  private static final int MARGIN = 5;
  private static final int DEFAULT_WIDTH = 900;
  private static final int DEFAULT_HEIGHT = 700;

  private final String dialogTitle;
  private String url;
  private final String homeURL;

  private Browser wBrowser;

  private ToolItem tltmBack;
  private ToolItem tltmForward;

  private Text textURL;

  private Shell shell;

  public ShowHelpDialog(Shell parent, String url) {
    this(parent, BaseMessages.getString(PKG, "HopGui.Documentation.Hop.Title"), url);
  }

  public ShowHelpDialog(Shell parent, String dialogTitle, String url) {
    super(parent, SWT.NONE);
    this.dialogTitle =
        Utils.isEmpty(dialogTitle)
            ? BaseMessages.getString(PKG, "HopGui.Documentation.Hop.Title")
            : dialogTitle;
    this.url = url;
    this.homeURL = Const.getDocUrl("");
  }

  protected Shell createShell(Shell parent) {
    return new Shell(parent, helpDialogStyle());
  }

  static int helpDialogStyle() {
    if (EnvironmentUtils.getInstance().isWeb()) {
      return SWT.DIALOG_TRIM | SWT.RESIZE;
    }
    return SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN;
  }

  public void open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();
    PropsUi props = PropsUi.getInstance();

    shell = createShell(parent);
    shell.setImage(GuiResource.getInstance().getImageHelp());
    shell.setLayout(new FormLayout());
    shell.setText(dialogTitle);
    PropsUi.setLook(shell);

    Cursor cursorHand = new Cursor(display, SWT.CURSOR_HAND);
    Color urlColor = new Color(display, props.contrastColor(101, 101, 101));
    shell.addListener(SWT.Close, e -> PropsUi.getInstance().setScreen(new WindowProperty(shell)));
    shell.addDisposeListener(
        e -> {
          if (cursorHand != null && !cursorHand.isDisposed()) {
            cursorHand.dispose();
          }
          if (urlColor != null && !urlColor.isDisposed()) {
            urlColor.dispose();
          }
          if (parent != null && !parent.isDisposed() && parent.getData(SHELL_DATA_KEY) == this) {
            parent.setData(SHELL_DATA_KEY, null);
          }
        });

    ToolBar navigateToolBar = new ToolBar(shell, SWT.FLAT);
    FormData fdtoolBarBack = new FormData();
    fdtoolBarBack.top = new FormAttachment(0, MARGIN);
    fdtoolBarBack.left = new FormAttachment(0, 0);
    navigateToolBar.setLayoutData(fdtoolBarBack);
    navigateToolBar.setCursor(cursorHand);
    navigateToolBar.setBackground(navigateToolBar.getParent().getBackground());

    ToolItem tltmHome = new ToolItem(navigateToolBar, SWT.NONE);
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
      tltmForward.setToolTipText(
          BaseMessages.getString(PKG, "HopGui.Documentation.Tooltip.Forward"));
      tltmForward.setEnabled(false);
      tltmForward.addListener(SWT.Selection, e -> forward());
    }

    ToolItem tltmRefresh = new ToolItem(navigateToolBar, SWT.NONE);
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

    ToolItem tltmPrint = new ToolItem(printToolBar, SWT.NONE);
    tltmPrint.setImage(GuiResource.getInstance().getImagePrint());
    tltmPrint.setToolTipText(BaseMessages.getString(PKG, "HopGui.Documentation.Tooltip.Print"));
    tltmPrint.setEnabled(true);
    tltmPrint.addListener(SWT.Selection, e -> print());

    ToolItem tltmExternal = new ToolItem(printToolBar, SWT.NONE);
    tltmExternal.setImage(GuiResource.getInstance().getImage("ui/images/html.svg"));
    tltmExternal.setToolTipText(
        BaseMessages.getString(PKG, "HopGui.Documentation.Tooltip.OpenExternal"));
    tltmExternal.addListener(SWT.Selection, e -> openExternal());

    textURL = new Text(shell, SWT.BORDER);
    FormData fdtext = new FormData();
    fdtext.top = new FormAttachment(0, MARGIN);
    fdtext.right = new FormAttachment(printToolBar, -MARGIN);
    fdtext.left = new FormAttachment(navigateToolBar, MARGIN);
    textURL.setLayoutData(fdtext);
    textURL.setForeground(urlColor);
    textURL.setText(Const.NVL(url, ""));
    textURL.addListener(
        SWT.DefaultSelection,
        e -> {
          String location = textURL.getText();
          if (!Utils.isEmpty(location) && wBrowser != null && !wBrowser.isDisposed()) {
            wBrowser.setUrl(location);
          }
        });

    try {
      wBrowser = new Browser(shell, SWT.NONE);
    } catch (RuntimeException e) {
      dispose();
      throw e;
    }
    FormData fdBrowser = new FormData();
    fdBrowser.top = new FormAttachment(textURL, MARGIN);
    fdBrowser.right = new FormAttachment(100, 0);
    fdBrowser.bottom = new FormAttachment(100, 0);
    fdBrowser.left = new FormAttachment(0, 0);
    wBrowser.setLayoutData(fdBrowser);
    wBrowser.setUrl(url);
    PropsUi.setLook(wBrowser);

    addProgressAndLocationListener();

    shell.addListener(
        SWT.Traverse,
        e -> {
          if (e.detail == SWT.TRAVERSE_ESCAPE) {
            e.doit = false;
            dispose();
          }
        });

    BaseTransformDialog.setSize(shell, DEFAULT_WIDTH, DEFAULT_HEIGHT);
    textURL.setFocus();
    shell.open();
  }

  private void addProgressAndLocationListener() {
    ProgressListener progressListener =
        new ProgressListener() {
          @Override
          public void changed(ProgressEvent event) {
            // Disable changed event
          }

          @Override
          public void completed(ProgressEvent event) {
            if (!EnvironmentUtils.getInstance().isWeb() && tltmBack != null) {
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
            if (event.location != null && event.location.endsWith(".pdf")) {
              try {
                EnvironmentUtils.getInstance().openUrl(event.location);
              } catch (Exception e) {
                new ErrorDialog(shell, "Error", "Error opening URL", e);
              }
              event.doit = false;
            }
          }

          @Override
          public void changed(LocationEvent event) {
            if (event.location != null && textURL != null && !textURL.isDisposed()) {
              textURL.setText(event.location);
            }
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
    wBrowser.execute(PRINT_SCRIPT);
  }

  private void openExternal() {
    String location = wBrowser.getUrl();
    if (Utils.isEmpty(location)) {
      location = url;
    }
    try {
      EnvironmentUtils.getInstance().openUrl(location);
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error opening URL", e);
    }
  }

  private void setBackEnable(boolean enable) {
    if (tltmBack != null && !tltmBack.isDisposed()) {
      tltmBack.setEnabled(enable);
    }
  }

  private void setForwardEnable(boolean enable) {
    if (tltmForward != null && !tltmForward.isDisposed()) {
      tltmForward.setEnabled(enable);
    }
  }

  public void setUrl(String url) {
    this.url = url;
    if (wBrowser != null && !wBrowser.isDisposed()) {
      wBrowser.setUrl(url);
    }
    if (textURL != null && !textURL.isDisposed() && url != null) {
      textURL.setText(url);
    }
  }

  public boolean isDisposed() {
    return shell == null || shell.isDisposed();
  }

  public void forceActive() {
    if (shell != null && !shell.isDisposed()) {
      shell.setMinimized(false);
      shell.setActive();
      shell.forceActive();
    }
  }

  public void dispose() {
    if (shell != null && !shell.isDisposed()) {
      shell.dispose();
    }
  }
}
