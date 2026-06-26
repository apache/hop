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

package org.apache.hop.ui.core.database;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.IRunnableWithProgress;
import org.apache.hop.core.database.DriverDownload;
import org.apache.hop.driver.DriverInstaller;
import org.apache.hop.driver.DriverResolver;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.ProgressMonitorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.program.Program;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Link;
import org.eclipse.swt.widgets.Shell;

/**
 * A resizable, movable dialog that confirms downloading a JDBC driver and - for restricted
 * (Category X) drivers - has the user accept the vendor license, then downloads it from Maven
 * Central into lib/jdbc and hot-loads it into the database plugin's classloader.
 *
 * <p>ASF compliance: Apache Hop never bundles, hosts or mirrors restricted drivers. This dialog
 * surfaces the license and only fetches the driver, from Maven Central onto the user's machine,
 * after the user explicitly accepts and clicks Download. The driver descriptor itself comes from
 * the database plugin ({@link DriverDownload}).
 */
public class JdbcDriverDownloadDialog {
  private static final Class<?> PKG = JdbcDriverDownloadDialog.class;

  private final Shell parent;
  private final String databaseType;
  private final String driverName;
  private final DriverDownload download;

  private Shell shell;
  private final PropsUi props;
  private Button wAccept;
  private Button wDownload;
  private boolean installed;

  public JdbcDriverDownloadDialog(
      Shell parent, String databaseType, String driverName, DriverDownload download) {
    this.parent = parent;
    this.databaseType = databaseType;
    this.driverName = driverName;
    this.download = download;
    this.props = PropsUi.getInstance();
  }

  /**
   * @return true if the driver was downloaded and installed, false if the user cancelled or it
   *     failed.
   */
  public boolean open() {
    // A normal resizable, movable window (no SWT.SHEET, which on macOS attaches a fixed sheet).
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.APPLICATION_MODAL);
    PropsUi.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    shell.setText(BaseMessages.getString(PKG, "JdbcDriverDownloadDialog.Shell.Title"));

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);

    int margin = PropsUi.getMargin();
    boolean restricted = download.isRestricted();

    // Intro line
    Label wIntro = new Label(shell, SWT.WRAP);
    PropsUi.setLook(wIntro);
    wIntro.setText(
        restricted
            ? BaseMessages.getString(PKG, "JdbcDriverDownloadDialog.Intro.Restricted", driverName)
            : BaseMessages.getString(PKG, "JdbcDriverDownloadDialog.Intro.Open", driverName));
    FormData fdIntro = new FormData();
    fdIntro.top = new FormAttachment(0, margin);
    fdIntro.left = new FormAttachment(0, 0);
    fdIntro.right = new FormAttachment(100, 0);
    wIntro.setLayoutData(fdIntro);
    Control last = wIntro;

    last = addInfoRow(last, margin, "JdbcDriverDownloadDialog.Field.Artifact", driverCoordinate());
    last =
        addInfoRow(
            last,
            margin,
            "JdbcDriverDownloadDialog.Field.License",
            download.getLicenseName() + " (category " + download.getLicenseCategory() + ")");
    last =
        addLinkRow(
            last, margin, "JdbcDriverDownloadDialog.Field.LicenseUrl", download.getLicenseUrl());
    last =
        addLinkRow(last, margin, "JdbcDriverDownloadDialog.Field.Vendor", download.getVendorUrl());
    last =
        addInfoRow(
            last, margin, "JdbcDriverDownloadDialog.Field.Source", DriverResolver.MAVEN_CENTRAL);

    if (download.getNotes() != null) {
      last = addInfoRow(last, margin, "JdbcDriverDownloadDialog.Field.Notes", download.getNotes());
    }

    // License acceptance checkbox (restricted drivers only)
    if (restricted) {
      wAccept = new Button(shell, SWT.CHECK);
      PropsUi.setLook(wAccept);
      wAccept.setText(
          BaseMessages.getString(
              PKG, "JdbcDriverDownloadDialog.AcceptLicense", download.getLicenseName()));
      FormData fdAccept = new FormData();
      fdAccept.top = new FormAttachment(last, margin * 2);
      fdAccept.left = new FormAttachment(0, 0);
      fdAccept.right = new FormAttachment(100, 0);
      wAccept.setLayoutData(fdAccept);
      wAccept.addListener(SWT.Selection, e -> wDownload.setEnabled(wAccept.getSelection()));
      last = wAccept;
    }

    // Buttons
    wDownload = new Button(shell, SWT.PUSH);
    wDownload.setText(BaseMessages.getString(PKG, "JdbcDriverDownloadDialog.Button.Download"));
    wDownload.setEnabled(!restricted);
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wDownload, wCancel}, margin, last);

    wDownload.addListener(SWT.Selection, e -> doDownload());
    wCancel.addListener(SWT.Selection, e -> dispose());

    BaseDialog.defaultShellHandling(shell, c -> doDownload(), c -> dispose());
    return installed;
  }

  private String driverCoordinate() {
    return download.getMavenCoordinate() + ":" + download.getDefaultVersion();
  }

  /** A label + value row of plain text. */
  private Control addInfoRow(Control top, int margin, String labelKey, String value) {
    Label wLabel = new Label(shell, SWT.RIGHT);
    PropsUi.setLook(wLabel);
    wLabel.setText(BaseMessages.getString(PKG, labelKey));
    FormData fdl = new FormData();
    fdl.top = new FormAttachment(top, margin);
    fdl.left = new FormAttachment(0, 0);
    fdl.right = new FormAttachment(props.getMiddlePct(), -margin);
    wLabel.setLayoutData(fdl);

    Label wValue = new Label(shell, SWT.WRAP);
    PropsUi.setLook(wValue);
    wValue.setText(value == null ? "" : value);
    FormData fdv = new FormData();
    fdv.top = new FormAttachment(top, margin);
    fdv.left = new FormAttachment(props.getMiddlePct(), 0);
    fdv.right = new FormAttachment(100, 0);
    wValue.setLayoutData(fdv);
    return wValue;
  }

  /** A label + clickable link row. */
  private Control addLinkRow(Control top, int margin, String labelKey, String url) {
    Label wLabel = new Label(shell, SWT.RIGHT);
    PropsUi.setLook(wLabel);
    wLabel.setText(BaseMessages.getString(PKG, labelKey));
    FormData fdl = new FormData();
    fdl.top = new FormAttachment(top, margin);
    fdl.left = new FormAttachment(0, 0);
    fdl.right = new FormAttachment(props.getMiddlePct(), -margin);
    wLabel.setLayoutData(fdl);

    Link wLink = new Link(shell, SWT.NONE);
    PropsUi.setLook(wLink);
    String safeUrl = url == null ? "" : url;
    wLink.setText(safeUrl.isEmpty() ? "" : "<a>" + safeUrl + "</a>");
    wLink.addListener(SWT.Selection, e -> Program.launch(safeUrl));
    FormData fdv = new FormData();
    fdv.top = new FormAttachment(top, margin);
    fdv.left = new FormAttachment(props.getMiddlePct(), 0);
    fdv.right = new FormAttachment(100, 0);
    wLink.setLayoutData(fdv);
    return wLink;
  }

  private void doDownload() {
    if (download.isRestricted() && (wAccept == null || !wAccept.getSelection())) {
      return; // license not accepted yet
    }

    File target = DriverInstaller.defaultInstallFolder();
    final List<File> result = new ArrayList<>();
    IRunnableWithProgress op =
        monitor -> {
          monitor.beginTask(
              BaseMessages.getString(
                  PKG, "JdbcDriverDownloadDialog.Progress.Downloading", driverName),
              1);
          try {
            result.addAll(new DriverInstaller().install(download, null, null, target));
          } catch (Exception ex) {
            throw new InvocationTargetException(ex, ex.getMessage());
          }
          monitor.worked(1);
          monitor.done();
        };

    try {
      ProgressMonitorDialog pmd = new ProgressMonitorDialog(shell);
      pmd.run(true, op);
    } catch (InvocationTargetException | InterruptedException ex) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "JdbcDriverDownloadDialog.Error.Title"),
          BaseMessages.getString(PKG, "JdbcDriverDownloadDialog.Error.Message", driverName),
          ex);
      if (ex instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      return;
    }

    installed = true;

    // Try to hot-load so no restart is needed.
    boolean hotLoaded = DriverInstaller.hotLoad(databaseType, result);

    MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
    box.setText(BaseMessages.getString(PKG, "JdbcDriverDownloadDialog.Done.Title"));
    box.setMessage(
        BaseMessages.getString(
            PKG,
            hotLoaded
                ? "JdbcDriverDownloadDialog.Done.MessageReady"
                : "JdbcDriverDownloadDialog.Done.Message",
            Integer.toString(result.size()),
            target.getAbsolutePath(),
            driverName));
    box.open();
    dispose();
  }

  private void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }
}
