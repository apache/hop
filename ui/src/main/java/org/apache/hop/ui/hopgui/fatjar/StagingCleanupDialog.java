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

package org.apache.hop.ui.hopgui.fatjar;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.TreeSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fatjar.IHasStagingFolder;
import org.apache.hop.core.fatjar.StagingCleanup;
import org.apache.hop.core.fatjar.VfsStagingStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

/**
 * Lists the files Hop staged for cluster runs in a folder, and removes the ones no run used within
 * the retention.
 */
public class StagingCleanupDialog extends Dialog {

  private static final Class<?> PKG = StagingCleanupDialog.class;

  private static final DateTimeFormatter LAST_USED =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm").withZone(ZoneId.systemDefault());

  private final IVariables variables;
  private final IHopMetadataProvider metadataProvider;
  private final ILogChannel log;

  private Shell shell;
  private ComboVar wFolder;
  private TextVar wRetention;
  private TableView wFiles;
  private Button wRemove;
  private List<StagingCleanup.Entry> entries = List.of();

  public StagingCleanupDialog(
      Shell parent, IVariables variables, IHopMetadataProvider metadataProvider, ILogChannel log) {
    super(parent, SWT.NONE);
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.log = log;
  }

  public void open() {
    shell = new Shell(getParent(), SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.APPLICATION_MODAL);
    PropsUi.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    shell.setText(BaseMessages.getString(PKG, "StagingCleanupDialog.Title"));
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);
    int margin = PropsUi.getMargin();
    int middle = PropsUi.getInstance().getMiddlePct();

    // Staging folder, with the folders of the run configurations to pick from
    Label wlFolder = label("StagingCleanupDialog.Folder.Label", middle, null, margin);
    Button wBrowse = new Button(shell, SWT.PUSH);
    PropsUi.setLook(wBrowse);
    wBrowse.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdBrowse = new FormData();
    fdBrowse.right = new FormAttachment(100, 0);
    fdBrowse.top = new FormAttachment(wlFolder, 0, SWT.CENTER);
    wBrowse.setLayoutData(fdBrowse);
    wFolder = new ComboVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFolder);
    wFolder.setToolTipText(BaseMessages.getString(PKG, "StagingCleanupDialog.Folder.Tooltip"));
    wFolder.setItems(runConfigurationFolders());
    if (wFolder.getItemCount() > 0) {
      wFolder.select(0);
    }
    place(wFolder, wlFolder, middle, margin, wBrowse);
    wBrowse.addListener(
        SWT.Selection,
        e -> {
          String folder =
              BaseDialog.presentDirectoryDialog(shell, wFolder.getText(), null, variables);
          if (folder != null) {
            wFolder.setText(folder);
          }
        });

    // Retention
    Label wlRetention = label("StagingCleanupDialog.Retention.Label", middle, wFolder, margin);
    wRetention = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wRetention);
    wRetention.setToolTipText(
        BaseMessages.getString(PKG, "StagingCleanupDialog.Retention.Tooltip"));
    int days = StagingCleanup.retentionDays(variables);
    wRetention.setText(Integer.toString(days > 0 ? days : StagingCleanup.DEFAULT_RETENTION_DAYS));
    place(wRetention, wlRetention, middle, margin, null);

    Button wList = new Button(shell, SWT.PUSH);
    PropsUi.setLook(wList);
    wList.setText(BaseMessages.getString(PKG, "StagingCleanupDialog.List.Button"));
    FormData fdList = new FormData();
    fdList.left = new FormAttachment(middle, margin);
    fdList.top = new FormAttachment(wRetention, margin);
    wList.setLayoutData(fdList);
    wList.addListener(SWT.Selection, e -> refresh());

    // Buttons at the bottom, the files in between
    wRemove = new Button(shell, SWT.PUSH);
    wRemove.setText(BaseMessages.getString(PKG, "StagingCleanupDialog.Remove.Button"));
    wRemove.setEnabled(false);
    wRemove.addListener(SWT.Selection, e -> removeExpired());
    Button wClose = new Button(shell, SWT.PUSH);
    wClose.setText(BaseMessages.getString(PKG, "System.Button.Close"));
    wClose.addListener(SWT.Selection, e -> dispose());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wRemove, wClose}, margin, null);

    ColumnInfo[] columns = {
      column("StagingCleanupDialog.Column.File"),
      column("StagingCleanupDialog.Column.Size"),
      column("StagingCleanupDialog.Column.LastUsed"),
      column("StagingCleanupDialog.Column.State"),
    };
    wFiles =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            0,
            true,
            null,
            PropsUi.getInstance());
    FormData fdFiles = new FormData();
    fdFiles.left = new FormAttachment(0, 0);
    fdFiles.top = new FormAttachment(wList, margin);
    fdFiles.right = new FormAttachment(100, 0);
    fdFiles.bottom = new FormAttachment(wClose, -2 * margin);
    wFiles.setLayoutData(fdFiles);

    BaseDialog.defaultShellHandling(shell, c -> dispose(), c -> dispose());
  }

  private void refresh() {
    wFiles.clearAll(false);
    entries = List.of();
    wRemove.setEnabled(false);
    try {
      VfsStagingStore store = store();
      entries = StagingCleanup.inspect(store, retention(), Instant.now());
      for (StagingCleanup.Entry entry : entries) {
        TableItem item = new TableItem(wFiles.table, SWT.NONE);
        item.setText(1, entry.name());
        item.setText(2, megabytes(entry.size()));
        item.setText(3, entry.lastUsed() != null ? LAST_USED.format(entry.lastUsed()) : "");
        item.setText(4, entry.state().getDescription());
      }
      wFiles.removeEmptyRows();
      wFiles.setRowNums();
      wFiles.optWidth(true);
      wRemove.setEnabled(entries.stream().anyMatch(e -> e.state() == StagingCleanup.State.EXPIRED));
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "StagingCleanupDialog.Error.Header"),
          BaseMessages.getString(PKG, "StagingCleanupDialog.Error.List"),
          e);
    }
  }

  private void removeExpired() {
    List<StagingCleanup.Entry> expired =
        entries.stream().filter(e -> e.state() == StagingCleanup.State.EXPIRED).toList();
    long bytes = expired.stream().mapToLong(StagingCleanup.Entry::size).sum();
    MessageBox confirm = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION);
    confirm.setText(BaseMessages.getString(PKG, "StagingCleanupDialog.Confirm.Header"));
    confirm.setMessage(
        BaseMessages.getString(
            PKG,
            "StagingCleanupDialog.Confirm.Message",
            Integer.toString(expired.size()),
            megabytes(bytes)));
    if (confirm.open() != SWT.YES) {
      return;
    }
    try {
      StagingCleanup.Result result =
          StagingCleanup.cleanUp(log, store(), retention(), Instant.now());
      if (!result.failed().isEmpty()) {
        throw new HopException(
            String.join(
                "\n",
                result.failed().entrySet().stream()
                    .map(f -> f.getKey() + ": " + f.getValue())
                    .toList()));
      }
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "StagingCleanupDialog.Error.Header"),
          BaseMessages.getString(PKG, "StagingCleanupDialog.Error.Remove"),
          e);
    }
    refresh();
  }

  private VfsStagingStore store() throws HopException {
    return new VfsStagingStore(
        variables,
        wFolder.getText(),
        null,
        BaseMessages.getString(PKG, "StagingCleanupDialog.Error.NoFolder"));
  }

  private Duration retention() throws HopException {
    String value = variables.resolve(wRetention.getText());
    try {
      int days = Integer.parseInt(StringUtils.trimToEmpty(value));
      if (days >= 1) {
        return Duration.ofDays(days);
      }
    } catch (NumberFormatException e) {
      // reported below
    }
    throw new HopException(
        BaseMessages.getString(PKG, "StagingCleanupDialog.Error.Retention", value));
  }

  /** The staging folders of the pipeline run configurations, as they are written. */
  private String[] runConfigurationFolders() {
    TreeSet<String> folders = new TreeSet<>();
    try {
      for (PipelineRunConfiguration configuration :
          metadataProvider.getSerializer(PipelineRunConfiguration.class).loadAll()) {
        if (configuration.getEngineRunConfiguration() instanceof IHasStagingFolder staged
            && StringUtils.isNotBlank(staged.getStagingFolder())) {
          folders.add(staged.getStagingFolder().trim());
        }
      }
    } catch (Exception e) {
      log.logDetailed("Unable to read the staging folders of the run configurations: " + e);
    }
    return folders.toArray(new String[0]);
  }

  private Label label(String key, int middle, Control above, int margin) {
    Label label = new Label(shell, SWT.RIGHT);
    PropsUi.setLook(label);
    label.setText(BaseMessages.getString(PKG, key));
    FormData fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(middle, -margin);
    fd.top = above == null ? new FormAttachment(0, margin) : new FormAttachment(above, margin);
    label.setLayoutData(fd);
    return label;
  }

  private static void place(Control control, Label label, int middle, int margin, Control right) {
    FormData fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(label, 0, SWT.CENTER);
    fd.right = right == null ? new FormAttachment(100, 0) : new FormAttachment(right, -margin);
    control.setLayoutData(fd);
  }

  private static ColumnInfo column(String key) {
    ColumnInfo column =
        new ColumnInfo(BaseMessages.getString(PKG, key), ColumnInfo.COLUMN_TYPE_TEXT, false, true);
    column.setReadOnly(true);
    return column;
  }

  private static String megabytes(long bytes) {
    return String.format("%,.1f MB", bytes / (1024.0 * 1024.0));
  }

  private void dispose() {
    PropsUi.getInstance().setScreen(new WindowProperty(shell));
    shell.dispose();
  }
}
