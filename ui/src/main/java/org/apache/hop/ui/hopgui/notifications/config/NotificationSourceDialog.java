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

package org.apache.hop.ui.hopgui.notifications.config;

import java.util.UUID;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.ColorDialog;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/** Dialog for adding or editing a notification source configuration. */
public class NotificationSourceDialog {

  private static final Class<?> PKG = NotificationSourceDialog.class;

  private Shell shell;
  private Shell parentShell;
  private NotificationSourceConfig sourceConfig;
  private boolean cancelled = false;
  private PropsUi props = PropsUi.getInstance();

  // UI widgets
  private TextVar wName;
  private Combo wType;
  private Button wEnabled;
  private Button wColorButton;
  private Label wColorPreview;
  private Composite wTypeSpecificComposite;
  private TextVar wGithubUrl; // For URL input
  private TextVar wGithubOwner;
  private TextVar wGithubRepo;
  private Button wGithubIncludePrereleases;
  private TextVar wRssUrl;
  private TextVar wPluginId;
  private TextVar wPollInterval;
  private TextVar wDaysToGoBack;

  public NotificationSourceDialog(Shell parent, NotificationSourceConfig sourceConfig) {
    this.parentShell = parent;
    this.shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    this.sourceConfig = sourceConfig != null ? sourceConfig : new NotificationSourceConfig();
    props.setLook(this.shell);
  }

  public String open() {
    Display display = parentShell.getDisplay();

    shell.setText(BaseMessages.getString(PKG, "NotificationSourceDialog.Title"));

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);

    int margin = PropsUi.getMargin();
    int middle = 50;

    // Name field
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "NotificationSourceDialog.Name"));
    props.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    fdlName.top = new FormAttachment(0, margin);
    wlName.setLayoutData(fdlName);

    wName =
        new TextVar(HopGui.getInstance().getVariables(), shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wName);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, 0);
    fdName.right = new FormAttachment(100, 0);
    fdName.top = new FormAttachment(0, margin);
    wName.setLayoutData(fdName);

    // Type field
    Label wlType = new Label(shell, SWT.RIGHT);
    wlType.setText(BaseMessages.getString(PKG, "NotificationSourceDialog.Type"));
    props.setLook(wlType);
    FormData fdlType = new FormData();
    fdlType.left = new FormAttachment(0, 0);
    fdlType.right = new FormAttachment(middle, -margin);
    fdlType.top = new FormAttachment(wName, margin);
    wlType.setLayoutData(fdlType);

    wType = new Combo(shell, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    props.setLook(wType);
    for (NotificationSourceConfig.SourceType type : NotificationSourceConfig.SourceType.values()) {
      wType.add(type.getDisplayName());
    }
    FormData fdType = new FormData();
    fdType.left = new FormAttachment(middle, 0);
    fdType.right = new FormAttachment(100, 0);
    fdType.top = new FormAttachment(wName, margin);
    wType.setLayoutData(fdType);
    wType.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            updateTypeSpecificFields();
          }
        });

    // Enabled checkbox
    wEnabled = new Button(shell, SWT.CHECK);
    wEnabled.setText(BaseMessages.getString(PKG, "NotificationSourceDialog.Enabled"));
    props.setLook(wEnabled);
    FormData fdEnabled = new FormData();
    fdEnabled.left = new FormAttachment(middle, 0);
    fdEnabled.top = new FormAttachment(wType, margin);
    wEnabled.setLayoutData(fdEnabled);

    // Color picker
    Label wlColor = new Label(shell, SWT.RIGHT);
    wlColor.setText(BaseMessages.getString(PKG, "NotificationSourceDialog.Color"));
    props.setLook(wlColor);
    FormData fdlColor = new FormData();
    fdlColor.left = new FormAttachment(0, 0);
    fdlColor.right = new FormAttachment(middle, -margin);
    fdlColor.top = new FormAttachment(wEnabled, margin);
    wlColor.setLayoutData(fdlColor);

    Composite wColorComposite = new Composite(shell, SWT.NONE);
    FormLayout colorLayout = new FormLayout();
    wColorComposite.setLayout(colorLayout);
    FormData fdColorComposite = new FormData();
    fdColorComposite.left = new FormAttachment(middle, 0);
    fdColorComposite.top = new FormAttachment(wEnabled, margin);
    wColorComposite.setLayoutData(fdColorComposite);

    wColorPreview = new Label(wColorComposite, SWT.BORDER);
    wColorPreview.setText("  ");
    props.setLook(wColorPreview);
    FormData fdColorPreview = new FormData();
    fdColorPreview.left = new FormAttachment(0, 0);
    fdColorPreview.top = new FormAttachment(0, 0);
    fdColorPreview.width = 30;
    fdColorPreview.height = 20;
    wColorPreview.setLayoutData(fdColorPreview);

    wColorButton = new Button(wColorComposite, SWT.PUSH);
    wColorButton.setText(BaseMessages.getString(PKG, "NotificationSourceDialog.ChooseColor"));
    props.setLook(wColorButton);
    FormData fdColorButton = new FormData();
    fdColorButton.left = new FormAttachment(wColorPreview, margin);
    fdColorButton.top = new FormAttachment(0, 0);
    wColorButton.setLayoutData(fdColorButton);
    wColorButton.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            ColorDialog colorDialog = new ColorDialog(shell);
            if (sourceConfig.getColor() != null) {
              try {
                int colorValue = Integer.parseInt(sourceConfig.getColor().substring(1), 16);
                colorDialog.setRGB(
                    new org.eclipse.swt.graphics.RGB(
                        (colorValue >> 16) & 0xFF, (colorValue >> 8) & 0xFF, colorValue & 0xFF));
              } catch (Exception ex) {
                // Ignore
              }
            }
            org.eclipse.swt.graphics.RGB rgb = colorDialog.open();
            if (rgb != null) {
              String hexColor = String.format("#%02X%02X%02X", rgb.red, rgb.green, rgb.blue);
              sourceConfig.setColor(hexColor);
              updateColorPreview();
            }
          }
        });

    // Type-specific composite
    wTypeSpecificComposite = new Composite(shell, SWT.NONE);
    props.setLook(wTypeSpecificComposite);
    wTypeSpecificComposite.setLayout(new FormLayout());
    FormData fdTypeSpecific = new FormData();
    fdTypeSpecific.left = new FormAttachment(0, 0);
    fdTypeSpecific.right = new FormAttachment(100, 0);
    fdTypeSpecific.top = new FormAttachment(wColorComposite, margin);
    fdTypeSpecific.bottom = new FormAttachment(100, -50);
    wTypeSpecificComposite.setLayoutData(fdTypeSpecific);

    // Buttons
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "NotificationSourceDialog.OK"));
    wOk.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            if (saveSource()) {
              cancelled = false;
              shell.dispose();
            }
          }
        });

    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "NotificationSourceDialog.Cancel"));
    wCancel.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            cancelled = true;
            shell.dispose();
          }
        });

    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    // Load existing values
    loadSource();

    // Set initial type-specific fields
    updateTypeSpecificFields();

    shell.pack();
    shell.setSize(500, 400);

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }

    return cancelled ? null : sourceConfig.getId();
  }

  private void loadSource() {
    if (sourceConfig.getName() != null) {
      wName.setText(sourceConfig.getName());
    }
    if (sourceConfig.getType() != null) {
      wType.select(sourceConfig.getType().ordinal());
    } else {
      wType.select(0);
    }
    wEnabled.setSelection(sourceConfig.isEnabled());
    updateColorPreview();
  }

  private void updateColorPreview() {
    if (sourceConfig.getColor() != null && !sourceConfig.getColor().isEmpty()) {
      try {
        String hex =
            sourceConfig.getColor().startsWith("#")
                ? sourceConfig.getColor().substring(1)
                : sourceConfig.getColor();
        int colorValue = Integer.parseInt(hex, 16);
        org.eclipse.swt.graphics.RGB rgb =
            new org.eclipse.swt.graphics.RGB(
                (colorValue >> 16) & 0xFF, (colorValue >> 8) & 0xFF, colorValue & 0xFF);
        wColorPreview.setBackground(shell.getDisplay().getSystemColor(SWT.COLOR_WIDGET_BACKGROUND));
        // Note: We can't easily set background color without creating a Color object
        // For now, just show the hex value as text
        wColorPreview.setText(sourceConfig.getColor());
      } catch (Exception e) {
        wColorPreview.setText("#000000");
      }
    } else {
      wColorPreview.setText("#000000");
    }
  }

  private void updateTypeSpecificFields() {
    // Dispose existing widgets
    for (Control child : wTypeSpecificComposite.getChildren()) {
      child.dispose();
    }

    NotificationSourceConfig.SourceType selectedType =
        NotificationSourceConfig.SourceType.values()[wType.getSelectionIndex()];

    int margin = PropsUi.getMargin();
    int middle = 50;
    int yPos = margin;

    switch (selectedType) {
      case GITHUB_RELEASES:
        // GitHub URL field (for easy input)
        Label wlGithubUrl = new Label(wTypeSpecificComposite, SWT.RIGHT);
        wlGithubUrl.setText(BaseMessages.getString(PKG, "NotificationSourceDialog.GithubUrl"));
        props.setLook(wlGithubUrl);
        FormData fdlGithubUrl = new FormData();
        fdlGithubUrl.left = new FormAttachment(0, 0);
        fdlGithubUrl.right = new FormAttachment(middle, -margin);
        fdlGithubUrl.top = new FormAttachment(0, yPos);
        wlGithubUrl.setLayoutData(fdlGithubUrl);

        wGithubUrl =
            new TextVar(
                HopGui.getInstance().getVariables(),
                wTypeSpecificComposite,
                SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wGithubUrl);
        FormData fdGithubUrl = new FormData();
        fdGithubUrl.left = new FormAttachment(middle, 0);
        fdGithubUrl.right = new FormAttachment(100, -80);
        fdGithubUrl.top = new FormAttachment(0, yPos);
        wGithubUrl.setLayoutData(fdGithubUrl);

        // Pre-fill URL if owner/repo exist
        if (sourceConfig.getGithubOwner() != null && sourceConfig.getGithubRepo() != null) {
          wGithubUrl.setText(
              "https://github.com/"
                  + sourceConfig.getGithubOwner()
                  + "/"
                  + sourceConfig.getGithubRepo());
        }

        Button wParseUrl = new Button(wTypeSpecificComposite, SWT.PUSH);
        wParseUrl.setText(BaseMessages.getString(PKG, "NotificationSourceDialog.Parse"));
        props.setLook(wParseUrl);
        FormData fdParseUrl = new FormData();
        fdParseUrl.left = new FormAttachment(wGithubUrl, margin);
        fdParseUrl.top = new FormAttachment(0, yPos);
        wParseUrl.setLayoutData(fdParseUrl);
        wParseUrl.addSelectionListener(
            new SelectionAdapter() {
              @Override
              public void widgetSelected(SelectionEvent e) {
                String url = wGithubUrl.getText().trim();
                if (!Utils.isEmpty(url)) {
                  parseGitHubUrl(url);
                }
              }

              private void parseGitHubUrl(String input) {
                String owner = null;
                String repo = null;

                if (input.contains("github.com/")) {
                  String[] parts = input.split("github.com/");
                  if (parts.length > 1) {
                    String path =
                        parts[1].split("\\?")[0].split("#")[0]; // Remove query params and fragments
                    String[] ownerRepo = path.split("/");
                    if (ownerRepo.length >= 2) {
                      owner = ownerRepo[0].trim();
                      repo = ownerRepo[1].trim();
                    }
                  }
                } else if (input.contains("/")) {
                  String[] parts = input.split("/");
                  if (parts.length >= 2) {
                    owner = parts[0].trim();
                    repo = parts[1].trim();
                  }
                }

                if (owner != null && !owner.isEmpty() && repo != null && !repo.isEmpty()) {
                  wGithubOwner.setText(owner);
                  wGithubRepo.setText(repo);
                } else {
                  new ErrorDialog(
                      shell,
                      "Error",
                      "Could not parse GitHub URL. Please enter a URL like https://github.com/owner/repo or owner/repo",
                      new Exception());
                }
              }
            });

        yPos += 30;

        Label wlGithubOwner = new Label(wTypeSpecificComposite, SWT.RIGHT);
        wlGithubOwner.setText(BaseMessages.getString(PKG, "NotificationSourceDialog.Owner"));
        props.setLook(wlGithubOwner);
        FormData fdlGithubOwner = new FormData();
        fdlGithubOwner.left = new FormAttachment(0, 0);
        fdlGithubOwner.right = new FormAttachment(middle, -margin);
        fdlGithubOwner.top = new FormAttachment(0, yPos);
        wlGithubOwner.setLayoutData(fdlGithubOwner);

        wGithubOwner =
            new TextVar(
                HopGui.getInstance().getVariables(),
                wTypeSpecificComposite,
                SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wGithubOwner);
        FormData fdGithubOwner = new FormData();
        fdGithubOwner.left = new FormAttachment(middle, 0);
        fdGithubOwner.right = new FormAttachment(100, 0);
        fdGithubOwner.top = new FormAttachment(0, yPos);
        wGithubOwner.setLayoutData(fdGithubOwner);
        if (sourceConfig.getGithubOwner() != null) {
          wGithubOwner.setText(sourceConfig.getGithubOwner());
        }
        yPos += 30;

        Label wlGithubRepo = new Label(wTypeSpecificComposite, SWT.RIGHT);
        wlGithubRepo.setText(BaseMessages.getString(PKG, "NotificationSourceDialog.Repository"));
        props.setLook(wlGithubRepo);
        FormData fdlGithubRepo = new FormData();
        fdlGithubRepo.left = new FormAttachment(0, 0);
        fdlGithubRepo.right = new FormAttachment(middle, -margin);
        fdlGithubRepo.top = new FormAttachment(0, yPos);
        wlGithubRepo.setLayoutData(fdlGithubRepo);

        wGithubRepo =
            new TextVar(
                HopGui.getInstance().getVariables(),
                wTypeSpecificComposite,
                SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wGithubRepo);
        FormData fdGithubRepo = new FormData();
        fdGithubRepo.left = new FormAttachment(middle, 0);
        fdGithubRepo.right = new FormAttachment(100, 0);
        fdGithubRepo.top = new FormAttachment(0, yPos);
        wGithubRepo.setLayoutData(fdGithubRepo);
        if (sourceConfig.getGithubRepo() != null) {
          wGithubRepo.setText(sourceConfig.getGithubRepo());
        }
        yPos += 30;

        wGithubIncludePrereleases = new Button(wTypeSpecificComposite, SWT.CHECK);
        wGithubIncludePrereleases.setText(
            BaseMessages.getString(PKG, "NotificationSourceDialog.IncludePrereleases"));
        props.setLook(wGithubIncludePrereleases);
        FormData fdGithubIncludePrereleases = new FormData();
        fdGithubIncludePrereleases.left = new FormAttachment(middle, 0);
        fdGithubIncludePrereleases.top = new FormAttachment(0, yPos);
        wGithubIncludePrereleases.setLayoutData(fdGithubIncludePrereleases);
        wGithubIncludePrereleases.setSelection(sourceConfig.isGithubIncludePrereleases());
        break;

      case RSS_FEED:
        Label wlRssUrl = new Label(wTypeSpecificComposite, SWT.RIGHT);
        wlRssUrl.setText(BaseMessages.getString(PKG, "NotificationSourceDialog.FeedUrl"));
        props.setLook(wlRssUrl);
        FormData fdlRssUrl = new FormData();
        fdlRssUrl.left = new FormAttachment(0, 0);
        fdlRssUrl.right = new FormAttachment(middle, -margin);
        fdlRssUrl.top = new FormAttachment(0, yPos);
        wlRssUrl.setLayoutData(fdlRssUrl);

        wRssUrl =
            new TextVar(
                HopGui.getInstance().getVariables(),
                wTypeSpecificComposite,
                SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wRssUrl);
        FormData fdRssUrl = new FormData();
        fdRssUrl.left = new FormAttachment(middle, 0);
        fdRssUrl.right = new FormAttachment(100, 0);
        fdRssUrl.top = new FormAttachment(0, yPos);
        wRssUrl.setLayoutData(fdRssUrl);
        if (sourceConfig.getRssUrl() != null) {
          wRssUrl.setText(sourceConfig.getRssUrl());
        }
        break;

      case CUSTOM_PLUGIN:
        Label wlPluginId = new Label(wTypeSpecificComposite, SWT.RIGHT);
        wlPluginId.setText(BaseMessages.getString(PKG, "NotificationSourceDialog.PluginId"));
        props.setLook(wlPluginId);
        FormData fdlPluginId = new FormData();
        fdlPluginId.left = new FormAttachment(0, 0);
        fdlPluginId.right = new FormAttachment(middle, -margin);
        fdlPluginId.top = new FormAttachment(0, yPos);
        wlPluginId.setLayoutData(fdlPluginId);

        wPluginId =
            new TextVar(
                HopGui.getInstance().getVariables(),
                wTypeSpecificComposite,
                SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wPluginId);
        FormData fdPluginId = new FormData();
        fdPluginId.left = new FormAttachment(middle, 0);
        fdPluginId.right = new FormAttachment(100, 0);
        fdPluginId.top = new FormAttachment(0, yPos);
        wPluginId.setLayoutData(fdPluginId);
        if (sourceConfig.getPluginId() != null) {
          wPluginId.setText(sourceConfig.getPluginId());
        }
        break;
    }

    // Poll interval (common to all types)
    yPos += 40;
    Label wlPollInterval = new Label(wTypeSpecificComposite, SWT.RIGHT);
    wlPollInterval.setText(BaseMessages.getString(PKG, "NotificationSourceDialog.PollInterval"));
    props.setLook(wlPollInterval);
    FormData fdlPollInterval = new FormData();
    fdlPollInterval.left = new FormAttachment(0, 0);
    fdlPollInterval.right = new FormAttachment(middle, -margin);
    fdlPollInterval.top = new FormAttachment(0, yPos);
    wlPollInterval.setLayoutData(fdlPollInterval);

    wPollInterval =
        new TextVar(
            HopGui.getInstance().getVariables(),
            wTypeSpecificComposite,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPollInterval);
    FormData fdPollInterval = new FormData();
    fdPollInterval.left = new FormAttachment(middle, 0);
    fdPollInterval.right = new FormAttachment(100, 0);
    fdPollInterval.top = new FormAttachment(0, yPos);
    wPollInterval.setLayoutData(fdPollInterval);
    if (sourceConfig.getPollIntervalMinutes() != null) {
      wPollInterval.setText(sourceConfig.getPollIntervalMinutes());
    } else {
      // Use global default
      String globalPollInterval =
          org.apache.hop.core.config.HopConfig.readOptionString(
              "notification.global.pollIntervalMinutes", "60");
      wPollInterval.setText(globalPollInterval);
    }

    yPos += 30;
    Label wlDaysToGoBack = new Label(wTypeSpecificComposite, SWT.RIGHT);
    wlDaysToGoBack.setText(BaseMessages.getString(PKG, "NotificationSourceDialog.DaysToGoBack"));
    props.setLook(wlDaysToGoBack);
    FormData fdlDaysToGoBack = new FormData();
    fdlDaysToGoBack.left = new FormAttachment(0, 0);
    fdlDaysToGoBack.right = new FormAttachment(middle, -margin);
    fdlDaysToGoBack.top = new FormAttachment(0, yPos);
    wlDaysToGoBack.setLayoutData(fdlDaysToGoBack);

    wDaysToGoBack =
        new TextVar(
            HopGui.getInstance().getVariables(),
            wTypeSpecificComposite,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wDaysToGoBack);
    FormData fdDaysToGoBack = new FormData();
    fdDaysToGoBack.left = new FormAttachment(middle, 0);
    fdDaysToGoBack.right = new FormAttachment(100, 0);
    fdDaysToGoBack.top = new FormAttachment(0, yPos);
    wDaysToGoBack.setLayoutData(fdDaysToGoBack);
    if (sourceConfig.getDaysToGoBack() != null) {
      wDaysToGoBack.setText(sourceConfig.getDaysToGoBack());
    } else {
      wDaysToGoBack.setText("0"); // 0 means use global default
    }

    wTypeSpecificComposite.layout();
  }

  private boolean saveSource() {
    String name = wName.getText().trim();
    if (Utils.isEmpty(name)) {
      new ErrorDialog(
          shell, "Error", "Please enter a name for the notification source", new Exception());
      return false;
    }

    sourceConfig.setName(name);
    sourceConfig.setType(NotificationSourceConfig.SourceType.values()[wType.getSelectionIndex()]);
    sourceConfig.setEnabled(wEnabled.getSelection());

    // Generate ID if new
    if (Utils.isEmpty(sourceConfig.getId())) {
      sourceConfig.setId("source-" + UUID.randomUUID().toString().substring(0, 8));
    }

    // Save type-specific fields
    switch (sourceConfig.getType()) {
      case GITHUB_RELEASES:
        if (wGithubOwner == null
            || wGithubOwner.isDisposed()
            || wGithubRepo == null
            || wGithubRepo.isDisposed()) {
          new ErrorDialog(
              shell,
              "Error",
              "GitHub source fields are not initialized. Please select the type again.",
              new Exception());
          return false;
        }
        String owner = wGithubOwner.getText().trim();
        String repo = wGithubRepo.getText().trim();

        // If owner/repo are empty but URL field has a value, try to parse it
        if ((Utils.isEmpty(owner) || Utils.isEmpty(repo))
            && wGithubUrl != null
            && !wGithubUrl.isDisposed()) {
          String url = wGithubUrl.getText().trim();
          if (!Utils.isEmpty(url)) {
            parseGitHubUrlFromString(url);
            owner = wGithubOwner.getText().trim();
            repo = wGithubRepo.getText().trim();
          }
        }

        if (Utils.isEmpty(owner) || Utils.isEmpty(repo)) {
          new ErrorDialog(
              shell,
              "Error",
              "Please enter both owner and repository for GitHub source (or a GitHub URL)",
              new Exception());
          return false;
        }
        sourceConfig.setGithubOwner(owner);
        sourceConfig.setGithubRepo(repo);
        if (wGithubIncludePrereleases != null && !wGithubIncludePrereleases.isDisposed()) {
          sourceConfig.setGithubIncludePrereleases(wGithubIncludePrereleases.getSelection());
        }
        break;

      case RSS_FEED:
        if (wRssUrl == null || wRssUrl.isDisposed()) {
          new ErrorDialog(
              shell,
              "Error",
              "RSS feed field is not initialized. Please select the type again.",
              new Exception());
          return false;
        }
        String url = wRssUrl.getText().trim();
        if (Utils.isEmpty(url)) {
          new ErrorDialog(
              shell, "Error", "Please enter a feed URL for RSS source", new Exception());
          return false;
        }
        sourceConfig.setRssUrl(url);
        break;

      case CUSTOM_PLUGIN:
        if (wPluginId == null || wPluginId.isDisposed()) {
          new ErrorDialog(
              shell,
              "Error",
              "Plugin ID field is not initialized. Please select the type again.",
              new Exception());
          return false;
        }
        String pluginId = wPluginId.getText().trim();
        if (Utils.isEmpty(pluginId)) {
          new ErrorDialog(
              shell, "Error", "Please enter a plugin ID for custom plugin source", new Exception());
          return false;
        }
        sourceConfig.setPluginId(pluginId);
        // Use plugin ID as source id so provider lookup works (provider is registered under
        // pluginId)
        sourceConfig.setId(pluginId);
        break;
    }

    if (wPollInterval == null || wPollInterval.isDisposed()) {
      // Default poll interval if widget not available
      sourceConfig.setPollIntervalMinutes("60");
    } else {
      String pollInterval = wPollInterval.getText().trim();
      if (!Utils.isEmpty(pollInterval)) {
        try {
          int minutes = Integer.parseInt(pollInterval);
          if (minutes <= 0) {
            new ErrorDialog(
                shell, "Error", "Poll interval must be greater than 0", new Exception());
            return false;
          }
          sourceConfig.setPollIntervalMinutes(pollInterval);
        } catch (NumberFormatException e) {
          new ErrorDialog(shell, "Error", "Poll interval must be a valid number", e);
          return false;
        }
      } else {
        // Use global default
        String globalPollInterval =
            org.apache.hop.core.config.HopConfig.readOptionString(
                "notification.global.pollIntervalMinutes", "60");
        sourceConfig.setPollIntervalMinutes(globalPollInterval);
      }
    }

    // Save days to go back
    if (wDaysToGoBack != null && !wDaysToGoBack.isDisposed()) {
      String daysToGoBack = wDaysToGoBack.getText().trim();
      if (!Utils.isEmpty(daysToGoBack)) {
        try {
          int days = Integer.parseInt(daysToGoBack);
          if (days < 0) {
            new ErrorDialog(
                shell, "Error", "Days to go back must be 0 or greater", new Exception());
            return false;
          }
          sourceConfig.setDaysToGoBack(daysToGoBack);
        } catch (NumberFormatException e) {
          new ErrorDialog(shell, "Error", "Days to go back must be a valid number", e);
          return false;
        }
      } else {
        sourceConfig.setDaysToGoBack("0"); // 0 means use global default
      }
    }

    return true;
  }

  private void parseGitHubUrlFromString(String input) {
    if (Utils.isEmpty(input)
        || wGithubOwner == null
        || wGithubOwner.isDisposed()
        || wGithubRepo == null
        || wGithubRepo.isDisposed()) {
      return;
    }

    String owner = null;
    String repo = null;

    if (input.contains("github.com/")) {
      String[] parts = input.split("github.com/");
      if (parts.length > 1) {
        String path = parts[1].split("\\?")[0].split("#")[0]; // Remove query params and fragments
        String[] ownerRepo = path.split("/");
        if (ownerRepo.length >= 2) {
          owner = ownerRepo[0].trim();
          repo = ownerRepo[1].trim();
        }
      }
    } else if (input.contains("/")) {
      String[] parts = input.split("/");
      if (parts.length >= 2) {
        owner = parts[0].trim();
        repo = parts[1].trim();
      }
    }

    if (owner != null && !owner.isEmpty() && repo != null && !repo.isEmpty()) {
      wGithubOwner.setText(owner);
      wGithubRepo.setText(repo);
    }
  }

  public NotificationSourceConfig getSourceConfig() {
    return sourceConfig;
  }
}
