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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.tab.GuiTab;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.JsonUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.configuration.ConfigurationPerspective;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

/**
 * Configuration plugin for notification system settings. Allows users to configure multiple
 * notification sources (GitHub, RSS, custom plugins) in a table-based interface.
 */
@ConfigPlugin(
    id = "NotificationConfigPlugin",
    description = "Configuration options for the notification system")
@GuiPlugin(description = "Notifications") // Required for @GuiTab discovery
public class NotificationConfigPlugin implements IConfigOptions {

  private static final Class<?> PKG = NotificationConfigPlugin.class;
  private static final String CONFIG_KEY_ENABLE_NOTIFICATIONS = "notification.system.enabled";
  private static final String CONFIG_KEY_SOURCES = "notification.sources";
  private static final String CONFIG_KEY_GLOBAL_POLL_INTERVAL =
      "notification.global.pollIntervalMinutes";
  private static final String CONFIG_KEY_GLOBAL_DAYS_TO_GO_BACK =
      "notification.global.daysToGoBack";
  private static final String CONFIG_KEY_SHOW_READ_NOTIFICATIONS =
      "notification.showReadNotifications";

  private TableView wSourcesTable;
  private Button wEnableNotifications;
  private org.apache.hop.ui.core.widget.TextVar wGlobalPollInterval;
  private org.apache.hop.ui.core.widget.TextVar wGlobalDaysToGoBack;
  private Button wShowReadNotifications;
  private List<NotificationSourceConfig> sources;
  private PropsUi props = PropsUi.getInstance();

  /**
   * Get instance with values loaded from HopConfig
   *
   * @return NotificationConfigPlugin instance
   */
  public static NotificationConfigPlugin getInstance() {
    NotificationConfigPlugin instance = new NotificationConfigPlugin();
    instance.loadSources();
    return instance;
  }

  /**
   * Get the list of notification sources (for use by NotificationPanel)
   *
   * @return List of notification source configurations
   */
  public List<NotificationSourceConfig> getSources() {
    if (sources == null) {
      loadSources();
    }
    return sources;
  }

  @GuiTab(
      id = "10200-config-perspective-notifications-tab",
      parentId = ConfigurationPerspective.CONFIG_PERSPECTIVE_TABS,
      description = "Notifications configuration tab")
  public void addNotificationsTab(CTabFolder wTabFolder) {
    try {
      Shell shell = wTabFolder.getShell();
      int margin = PropsUi.getMargin();

      CTabItem wNotificationsTab = new CTabItem(wTabFolder, SWT.NONE);
      wNotificationsTab.setFont(GuiResource.getInstance().getFontDefault());
      wNotificationsTab.setText(
          BaseMessages.getString(PKG, "NotificationConfigPlugin.Tab.Notifications"));
      wNotificationsTab.setImage(GuiResource.getInstance().getImagePlugin());

      ScrolledComposite sNotificationsComp =
          new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
      sNotificationsComp.setLayout(new FillLayout());

      Composite wNotificationsTabComp = new Composite(sNotificationsComp, SWT.NONE);
      PropsUi.setLook(wNotificationsTabComp);

      FormLayout lookLayout = new FormLayout();
      lookLayout.marginWidth = PropsUi.getFormMargin();
      lookLayout.marginHeight = PropsUi.getFormMargin();
      wNotificationsTabComp.setLayout(lookLayout);

      IVariables variables = null;
      try {
        variables = HopGui.getInstance().getVariables();
      } catch (Exception e) {
        variables = org.apache.hop.core.variables.Variables.getADefaultVariableSpace();
      }

      // Enable notifications - label left, checkbox right (same layout as Show read notifications)
      int middle = 50;
      Label wlEnableNotifications = new Label(wNotificationsTabComp, SWT.RIGHT);
      wlEnableNotifications.setText(
          BaseMessages.getString(PKG, "NotificationConfigPlugin.EnableNotificationSystem"));
      props.setLook(wlEnableNotifications);
      FormData fdlEnableNotifications = new FormData();
      fdlEnableNotifications.left = new FormAttachment(0, 0);
      fdlEnableNotifications.right = new FormAttachment(middle, -margin);
      fdlEnableNotifications.top = new FormAttachment(0, margin);
      wlEnableNotifications.setLayoutData(fdlEnableNotifications);

      wEnableNotifications = new Button(wNotificationsTabComp, SWT.CHECK);
      props.setLook(wEnableNotifications);
      FormData fdEnableNotifications = new FormData();
      fdEnableNotifications.left = new FormAttachment(middle, 0);
      fdEnableNotifications.top = new FormAttachment(0, margin);
      wEnableNotifications.setLayoutData(fdEnableNotifications);
      boolean enabled =
          HopConfig.readOptionString(CONFIG_KEY_ENABLE_NOTIFICATIONS, "true")
              .equalsIgnoreCase("true");
      wEnableNotifications.setSelection(enabled);
      wlEnableNotifications.addListener(
          org.eclipse.swt.SWT.MouseDown,
          e -> {
            wEnableNotifications.setSelection(!wEnableNotifications.getSelection());
            saveEnableState();
          });
      wEnableNotifications.addSelectionListener(
          new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
              saveEnableState();
            }
          });

      // Default poll interval - right below Enable
      Label wlGlobalPollInterval = new Label(wNotificationsTabComp, SWT.RIGHT);
      wlGlobalPollInterval.setText(
          BaseMessages.getString(PKG, "NotificationConfigPlugin.DefaultPollInterval"));
      props.setLook(wlGlobalPollInterval);
      FormData fdlGlobalPollInterval = new FormData();
      fdlGlobalPollInterval.left = new FormAttachment(0, 0);
      fdlGlobalPollInterval.right = new FormAttachment(middle, -margin);
      fdlGlobalPollInterval.top = new FormAttachment(wEnableNotifications, margin);
      wlGlobalPollInterval.setLayoutData(fdlGlobalPollInterval);

      wGlobalPollInterval =
          new org.apache.hop.ui.core.widget.TextVar(
              variables, wNotificationsTabComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
      props.setLook(wGlobalPollInterval);
      FormData fdGlobalPollInterval = new FormData();
      fdGlobalPollInterval.left = new FormAttachment(middle, 0);
      fdGlobalPollInterval.right = new FormAttachment(100, 0);
      fdGlobalPollInterval.top = new FormAttachment(wEnableNotifications, margin);
      wGlobalPollInterval.setLayoutData(fdGlobalPollInterval);
      wGlobalPollInterval.setText(getGlobalPollIntervalMinutes());

      Label wlGlobalDaysToGoBack = new Label(wNotificationsTabComp, SWT.RIGHT);
      wlGlobalDaysToGoBack.setText(
          BaseMessages.getString(PKG, "NotificationConfigPlugin.DefaultDaysToGoBack"));
      props.setLook(wlGlobalDaysToGoBack);
      FormData fdlGlobalDaysToGoBack = new FormData();
      fdlGlobalDaysToGoBack.left = new FormAttachment(0, 0);
      fdlGlobalDaysToGoBack.right = new FormAttachment(middle, -margin);
      fdlGlobalDaysToGoBack.top = new FormAttachment(wGlobalPollInterval, margin);
      wlGlobalDaysToGoBack.setLayoutData(fdlGlobalDaysToGoBack);

      wGlobalDaysToGoBack =
          new org.apache.hop.ui.core.widget.TextVar(
              variables, wNotificationsTabComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
      props.setLook(wGlobalDaysToGoBack);
      FormData fdGlobalDaysToGoBack = new FormData();
      fdGlobalDaysToGoBack.left = new FormAttachment(middle, 0);
      fdGlobalDaysToGoBack.right = new FormAttachment(100, 0);
      fdGlobalDaysToGoBack.top = new FormAttachment(wGlobalPollInterval, margin);
      wGlobalDaysToGoBack.setLayoutData(fdGlobalDaysToGoBack);
      wGlobalDaysToGoBack.setText(getGlobalDaysToGoBack());

      wShowReadNotifications = new Button(wNotificationsTabComp, SWT.CHECK);
      wShowReadNotifications.setText(
          BaseMessages.getString(PKG, "NotificationConfigPlugin.ShowReadNotifications"));
      props.setLook(wShowReadNotifications);
      FormData fdShowReadNotifications = new FormData();
      fdShowReadNotifications.left = new FormAttachment(middle, 0);
      fdShowReadNotifications.top = new FormAttachment(wGlobalDaysToGoBack, margin);
      wShowReadNotifications.setLayoutData(fdShowReadNotifications);
      wShowReadNotifications.setSelection(isShowReadNotifications());

      // Sources table
      String[] sourceTypes = {
        NotificationSourceConfig.SourceType.GITHUB_RELEASES.getDisplayName(),
        NotificationSourceConfig.SourceType.RSS_FEED.getDisplayName(),
        NotificationSourceConfig.SourceType.CUSTOM_PLUGIN.getDisplayName(),
      };

      String yesLabel = BaseMessages.getString(PKG, "NotificationConfigPlugin.Yes");
      String noLabel = BaseMessages.getString(PKG, "NotificationConfigPlugin.No");
      ColumnInfo[] columns = {
        new ColumnInfo(
            BaseMessages.getString(PKG, "NotificationConfigPlugin.Table.Name"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false,
            false),
        new ColumnInfo(
            BaseMessages.getString(PKG, "NotificationConfigPlugin.Table.Type"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            sourceTypes,
            false),
        new ColumnInfo(
            BaseMessages.getString(PKG, "NotificationConfigPlugin.Table.Enabled"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {yesLabel, noLabel},
            false),
        new ColumnInfo(
            BaseMessages.getString(PKG, "NotificationConfigPlugin.Table.Details"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false,
            false),
        new ColumnInfo(
            BaseMessages.getString(PKG, "NotificationConfigPlugin.Table.PollInterval"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false,
            false),
        new ColumnInfo(
            BaseMessages.getString(PKG, "NotificationConfigPlugin.Table.Color"),
            ColumnInfo.COLUMN_TYPE_BUTTON,
            false,
            false),
      };

      // Set button text for color column
      columns[5].setButtonText(
          BaseMessages.getString(PKG, "NotificationConfigPlugin.Table.ChooseColor"));
      columns[5].setToolTip("Click to choose a color for this notification source");

      wSourcesTable =
          new TableView(
              variables,
              wNotificationsTabComp,
              SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL,
              columns,
              0,
              null,
              PropsUi.getInstance());
      wSourcesTable.setReadonly(false);

      // Add handler for color button column
      columns[5].setSelectionAdapter(
          new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
              // Get the row from the button's data or selection
              int row = wSourcesTable.getSelectionIndex();
              if (row >= 0) {
                handleColorButtonClick(row);
              }
            }
          });

      // Add handler for table cell modifications - update source objects when cells are edited
      wSourcesTable.addModifyListener(
          new org.eclipse.swt.events.ModifyListener() {
            @Override
            public void modifyText(org.eclipse.swt.events.ModifyEvent e) {
              handleTableCellModified();
            }
          });

      FormData fdSourcesTable = new FormData();
      fdSourcesTable.left = new FormAttachment(0, 0);
      fdSourcesTable.top = new FormAttachment(wShowReadNotifications, margin);
      fdSourcesTable.right = new FormAttachment(100, 0);
      fdSourcesTable.bottom = new FormAttachment(100, -50);
      wSourcesTable.setLayoutData(fdSourcesTable);

      // Buttons
      Button wAdd = new Button(wNotificationsTabComp, SWT.PUSH);
      wAdd.setText(BaseMessages.getString(PKG, "NotificationConfigPlugin.Add"));
      props.setLook(wAdd);
      FormData fdAdd = new FormData();
      fdAdd.left = new FormAttachment(0, 0);
      fdAdd.top = new FormAttachment(wSourcesTable, margin);
      wAdd.setLayoutData(fdAdd);
      wAdd.addSelectionListener(
          new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
              addSource();
            }
          });

      Button wEdit = new Button(wNotificationsTabComp, SWT.PUSH);
      wEdit.setText(BaseMessages.getString(PKG, "NotificationConfigPlugin.Edit"));
      props.setLook(wEdit);
      FormData fdEdit = new FormData();
      fdEdit.left = new FormAttachment(wAdd, margin);
      fdEdit.top = new FormAttachment(wSourcesTable, margin);
      wEdit.setLayoutData(fdEdit);
      wEdit.addSelectionListener(
          new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
              editSource();
            }
          });

      Button wDelete = new Button(wNotificationsTabComp, SWT.PUSH);
      wDelete.setText(BaseMessages.getString(PKG, "NotificationConfigPlugin.Delete"));
      props.setLook(wDelete);
      FormData fdDelete = new FormData();
      fdDelete.left = new FormAttachment(wEdit, margin);
      fdDelete.top = new FormAttachment(wSourcesTable, margin);
      wDelete.setLayoutData(fdDelete);
      wDelete.addSelectionListener(
          new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
              deleteSource();
            }
          });

      Button wSave = new Button(wNotificationsTabComp, SWT.PUSH);
      wSave.setText(BaseMessages.getString(PKG, "NotificationConfigPlugin.Save"));
      props.setLook(wSave);
      BaseTransformDialog.positionBottomButtons(
          wNotificationsTabComp, new Button[] {wSave}, margin, null);
      wSave.addSelectionListener(
          new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
              saveGlobalOptions();
              saveSources();
            }
          });

      // Initialize sources list from config
      loadSources();

      // Load sources into table
      loadSourcesIntoTable();

      // Set up ScrolledComposite
      wNotificationsTabComp.layout(true, true);
      org.eclipse.swt.graphics.Rectangle bounds = wNotificationsTabComp.getBounds();
      sNotificationsComp.setContent(wNotificationsTabComp);
      sNotificationsComp.setExpandHorizontal(true);
      sNotificationsComp.setExpandVertical(true);
      sNotificationsComp.setMinWidth(bounds.width);
      sNotificationsComp.setMinHeight(bounds.height);

      wNotificationsTab.setControl(sNotificationsComp);
    } catch (Exception e) {
      new org.apache.hop.ui.core.dialog.ErrorDialog(
          wTabFolder.getShell(),
          BaseMessages.getString(PKG, "NotificationConfigPlugin.Error"),
          BaseMessages.getString(PKG, "NotificationConfigPlugin.ErrorCreatingTab"),
          e);
    }
  }

  private void loadSources() {
    sources = new ArrayList<>();
    try {
      String sourcesJson = HopConfig.readOptionString(CONFIG_KEY_SOURCES, null);
      if (!Utils.isEmpty(sourcesJson)) {
        ObjectMapper mapper = JsonUtil.jsonMapper();
        sources =
            mapper.readValue(sourcesJson, new TypeReference<List<NotificationSourceConfig>>() {});
      }
    } catch (Exception e) {
      // If loading fails, start with empty list
      sources = new ArrayList<>();
    }

    // If no sources are configured, create a default one
    if (sources.isEmpty()) {
      NotificationSourceConfig defaultSource = new NotificationSourceConfig();
      defaultSource.setId("github-apache-hop");
      defaultSource.setName("Apache Hop Releases");
      defaultSource.setType(NotificationSourceConfig.SourceType.GITHUB_RELEASES);
      defaultSource.setEnabled(true);
      defaultSource.setGithubOwner("apache");
      defaultSource.setGithubRepo("hop");
      defaultSource.setGithubIncludePrereleases(false);
      defaultSource.setPollIntervalMinutes("60");
      defaultSource.setColor("#FF5733"); // Default color
      sources.add(defaultSource);
    }
  }

  private void loadSourcesIntoTable() {
    if (wSourcesTable == null || wSourcesTable.table == null || wSourcesTable.table.isDisposed()) {
      return;
    }
    // Only load from config if sources list is not initialized
    if (sources == null) {
      loadSources();
    }
    String yesLabel = BaseMessages.getString(PKG, "NotificationConfigPlugin.Yes");
    String noLabel = BaseMessages.getString(PKG, "NotificationConfigPlugin.No");
    wSourcesTable.table.removeAll();
    for (NotificationSourceConfig source : sources) {
      TableItem item = new TableItem(wSourcesTable.table, SWT.NONE);
      int col = 1;
      item.setText(col++, Const.NVL(source.getName(), ""));
      item.setText(col++, Const.NVL(source.getType().getDisplayName(), ""));
      item.setText(col++, source.isEnabled() ? yesLabel : noLabel);
      item.setText(col++, Const.NVL(source.getDetailsDisplay(), ""));
      String pollInterval = source.getPollIntervalMinutes();
      if (Utils.isEmpty(pollInterval)) {
        pollInterval = getGlobalPollIntervalMinutes();
      }
      item.setText(col++, Const.NVL(pollInterval, "60"));
      item.setText(col, Const.NVL(source.getColor(), "#000000"));
      item.setData(source);
    }
    wSourcesTable.optimizeTableView();
  }

  private void addSource() {
    NotificationSourceDialog dialog = new NotificationSourceDialog(wSourcesTable.getShell(), null);
    String result = dialog.open();
    if (result != null) {
      NotificationSourceConfig newSource = dialog.getSourceConfig();
      sources.add(newSource);
      loadSourcesIntoTable();
      saveSources();
    }
  }

  private void editSource() {
    int index = wSourcesTable.getSelectionIndex();
    if (index < 0) {
      return;
    }
    TableItem item = wSourcesTable.table.getItem(index);
    NotificationSourceConfig source = (NotificationSourceConfig) item.getData();
    if (source != null) {
      NotificationSourceDialog dialog =
          new NotificationSourceDialog(wSourcesTable.getShell(), source);
      String result = dialog.open();
      if (result != null) {
        NotificationSourceConfig updatedSource = dialog.getSourceConfig();
        // Update the source in the list
        int sourceIndex = sources.indexOf(source);
        if (sourceIndex >= 0) {
          sources.set(sourceIndex, updatedSource);
        }
        loadSourcesIntoTable();
        saveSources();
      }
    }
  }

  private void deleteSource() {
    int[] indices = wSourcesTable.table.getSelectionIndices();
    if (indices.length == 0) {
      return;
    }
    // Remove from back to front to maintain indices
    for (int i = indices.length - 1; i >= 0; i--) {
      TableItem item = wSourcesTable.table.getItem(indices[i]);
      NotificationSourceConfig source = (NotificationSourceConfig) item.getData();
      if (source != null) {
        sources.remove(source);
      }
    }
    loadSourcesIntoTable();
    saveSources();
  }

  private void saveSources() {
    try {
      ObjectMapper mapper = JsonUtil.jsonMapper();
      String sourcesJson = mapper.writeValueAsString(sources);
      HopConfig.getInstance().saveOption(CONFIG_KEY_SOURCES, sourcesJson);
      HopConfig.getInstance().saveToFile();
      notifyConfigChanged();
    } catch (Exception e) {
      new ErrorDialog(
          wSourcesTable.getShell(), "Error", "Error saving notification sources configuration", e);
    }
  }

  private void saveEnableState() {
    HopConfig.getInstance()
        .saveOption(
            CONFIG_KEY_ENABLE_NOTIFICATIONS, String.valueOf(wEnableNotifications.getSelection()));
    try {
      HopConfig.getInstance().saveToFile();
      notifyConfigChanged();
    } catch (Exception e) {
      new ErrorDialog(
          wSourcesTable.getShell(), "Error", "Error saving notification system enabled state", e);
    }
  }

  private void notifyConfigChanged() {
    try {
      org.apache.hop.ui.hopgui.notifications.NotificationService.getInstance().reloadFromConfig();
    } catch (Exception e) {
      org.apache.hop.core.logging.LogChannel.GENERAL.logError(
          "Error reloading notification providers after config change", e);
    }
  }

  private void saveGlobalOptions() {
    try {
      if (wGlobalPollInterval != null && !wGlobalPollInterval.isDisposed()) {
        String pollInterval = wGlobalPollInterval.getText().trim();
        if (!Utils.isEmpty(pollInterval)) {
          HopConfig.getInstance().saveOption(CONFIG_KEY_GLOBAL_POLL_INTERVAL, pollInterval);
        }
      }
      if (wGlobalDaysToGoBack != null && !wGlobalDaysToGoBack.isDisposed()) {
        String daysToGoBack = wGlobalDaysToGoBack.getText().trim();
        if (!Utils.isEmpty(daysToGoBack)) {
          HopConfig.getInstance().saveOption(CONFIG_KEY_GLOBAL_DAYS_TO_GO_BACK, daysToGoBack);
        }
      }
      if (wShowReadNotifications != null && !wShowReadNotifications.isDisposed()) {
        HopConfig.getInstance()
            .saveOption(
                CONFIG_KEY_SHOW_READ_NOTIFICATIONS,
                String.valueOf(wShowReadNotifications.getSelection()));
      }
      HopConfig.getInstance().saveToFile();
    } catch (Exception e) {
      new ErrorDialog(
          wSourcesTable.getShell(), "Error", "Error saving global notification options", e);
    }
  }

  private String getGlobalPollIntervalMinutes() {
    return HopConfig.readOptionString(CONFIG_KEY_GLOBAL_POLL_INTERVAL, "60");
  }

  private String getGlobalDaysToGoBack() {
    return HopConfig.readOptionString(CONFIG_KEY_GLOBAL_DAYS_TO_GO_BACK, "30");
  }

  private boolean isShowReadNotifications() {
    return HopConfig.readOptionString(CONFIG_KEY_SHOW_READ_NOTIFICATIONS, "true")
        .equalsIgnoreCase("true");
  }

  private void handleColorButtonClick(int row) {
    if (wSourcesTable == null || wSourcesTable.table == null || wSourcesTable.table.isDisposed()) {
      return;
    }
    if (row < 0 || row >= wSourcesTable.table.getItemCount()) {
      return;
    }
    TableItem item = wSourcesTable.table.getItem(row);
    NotificationSourceConfig source = (NotificationSourceConfig) item.getData();
    if (source == null) {
      return;
    }

    // Open color dialog
    org.eclipse.swt.widgets.ColorDialog colorDialog =
        new org.eclipse.swt.widgets.ColorDialog(wSourcesTable.getShell());
    if (source.getColor() != null && !source.getColor().isEmpty()) {
      try {
        String hex =
            source.getColor().startsWith("#") ? source.getColor().substring(1) : source.getColor();
        int colorValue = Integer.parseInt(hex, 16);
        colorDialog.setRGB(
            new org.eclipse.swt.graphics.RGB(
                (colorValue >> 16) & 0xFF, (colorValue >> 8) & 0xFF, colorValue & 0xFF));
      } catch (Exception e) {
        // Ignore
      }
    }
    org.eclipse.swt.graphics.RGB rgb = colorDialog.open();
    if (rgb != null) {
      String hexColor = String.format("#%02X%02X%02X", rgb.red, rgb.green, rgb.blue);
      source.setColor(hexColor);
      item.setText(6, hexColor);
      saveSources();
    }
  }

  private void handleTableCellModified() {
    if (wSourcesTable == null || wSourcesTable.table == null || wSourcesTable.table.isDisposed()) {
      return;
    }

    // Update source objects from table data
    for (int i = 0; i < wSourcesTable.table.getItemCount(); i++) {
      TableItem item = wSourcesTable.table.getItem(i);
      NotificationSourceConfig source = (NotificationSourceConfig) item.getData();
      if (source == null) {
        continue;
      }

      // Update from table columns (col 1 = Name, col 2 = Type, col 3 = Enabled, col 4 = Details,
      // col 5 = Poll Interval)
      String name = item.getText(1);
      if (!Utils.isEmpty(name)) {
        source.setName(name);
      }

      String typeStr = item.getText(2);
      for (NotificationSourceConfig.SourceType type :
          NotificationSourceConfig.SourceType.values()) {
        if (type.getDisplayName().equals(typeStr)) {
          source.setType(type);
          break;
        }
      }

      String enabledStr = item.getText(3);
      source.setEnabled(
          BaseMessages.getString(PKG, "NotificationConfigPlugin.Yes").equals(enabledStr));

      // For Details column, if it's a GitHub URL, parse it
      String details = item.getText(4);
      if (source.getType() == NotificationSourceConfig.SourceType.GITHUB_RELEASES) {
        parseGitHubUrl(source, details);
      } else if (source.getType() == NotificationSourceConfig.SourceType.RSS_FEED) {
        source.setRssUrl(details);
      } else if (source.getType() == NotificationSourceConfig.SourceType.CUSTOM_PLUGIN) {
        if (!Utils.isEmpty(details)) {
          source.setPluginId(details);
          source.setId(details); // Keep id and pluginId in sync for provider lookup
        }
      }

      // Poll interval column
      String pollInterval = item.getText(5);
      if (!Utils.isEmpty(pollInterval)) {
        source.setPollIntervalMinutes(pollInterval);
      } else {
        source.setPollIntervalMinutes(null); // Use global default
      }
    }

    // Reload table to update display
    loadSourcesIntoTable();
  }

  private void parseGitHubUrl(NotificationSourceConfig source, String input) {
    if (Utils.isEmpty(input)) {
      return;
    }

    // Try to parse GitHub URL formats:
    // https://github.com/owner/repo
    // https://github.com/owner/repo/
    // github.com/owner/repo
    // owner/repo
    String owner = null;
    String repo = null;

    if (input.contains("github.com/")) {
      // Extract from URL
      String[] parts = input.split("github.com/");
      if (parts.length > 1) {
        String path = parts[1].split("/")[0] + "/" + parts[1].split("/")[1];
        String[] ownerRepo = path.split("/");
        if (ownerRepo.length >= 2) {
          owner = ownerRepo[0];
          repo = ownerRepo[1].replaceAll("/.*", ""); // Remove trailing path
        }
      }
    } else if (input.contains("/")) {
      // Assume format is owner/repo
      String[] parts = input.split("/");
      if (parts.length >= 2) {
        owner = parts[0].trim();
        repo = parts[1].trim();
      }
    }

    if (owner != null && !owner.isEmpty() && repo != null && !repo.isEmpty()) {
      source.setGithubOwner(owner);
      source.setGithubRepo(repo);
    }
  }

  /**
   * Reload values from HopConfig into the widgets. Called when the Configuration perspective is
   * reactivated so that changes made elsewhere (or in another tab) are reflected.
   */
  public void reloadValues() {
    if (wEnableNotifications == null || wEnableNotifications.isDisposed()) {
      return; // Tab not yet initialized or already disposed
    }

    try {
      // Reload enable state
      boolean enabled =
          HopConfig.readOptionString(CONFIG_KEY_ENABLE_NOTIFICATIONS, "true")
              .equalsIgnoreCase("true");
      wEnableNotifications.setSelection(enabled);

      // Reload global options
      if (wGlobalPollInterval != null && !wGlobalPollInterval.isDisposed()) {
        wGlobalPollInterval.setText(getGlobalPollIntervalMinutes());
      }
      if (wGlobalDaysToGoBack != null && !wGlobalDaysToGoBack.isDisposed()) {
        wGlobalDaysToGoBack.setText(getGlobalDaysToGoBack());
      }
      if (wShowReadNotifications != null && !wShowReadNotifications.isDisposed()) {
        wShowReadNotifications.setSelection(isShowReadNotifications());
      }

      // Reload sources from config and refresh table
      loadSources();
      loadSourcesIntoTable();
    } catch (Exception e) {
      org.apache.hop.core.logging.LogChannel.GENERAL.logError(
          "Error reloading notification config values", e);
    }
  }

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables)
      throws HopException {
    // Options are handled via the GUI tab
    return false;
  }
}
