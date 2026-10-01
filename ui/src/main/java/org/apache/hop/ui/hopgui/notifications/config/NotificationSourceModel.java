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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.eclipse.swt.graphics.RGB;
import org.eclipse.swt.widgets.ColorDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

/**
 * What {@link NotificationSourceDialog} edits, as annotated widgets.
 *
 * <p>The dialog used to lay out every row by hand. Declaring the form here instead means {@link
 * org.apache.hop.ui.core.gui.GuiCompositeWidgets} builds it: labels, variable-aware text fields,
 * the password mask and the scrolling all come from the framework, and a plugin can add or hide a
 * field without the dialog knowing about it.
 *
 * <p>This is a separate object from {@link NotificationSourceConfig} because that one keeps
 * everything but its name, type and enabled flag in a property map that is serialised to
 * hop-config.json. The framework works from fields, so the form is described here and {@link
 * #fromConfig} and {@link #toConfig} carry the values across.
 *
 * <p>Every field is declared once and the irrelevant ones are hidden per source type, rather than
 * disposing and rebuilding a composite on every change of the type combo.
 */
@GuiPlugin(description = "Notification source settings")
@Getter
@Setter
public class NotificationSourceModel {

  private static final Class<?> PKG = NotificationSourceDialog.class;

  /** The parent the dialog asks the registry for. */
  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "NotificationSourceModel-Widgets";

  public static final String WIDGET_NAME = "notification-source-name";
  public static final String WIDGET_TYPE = "notification-source-type";
  public static final String WIDGET_ENABLED = "notification-source-enabled";
  public static final String WIDGET_COLOR = "notification-source-color";
  public static final String WIDGET_CHOOSE_COLOR = "notification-source-choose-color";
  public static final String WIDGET_GITHUB_URL = "notification-source-github-url";
  public static final String WIDGET_PARSE_GITHUB_URL = "notification-source-parse-github-url";
  public static final String WIDGET_GITHUB_OWNER = "notification-source-github-owner";
  public static final String WIDGET_GITHUB_REPO = "notification-source-github-repo";
  public static final String WIDGET_GITHUB_PRERELEASES = "notification-source-github-prereleases";
  public static final String WIDGET_MINIMUM_VERSION = "notification-source-minimum-version";
  public static final String WIDGET_RSS_URL = "notification-source-rss-url";
  public static final String WIDGET_PLUGIN_ID = "notification-source-plugin-id";
  public static final String WIDGET_POLL_INTERVAL = "notification-source-poll-interval";
  public static final String WIDGET_DAYS_TO_GO_BACK = "notification-source-days-to-go-back";
  public static final String WIDGET_USERNAME = "notification-source-username";
  public static final String WIDGET_PASSWORD = "notification-source-password";

  @GuiWidgetElement(
      id = WIDGET_NAME,
      order = "010",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::NotificationSourceDialog.Name")
  private String name;

  @GuiWidgetElement(
      id = WIDGET_TYPE,
      order = "020",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.COMBO,
      variables = false,
      comboValuesMethod = "getTypeNames",
      label = "i18n::NotificationSourceDialog.Type")
  private String type;

  @GuiWidgetElement(
      id = WIDGET_ENABLED,
      order = "030",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::NotificationSourceDialog.Enabled")
  private boolean enabled = true;

  @GuiWidgetElement(
      id = WIDGET_COLOR,
      order = "040",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      variables = false,
      toolTip = "i18n::NotificationSourceDialog.Color.Tooltip",
      label = "i18n::NotificationSourceDialog.Color")
  private String color;

  @GuiWidgetElement(
      id = WIDGET_GITHUB_URL,
      order = "110",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      toolTip = "i18n::NotificationSourceDialog.GithubUrl.Tooltip",
      label = "i18n::NotificationSourceDialog.GithubUrl")
  private String githubUrl;

  @GuiWidgetElement(
      id = WIDGET_GITHUB_OWNER,
      order = "120",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::NotificationSourceDialog.Owner")
  private String githubOwner;

  @GuiWidgetElement(
      id = WIDGET_GITHUB_REPO,
      order = "130",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::NotificationSourceDialog.Repository")
  private String githubRepo;

  @GuiWidgetElement(
      id = WIDGET_GITHUB_PRERELEASES,
      order = "140",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::NotificationSourceDialog.IncludePrereleases")
  private boolean githubIncludePrereleases;

  @GuiWidgetElement(
      id = WIDGET_MINIMUM_VERSION,
      order = "150",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      toolTip = "i18n::NotificationSourceDialog.MinimumVersion.Tooltip",
      label = "i18n::NotificationSourceDialog.MinimumVersion")
  private String minimumVersion;

  @GuiWidgetElement(
      id = WIDGET_RSS_URL,
      order = "210",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::NotificationSourceDialog.FeedUrl")
  private String rssUrl;

  @GuiWidgetElement(
      id = WIDGET_PLUGIN_ID,
      order = "310",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::NotificationSourceDialog.PluginId")
  private String pluginId;

  @GuiWidgetElement(
      id = WIDGET_POLL_INTERVAL,
      order = "410",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      toolTip = "i18n::NotificationSourceDialog.PollInterval.Tooltip",
      label = "i18n::NotificationSourceDialog.PollInterval")
  private String pollIntervalMinutes;

  @GuiWidgetElement(
      id = WIDGET_DAYS_TO_GO_BACK,
      order = "420",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      toolTip = "i18n::NotificationSourceDialog.DaysToGoBack.Tooltip",
      label = "i18n::NotificationSourceDialog.DaysToGoBack")
  private String daysToGoBack;

  @GuiWidgetElement(
      id = WIDGET_USERNAME,
      order = "510",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      toolTip = "i18n::NotificationSourceDialog.Username.Tooltip",
      label = "i18n::NotificationSourceDialog.Username")
  private String username;

  @GuiWidgetElement(
      id = WIDGET_PASSWORD,
      order = "520",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      password = true,
      toolTip = "i18n::NotificationSourceDialog.Password.Tooltip",
      label = "i18n::NotificationSourceDialog.Password")
  private String password;

  /**
   * Pick the colour that marks this source in the panel.
   *
   * <p>The framework instantiates this class to call the method and hands the model being edited in
   * as {@code object}, then re-reads the fields, so writing the colour here is enough to update the
   * text next to the button.
   *
   * @param object The model being edited
   */
  @GuiWidgetElement(
      id = WIDGET_CHOOSE_COLOR,
      order = "045",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.BUTTON,
      toolTip = "i18n::NotificationSourceDialog.Color.Tooltip",
      label = "i18n::NotificationSourceDialog.ChooseColor")
  public void chooseColor(Object object) {
    if (!(object instanceof NotificationSourceModel model)) {
      return;
    }
    Shell shell = Display.getCurrent() == null ? null : Display.getCurrent().getActiveShell();
    if (shell == null) {
      return;
    }
    ColorDialog colorDialog = new ColorDialog(shell);
    RGB current = toRgb(model.color);
    if (current != null) {
      colorDialog.setRGB(current);
    }
    RGB chosen = colorDialog.open();
    if (chosen != null) {
      model.color = String.format("#%02X%02X%02X", chosen.red, chosen.green, chosen.blue);
    }
  }

  /**
   * Read the owner and repository out of the URL that was typed.
   *
   * @param object The model being edited
   */
  @GuiWidgetElement(
      id = WIDGET_PARSE_GITHUB_URL,
      order = "115",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.BUTTON,
      toolTip = "i18n::NotificationSourceDialog.ParseUrl.Tooltip",
      label = "i18n::NotificationSourceDialog.ParseUrl")
  public void parseGithubUrl(Object object) {
    if (!(object instanceof NotificationSourceModel model)) {
      return;
    }
    String[] ownerRepo = parseOwnerAndRepo(model.githubUrl);
    if (ownerRepo == null) {
      // Pressing the button is a deliberate "read this for me", so an unreadable URL is worth
      // saying out loud. What is already in the owner and repository is left alone: clearing it
      // would punish a typo in the URL by throwing away a repository that was already correct.
      Shell shell = Display.getCurrent() == null ? null : Display.getCurrent().getActiveShell();
      if (shell != null) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "NotificationSourceDialog.Error.Title"),
            BaseMessages.getString(PKG, "NotificationSourceDialog.Error.GithubUrl"),
            new HopException(Const.NVL(model.githubUrl, "")));
      }
      return;
    }
    model.githubOwner = ownerRepo[0];
    model.githubRepo = ownerRepo[1];
  }

  /**
   * Read a GitHub owner and repository out of a URL or an {@code owner/repo} pair.
   *
   * @param input The text to read
   * @return The owner and the repository, or null when the text is not one of those
   */
  public static String[] parseOwnerAndRepo(String input) {
    if (Utils.isEmpty(input)) {
      return null;
    }
    String path;
    if (input.contains("github.com/")) {
      String[] parts = input.split("github.com/");
      if (parts.length < 2) {
        return null;
      }
      path = parts[1];
    } else if (input.contains("/")) {
      path = input;
    } else {
      return null;
    }
    // Drop any query string or fragment before splitting the path.
    path = path.split("\\?")[0].split("#")[0];
    String[] segments = path.split("/");
    if (segments.length < 2) {
      return null;
    }
    String owner = segments[0].trim();
    String repo = segments[1].trim();
    if (owner.isEmpty() || repo.isEmpty()) {
      return null;
    }
    return new String[] {owner, repo};
  }

  private static RGB toRgb(String hex) {
    if (Utils.isEmpty(hex)) {
      return null;
    }
    try {
      int value = Integer.parseInt(hex.startsWith("#") ? hex.substring(1) : hex, 16);
      return new RGB((value >> 16) & 0xFF, (value >> 8) & 0xFF, value & 0xFF);
    } catch (NumberFormatException e) {
      // Not a colour we can show, so open the picker on its own default.
      return null;
    }
  }

  /**
   * The source types, by the name they are shown under.
   *
   * <p>The signature is what {@code GuiCompositeWidgets} looks for on a {@code comboValuesMethod}.
   *
   * @param log Unused, part of the contract
   * @param metadataProvider Unused, part of the contract
   * @return The display name of every source type, in declaration order
   */
  public List<String> getTypeNames(ILogChannel log, IHopMetadataProvider metadataProvider) {
    List<String> names = new ArrayList<>();
    for (NotificationSourceConfig.SourceType sourceType :
        NotificationSourceConfig.SourceType.values()) {
      names.add(sourceType.getDisplayName());
    }
    return names;
  }

  /**
   * @return The type currently chosen, or the first one when the combo says nothing usable
   */
  public NotificationSourceConfig.SourceType selectedType() {
    for (NotificationSourceConfig.SourceType sourceType :
        NotificationSourceConfig.SourceType.values()) {
      if (sourceType.getDisplayName().equals(type)) {
        return sourceType;
      }
    }
    return NotificationSourceConfig.SourceType.values()[0];
  }

  /**
   * The widgets that do not apply to the type currently chosen.
   *
   * <p>A plugin's provider authenticates itself, so a plugin source is not asked for credentials.
   * Everything else is a question only one kind of source can answer.
   *
   * @return The ids to hide
   */
  public Set<String> widgetsToHide() {
    Set<String> hidden = new LinkedHashSet<>();
    NotificationSourceConfig.SourceType sourceType = selectedType();
    if (sourceType != NotificationSourceConfig.SourceType.GITHUB_RELEASES) {
      hidden.add(WIDGET_GITHUB_URL);
      hidden.add(WIDGET_PARSE_GITHUB_URL);
      hidden.add(WIDGET_GITHUB_OWNER);
      hidden.add(WIDGET_GITHUB_REPO);
      hidden.add(WIDGET_GITHUB_PRERELEASES);
      hidden.add(WIDGET_MINIMUM_VERSION);
    }
    if (sourceType != NotificationSourceConfig.SourceType.RSS_FEED) {
      hidden.add(WIDGET_RSS_URL);
    }
    if (sourceType != NotificationSourceConfig.SourceType.CUSTOM_PLUGIN) {
      hidden.add(WIDGET_PLUGIN_ID);
    }
    if (sourceType == NotificationSourceConfig.SourceType.CUSTOM_PLUGIN) {
      hidden.add(WIDGET_USERNAME);
      hidden.add(WIDGET_PASSWORD);
    }
    return hidden;
  }

  /**
   * Fill the form from a stored source.
   *
   * @param config The source being edited
   * @return A model showing what that source says
   */
  public static NotificationSourceModel fromConfig(NotificationSourceConfig config) {
    NotificationSourceModel model = new NotificationSourceModel();
    model.name = config.getName();
    model.type =
        (config.getType() == null
                ? NotificationSourceConfig.SourceType.values()[0]
                : config.getType())
            .getDisplayName();
    model.enabled = config.isEnabled();
    model.color = Utils.isEmpty(config.getColor()) ? "#000000" : config.getColor();
    model.githubOwner = config.getGithubOwner();
    model.githubRepo = config.getGithubRepo();
    model.githubIncludePrereleases = config.isGithubIncludePrereleases();
    model.minimumVersion = config.getMinimumVersion();
    model.rssUrl = config.getRssUrl();
    model.pluginId = config.getPluginId();
    model.pollIntervalMinutes = config.getPollIntervalMinutes();
    model.daysToGoBack = Utils.isEmpty(config.getDaysToGoBack()) ? "0" : config.getDaysToGoBack();
    model.username = config.getUsername();
    model.password = config.getPassword();
    if (!Utils.isEmpty(model.githubOwner) && !Utils.isEmpty(model.githubRepo)) {
      model.githubUrl = "https://github.com/" + model.githubOwner + "/" + model.githubRepo;
    }
    return model;
  }

  /**
   * Write the form back onto the source.
   *
   * <p>Only the fields the chosen type uses are written, so switching a source to RSS and back does
   * not leave it carrying an owner and a repository it no longer polls. Credentials are stored as
   * they were typed, so a variable stays a reference in the configuration file rather than being
   * expanded into it.
   *
   * @param config The source to write to
   */
  public void toConfig(NotificationSourceConfig config) {
    config.setName(trimmed(name));
    config.setType(selectedType());
    config.setEnabled(enabled);
    config.setColor(trimmed(color));
    config.setPollIntervalMinutes(trimmed(pollIntervalMinutes));
    config.setDaysToGoBack(Utils.isEmpty(trimmed(daysToGoBack)) ? "0" : trimmed(daysToGoBack));

    switch (selectedType()) {
      case GITHUB_RELEASES:
        config.setGithubOwner(trimmed(githubOwner));
        config.setGithubRepo(trimmed(githubRepo));
        config.setGithubIncludePrereleases(githubIncludePrereleases);
        config.setMinimumVersion(trimmed(minimumVersion));
        break;
      case RSS_FEED:
        config.setRssUrl(trimmed(rssUrl));
        break;
      case CUSTOM_PLUGIN:
        config.setPluginId(trimmed(pluginId));
        // The provider is registered under its plugin id, so that has to be the source id too or
        // it would never be found again.
        config.setId(trimmed(pluginId));
        break;
    }

    if (selectedType() == NotificationSourceConfig.SourceType.CUSTOM_PLUGIN) {
      return;
    }
    config.setUsername(trimmed(username));
    config.setPassword(trimmed(password));
  }

  private static String trimmed(String value) {
    return value == null ? null : value.trim();
  }
}
