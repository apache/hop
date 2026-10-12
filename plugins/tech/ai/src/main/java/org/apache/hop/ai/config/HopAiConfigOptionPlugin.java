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

package org.apache.hop.ai.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.ai.session.AiAdvisorSessionStore;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.bus.HopGuiEvents;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.configuration.tabs.ConfigPluginOptionsTab;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Link;
import org.eclipse.swt.widgets.Text;
import picocli.CommandLine;

@Getter
@Setter
@ConfigPlugin(
    id = "HopAiConfigOptionPlugin",
    description = "Configuration options for the Hop AI Assistant",
    category = ConfigPlugin.CATEGORY_CONFIG,
    classLoaderGroup = "hop-ai",
    configKey = HopAiConfig.HOP_CONFIG_KEY,
    configClass = HopAiConfig.class)
@GuiPlugin(description = "i18n::HopAiConfig.Tab.Name", classLoaderGroup = "hop-ai")
public class HopAiConfigOptionPlugin implements IConfigOptions, IGuiPluginCompositeWidgetsListener {

  private static final Class<?> PKG = HopAiConfigOptionPlugin.class;
  private static final String PARENT = ConfigPluginOptionsTab.GUI_WIDGETS_PARENT_ID;

  static final String WIDGET_DEFAULT_PROVIDER = "0200-ai-default-provider";
  static final String WIDGET_DEFAULT_PROVIDER_WARNING = "0210-ai-default-provider-warning";
  static final String WIDGET_EXTRA_CONTEXT = "0400-ai-extra-context";
  static final String WIDGET_CONTEXT_FILES = "0500-ai-extra-context-files";
  static final String HINT_DATA_KEY = HopAiConfigOptionPlugin.class.getName() + ".hint";

  @GuiWidgetElement(
      id = "0050-ai-git-warning",
      order = "0050",
      parentId = PARENT,
      type = GuiElementType.LINK,
      label = "i18n::HopAiConfigOptionPlugin.GitWarning.Label",
      toolTip = "i18n::HopAiConfigOptionPlugin.GitWarning.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::HopAiConfigOptionPlugin.Group")
  public void gitWarning(Event event) {
    // Notice text only; LINK is the widget type this form can show without a value field.
  }

  @GuiWidgetElement(
      id = "0100-ai-enabled",
      order = "0100",
      parentId = PARENT,
      type = GuiElementType.CHECKBOX,
      variables = false,
      label = "i18n::HopAiConfigOptionPlugin.AiEnabled.Label",
      toolTip = "i18n::HopAiConfigOptionPlugin.AiEnabled.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::HopAiConfigOptionPlugin.Group")
  @CommandLine.Option(
      names = {"--hop-ai-enabled"},
      description = "Enable the AI Assistant in Hop GUI",
      negatable = true)
  private Boolean aiEnabled;

  @GuiWidgetElement(
      id = WIDGET_DEFAULT_PROVIDER,
      order = "0200",
      parentId = PARENT,
      type = GuiElementType.METADATA,
      metadata = AiProvider.class,
      label = "i18n::HopAiConfigOptionPlugin.DefaultProvider.Label",
      toolTip = "i18n::HopAiConfigOptionPlugin.DefaultProvider.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::HopAiConfigOptionPlugin.Group")
  @CommandLine.Option(
      names = {"--hop-ai-default-provider"},
      description = "Default AI provider metadata name")
  private String defaultProviderName;

  @GuiWidgetElement(
      id = WIDGET_DEFAULT_PROVIDER_WARNING,
      order = "0210",
      parentId = PARENT,
      type = GuiElementType.LINK,
      label = "",
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::HopAiConfigOptionPlugin.Group")
  public void defaultProviderWarning(Event event) {
    // Text only, set while the default provider is not defined in the open project.
  }

  @GuiWidgetElement(
      id = "0300-ai-allow-full-xml",
      order = "0300",
      parentId = PARENT,
      type = GuiElementType.CHECKBOX,
      variables = false,
      label = "i18n::HopAiConfigOptionPlugin.AllowFullXml.Label",
      toolTip = "i18n::HopAiConfigOptionPlugin.AllowFullXml.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::HopAiConfigOptionPlugin.Group")
  @CommandLine.Option(
      names = {"--hop-ai-allow-full-xml"},
      description = "Allow the AI Assistant to send full pipeline or workflow XML",
      negatable = true)
  private Boolean allowSendFullXml;

  @GuiWidgetElement(
      id = "0310-ai-keep-conversations",
      order = "0310",
      parentId = PARENT,
      type = GuiElementType.CHECKBOX,
      variables = false,
      label = "i18n::HopAiConfigOptionPlugin.KeepConversations.Label",
      toolTip = "i18n::HopAiConfigOptionPlugin.KeepConversations.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::HopAiConfigOptionPlugin.Group")
  @CommandLine.Option(
      names = {"--hop-ai-keep-conversations"},
      description = "Keep AI Assistant conversations when Hop GUI closes",
      negatable = true)
  private Boolean keepConversations;

  @GuiWidgetElement(
      id = "0320-ai-request-on-close",
      order = "0320",
      parentId = PARENT,
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::HopAiConfigOptionPlugin.RequestOnClose.Label",
      toolTip = "i18n::HopAiConfigOptionPlugin.RequestOnClose.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::HopAiConfigOptionPlugin.Group")
  @CommandLine.Option(
      names = {"--hop-ai-request-on-close"},
      description =
          "What happens to a running AI Assistant question when its window closes:"
              + " FINISH_IN_BACKGROUND or CANCEL")
  private AiRequestOnClose requestOnClose;

  @GuiWidgetElement(
      id = WIDGET_EXTRA_CONTEXT,
      order = "0400",
      parentId = PARENT,
      type = GuiElementType.MULTI_LINE_TEXT,
      multiLineTextHeight = 6,
      variables = true,
      label = "i18n::HopAiConfigOptionPlugin.ExtraContext.Label",
      toolTip = "i18n::HopAiConfigOptionPlugin.ExtraContext.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::HopAiConfigOptionPlugin.Group")
  @CommandLine.Option(
      names = {"--hop-ai-extra-context"},
      description =
          "Standing notes sent with every AI Assistant question. An empty value removes them.")
  private String extraContext;

  @GuiWidgetElement(
      id = WIDGET_CONTEXT_FILES,
      order = "0500",
      parentId = PARENT,
      type = GuiElementType.MULTI_LINE_TEXT,
      multiLineTextHeight = 4,
      variables = true,
      label = "i18n::HopAiConfigOptionPlugin.ExtraContextFiles.Label",
      toolTip = "i18n::HopAiConfigOptionPlugin.ExtraContextFiles.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::HopAiConfigOptionPlugin.Group")
  @CommandLine.Option(
      names = {"--hop-ai-context-files"},
      description =
          "Files sent with every AI Assistant question: VFS paths, one per line or separated by"
              + " commas. An empty value removes them.")
  private String extraContextFiles;

  @GuiWidgetElement(
      id = "0510-ai-add-context-file",
      order = "0510",
      parentId = PARENT,
      type = GuiElementType.BUTTON,
      label = "i18n::HopAiConfigOptionPlugin.AddContextFile.Label",
      toolTip = "i18n::HopAiConfigOptionPlugin.AddContextFile.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::HopAiConfigOptionPlugin.Group")
  public void addContextFile() {
    HopGui hopGui = HopGui.getInstance();
    IVariables variables = hopGui.getVariables();
    String filename =
        BaseDialog.presentFileDialog(
            hopGui.getShell(),
            null,
            variables,
            new String[] {"*.md;*.txt", "*"},
            new String[] {
              BaseMessages.getString(PKG, "HopAiConfigOptionPlugin.AddContextFile.Filter"),
              BaseMessages.getString("System.FileType.AllFiles")
            },
            true);
    if (Utils.isEmpty(filename)) {
      return;
    }
    extraContextFiles = appendPath(extraContextFiles, projectRelative(filename, variables));
    HopAiConfigSingleton.getConfig().setExtraContextFiles(extraContextFiles);
    saveQuietly();
  }

  /** The file list with one more path on a line of its own. */
  static String appendPath(String files, String path) {
    String current = Const.NVL(files, "").stripTrailing();
    return current.isEmpty() ? path : current + "\n" + path;
  }

  /**
   * A file in the project written as {@code ${PROJECT_HOME}/…}, so the list works for everyone who
   * opens the project, wherever it is.
   */
  static String projectRelative(String filename, IVariables variables) {
    String path = HopVfs.separatorsToUnix(filename);
    String home = variables != null ? variables.getVariable("PROJECT_HOME") : null;
    if (Utils.isEmpty(home)) {
      return path;
    }
    String prefix = HopVfs.separatorsToUnix(home);
    prefix = prefix.endsWith("/") ? prefix : prefix + "/";
    return path.startsWith(prefix) ? "${PROJECT_HOME}/" + path.substring(prefix.length()) : path;
  }

  public static HopAiConfigOptionPlugin getInstance() {
    HopAiConfigOptionPlugin instance = new HopAiConfigOptionPlugin();
    try {
      HopGui hopGui = HopGui.getInstance();
      if (hopGui != null) {
        HopAiLegacyConfigMigrator.migrate(hopGui.getMetadataProvider());
      }
    } catch (Throwable ignored) {
      // CLI / tests may have no GUI.
    }
    HopAiConfig config = HopAiConfigSingleton.getConfig();
    instance.aiEnabled = config.isAiEnabled();
    instance.defaultProviderName = config.getDefaultProviderName();
    instance.allowSendFullXml = config.isAllowSendFullXml();
    instance.keepConversations = config.isKeepConversations();
    instance.requestOnClose = config.getRequestOnClose();
    instance.extraContext = config.getExtraContext();
    instance.extraContextFiles = config.getExtraContextFiles();
    return instance;
  }

  @Override
  public boolean handleOption(
      ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables)
      throws HopException {
    try {
      if (hasHopMetadataProvider != null && hasHopMetadataProvider.getMetadataProvider() != null) {
        HopAiLegacyConfigMigrator.migrate(hasHopMetadataProvider.getMetadataProvider());
      }
      HopAiConfig config = HopAiConfigSingleton.getConfig();
      boolean changed = false;
      if (aiEnabled != null && config.isAiEnabled() != aiEnabled) {
        config.setAiEnabled(aiEnabled);
        log.logBasic(aiEnabled ? "Enabled the Hop AI Assistant" : "Disabled the Hop AI Assistant");
        changed = true;
      }
      if (defaultProviderName != null
          && !defaultProviderName.equals(config.getDefaultProviderName())) {
        config.setDefaultProviderName(defaultProviderName);
        changed = true;
      }
      if (allowSendFullXml != null && config.isAllowSendFullXml() != allowSendFullXml) {
        config.setAllowSendFullXml(allowSendFullXml);
        changed = true;
      }
      if (keepConversations != null && config.isKeepConversations() != keepConversations) {
        config.setKeepConversations(keepConversations);
        changed = true;
      }
      if (requestOnClose != null && config.getRequestOnClose() != requestOnClose) {
        config.setRequestOnClose(requestOnClose);
        changed = true;
      }
      if (extraContext != null
          && !HopAiConfig.normalizeExtraContext(extraContext).equals(config.getExtraContext())) {
        config.setExtraContext(extraContext);
        changed = true;
      }
      if (extraContextFiles != null) {
        String files = contextFilesFromCommandLine(extraContextFiles);
        if (!files.equals(contextFilesFromCommandLine(config.getExtraContextFiles()))) {
          config.setExtraContextFiles(files);
          changed = true;
        }
      }
      if (changed) {
        HopAiConfigSingleton.saveConfig();
      }
      return changed;
    } catch (Exception e) {
      throw new HopException("Error handling Hop AI configuration options", e);
    }
  }

  /** A command line holds one line, so commas separate the paths there as well. */
  static String contextFilesFromCommandLine(String value) {
    List<String> paths = new ArrayList<>();
    if (value == null) {
      return "";
    }
    for (String path : value.split("[,\\r\\n]+")) {
      if (!path.isBlank()) {
        paths.add(path.trim());
      }
    }
    return String.join("\n", paths);
  }

  @Override
  public void widgetsCreated(GuiCompositeWidgets compositeWidgets) {
    // Do nothing
  }

  @Override
  public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {
    addContextFilesHint(compositeWidgets);
    alignWithProviderLine(compositeWidgets);
    updateDefaultProviderWarning(compositeWidgets);
    // The providers are metadata of the open project; the list follows when another one opens.
    HopGui hopGui = HopGui.peekInstance();
    if (hopGui != null && hopGui.getEventsHandler() != null) {
      hopGui
          .getEventsHandler()
          .addEventListener(
              HopAiConfigOptionPlugin.class.getName() + "ProjectActivated",
              e ->
                  hopGui
                      .getDisplay()
                      .asyncExec(
                          () -> reloadProviders(compositeWidgets, hopGui.getMetadataProvider())),
              HopGuiEvents.ProjectActivated.name());
    }
  }

  @Override
  public void widgetModified(
      GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
    persistContents(compositeWidgets);
    if (WIDGET_DEFAULT_PROVIDER.equals(widgetId)) {
      updateDefaultProviderWarning(compositeWidgets);
    }
  }

  /** List the providers of the project that is open now, keeping the configured name. */
  static void reloadProviders(
      GuiCompositeWidgets compositeWidgets, IHopMetadataProvider metadataProvider) {
    if (!(compositeWidgets.getWidgetsMap().get(WIDGET_DEFAULT_PROVIDER)
            instanceof MetaSelectionLine<?> line)
        || line.isDisposed()) {
      return;
    }
    String name = HopAiConfigSingleton.getConfig().getDefaultProviderName();
    line.setMetadataProvider(metadataProvider);
    try {
      line.fillItems();
    } catch (HopException e) {
      LogChannel.UI.logError("Unable to list the AI providers of the open project", e);
    }
    if (!Const.NVL(name, "").equals(Const.NVL(line.getText(), ""))) {
      line.setText(Const.NVL(name, ""));
    }
    updateDefaultProviderWarning(compositeWidgets);
  }

  /**
   * The default provider is a Hop-wide setting, the providers are project metadata. Say so when the
   * open project has no provider by that name: its sessions then use the provider picked last.
   */
  static void updateDefaultProviderWarning(GuiCompositeWidgets compositeWidgets) {
    if (!(compositeWidgets.getWidgetsMap().get(WIDGET_DEFAULT_PROVIDER_WARNING)
            instanceof Link link)
        || link.isDisposed()) {
      return;
    }
    String name = null;
    if (compositeWidgets.getWidgetsMap().get(WIDGET_DEFAULT_PROVIDER)
        instanceof MetaSelectionLine<?> line) {
      name = line.getText();
      if (!Utils.isEmpty(name) && Arrays.asList(line.getItems()).contains(name)) {
        name = null;
      }
    }
    String text =
        Utils.isEmpty(name)
            ? ""
            : BaseMessages.getString(
                PKG, "HopAiConfigOptionPlugin.DefaultProvider.Missing", name.replace("&", "&&"));
    if (text.equals(link.getText()) && link.getVisible() == !text.isEmpty()) {
      return;
    }
    link.setText(text);
    link.setVisible(!text.isEmpty());
    if (link.getLayoutData() instanceof FormData data) {
      data.height = text.isEmpty() ? 0 : SWT.DEFAULT;
    }
    link.getParent().layout(true, true);
  }

  /**
   * The text fields end where the provider field ends, before its buttons, so their variable icons
   * line up with the provider's in one column.
   */
  static void alignWithProviderLine(GuiCompositeWidgets compositeWidgets) {
    if (!(compositeWidgets.getWidgetsMap().get(WIDGET_DEFAULT_PROVIDER)
            instanceof MetaSelectionLine<?> line)
        || line.getwToolBar() == null) {
      return;
    }
    int inset = line.getwToolBar().computeSize(SWT.DEFAULT, SWT.DEFAULT).x + PropsUi.getMargin();
    Composite parent = null;
    for (String id : new String[] {WIDGET_EXTRA_CONTEXT, WIDGET_CONTEXT_FILES}) {
      Control control = compositeWidgets.getWidgetsMap().get(id);
      if (control != null && control.getLayoutData() instanceof FormData data) {
        data.right = new FormAttachment(100, -inset);
        parent = control.getParent();
      }
    }
    if (parent != null) {
      parent.layout(true, true);
    }
  }

  /**
   * An example of what Context files takes, in the empty field. A multi-line field on macOS shows
   * no message of its own, so it is painted.
   */
  static void addContextFilesHint(GuiCompositeWidgets compositeWidgets) {
    Control control = compositeWidgets.getWidgetsMap().get(WIDGET_CONTEXT_FILES);
    Text text = control instanceof TextVar textVar ? textVar.getTextWidget() : null;
    if (text == null || text.isDisposed() || text.getData(HINT_DATA_KEY) != null) {
      return;
    }
    text.setData(HINT_DATA_KEY, Boolean.TRUE);
    String hint = BaseMessages.getString(PKG, "HopAiConfigOptionPlugin.ExtraContextFiles.Hint");
    if (EnvironmentUtils.getInstance().isWeb()) {
      text.setMessage(hint);
      return;
    }
    text.addPaintListener(
        e -> {
          if (text.getCharCount() > 0) {
            return;
          }
          e.gc.setForeground(GuiResource.getInstance().getColorDarkGray());
          e.gc.drawText(hint, PropsUi.getMargin(), PropsUi.getMargin() / 2, true);
        });
    text.addListener(SWT.Modify, e -> text.redraw());
  }

  @Override
  public void persistContents(GuiCompositeWidgets compositeWidgets) {
    compositeWidgets.getWidgetsContents(this, PARENT);
    HopAiConfig config = HopAiConfigSingleton.getConfig();
    if (aiEnabled != null && aiEnabled != config.isAiEnabled()) {
      config.setAiEnabled(aiEnabled);
      // Open AI Assistant views enable or disable their controls.
      HopGui hopGui = HopGui.peekInstance();
      if (hopGui != null) {
        AiAdvisorSessionStore.get(hopGui).fireChanged();
      }
    }
    if (defaultProviderName != null) {
      config.setDefaultProviderName(defaultProviderName);
    }
    if (allowSendFullXml != null) {
      config.setAllowSendFullXml(allowSendFullXml);
    }
    if (keepConversations != null) {
      config.setKeepConversations(keepConversations);
    }
    if (requestOnClose != null) {
      config.setRequestOnClose(requestOnClose);
    }
    if (extraContext != null) {
      config.setExtraContext(extraContext);
    }
    if (extraContextFiles != null) {
      config.setExtraContextFiles(extraContextFiles.isBlank() ? "" : extraContextFiles);
    }
    saveQuietly();
  }

  private static void saveQuietly() {
    try {
      HopAiConfigSingleton.saveConfig();
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          BaseMessages.getString(PKG, "HopAiConfigOptionPlugin.Save.Error.Title"),
          BaseMessages.getString(PKG, "HopAiConfigOptionPlugin.Save.Error.Message"),
          e);
    }
  }
}
