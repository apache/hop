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

package org.apache.hop.ui.hopgui.perspective.configuration;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.apache.hop.core.Props;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.gui.plugin.tab.GuiTabItem;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiKeyHandler;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePlugin;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.hopgui.perspective.configuration.tabs.ConfigPluginOptionsTab;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.custom.StackLayout;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.FontData;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

@HopPerspectivePlugin(
    id = "160-HopConfigurationPerspective",
    name = "i18n::ConfigurationPerspective.Name",
    description = "i18n::ConfigurationPerspective.Description",
    image = "ui/images/gear.svg",
    documentationUrl = "/hop-gui/perspective-configuration.html")
@GuiPlugin(
    name = "i18n::ConfigurationPerspective.Name",
    description = "i18n::HopConfigurationPerspective.GuiPlugin.Description")
public class ConfigurationPerspective implements IHopPerspective {

  private static final Class<?> PKG = ConfigurationPerspective.class;
  public static final String CONFIG_PERSPECTIVE_TABS = "ConfigurationPerspective.Tabs.ID";

  private static final String ORIGINAL = "Original";
  private static final String ORIGINAL_BACKGROUND = "BackgroundColor";
  private static final String ORIGINAL_FONT = "Font";

  private HopGui hopGui;
  private SashForm sashForm;

  /** Content area showing one config panel at a time (no tab bar; tree on the left selects). */
  private Composite contentArea;

  private StackLayout contentStackLayout;

  /** Temporary CTabFolder used only to build tab content; disposed after reparenting. */
  private CTabFolder configTabs;

  private Tree categoryTree;

  /** Category name -> the panel control (reparented from CTabItem). */
  private Map<String, Control> categoryTabs = new HashMap<>();

  private String currentCategoryName;
  private List<Object> tabInstances = new ArrayList<>(); // Store tab instances for refreshing
  private List<Control> highlightedControls = new ArrayList<>();
  private String currentSearchText = ""; // Track current search for re-applying highlights
  private Color highlightColor; // Custom neutral highlight color
  @Getter private static ConfigurationPerspective instance;

  public ConfigurationPerspective() {
    instance = this;
  }

  @Override
  public List<IGuiContextHandler> getContextHandlers() {
    return new ArrayList<>();
  }

  @Override
  public String getId() {
    return "configuration";
  }

  @GuiKeyboardShortcut(control = true, shift = true, key = 'c', global = true)
  @GuiOsxKeyboardShortcut(command = true, shift = true, key = 'c', global = true)
  @Override
  public void activate() {
    hopGui.setActivePerspective(this);
  }

  @Override
  public void perspectiveActivated() {
    // Reload values in all tabs when perspective is activated
    // This ensures that changes made outside the dialog (e.g., "Do not ask this again") are
    // reflected
    reloadAllTabValues();
  }

  /** Reload values in all tabs that support it */
  private void reloadAllTabValues() {
    for (Object tabInstance : tabInstances) {
      try {
        // Use reflection to call reloadValues() if it exists
        java.lang.reflect.Method reloadMethod = tabInstance.getClass().getMethod("reloadValues");
        reloadMethod.invoke(tabInstance);
      } catch (NoSuchMethodException e) {
        // Tab doesn't have a reloadValues method, skip it
      } catch (Exception e) {
        LogChannel.GENERAL.logError(
            "Error message",
            "Error reloading values for tab "
                + tabInstance.getClass().getName()
                + ": "
                + e.getMessage());
      }
    }
  }

  @Override
  public boolean isActive() {
    return hopGui.isActivePerspective(this);
  }

  @Override
  public void initialize(HopGui hopGui, Composite parent) {
    this.hopGui = hopGui;

    // Create a neutral highlight color (light blue-gray)
    highlightColor = GuiResource.getInstance().getColorLightBlue();

    // SashForm for tree on left and content on right
    sashForm = new SashForm(parent, SWT.HORIZONTAL | SWT.SMOOTH);
    PropsUi.setLook(sashForm);
    sashForm.setLayoutData(new FormDataBuilder().fullSize().result());

    // Left side: Tree navigation
    Composite treeComposite = new Composite(sashForm, SWT.NONE);
    PropsUi.setLook(treeComposite);
    treeComposite.setLayout(new FormLayout());

    // Search box at the top
    Text searchBox =
        new Text(treeComposite, SWT.SEARCH | SWT.ICON_SEARCH | SWT.ICON_CANCEL | SWT.BORDER);
    PropsUi.setLook(searchBox);
    searchBox.setMessage(BaseMessages.getString(PKG, "HopConfigurationperspective.Search.Text"));
    searchBox.setLayoutData(new FormDataBuilder().top().fullWidth().result());
    searchBox.addListener(SWT.Modify, e -> filterSettings(searchBox.getText()));

    categoryTree = new Tree(treeComposite, SWT.SINGLE | SWT.V_SCROLL | SWT.BORDER);
    categoryTree.setLayoutData(
        new FormDataBuilder().top(searchBox, PropsUi.getMargin()).bottom().fullWidth().result());
    PropsUi.setLook(categoryTree, Props.WIDGET_STYLE_TREE);
    categoryTree.addListener(
        SWT.Selection,
        e -> {
          TreeItem[] selection = categoryTree.getSelection();
          if (selection.length > 0) {
            TreeItem selectedItem = selection[0];

            // Check if this is a plugin sub-item
            Boolean isPlugin = (Boolean) selectedItem.getData("isPlugin");
            if (isPlugin != null && isPlugin) {
              // This is a plugin sub-item
              String pluginName = (String) selectedItem.getData("pluginName");

              // First, show the Plugins tab (with highlighting)
              TreeItem parentItem = selectedItem.getParentItem();
              if (parentItem != null) {
                showCategory(parentItem.getText(), false); // Don't apply highlighting yet

                // Then show the specific plugin settings
                Control pluginPanel = categoryTabs.get(parentItem.getText());
                if (pluginPanel != null && !pluginPanel.isDisposed()) {
                  if (pluginPanel instanceof Composite tabComposite) {
                    Composite pluginComposite = findPluginComposite(tabComposite);
                    if (pluginComposite != null) {
                      ConfigPluginOptionsTab.showConfigPluginSettings(pluginName, pluginComposite);

                      // Re-apply highlighting if there's an active search
                      if (currentSearchText != null && !currentSearchText.trim().isEmpty()) {
                        applyHighlightingToCurrentTab();
                      }
                    }
                  }
                }
              }
            } else {
              // Regular category item (with highlighting)
              String categoryName = selectedItem.getText();
              showCategory(categoryName, true);

              // If it's the Plugins category (parent), show instructions
              if (categoryName.equalsIgnoreCase("Plugins") || categoryName.contains("plugin")) {
                Control pluginPanel = categoryTabs.get(categoryName);
                if (pluginPanel != null && !pluginPanel.isDisposed()) {
                  if (pluginPanel instanceof Composite tabComposite) {
                    Composite pluginComposite = findPluginComposite(tabComposite);
                    if (pluginComposite != null) {
                      ConfigPluginOptionsTab.showPluginInstructions(pluginComposite);
                    }
                  }
                }
              }
            }
          }
        });

    // Right side: Content area (stack of panels; no tab bar – tree on the left selects)
    contentArea = new Composite(sashForm, SWT.NONE);
    contentStackLayout = new StackLayout();
    contentArea.setLayout(contentStackLayout);
    PropsUi.setLook(contentArea);
    contentArea.setLayoutData(new FormDataBuilder().fullSize().result());

    // Build tabs in a temporary CTabFolder so existing @GuiTab methods work unchanged
    configTabs = new CTabFolder(contentArea, SWT.BORDER);
    configTabs.setLayoutData(new FormDataBuilder().fullSize().result());
    PropsUi.setLook(configTabs, Props.WIDGET_STYLE_TAB);
    configTabs.setTabHeight(0);

    // Load all setting tabs (adds CTabItems to configTabs)
    loadSettingCategories();

    // Reparent each tab's control into contentArea and dispose the tab folder
    CTabItem[] items = configTabs.getItems();
    for (CTabItem item : items) {
      Control c = item.getControl();
      if (c != null && !c.isDisposed()) {
        c.setParent(contentArea);
        c.setLayoutData(new FormDataBuilder().fullSize().result());
        categoryTabs.put(item.getText(), c);
      }
    }
    for (CTabItem item : items) {
      if (!item.isDisposed()) {
        item.setControl(null);
        item.dispose();
      }
    }
    if (!configTabs.isDisposed()) {
      configTabs.dispose();
    }
    configTabs = null;

    // Select first category and show its panel
    if (categoryTree.getItemCount() > 0) {
      TreeItem firstItem = categoryTree.getItem(0);
      String firstName = firstItem.getText();
      currentCategoryName = firstName;
      Control firstPanel = categoryTabs.get(firstName);
      if (firstPanel != null && !firstPanel.isDisposed()) {
        contentStackLayout.topControl = firstPanel;
        contentArea.layout(true, true);
      }
      categoryTree.setSelection(firstItem);
    }

    sashForm.setWeights(20, 80);

    // Register with key handler so activate shortcut (Ctrl+Shift+C / Cmd+Shift+C) works
    HopGuiKeyHandler keyHandler = HopGuiKeyHandler.getInstance();
    keyHandler.addParentObjectToHandle(this);
    Shell shell = (sashForm != null && !sashForm.isDisposed()) ? sashForm.getShell() : null;
    if (shell != null) {
      HopGui.getInstance().replaceKeyboardShortcutListeners(shell, keyHandler);
    }
  }

  private void loadSettingCategories() {
    GuiRegistry guiRegistry = GuiRegistry.getInstance();
    List<GuiTabItem> tabsList = guiRegistry.getGuiTabsMap().get(CONFIG_PERSPECTIVE_TABS);

    if (tabsList != null) {
      tabsList.sort(Comparator.comparing(GuiTabItem::getId));

      // Invoke all tab methods to populate configTabs
      for (GuiTabItem tabItem : tabsList) {
        try {
          Object object = tabItem.getMethod().getDeclaringClass().getConstructor().newInstance();
          tabItem.getMethod().invoke(object, configTabs);
          // Store the tab instance so we can reload its values later
          tabInstances.add(object);
        } catch (Exception e) {
          new ErrorDialog(
              hopGui.getShell(),
              "Error",
              "Hop was unable to invoke @GuiTab method "
                  + tabItem.getMethod().getName()
                  + " with the parent composite as argument",
              e);
        }
      }

      // Now create tree items for each tab
      CTabItem[] allTabs = configTabs.getItems();
      for (CTabItem tab : allTabs) {
        String categoryName = tab.getText();

        // Check if this is the Plugins tab - we'll handle it specially
        if (categoryName.equalsIgnoreCase("Plugins") || categoryName.contains("plugin")) {
          // Create parent Plugins tree item
          TreeItem pluginsTreeItem = new TreeItem(categoryTree, SWT.NONE);
          pluginsTreeItem.setText(categoryName);
          if (tab.getImage() != null) {
            pluginsTreeItem.setImage(tab.getImage());
          }

          // Tab stored in categoryTabs when we reparent (no put here)
          expandPluginsIntoTree(tab, pluginsTreeItem);
        } else {
          // Regular tree item
          TreeItem treeItem = new TreeItem(categoryTree, SWT.NONE);
          treeItem.setText(categoryName);
          if (tab.getImage() != null) {
            treeItem.setImage(tab.getImage());
          }
          // Tab stored in categoryTabs when we reparent (no put here)
        }
      }

      // Select first item and show its panel (done after reparent in initialize())
    }
  }

  private void expandPluginsIntoTree(CTabItem pluginTab, TreeItem parentItem) {
    // Tab will be stored in categoryTabs when we reparent (by tab text = parentItem.getText())

    // Get plugin names from ConfigPluginOptionsTab
    Set<String> pluginNames = ConfigPluginOptionsTab.getPluginNames();

    if (pluginNames != null && !pluginNames.isEmpty()) {
      // Create a tree item for each plugin
      for (String pluginName : pluginNames) {
        TreeItem pluginItem = new TreeItem(parentItem, SWT.NONE);
        pluginItem.setText(pluginName);

        // Store the plugin name for later use
        pluginItem.setData("pluginName", pluginName);
        pluginItem.setData("isPlugin", true);
      }

      // Expand the parent by default
      parentItem.setExpanded(true);
    }
  }

  private void showCategory(String categoryName) {
    showCategory(categoryName, true);
  }

  private void showCategory(String categoryName, boolean applyHighlighting) {
    Control panel = categoryTabs.get(categoryName);
    if (panel != null && !panel.isDisposed()) {
      currentCategoryName = categoryName;
      contentStackLayout.topControl = panel;
      contentArea.layout(true, true);

      // Re-apply highlighting if there's an active search and it's requested
      if (applyHighlighting && currentSearchText != null && !currentSearchText.trim().isEmpty()) {
        applyHighlightingToCurrentTab();
      }
    }
  }

  private void filterSettings(String searchText) {
    // Store the current search text
    currentSearchText = searchText != null ? searchText.trim() : "";

    // Not enough characters to search
    if (currentSearchText.length() < 3) {
      currentSearchText = "";
    }

    // Clear previous highlights
    clearHighlights();

    if (currentSearchText.isEmpty()) {
      // Show all categories
      for (TreeItem item : categoryTree.getItems()) {
        item.setFont(null);
        // Reset children too
        for (TreeItem child : item.getItems()) {
          child.setFont(null);
        }
      }
      return;
    }

    final String lowerSearch = currentSearchText.toLowerCase();

    TreeItem firstMatch = null;

    // Search through all categories and their content
    for (TreeItem item : categoryTree.getItems()) {
      String categoryName = item.getText().toLowerCase();
      boolean categoryMatches = categoryName.contains(lowerSearch);
      boolean hasMatchingContent = false;

      // Check if category name matches
      if (categoryMatches) {
        item.setForeground(null);
        if (firstMatch == null) {
          firstMatch = item;
        }
      }

      // Search within the category's content
      Control panel = categoryTabs.get(item.getText());
      if (panel != null && !panel.isDisposed()) {
        Control searchRoot = panel;
        if (panel instanceof ScrolledComposite sc) {
          Control content = sc.getContent();
          if (content instanceof Composite) {
            searchRoot = content;
          }
        }
        if (searchRoot instanceof Composite controlComposite) {
          List<Control> matches = searchInComposite(controlComposite, lowerSearch);
          if (!matches.isEmpty()) {
            hasMatchingContent = true;
            if (firstMatch == null) {
              firstMatch = item;
            }

            // Highlight the matched controls
            for (Control match : matches) {
              highlightControl(match, highlightColor);
            }
          }
        }
      }

      // Handle plugin sub-items - need to search their content too
      boolean hasMatchingPlugin = false;
      for (TreeItem childItem : item.getItems()) {
        String pluginName = childItem.getText();
        String pluginNameLower = pluginName.toLowerCase();
        boolean pluginMatches = pluginNameLower.contains(lowerSearch);

        // Also search within the plugin's content
        boolean pluginContentMatches = searchInPluginContent(pluginName, lowerSearch);

        if (pluginMatches || pluginContentMatches) {
          childItem.setFont(GuiResource.getInstance().getFontBold());
          hasMatchingPlugin = true;
          if (firstMatch == null) {
            firstMatch = childItem;
          }
        } else {
          childItem.setFont(null);
        }
      }

      // Set tree item appearance based on matches
      if (categoryMatches || hasMatchingContent || hasMatchingPlugin) {
        item.setFont(GuiResource.getInstance().getFontBold());
        item.setExpanded(true); // Expand if has matching children
      } else {
        item.setFont(null);
      }
    }

    // Navigate to first match
    if (firstMatch != null) {
      categoryTree.setSelection(firstMatch);
      categoryTree.showSelection();

      // Trigger selection to show the content
      Event event = new Event();
      event.item = firstMatch;
      categoryTree.notifyListeners(SWT.Selection, event);

      // After navigation, search and highlight in the displayed content
      // Use timerExec with delay to ensure plugin content is fully loaded
      hopGui
          .getDisplay()
          .timerExec(
              100, // 100ms delay to allow plugin content to render
              () -> {
                Control top = contentStackLayout.topControl;
                if (top != null && !top.isDisposed()) {
                  Control searchIn = top;
                  if (top instanceof ScrolledComposite sc) {
                    Control content = sc.getContent();
                    if (content instanceof Composite) {
                      searchIn = content;
                    }
                  }
                  if (searchIn instanceof Composite controlComposite) {
                    List<Control> matches = searchInComposite(controlComposite, lowerSearch);
                    for (Control match : matches) {
                      if (!match.isDisposed()) {
                        highlightControl(match, highlightColor);
                      }
                    }
                    if (!top.isDisposed()) {
                      top.redraw();
                    }
                  }
                }
              });
    }
  }

  private boolean searchInPluginContent(String pluginName, String searchText) {
    // Create a temporary composite to load the plugin's settings
    Composite tempComposite = new Composite(sashForm, SWT.NONE);
    tempComposite.setLayout(new FillLayout());

    try {
      // Load the plugin settings into the temp composite
      ConfigPluginOptionsTab.showConfigPluginSettings(pluginName, tempComposite);

      // Force layout to ensure all widgets are created
      tempComposite.layout(true, true);

      // Search within the composite
      List<Control> matches = searchInComposite(tempComposite, searchText);

      // Clean up
      tempComposite.dispose();

      return !matches.isEmpty();
    } catch (Exception e) {
      // If there's an error loading the plugin, assume no match
      if (!tempComposite.isDisposed()) {
        tempComposite.dispose();
      }
      return false;
    }
  }

  private List<Control> searchInComposite(Composite composite, String searchText) {
    List<Control> matches = new ArrayList<>();

    if (composite == null || composite.isDisposed()) {
      return matches;
    }

    // Don't match anything if search text is empty
    if (searchText == null || searchText.trim().isEmpty()) {
      return matches;
    }

    try {
      for (Control control : composite.getChildren()) {
        if (control == null || control.isDisposed()) {
          continue;
        }

        boolean controlMatches = false;

        // Check labels
        if (control instanceof Label label) {
          String text = label.getText();
          if (text != null && text.toLowerCase().contains(searchText)) {
            matches.add(control);
            controlMatches = true;
          }
        }
        // Check text fields
        else if (control instanceof Text text) {
          String value = text.getText();
          if (value != null && value.toLowerCase().contains(searchText)) {
            matches.add(control);
            controlMatches = true;
          }
        }
        // Check buttons
        else if (control instanceof Button button) {
          String text = button.getText();
          if (text != null && text.toLowerCase().contains(searchText)) {
            matches.add(control);
            controlMatches = true;
          }
        }
        // Check tables (both SWT Table and Hop TableView)
        else if (control instanceof Table table) {
          // Search through all table items
          for (TableItem item : table.getItems()) {
            for (int i = 0; i < table.getColumnCount(); i++) {
              String cellText = item.getText(i);
              if (cellText != null && cellText.toLowerCase().contains(searchText)) {
                matches.add(control);
                controlMatches = true;
                break;
              }
            }
            if (controlMatches) {
              break;
            }
          }
        }
        // Check Hop's TableView widget
        else if (control instanceof TableView tableView) {
          Table table = tableView.table;
          // Search through all table items
          for (TableItem item : table.getItems()) {
            for (int i = 0; i < table.getColumnCount(); i++) {
              String cellText = item.getText(i);
              if (cellText != null && cellText.toLowerCase().contains(searchText)) {
                matches.add(control);
                controlMatches = true;
                break;
              }
            }
            if (controlMatches) {
              break;
            }
          }
        }

        // Check tooltips
        if (!controlMatches) {
          String tooltip = control.getToolTipText();
          if (tooltip != null && tooltip.toLowerCase().contains(searchText)) {
            matches.add(control);
            controlMatches = true;
          }
        }

        // Recursively search in child composites (including ScrolledComposite)
        if (control instanceof ScrolledComposite sc) {
          Control content = sc.getContent();
          if (content instanceof Composite composite1) {
            matches.addAll(searchInComposite(composite1, searchText));
          }
        } else if (control instanceof Composite composite1) {
          matches.addAll(searchInComposite(composite1, searchText));
        }
      }
    } catch (Exception e) {
      // Silently ignore any errors during search
    }

    return matches;
  }

  private void highlightControl(Control control, Color highlightColor) {
    if (control == null || control.isDisposed()) {
      return;
    }

    // Special handling for tables - highlight matching rows
    if (control instanceof Table table) {
      highlightTableRows(table, highlightColor);
      return;
    } else if (control instanceof TableView tableView) {
      highlightTableRows(tableView.table, highlightColor);
      return;
    }

    // Store original font and background color only once
    if (control.getData(ORIGINAL) == null) {
      control.setData(ORIGINAL, Boolean.TRUE);
      control.setData(ORIGINAL_BACKGROUND, control.getBackground());
      control.setData(ORIGINAL_FONT, control.getFont());
    }

    // Apply highlight
    control.setBackground(highlightColor);

    // Make text bold if it's a label
    if (control instanceof Label || control instanceof Text) {
      Font currentFont = control.getFont();
      if (currentFont != null) {
        FontData[] fontData = currentFont.getFontData();
        for (FontData fd : fontData) {
          fd.setStyle(fd.getStyle() | SWT.BOLD);
        }
        Font boldFont = new Font(control.getDisplay(), fontData);
        control.setFont(boldFont);
      }
    }

    highlightedControls.add(control);
  }

  private void highlightTableRows(Table table, Color highlightColor) {
    if (table == null
        || table.isDisposed()
        || currentSearchText == null
        || currentSearchText.trim().isEmpty()) {
      return;
    }

    String lowerSearch = currentSearchText.toLowerCase();

    // Search through all table items and highlight matching rows
    for (TableItem item : table.getItems()) {
      boolean rowMatches = false;

      // Check all columns in the row
      for (int i = 0; i < table.getColumnCount(); i++) {
        String cellText = item.getText(i);
        if (cellText != null && cellText.toLowerCase().contains(lowerSearch)) {
          rowMatches = true;
          break;
        }
      }

      // Highlight the row if it matches
      if (rowMatches) {
        // Store original background color
        item.setData(ORIGINAL_BACKGROUND, item.getBackground());
        item.setBackground(highlightColor);

        // Track this table for cleanup
        if (!highlightedControls.contains(table)) {
          highlightedControls.add(table);
        }
      }
    }
  }

  private void clearHighlights() {
    for (Control control : highlightedControls) {
      if (!control.isDisposed()) {
        // Special handling for tables - need to clear all row backgrounds
        if (control instanceof Table table) {
          for (TableItem item : table.getItems()) {
            if (!item.isDisposed()) {
              item.setBackground(null); // Reset to default
            }
          }
        } else {
          // Restore original background
          Color originalBg = (Color) control.getData(ORIGINAL_BACKGROUND);
          if (originalBg != null) {
            control.setBackground(originalBg);
          }

          // Restore original font
          Font originalFont = (Font) control.getData(ORIGINAL_FONT);
          if (originalFont != null) {
            // Dispose the bold font before restoring
            Font currentFont = control.getFont();
            control.setFont(originalFont);
            if (currentFont != null && !currentFont.equals(originalFont)) {
              currentFont.dispose();
            }
          }
        }
      }
    }

    highlightedControls.clear();
  }

  private void applyHighlightingToCurrentTab() {
    if (currentSearchText == null || currentSearchText.trim().isEmpty()) {
      return;
    }

    final String lowerSearch = currentSearchText.toLowerCase();

    // Use timerExec with delay to ensure content is fully loaded (important for plugin content)
    hopGui
        .getDisplay()
        .timerExec(
            100, // 100ms delay to allow plugin content to render
            () -> {
              // Clear existing highlights first
              clearHighlights();

              Control top = contentStackLayout.topControl;
              if (top != null && !top.isDisposed()) {
                Control searchIn = top;
                if (top instanceof ScrolledComposite sc) {
                  Control content = sc.getContent();
                  if (content instanceof Composite) {
                    searchIn = content;
                  }
                }
                if (searchIn instanceof Composite composite1) {
                  List<Control> matches = searchInComposite(composite1, lowerSearch);
                  for (Control match : matches) {
                    if (!match.isDisposed()) {
                      highlightControl(match, highlightColor);
                    }
                  }
                  if (!top.isDisposed()) {
                    top.redraw();
                  }
                }
              }
            });
  }

  public void showSystemVariablesTab() {
    // Navigate to system variables in the tree
    for (TreeItem item : categoryTree.getItems()) {
      if (item.getText().toLowerCase().contains("variable")) {
        categoryTree.setSelection(item);
        showCategory(item.getText());
        break;
      }
    }
  }

  @Override
  public Control getControl() {
    return sashForm;
  }

  private Composite findPluginComposite(Composite parent) {
    // Recursively search for the plugin composite
    for (Control child : parent.getChildren()) {
      if (child instanceof Composite childComposite) {
        // Check if this is the plugin composite by checking its layout data
        Object layoutData = childComposite.getLayoutData();
        if (layoutData instanceof FormData fd
            && fd.left != null
            && fd.right != null
            && fd.top != null
            && fd.bottom != null) {
          // The plugin composite has specific layout characteristics
          return childComposite;
        }
        // Recursively search children
        Composite found = findPluginComposite(childComposite);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }
}
