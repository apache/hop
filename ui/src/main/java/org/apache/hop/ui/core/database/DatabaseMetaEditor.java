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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.database.DatabaseTestResults;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementFilter;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.ShowMessageDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.metadata.MetadataPerspective;
import org.apache.hop.ui.util.HelpUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@GuiPlugin(description = "This is the editor for database connection metadata")
/**
 * The metadata editor for DatabaseMeta Don't move this class around as it's sync'ed with the
 * DatabaseMeta package to find the dialog.
 */
public class DatabaseMetaEditor extends MetadataEditor<DatabaseMeta> {
  private static final Class<?> PKG = DatabaseMetaEditor.class; // For Translator

  private CTabFolder wTabFolder;

  private Composite wGeneralComp;
  private Text wName;
  private Combo wConnectionType;
  private TextVar wManualUrl;
  private Label wlUsername;
  private TextVar wUsername;
  private Label wlPassword;
  private TextVar wPassword;

  private Composite wDatabaseSpecificComp;
  private GuiCompositeWidgets guiCompositeWidgets;

  private Button wSupportsBoolean;
  private Button wSupportsTimestamp;
  private Button wQuoteAll;
  private Button wForceLowercase;
  private Button wForceUppercase;
  private Button wPreserveCase;
  private TextVar wPreferredSchema;
  private TextVar wSqlStatements;

  private TableView wOptions;

  private PropsUi props;
  private int middle;
  private int margin;

  private Map<Class<? extends IDatabase>, IDatabase> metaMap;

  /**
   * @param hopGui The hop GUI
   * @param manager The metadata
   * @param databaseMeta The object to edit
   */
  public DatabaseMetaEditor(
      HopGui hopGui, MetadataManager<DatabaseMeta> manager, DatabaseMeta databaseMeta) {
    super(hopGui, manager, databaseMeta);
    props = PropsUi.getInstance();
    metaMap = populateMetaMap();
    metaMap.put(databaseMeta.getIDatabase().getClass(), databaseMeta.getIDatabase());
  }

  private Map<Class<? extends IDatabase>, IDatabase> populateMetaMap() {
    metaMap = new HashMap<>();
    List<IPlugin> plugins = PluginRegistry.getInstance().getPlugins(DatabasePluginType.class);
    for (IPlugin plugin : plugins) {
      try {
        IDatabase database = (IDatabase) PluginRegistry.getInstance().loadClass(plugin);
        if (database.getDefaultDatabasePort() > 0) {
          database.setPort(Integer.toString(database.getDefaultDatabasePort()));
        }
        database.setPluginId(plugin.getIds()[0]);
        database.setPluginName(plugin.getName());
        database.addDefaultOptions();

        metaMap.put(database.getClass(), database);
      } catch (Exception e) {
        HopGui.getInstance().getLog().logError("Error instantiating database metadata", e);
      }
    }

    return metaMap;
  }

  @Override
  public void createControl(Composite parent) {
    // Create a tabbed interface instead of the confusing left hand side options
    // This will make it more conforming the rest.
    //

    middle = props.getMiddlePct();
    margin = PropsUi.getMargin();

    Label wIcon = new Label(parent, SWT.RIGHT);
    wIcon.setImage(getImage());
    FormData fdlicon = new FormData();
    fdlicon.top = new FormAttachment(0, 0);
    fdlicon.right = new FormAttachment(100, 0);
    wIcon.setLayoutData(fdlicon);
    PropsUi.setLook(wIcon);

    // What's the name
    Label wlName = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "DatabaseDialog.label.ConnectionName"));
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(0, 0);
    fdlName.left = new FormAttachment(0, 0);
    wlName.setLayoutData(fdlName);

    wName = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(wlName, margin);
    fdName.left = new FormAttachment(0, 0);
    fdName.right = new FormAttachment(wIcon, -margin);
    wName.setLayoutData(fdName);

    Label spacer = new Label(parent, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdSpacer = new FormData();
    fdSpacer.left = new FormAttachment(0, 0);
    fdSpacer.top = new FormAttachment(wName, 15);
    fdSpacer.right = new FormAttachment(100, 0);
    spacer.setLayoutData(fdSpacer);

    // Now create the tabs above the buttons...
    wTabFolder = new CTabFolder(parent, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    addGeneralTab();
    addAdvancedTab();
    addOptionsTab();

    // Select the general tab
    //
    wTabFolder.setSelection(0);
    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(spacer, 15);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(100, -15);
    wTabFolder.setLayoutData(fdTabFolder);

    setWidgetsContent();

    // Some widget set changed
    resetChanged();

    // Add listener to detect change after loading data
    Listener modifyListener =
        event -> {
          setChanged();
          MetadataPerspective.getInstance().updateEditor(this);
        };
    wName.addListener(SWT.Modify, modifyListener);
    wConnectionType.addListener(SWT.Modify, modifyListener);
    wConnectionType.addListener(SWT.Modify, event -> changeConnectionType());
    wUsername.addListener(SWT.Modify, modifyListener);
    wPassword.addListener(SWT.Modify, modifyListener);
    wManualUrl.addListener(SWT.Modify, modifyListener);
    wSupportsBoolean.addListener(SWT.Selection, modifyListener);
    wSupportsTimestamp.addListener(SWT.Selection, modifyListener);
    wQuoteAll.addListener(SWT.Selection, modifyListener);
    wForceLowercase.addListener(SWT.Selection, modifyListener);
    wForceUppercase.addListener(SWT.Selection, modifyListener);
    wPreserveCase.addListener(SWT.Selection, modifyListener);
    wPreferredSchema.addListener(SWT.Modify, modifyListener);
    wSqlStatements.addListener(SWT.Modify, modifyListener);
    wOptions.addListener(SWT.Modify, modifyListener);
  }

  private void addGeneralTab() {

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setFont(GuiResource.getInstance().getFontDefault());
    wGeneralTab.setText("   " + BaseMessages.getString(PKG, "DatabaseDialog.DbTab.title") + "   ");

    wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wGeneralComp);

    FormLayout genLayout = new FormLayout();
    genLayout.marginWidth = PropsUi.getFormMargin() * 2;
    genLayout.marginHeight = PropsUi.getFormMargin() * 2;
    wGeneralComp.setLayout(genLayout);

    // What's the type of database access?
    //
    Label wlConnectionType = new Label(wGeneralComp, SWT.RIGHT);
    PropsUi.setLook(wlConnectionType);
    wlConnectionType.setText(BaseMessages.getString(PKG, "DatabaseDialog.label.ConnectionType"));
    FormData fdlConnectionType = new FormData();
    fdlConnectionType.top = new FormAttachment(0, margin);
    fdlConnectionType.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlConnectionType.right = new FormAttachment(middle, -margin);
    wlConnectionType.setLayoutData(fdlConnectionType);

    ToolBar wToolBar = new ToolBar(wGeneralComp, SWT.FLAT | SWT.HORIZONTAL);
    FormData fdToolBar = new FormData();
    fdToolBar.right = new FormAttachment(100, 0);
    fdToolBar.top = new FormAttachment(0, 0);
    wToolBar.setLayoutData(fdToolBar);
    PropsUi.setLook(wToolBar);

    ToolItem item = new ToolItem(wToolBar, SWT.PUSH);
    item.setImage(GuiResource.getInstance().getImageHelpWeb());
    item.setToolTipText(BaseMessages.getString(PKG, "System.Tooltip.Help"));
    item.addListener(SWT.Selection, e -> onHelpDatabaseType());

    wConnectionType = new Combo(wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wConnectionType.setItems(getConnectionTypes());
    PropsUi.setLook(wConnectionType);
    FormData fdConnectionType = new FormData();
    fdConnectionType.top = new FormAttachment(wlConnectionType, 0, SWT.CENTER);
    fdConnectionType.left = new FormAttachment(middle, 0); // To the right of the label
    fdConnectionType.right = new FormAttachment(wToolBar, -margin);
    wConnectionType.setLayoutData(fdConnectionType);
    Control lastControl = wConnectionType;

    // Username field
    //
    wlUsername = new Label(wGeneralComp, SWT.RIGHT);
    PropsUi.setLook(wlUsername);
    wlUsername.setText(BaseMessages.getString(PKG, "DatabaseDialog.label.Username"));
    FormData fdlUsername = new FormData();
    fdlUsername.top = new FormAttachment(lastControl, margin * 2); // At the bottom of this tab
    fdlUsername.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlUsername.right = new FormAttachment(middle, -margin);
    wlUsername.setLayoutData(fdlUsername);
    wUsername =
        new TextVar(manager.getVariables(), wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wUsername);
    FormData fdUsername = new FormData();
    fdUsername.top = new FormAttachment(wlUsername, 0, SWT.CENTER);
    fdUsername.left = new FormAttachment(middle, 0); // To the right of the label
    fdUsername.right = new FormAttachment(100, 0);
    wUsername.setLayoutData(fdUsername);
    lastControl = wUsername;

    // Password field
    //
    wlPassword = new Label(wGeneralComp, SWT.RIGHT);
    PropsUi.setLook(wlPassword);
    wlPassword.setText(BaseMessages.getString(PKG, "DatabaseDialog.label.Password"));
    FormData fdlPassword = new FormData();
    fdlPassword.top = new FormAttachment(lastControl, margin * 2); // At the bottom of this tab
    fdlPassword.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlPassword.right = new FormAttachment(middle, -margin);
    wlPassword.setLayoutData(fdlPassword);
    wPassword =
        new TextVar(manager.getVariables(), wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wPassword.setEchoChar('*');
    PropsUi.setLook(wPassword);
    FormData fdPassword = new FormData();
    fdPassword.top = new FormAttachment(wlPassword, 0, SWT.CENTER);
    fdPassword.left = new FormAttachment(middle, 0); // To the right of the label
    fdPassword.right = new FormAttachment(100, 0);
    wPassword.setLayoutData(fdPassword);
    lastControl = wPassword;

    // Add a composite area
    //
    wDatabaseSpecificComp = new Composite(wGeneralComp, SWT.BACKGROUND);
    wDatabaseSpecificComp.setLayout(new FormLayout());
    FormData fdDatabaseSpecificComp = new FormData();
    fdDatabaseSpecificComp.left = new FormAttachment(0, 0);
    fdDatabaseSpecificComp.right = new FormAttachment(100, 0);
    fdDatabaseSpecificComp.top = new FormAttachment(lastControl, margin);
    wDatabaseSpecificComp.setLayoutData(fdDatabaseSpecificComp);
    PropsUi.setLook(wDatabaseSpecificComp);
    lastControl = wDatabaseSpecificComp;

    // Now add the database plugin specific widgets
    //
    guiCompositeWidgets = new GuiCompositeWidgets(manager.getVariables());
    guiCompositeWidgets.createCompositeWidgets(
        getMetadata().getIDatabase(),
        null,
        wDatabaseSpecificComp,
        DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
        null);

    // Add listener to detect change
    guiCompositeWidgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            setChanged();
          }
        });

    addCompositeWidgetsUsernamePassword();

    // manual URL field
    //
    Label wlManualUrl = new Label(wGeneralComp, SWT.RIGHT);
    PropsUi.setLook(wlManualUrl);
    wlManualUrl.setText(BaseMessages.getString(PKG, "DatabaseDialog.label.ManualUrl"));
    FormData fdlManualUrl = new FormData();
    fdlManualUrl.top = new FormAttachment(lastControl, margin * 2); // At the bottom of this tab
    fdlManualUrl.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlManualUrl.right = new FormAttachment(middle, -margin);
    wlManualUrl.setLayoutData(fdlManualUrl);
    wManualUrl =
        new TextVar(manager.getVariables(), wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wManualUrl);
    FormData fdManualUrl = new FormData();
    fdManualUrl.top = new FormAttachment(wlManualUrl, 0, SWT.CENTER);
    fdManualUrl.left = new FormAttachment(middle, 0); // To the right of the label
    fdManualUrl.right = new FormAttachment(100, 0);
    wManualUrl.setLayoutData(fdManualUrl);
    wManualUrl.addListener(SWT.Modify, e -> enableFields());

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment(0, 0);
    fdGeneralComp.top = new FormAttachment(0, 0);
    fdGeneralComp.right = new FormAttachment(100, 0);
    fdGeneralComp.bottom = new FormAttachment(100, 0);
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);
  }

  private void addCompositeWidgetsUsernamePassword() {
    // Add username and password to the mix so folks can enable/disable those
    //
    guiCompositeWidgets.getWidgetsMap().put(BaseDatabaseMeta.ID_USERNAME_LABEL, wlUsername);
    guiCompositeWidgets.getWidgetsMap().put(BaseDatabaseMeta.ID_USERNAME_WIDGET, wUsername);
    guiCompositeWidgets.getWidgetsMap().put(BaseDatabaseMeta.ID_PASSWORD_LABEL, wlPassword);
    guiCompositeWidgets.getWidgetsMap().put(BaseDatabaseMeta.ID_PASSWORD_WIDGET, wPassword);
  }

  private AtomicBoolean busyChangingConnectionType = new AtomicBoolean(false);

  private void changeConnectionType() {

    if (busyChangingConnectionType.get()) {
      return;
    }
    busyChangingConnectionType.set(true);

    DatabaseMeta databaseMeta = this.getMetadata();

    // Keep track of the old database type since this changes when getting the content
    //
    Class<? extends IDatabase> oldClass = databaseMeta.getIDatabase().getClass();
    String oldTypeName = databaseMeta.getPluginName();
    String newTypeName = wConnectionType.getText();
    wConnectionType.setText(databaseMeta.getPluginName());

    // Capture any information on the widgets
    //
    this.getWidgetsContent(databaseMeta);

    // Save the state of this type, so we can switch back and forth
    //
    metaMap.put(oldClass, databaseMeta.getIDatabase());

    // Now change the data type
    //
    wConnectionType.setText(newTypeName);
    databaseMeta.setDatabaseType(newTypeName);

    // Get possible information from the metadata map (from previous work)
    //
    databaseMeta.setIDatabase(metaMap.get(databaseMeta.getIDatabase().getClass()));

    // Remove existing children
    //
    for (Control child : wDatabaseSpecificComp.getChildren()) {
      child.dispose();
    }

    // Re-add the widgets
    //
    guiCompositeWidgets = new GuiCompositeWidgets(manager.getVariables());
    guiCompositeWidgets.createCompositeWidgets(
        databaseMeta.getIDatabase(),
        null,
        wDatabaseSpecificComp,
        DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
        null);
    guiCompositeWidgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            setChanged();
          }
        });
    addCompositeWidgetsUsernamePassword();

    // Put the data back
    //
    setWidgetsContent();

    wGeneralComp.layout(true, true);

    busyChangingConnectionType.set(false);
  }

  private void addAdvancedTab() {

    CTabItem wAdvancedTab = new CTabItem(wTabFolder, SWT.NONE);
    wAdvancedTab.setFont(GuiResource.getInstance().getFontDefault());
    wAdvancedTab.setText(
        "   " + BaseMessages.getString(PKG, "DatabaseDialog.AdvancedTab.title") + "   ");

    Composite wAdvancedComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wAdvancedComp);

    FormLayout advancedLayout = new FormLayout();
    advancedLayout.marginWidth = PropsUi.getFormMargin() * 2;
    advancedLayout.marginHeight = PropsUi.getFormMargin() * 2;
    wAdvancedComp.setLayout(advancedLayout);

    // Supports the Boolean data type?
    //
    Label wlSupportsBoolean = new Label(wAdvancedComp, SWT.RIGHT);
    PropsUi.setLook(wlSupportsBoolean);
    wlSupportsBoolean.setText(
        BaseMessages.getString(PKG, "DatabaseDialog.label.ConnectionSupportsBoolean"));
    FormData fdlSupportsBoolean = new FormData();
    fdlSupportsBoolean.top = new FormAttachment(0, 0);
    fdlSupportsBoolean.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlSupportsBoolean.right = new FormAttachment(middle, 0);
    wlSupportsBoolean.setLayoutData(fdlSupportsBoolean);
    wSupportsBoolean = new Button(wAdvancedComp, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wSupportsBoolean);
    FormData fdSupportsBoolean = new FormData();
    fdSupportsBoolean.top = new FormAttachment(wlSupportsBoolean, 0, SWT.CENTER);
    fdSupportsBoolean.left = new FormAttachment(middle, margin); // To the right of the label
    fdSupportsBoolean.right = new FormAttachment(100, 0);
    wSupportsBoolean.setLayoutData(fdSupportsBoolean);
    Control lastControl = wSupportsBoolean;

    // Supports the Timestamp data type?
    //
    Label wlSupportsTimestamp = new Label(wAdvancedComp, SWT.RIGHT);
    PropsUi.setLook(wlSupportsTimestamp);
    wlSupportsTimestamp.setText(
        BaseMessages.getString(PKG, "DatabaseDialog.label.ConnectionSupportsTimestamp"));
    FormData fdlSupportsTimestamp = new FormData();
    fdlSupportsTimestamp.top = new FormAttachment(lastControl, margin);
    fdlSupportsTimestamp.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlSupportsTimestamp.right = new FormAttachment(middle, 0);
    wlSupportsTimestamp.setLayoutData(fdlSupportsTimestamp);
    wSupportsTimestamp = new Button(wAdvancedComp, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wSupportsTimestamp);
    FormData fdSupportsTimestamp = new FormData();
    fdSupportsTimestamp.top = new FormAttachment(wlSupportsTimestamp, 0, SWT.CENTER);
    fdSupportsTimestamp.left = new FormAttachment(middle, margin); // To the right of the label
    fdSupportsTimestamp.right = new FormAttachment(100, 0);
    wSupportsTimestamp.setLayoutData(fdSupportsTimestamp);
    lastControl = wSupportsTimestamp;

    // Quote all in database?
    //
    Label wlQuoteAll = new Label(wAdvancedComp, SWT.RIGHT);
    PropsUi.setLook(wlQuoteAll);
    wlQuoteAll.setText(BaseMessages.getString(PKG, "DatabaseDialog.label.AdvancedQuoteAllFields"));
    FormData fdlQuoteAll = new FormData();
    fdlQuoteAll.top = new FormAttachment(lastControl, margin);
    fdlQuoteAll.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlQuoteAll.right = new FormAttachment(middle, 0);
    wlQuoteAll.setLayoutData(fdlQuoteAll);
    wQuoteAll = new Button(wAdvancedComp, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wQuoteAll);
    FormData fdQuoteAll = new FormData();
    fdQuoteAll.top = new FormAttachment(wlQuoteAll, 0, SWT.CENTER);
    fdQuoteAll.left = new FormAttachment(middle, margin); // To the right of the label
    fdQuoteAll.right = new FormAttachment(100, 0);
    wQuoteAll.setLayoutData(fdQuoteAll);
    lastControl = wQuoteAll;

    // Force all identifiers to lowercase?
    //
    Label wlForceLowercase = new Label(wAdvancedComp, SWT.RIGHT);
    PropsUi.setLook(wlForceLowercase);
    wlForceLowercase.setText(
        BaseMessages.getString(PKG, "DatabaseDialog.label.AdvancedForceIdentifiersLowerCase"));
    FormData fdlForceLowercase = new FormData();
    fdlForceLowercase.top = new FormAttachment(lastControl, margin);
    fdlForceLowercase.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlForceLowercase.right = new FormAttachment(middle, 0);
    wlForceLowercase.setLayoutData(fdlForceLowercase);
    wForceLowercase = new Button(wAdvancedComp, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wForceLowercase);
    FormData fdForceLowercase = new FormData();
    fdForceLowercase.top = new FormAttachment(wlForceLowercase, 0, SWT.CENTER);
    fdForceLowercase.left = new FormAttachment(middle, margin); // To the right of the label
    fdForceLowercase.right = new FormAttachment(100, 0);
    wForceLowercase.setLayoutData(fdForceLowercase);
    lastControl = wForceLowercase;

    // Force all identifiers to uppercase?
    //
    Label wlForceUppercase = new Label(wAdvancedComp, SWT.RIGHT);
    PropsUi.setLook(wlForceUppercase);
    wlForceUppercase.setText(
        BaseMessages.getString(PKG, "DatabaseDialog.label.AdvancedForceIdentifiersUpperCase"));
    FormData fdlForceUppercase = new FormData();
    fdlForceUppercase.top = new FormAttachment(lastControl, margin);
    fdlForceUppercase.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlForceUppercase.right = new FormAttachment(middle, 0);
    wlForceUppercase.setLayoutData(fdlForceUppercase);
    wForceUppercase = new Button(wAdvancedComp, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wForceUppercase);
    FormData fdForceUppercase = new FormData();
    fdForceUppercase.top = new FormAttachment(wlForceUppercase, 0, SWT.CENTER);
    fdForceUppercase.left = new FormAttachment(middle, margin); // To the right of the label
    fdForceUppercase.right = new FormAttachment(100, 0);
    wForceUppercase.setLayoutData(fdForceUppercase);
    lastControl = wForceUppercase;

    // Preserve case of reserved keywords?
    //
    Label wlPreserveCase = new Label(wAdvancedComp, SWT.RIGHT);
    PropsUi.setLook(wlPreserveCase);
    wlPreserveCase.setText(
        BaseMessages.getString(PKG, "DatabaseDialog.label.ConnectionPreserveCase"));
    FormData fdlPreserveCase = new FormData();
    fdlPreserveCase.top = new FormAttachment(lastControl, margin);
    fdlPreserveCase.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlPreserveCase.right = new FormAttachment(middle, 0);
    wlPreserveCase.setLayoutData(fdlPreserveCase);
    wPreserveCase = new Button(wAdvancedComp, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wPreserveCase);
    FormData fdPreserveCase = new FormData();
    fdPreserveCase.top = new FormAttachment(wlPreserveCase, 0, SWT.CENTER);
    fdPreserveCase.left = new FormAttachment(middle, margin); // To the right of the label
    fdPreserveCase.right = new FormAttachment(100, 0);
    wPreserveCase.setLayoutData(fdPreserveCase);
    lastControl = wPreserveCase;

    // The preferred schema to use
    //
    Label wlPreferredSchema = new Label(wAdvancedComp, SWT.RIGHT);
    PropsUi.setLook(wlPreferredSchema);
    wlPreferredSchema.setText(
        BaseMessages.getString(PKG, "DatabaseDialog.label.PreferredSchemaName"));
    FormData fdlPreferredSchema = new FormData();
    fdlPreferredSchema.top = new FormAttachment(lastControl, margin);
    fdlPreferredSchema.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlPreferredSchema.right = new FormAttachment(middle, 0);
    wlPreferredSchema.setLayoutData(fdlPreferredSchema);
    wPreferredSchema =
        new TextVar(manager.getVariables(), wAdvancedComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPreferredSchema);
    FormData fdPreferredSchema = new FormData();
    fdPreferredSchema.top = new FormAttachment(wlPreferredSchema, 0, SWT.CENTER);
    fdPreferredSchema.left = new FormAttachment(middle, margin); // To the right of the label
    fdPreferredSchema.right = new FormAttachment(100, 0);
    wPreferredSchema.setLayoutData(fdPreferredSchema);
    lastControl = wPreferredSchema;

    // SQL Statements to run after connecting
    //
    Label wlSqlStatements = new Label(wAdvancedComp, SWT.LEFT);
    PropsUi.setLook(wlSqlStatements);
    wlSqlStatements.setText(
        BaseMessages.getString(PKG, "DatabaseDialog.label.ConnectionSQLStatements"));
    FormData fdlSqlStatements = new FormData();
    fdlSqlStatements.top = new FormAttachment(lastControl, margin);
    fdlSqlStatements.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlSqlStatements.right = new FormAttachment(100, 0);
    wlSqlStatements.setLayoutData(fdlSqlStatements);
    wSqlStatements =
        new TextVar(
            manager.getVariables(),
            wAdvancedComp,
            SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(wSqlStatements);
    FormData fdSqlStatements = new FormData();
    fdSqlStatements.top = new FormAttachment(wlSqlStatements, margin);
    fdSqlStatements.bottom = new FormAttachment(100, 0);
    fdSqlStatements.left = new FormAttachment(0, 0); // To the right of the label
    fdSqlStatements.right = new FormAttachment(100, 0);
    wSqlStatements.setLayoutData(fdSqlStatements);

    FormData fdAdvancedComp = new FormData();
    fdAdvancedComp.left = new FormAttachment(0, 0);
    fdAdvancedComp.top = new FormAttachment(0, 0);
    fdAdvancedComp.right = new FormAttachment(100, 0);
    fdAdvancedComp.bottom = new FormAttachment(100, 0);
    wAdvancedComp.setLayoutData(fdAdvancedComp);

    wAdvancedComp.layout();
    wAdvancedTab.setControl(wAdvancedComp);
  }

  private void addOptionsTab() {

    DatabaseMeta databaseMeta = this.getMetadata();

    CTabItem wOptionsTab = new CTabItem(wTabFolder, SWT.NONE);
    wOptionsTab.setFont(GuiResource.getInstance().getFontDefault());
    wOptionsTab.setText(
        "   " + BaseMessages.getString(PKG, "DatabaseDialog.OptionsTab.title") + "   ");

    Composite wOptionsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wOptionsComp);

    FormLayout optionsLayout = new FormLayout();
    optionsLayout.marginWidth = PropsUi.getFormMargin() * 2;
    optionsLayout.marginHeight = PropsUi.getFormMargin() * 2;
    wOptionsComp.setLayout(optionsLayout);

    ColumnInfo[] optionsColumns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "DatabaseDialog.column.Parameter"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "DatabaseDialog.column.Value"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
        };
    optionsColumns[0].setUsingVariables(true);
    optionsColumns[1].setUsingVariables(true);

    // Options?
    //
    Label wlOptions = new Label(wOptionsComp, SWT.LEFT);
    PropsUi.setLook(wlOptions);
    wlOptions.setText(BaseMessages.getString(PKG, "DatabaseDialog.label.Options"));
    FormData fdlOptions = new FormData();
    fdlOptions.top = new FormAttachment(0, 0);
    fdlOptions.left = new FormAttachment(0, 0); // First one in the left top corner
    fdlOptions.right = new FormAttachment(100, 0);
    wlOptions.setLayoutData(fdlOptions);
    wOptions =
        new TableView(
            manager.getVariables(),
            wOptionsComp,
            SWT.BORDER,
            optionsColumns,
            databaseMeta.getExtraOptions().size(),
            event -> setChanged(),
            props);
    PropsUi.setLook(wOptions);
    FormData fdOptions = new FormData();
    fdOptions.top = new FormAttachment(wlOptions, margin * 2);
    fdOptions.bottom = new FormAttachment(100, 0);
    fdOptions.left = new FormAttachment(0, 0); // To the right of the label
    fdOptions.right = new FormAttachment(100, 0);
    wOptions.setLayoutData(fdOptions);

    FormData fdOptionsComp = new FormData();
    fdOptionsComp.left = new FormAttachment(0, 0);
    fdOptionsComp.top = new FormAttachment(0, 0);
    fdOptionsComp.right = new FormAttachment(100, 0);
    fdOptionsComp.bottom = new FormAttachment(100, 0);
    wOptionsComp.setLayoutData(fdOptionsComp);

    wOptionsComp.layout();
    wOptionsTab.setControl(wOptionsComp);
  }

  private void enableFields() {
    boolean manualUrl =
        StringUtils.isNotEmpty(wManualUrl.getText())
            && StringUtils.isNotBlank(wManualUrl.getText());

    // Also enable/disable the custom native fields
    //
    guiCompositeWidgets.enableWidgets(
        getMetadata().getIDatabase(), DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID, !manualUrl);
  }

  private void test() {
    DatabaseMeta meta = new DatabaseMeta();
    getWidgetsContent(meta);
    testConnection(getShell(), manager.getVariables(), meta);
  }

  private void explore() {
    DatabaseMeta meta = new DatabaseMeta();
    getWidgetsContent(meta);
    try {
      DatabaseExplorerDialog dialog =
          new DatabaseExplorerDialog(
              getShell(),
              SWT.NONE,
              manager.getVariables(),
              meta,
              manager.getSerializer().loadAll());
      dialog.open();
    } catch (Exception e) {
      new ErrorDialog(getShell(), "Error", "Error exploring database", e);
    }
  }

  private void onHelpDatabaseType() {
    PluginRegistry registry = PluginRegistry.getInstance();
    String name = wConnectionType.getText();
    for (IPlugin plugin : registry.getPlugins(DatabasePluginType.class)) {
      if (plugin.getName().equals(name)) {
        HelpUtils.openHelp(getShell(), plugin);
        break;
      }
    }
  }

  @Override
  public void setWidgetsContent() {

    DatabaseMeta databaseMeta = this.getMetadata();

    wName.setText(Const.NVL(databaseMeta.getName(), ""));
    wConnectionType.setText(Const.NVL(databaseMeta.getPluginName(), ""));

    wUsername.setText(Const.NVL(databaseMeta.getUsername(), ""));
    wPassword.setText(Const.NVL(databaseMeta.getPassword(), ""));

    guiCompositeWidgets.setWidgetsContents(
        databaseMeta.getIDatabase(),
        wDatabaseSpecificComp,
        DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);

    wManualUrl.setText(Const.NVL(databaseMeta.getManualUrl(), ""));
    wSupportsBoolean.setSelection(databaseMeta.supportsBooleanDataType());
    wSupportsTimestamp.setSelection(databaseMeta.supportsTimestampDataType());
    wQuoteAll.setSelection(databaseMeta.isQuoteAllFields());
    wForceLowercase.setSelection(databaseMeta.isForcingIdentifiersToLowerCase());
    wForceUppercase.setSelection(databaseMeta.isForcingIdentifiersToUpperCase());
    wPreserveCase.setSelection(databaseMeta.preserveReservedCase());
    wPreferredSchema.setText(Const.NVL(databaseMeta.getPreferredSchemaName(), ""));
    wSqlStatements.setText(Const.NVL(databaseMeta.getConnectSql(), ""));

    wOptions.clearAll(false);
    Map<String, String> optionsMap = databaseMeta.getExtraOptionsMap();
    List<String> options = new ArrayList<>(optionsMap.keySet());
    Collections.sort(options);
    for (String option : options) {
      String value = optionsMap.get(option);
      TableItem item = new TableItem(wOptions.table, SWT.NONE);
      item.setText(1, Const.NVL(option, ""));
      item.setText(2, Const.NVL(value, ""));
    }
    wOptions.removeEmptyRows();
    wOptions.setRowNums();
    wOptions.optWidth(true);

    enableFields();
  }

  @Override
  public void getWidgetsContent(DatabaseMeta meta) {

    meta.setName(wName.getText());
    meta.setDatabaseType(wConnectionType.getText());

    // Get the database specific information
    //
    guiCompositeWidgets.getWidgetsContents(
        meta.getIDatabase(), DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);

    meta.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE);
    meta.setManualUrl(wManualUrl.getText());
    meta.setUsername(wUsername.getText());
    meta.setPassword(wPassword.getText());
    meta.setSupportsBooleanDataType(wSupportsBoolean.getSelection());
    meta.setSupportsTimestampDataType(wSupportsTimestamp.getSelection());
    meta.setQuoteAllFields(wQuoteAll.getSelection());
    meta.setForcingIdentifiersToLowerCase(wForceLowercase.getSelection());
    meta.setForcingIdentifiersToUpperCase(wForceUppercase.getSelection());
    meta.setPreserveReservedCase(wPreserveCase.getSelection());
    meta.setPreferredSchemaName(wPreferredSchema.getText());
    meta.setConnectSql(wSqlStatements.getText());

    meta.getExtraOptions().clear();
    for (int i = 0; i < wOptions.nrNonEmpty(); i++) {
      TableItem item = wOptions.getNonEmpty(i);
      String option = item.getText(1);
      String value = item.getText(2);
      meta.addExtraOption(meta.getPluginId(), option, value);
    }
  }

  /** Test the database connection */
  public static final void testConnection(
      Shell shell, IVariables variables, DatabaseMeta databaseMeta) {
    String[] remarks = databaseMeta.checkParameters();
    if (remarks.length == 0) {
      // Get a "test" report from this database
      DatabaseTestResults databaseTestResults = databaseMeta.testConnectionSuccess(variables);
      String message = databaseTestResults.getMessage();
      boolean success = databaseTestResults.isSuccess();
      String title =
          success
              ? BaseMessages.getString(PKG, "DatabaseDialog.DatabaseConnectionTestSuccess.title")
              : BaseMessages.getString(PKG, "DatabaseDialog.DatabaseConnectionTest.title");
      if (success && message.contains(Const.CR)) {
        message =
            message.substring(0, message.indexOf(Const.CR))
                + Const.CR
                + message.substring(message.indexOf(Const.CR));
        message = message.substring(0, message.lastIndexOf(Const.CR));
      }
      ShowMessageDialog msgDialog =
          new ShowMessageDialog(
              shell, SWT.ICON_INFORMATION | SWT.OK, title, message, message.length() > 300);
      msgDialog.setType(
          success
              ? Const.SHOW_MESSAGE_DIALOG_DB_TEST_SUCCESS
              : Const.SHOW_MESSAGE_DIALOG_DB_TEST_DEFAULT);
      msgDialog.open();
    } else {
      String message = "";
      for (int i = 0; i < remarks.length; i++) {
        message += "    * " + remarks[i] + Const.CR;
      }

      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "DatabaseDialog.ErrorParameters2.title"));
      mb.setMessage(
          BaseMessages.getString(PKG, "DatabaseDialog.ErrorParameters2.description", message));
      mb.open();
    }
  }

  private String[] getConnectionTypes() {
    PluginRegistry registry = PluginRegistry.getInstance();
    List<IPlugin> plugins = registry.getPlugins(DatabasePluginType.class);
    String[] types = new String[plugins.size()];
    for (int i = 0; i < types.length; i++) {
      types[i] = plugins.get(i).getName();
    }
    Arrays.sort(types, String.CASE_INSENSITIVE_ORDER);
    return types;
  }

  @Override
  public Button[] createButtonsForButtonBar(Composite parent) {
    Button wExplore = new Button(parent, SWT.PUSH);
    wExplore.setText(BaseMessages.getString(PKG, "DatabaseDialog.button.Explore"));
    wExplore.addListener(SWT.Selection, e -> explore());

    Button wTest = new Button(parent, SWT.PUSH);
    wTest.setText(BaseMessages.getString(PKG, "System.Button.Test"));
    wTest.addListener(SWT.Selection, e -> test());

    return new Button[] {wExplore, wTest};
  }

  @Override
  public boolean setFocus() {
    if (wName == null || wName.isDisposed()) {
      return false;
    }
    return wName.setFocus();
  }
}
