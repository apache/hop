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

package org.apache.hop.ui.datacatalog;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.datacatalog.CatalogAnswer;
import org.apache.hop.datacatalog.DataCatalog;
import org.apache.hop.datacatalog.IDataCatalog;
import org.apache.hop.datacatalog.plugin.DataCatalogPluginType;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.NamingSchemeTypes;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Text;

/**
 * The editor of a {@link DataCatalog}: the catalog type, its own options, and a Test button that
 * works for every type. Found by its name, so it stays in the package that mirrors {@link
 * DataCatalog}'s.
 */
@GuiPlugin(description = "Editor for data catalog metadata")
public class DataCatalogEditor extends MetadataEditor<DataCatalog> {
  private static final Class<?> PKG = DataCatalogEditor.class;

  private final DataCatalog dataCatalog;
  private final DataCatalog workingCatalog;

  private TextVar wName;
  private Text wDescription;
  private ComboVar wPluginType;

  private Composite wPluginSpecificComp;
  private GuiCompositeWidgets guiCompositeWidgets;

  /** A catalog of every type, by plugin name, so switching back and forth keeps what was typed. */
  private final Map<String, IDataCatalog> catalogs;

  private final AtomicBoolean busyChangingPluginType = new AtomicBoolean(false);

  public DataCatalogEditor(
      HopGui hopGui, MetadataManager<DataCatalog> manager, DataCatalog dataCatalog) {
    super(hopGui, manager, dataCatalog);
    this.dataCatalog = dataCatalog;
    this.workingCatalog = new DataCatalog(dataCatalog);
    this.catalogs = newCatalogs();
    if (workingCatalog.getCatalog() != null) {
      catalogs.put(workingCatalog.getCatalog().getPluginName(), workingCatalog.getCatalog());
    }
  }

  private static Map<String, IDataCatalog> newCatalogs() {
    Map<String, IDataCatalog> map = new HashMap<>();
    PluginRegistry registry = PluginRegistry.getInstance();
    for (IPlugin plugin : registry.getPlugins(DataCatalogPluginType.class)) {
      try {
        IDataCatalog catalog = registry.loadClass(plugin, IDataCatalog.class);
        catalog.setPluginId(plugin.getIds()[0]);
        catalog.setPluginName(plugin.getName());
        map.put(plugin.getName(), catalog);
      } catch (Exception e) {
        HopGui.getInstance().getLog().logError("Error instantiating data catalog plugin", e);
      }
    }
    return map;
  }

  @Override
  public void createControl(Composite parent) {
    PropsUi props = PropsUi.getInstance();
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    ScrolledComposite wMainSComp = new ScrolledComposite(parent, SWT.V_SCROLL | SWT.H_SCROLL);
    wMainSComp.setLayout(new FillLayout());

    Composite wMainComp = new Composite(wMainSComp, SWT.NONE);
    PropsUi.setLook(wMainComp);
    FormLayout mainLayout = new FormLayout();
    mainLayout.marginWidth = 3;
    mainLayout.marginHeight = 3;
    wMainComp.setLayout(mainLayout);

    Label wlName = new Label(wMainComp, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "DataCatalogEditor.Name.Label"));
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(0, margin * 2);
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, 0);
    wlName.setLayoutData(fdlName);
    wName =
        new TextVar(hopGui.getVariables(), wMainComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER)
            .asNameField(NamingSchemeTypes.HOP_METADATA);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.left = new FormAttachment(middle, margin);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);

    Label wlDescription = new Label(wMainComp, SWT.RIGHT);
    PropsUi.setLook(wlDescription);
    wlDescription.setText(BaseMessages.getString(PKG, "DataCatalogEditor.Description.Label"));
    FormData fdlDescription = new FormData();
    fdlDescription.top = new FormAttachment(wName, margin * 2);
    fdlDescription.left = new FormAttachment(0, 0);
    fdlDescription.right = new FormAttachment(middle, 0);
    wlDescription.setLayoutData(fdlDescription);
    wDescription = new Text(wMainComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDescription);
    FormData fdDescription = new FormData();
    fdDescription.top = new FormAttachment(wlDescription, 0, SWT.CENTER);
    fdDescription.left = new FormAttachment(middle, margin);
    fdDescription.right = new FormAttachment(100, 0);
    wDescription.setLayoutData(fdDescription);

    Label wlPluginType = new Label(wMainComp, SWT.RIGHT);
    PropsUi.setLook(wlPluginType);
    wlPluginType.setText(BaseMessages.getString(PKG, "DataCatalogEditor.Type.Label"));
    FormData fdlPluginType = new FormData();
    fdlPluginType.top = new FormAttachment(wDescription, margin * 2);
    fdlPluginType.left = new FormAttachment(0, 0);
    fdlPluginType.right = new FormAttachment(middle, 0);
    wlPluginType.setLayoutData(fdlPluginType);
    wPluginType =
        new ComboVar(manager.getVariables(), wMainComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPluginType);
    wPluginType.setItems(getPluginTypes());
    FormData fdPluginType = new FormData();
    fdPluginType.top = new FormAttachment(wlPluginType, 0, SWT.CENTER);
    fdPluginType.left = new FormAttachment(middle, margin);
    fdPluginType.right = new FormAttachment(100, 0);
    wPluginType.setLayoutData(fdPluginType);

    wPluginSpecificComp = new Composite(wMainComp, SWT.BACKGROUND);
    PropsUi.setLook(wPluginSpecificComp);
    wPluginSpecificComp.setLayout(new FormLayout());
    FormData fdPluginSpecificComp = new FormData();
    fdPluginSpecificComp.left = new FormAttachment(0, 0);
    fdPluginSpecificComp.right = new FormAttachment(100, 0);
    fdPluginSpecificComp.top = new FormAttachment(wPluginType, margin);
    fdPluginSpecificComp.bottom = new FormAttachment(100, 0);
    wPluginSpecificComp.setLayoutData(fdPluginSpecificComp);

    addGuiCompositeWidgets();

    FormData fdMainSComp = new FormData();
    fdMainSComp.top = new FormAttachment(0, 0);
    fdMainSComp.left = new FormAttachment(0, 0);
    fdMainSComp.right = new FormAttachment(95, 0);
    fdMainSComp.bottom = new FormAttachment(95, 0);
    wMainSComp.setLayoutData(fdMainSComp);

    FormData fdMainComp = new FormData();
    fdMainComp.left = new FormAttachment(0, 0);
    fdMainComp.top = new FormAttachment(0, 0);
    fdMainComp.right = new FormAttachment(100, 0);
    fdMainComp.bottom = new FormAttachment(100, 0);
    wMainComp.setLayoutData(fdMainComp);

    wMainComp.pack();
    Rectangle mainBounds = wMainComp.getBounds();
    wMainSComp.setContent(wMainComp);
    wMainSComp.setExpandHorizontal(true);
    wMainSComp.setExpandVertical(true);
    wMainSComp.setMinWidth(mainBounds.width);
    wMainSComp.setMinHeight(mainBounds.height);

    setWidgetsContent();
    resetChanged();

    Listener modifyListener = e -> setChanged();
    wName.addListener(SWT.Modify, modifyListener);
    wDescription.addListener(SWT.Modify, modifyListener);
    wPluginType.addListener(SWT.Modify, modifyListener);
    wPluginType.addListener(SWT.Modify, e -> changePluginType());
  }

  /** The test is the same question for every catalog type, so the editor has the button. */
  @Override
  public Button[] createButtonsForButtonBar(Composite parent) {
    Button wTest = new Button(parent, SWT.PUSH);
    wTest.setText(BaseMessages.getString(PKG, "System.Button.Test"));
    wTest.setToolTipText(BaseMessages.getString(PKG, "DataCatalogEditor.Test.Tooltip"));
    wTest.addListener(SWT.Selection, e -> test());
    return new Button[] {wTest};
  }

  private void addGuiCompositeWidgets() {
    for (Control child : wPluginSpecificComp.getChildren()) {
      child.dispose();
    }

    if (workingCatalog.getCatalog() != null) {
      guiCompositeWidgets = new GuiCompositeWidgets(manager.getVariables());
      guiCompositeWidgets.createCompositeWidgets(
          workingCatalog.getCatalog(),
          null,
          wPluginSpecificComp,
          DataCatalog.GUI_PLUGIN_ELEMENT_PARENT_ID,
          null);
      guiCompositeWidgets.setWidgetsListener(
          new GuiCompositeWidgetsAdapter() {
            @Override
            public void widgetModified(
                GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
              setChanged();
            }
          });
      guiCompositeWidgets.setCompositeButtonsListener(
          sourceObject ->
              guiCompositeWidgets.getWidgetsContents(
                  sourceObject, DataCatalog.GUI_PLUGIN_ELEMENT_PARENT_ID));
    }

    wPluginSpecificComp.layout(true, true);
  }

  private void changePluginType() {
    if (!busyChangingPluginType.compareAndSet(false, true)) {
      return;
    }
    try {
      getWidgetsContent(workingCatalog);
      if (workingCatalog.getCatalog() != null) {
        catalogs.put(workingCatalog.getCatalog().getPluginName(), workingCatalog.getCatalog());
      }
      applyPluginType(workingCatalog);
      addGuiCompositeWidgets();
      setWidgetsContent();
    } finally {
      busyChangingPluginType.set(false);
    }
  }

  /** Ask the catalog as it is on the screen, with the variables of the GUI. */
  private void test() {
    String title = BaseMessages.getString(PKG, "DataCatalogEditor.Test.Title");
    DataCatalog toTest = new DataCatalog();
    applyPluginType(toTest);
    getWidgetsContent(toTest);
    try (IDataCatalog catalog =
        toTest.connect(manager.getVariables(), manager.getMetadataProvider())) {
      CatalogAnswer<String> answer = catalog.ping();
      MessageBox box =
          new MessageBox(
              hopGui.getShell(),
              SWT.OK
                  | (answer instanceof CatalogAnswer.Known<String>
                      ? SWT.ICON_INFORMATION
                      : SWT.ICON_ERROR));
      box.setText(title);
      box.setMessage(describe(answer));
      box.open();
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          title,
          BaseMessages.getString(PKG, "DataCatalogEditor.Test.Error.Message"),
          e);
    }
  }

  private static String describe(CatalogAnswer<String> answer) {
    return switch (answer) {
      case CatalogAnswer.Known<String> known ->
          BaseMessages.getString(PKG, "DataCatalogEditor.Test.Success", known.value());
      case CatalogAnswer.NotFound<String> notFound ->
          BaseMessages.getString(PKG, "DataCatalogEditor.Test.NoAnswer");
      case CatalogAnswer.Unknown<String> unknown ->
          BaseMessages.getString(
              PKG,
              "DataCatalogEditor.Test.Failure",
              unknown.reason().name(),
              Const.NVL(unknown.detail(), ""));
    };
  }

  @Override
  public void save() throws HopException {
    applyPluginType(dataCatalog);
    super.save();
  }

  @Override
  public void setWidgetsContent() {
    wName.setText(Const.NVL(workingCatalog.getName(), ""));
    wDescription.setText(Const.NVL(workingCatalog.getDescription(), ""));
    if (workingCatalog.getCatalog() != null) {
      wPluginType.setText(Const.NVL(workingCatalog.getCatalog().getPluginName(), ""));
      guiCompositeWidgets.setWidgetsContents(
          workingCatalog.getCatalog(),
          wPluginSpecificComp,
          DataCatalog.GUI_PLUGIN_ELEMENT_PARENT_ID);
    } else {
      wPluginType.setText("");
    }
  }

  @Override
  public void getWidgetsContent(DataCatalog catalog) {
    catalog.setName(wName.getText());
    catalog.setDescription(wDescription.getText());
    if (catalog.getCatalog() != null
        && guiCompositeWidgets != null
        && !guiCompositeWidgets.getWidgetsMap().isEmpty()) {
      guiCompositeWidgets.getWidgetsContents(
          catalog.getCatalog(), DataCatalog.GUI_PLUGIN_ELEMENT_PARENT_ID);
    }
  }

  /** Give {@code catalog} the catalog of the type selected on the screen. */
  private void applyPluginType(DataCatalog catalog) {
    catalog.setCatalog(catalogs.get(wPluginType.getText()));
  }

  private static String[] getPluginTypes() {
    List<IPlugin> plugins = PluginRegistry.getInstance().getPlugins(DataCatalogPluginType.class);
    String[] types = new String[plugins.size()];
    for (int i = 0; i < types.length; i++) {
      types[i] = plugins.get(i).getName();
    }
    Arrays.sort(types, String.CASE_INSENSITIVE_ORDER);
    return types;
  }
}
