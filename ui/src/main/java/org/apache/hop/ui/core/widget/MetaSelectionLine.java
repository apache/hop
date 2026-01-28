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

package org.apache.hop.ui.core.widget;

import java.util.Collections;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.ToolbarFacade;
import org.apache.hop.ui.hopgui.perspective.metadata.MetadataPerspective;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.TraverseListener;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;

/**
 * The goal of this composite is to add a line on a dialog which contains: - A label (for example:
 * Database connection) - A Combo Variable selection (editable ComboBox, for example containing all
 * connection values in the MetaStore) - New and Edit buttons (The latter opens up a generic
 * Metadata editor)
 */
@GuiPlugin
public class MetaSelectionLine<T extends IHopMetadata> extends Composite {
  private static final Class<?> PKG = MetaSelectionLine.class;

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "MetaSelectionLine-Toolbar";
  public static final String TOOLBAR_ITEM_EDIT = "10010-metadata-edit";
  public static final String TOOLBAR_ITEM_NEW = "10020-metadata-new";
  public static final String TOOLBAR_ITEM_META = "10030-metadata-perspective";

  private Composite parentComposite;
  private IHopMetadataProvider metadataProvider;
  private IVariables variables;
  private MetadataManager<T> manager;

  private Class<T> managedClass;
  private PropsUi props;
  private final Label wLabel;
  private ComboVar wCombo = null;
  private final Control wToolBar;

  public MetaSelectionLine(
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      Class<T> managedClass,
      Composite parentComposite,
      int flags,
      String labelText,
      String toolTipText) {
    this(
        variables,
        metadataProvider,
        managedClass,
        parentComposite,
        flags,
        labelText,
        toolTipText,
        false);
  }

  public MetaSelectionLine(
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      Class<T> managedClass,
      Composite parentComposite,
      int flags,
      String labelText,
      String toolTipText,
      boolean leftAlignedLabel) {
    this(
        variables,
        metadataProvider,
        managedClass,
        parentComposite,
        flags,
        labelText,
        toolTipText,
        leftAlignedLabel,
        true);
  }

  public MetaSelectionLine(
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      Class<T> managedClass,
      Composite parentComposite,
      int flags,
      String labelText,
      String toolTipText,
      boolean leftAlignedLabel,
      boolean negativeMargin) {
    super(parentComposite, SWT.NONE);
    this.parentComposite = parentComposite;
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.managedClass = managedClass;
    this.props = PropsUi.getInstance();

    this.manager =
        new MetadataManager<>(
            variables, metadataProvider, managedClass, parentComposite.getShell());

    PropsUi.setLook(this);

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 0;
    formLayout.marginHeight = 0;
    formLayout.marginLeft = 0;
    formLayout.marginRight = 0;
    formLayout.marginTop = 0;
    formLayout.marginBottom = 0;
    this.setLayout(formLayout);

    int labelFlags;
    if (leftAlignedLabel) {
      labelFlags = SWT.NONE | SWT.SINGLE;
    } else {
      labelFlags = SWT.RIGHT | SWT.SINGLE;
    }
    wLabel = new Label(this, labelFlags);
    PropsUi.setLook(wLabel);
    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment(0, 0);
    if (!leftAlignedLabel) {
      fdLabel.right = new FormAttachment(middle, negativeMargin ? -margin : 0);
    }
    fdLabel.top = new FormAttachment(0, margin + (EnvironmentUtils.getInstance().isWeb() ? 3 : 0));
    wLabel.setLayoutData(fdLabel);
    if (labelText != null) {
      wLabel.setText(labelText);
    }
    wLabel.setToolTipText(toolTipText);
    wLabel.requestLayout(); // Avoid GTK error in log

    HopMetadata metadata = HopMetadataUtil.getHopMetadataAnnotation(managedClass);
    Image editImage =
        SwtSvgImageUtil.getImage(
            getDisplay(),
            managedClass.getClassLoader(),
            metadata.image(),
            (int) (ConstUi.SMALL_ICON_SIZE * props.getZoomFactor()),
            (int) (ConstUi.SMALL_ICON_SIZE * props.getZoomFactor()));
    super.addListener(SWT.Dispose, e -> editImage.dispose());

    // Toolbar for default actions
    //
    IToolbarContainer toolBarContainer =
        ToolbarFacade.createToolbarContainer(this, SWT.FLAT | SWT.HORIZONTAL);
    wToolBar = toolBarContainer.getControl();
    PropsUi.setLook(wToolBar, Props.WIDGET_STYLE_DEFAULT);
    FormData fdToolBar = new FormData();
    fdToolBar.right = new FormAttachment(100, 0);
    fdToolBar.top = new FormAttachment(wLabel, 0, SWT.CENTER);
    wToolBar.setLayoutData(fdToolBar);

    // Add more toolbar items from plugins.
    //
    GuiToolbarWidgets toolbarWidgets = new GuiToolbarWidgets();
    toolbarWidgets.registerGuiPluginObject(this);
    toolbarWidgets.createToolbarWidgets(toolBarContainer, GUI_PLUGIN_TOOLBAR_PARENT_ID);

    int textFlags = SWT.SINGLE | SWT.LEFT | SWT.BORDER;
    if (flags != SWT.NONE) {
      textFlags = flags;
    }
    wCombo = new ComboVar(this.variables, this, textFlags, toolTipText);
    FormData fdCombo = new FormData();
    if (leftAlignedLabel) {
      if (labelText == null) {
        fdCombo.left = new FormAttachment(0, 0);
      } else {
        fdCombo.left = new FormAttachment(wLabel, margin, SWT.RIGHT);
      }
    } else {
      fdCombo.left = new FormAttachment(middle, 0);
    }
    fdCombo.right = new FormAttachment(wToolBar, -margin);
    fdCombo.top = new FormAttachment(wLabel, 0, SWT.CENTER);
    wCombo.setLayoutData(fdCombo);
    wCombo.setToolTipText(toolTipText);

    PropsUi.setLook(wCombo);

    layout(true, true);
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_EDIT,
      toolTip = "i18n::MetadataElement.Edit.Tooltip",
      imageMethod = "getEditIcon")
  public void editMetadataElement() {
    if (Utils.isEmpty(wCombo.getText())) this.newMetadata();
    else this.editMetadata();
  }

  public static String getEditIcon(Object guiPluginObject) {
    MetaSelectionLine<?> line = (MetaSelectionLine<?>) guiPluginObject;
    return line.getManagedClass().getAnnotation(HopMetadata.class).image();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_NEW,
      toolTip = "i18n::MetadataElement.New.Tooltip",
      image = "ui/images/new.svg")
  public void newMetadataElement() {
    T element = newMetadata();
    if (element != null) {
      wCombo.setText(Const.NVL(element.getName(), ""));
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_META,
      toolTip = "i18n::MetadataElement.View.Tooltip",
      image = "ui/images/metadata.svg")
  public void viewInPerspective() {
    MetadataPerspective perspective = HopGui.getMetadataPerspective();
    perspective.activate();
    String elementName = variables.resolve(wCombo.getText());
    if (StringUtils.isEmpty(elementName)) {
      // Visit the type of metadata
      //
      perspective.goToType(managedClass);
    } else {
      // Open the element in the perspective
      //
      perspective.goToElement(managedClass, elementName);
    }
    // Leave the current dialog in which the line is used open.
  }

  private String getMetadataDescription() {
    return managedClass.getAnnotation(HopMetadata.class).name();
  }

  protected void manageMetadata() {
    // Do nothing
  }

  /**
   * We look at the managed class name, add Dialog to it and then simply us that class to edit the
   * dialog.
   */
  protected boolean editMetadata() {
    String selected = wCombo.getText();
    if (StringUtils.isEmpty(selected)) {
      return false;
    }

    return manager.editMetadata(selected);
  }

  private T newMetadata() {
    T element = manager.newMetadata();
    if (element != null) {
      try {
        fillItems();
        getComboWidget().setText(Const.NVL(element.getName(), ""));
      } catch (Exception e) {
        LogChannel.UI.logError("Error updating list of element names from the metadata", e);
      }
    }
    return element;
  }

  /**
   * Look up the object names from the metadata and populate the items in the combobox with it.
   *
   * @throws HopException In case something went horribly wrong.
   */
  public void fillItems() throws HopException {
    List<String> elementNames = manager.getSerializer().listObjectNames();
    Collections.sort(elementNames);
    wCombo.setItems(elementNames.toArray(new String[0]));
  }

  /**
   * Load the selected element and return it. In case of errors, log them to LogChannel.UI
   *
   * @return The selected element or null if it doesn't exist or there was an error
   */
  public T loadSelectedElement() {
    String selectedItem = wCombo.getText();
    if (StringUtils.isEmpty(selectedItem)) {
      return null;
    }

    try {
      return manager.loadElement(selectedItem);
    } catch (Exception e) {
      LogChannel.UI.logError("Error loading element '" + selectedItem + "'", e);
      return null;
    }
  }

  /**
   * Adds the connection line for the given parent and previous control, and returns a meta
   * selection manager control
   *
   * @param parent the parent composite object
   * @param previous the previous control
   * @param selected
   * @param lsMod
   */
  public void addToConnectionLine(
      Composite parent, Control previous, T selected, ModifyListener lsMod) {

    try {
      fillItems();
    } catch (Exception e) {
      LogChannel.UI.logError("Error getting list of element names from the metadata", e);
    }
    if (lsMod != null) {
      addModifyListener(lsMod);
    }

    // Set a default value if there is only 1 connection in the list and nothing else is previously
    // selected...
    //
    if (selected == null) {
      if (getItemCount() == 1) {
        select(0);
      }
    } else {
      // Just set the value
      //
      setText(Const.NVL(selected.getName(), ""));
    }

    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment(0, 0);
    fdConnection.right = new FormAttachment(100, 0);
    if (previous != null) {
      fdConnection.top = new FormAttachment(previous, PropsUi.getMargin());
    } else {
      fdConnection.top = new FormAttachment(0, PropsUi.getMargin());
    }
    setLayoutData(fdConnection);
  }

  @Override
  public void addListener(int eventTYpe, Listener listener) {
    wCombo.addListener(eventTYpe, listener);
  }

  public void addModifyListener(ModifyListener lsMod) {
    wCombo.addModifyListener(lsMod);
  }

  public void addSelectionListener(SelectionListener lsDef) {
    wCombo.addSelectionListener(lsDef);
  }

  public void setText(String name) {
    wCombo.setText(name);
  }

  public String getText() {
    return wCombo.getText();
  }

  public void setItems(String[] items) {
    wCombo.setItems(items);
  }

  public void add(String item) {
    wCombo.add(item);
  }

  public String[] getItems() {
    return wCombo.getItems();
  }

  public int getItemCount() {
    return wCombo.getItemCount();
  }

  public void removeAll() {
    wCombo.removeAll();
  }

  public void remove(int index) {
    wCombo.remove(index);
  }

  public void select(int index) {
    wCombo.select(index);
  }

  public int getSelectionIndex() {
    return wCombo.getSelectionIndex();
  }

  @Override
  public void setEnabled(boolean flag) {
    wLabel.setEnabled(flag);
    wCombo.setEnabled(flag);
    wToolBar.setEnabled(flag);
  }

  @Override
  public boolean setFocus() {
    return wCombo.setFocus();
  }

  @Override
  public void addTraverseListener(TraverseListener tl) {
    wCombo.addTraverseListener(tl);
  }

  public CCombo getComboWidget() {
    return wCombo.getCComboWidget();
  }

  public Label getLabelWidget() {
    return wLabel;
  }

  /**
   * Gets metadataProvider
   *
   * @return value of metadataProvider
   */
  public IHopMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  /**
   * Gets variables
   *
   * @return value of variables
   */
  public IVariables getSpace() {
    return variables;
  }

  /**
   * Gets managedClass
   *
   * @return value of managedClass
   */
  public Class<T> getManagedClass() {
    return managedClass;
  }

  /**
   * Gets variables
   *
   * @return value of variables
   */
  public IVariables getVariables() {
    return variables;
  }

  /**
   * Gets manager
   *
   * @return value of manager
   */
  public MetadataManager<T> getManager() {
    return manager;
  }

  /**
   * Gets wLabel
   *
   * @return value of wLabel
   */
  public Label getwLabel() {
    return wLabel;
  }

  /**
   * Gets wCombo
   *
   * @return value of wCombo
   */
  public ComboVar getwCombo() {
    return wCombo;
  }

  /**
   * Gets wToolBar (the toolbar control; on desktop a ToolBar, on web a Composite with RowLayout).
   *
   * @return value of wToolBar
   */
  public Control getwToolBar() {
    return wToolBar;
  }
}
