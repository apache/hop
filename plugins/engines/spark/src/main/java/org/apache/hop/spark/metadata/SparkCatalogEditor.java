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

package org.apache.hop.spark.metadata;

import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

@GuiPlugin(description = "Editor for Spark catalog metadata")
public class SparkCatalogEditor extends MetadataEditor<SparkCatalog> {

  private static final Class<?> PKG = SparkCatalog.class;

  public static final String GUI_WIDGETS_PARENT_ID = "SparkCatalogEditor-GuiWidgetsParent";

  private Text wName;
  private Composite wWidgetsComposite;
  private GuiCompositeWidgets guiCompositeWidgets;

  public SparkCatalogEditor(
      HopGui hopGui, MetadataManager<SparkCatalog> manager, SparkCatalog metadata) {
    super(hopGui, manager, metadata);
  }

  @Override
  public void createControl(Composite parent) {
    PropsUi props = PropsUi.getInstance();
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin() + 2;

    Label wIcon = new Label(parent, SWT.RIGHT);
    wIcon.setImage(getImage());
    FormData fdlIcon = new FormData();
    fdlIcon.top = new FormAttachment(0, 0);
    fdlIcon.right = new FormAttachment(100, 0);
    wIcon.setLayoutData(fdlIcon);
    PropsUi.setLook(wIcon);

    Label wlName = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "SparkCatalog.Name.Label"));
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(0, margin);
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.left = new FormAttachment(middle, 0);
    fdName.right = new FormAttachment(wIcon, -margin);
    wName.setLayoutData(fdName);
    Control lastControl = wName;

    wWidgetsComposite = new Composite(parent, SWT.NONE);
    PropsUi.setLook(wWidgetsComposite);
    wWidgetsComposite.setLayout(new FormLayout());
    FormData fdWidgetsComposite = new FormData();
    fdWidgetsComposite.top = new FormAttachment(lastControl, margin);
    fdWidgetsComposite.left = new FormAttachment(0, 0);
    fdWidgetsComposite.right = new FormAttachment(100, 0);
    fdWidgetsComposite.bottom = new FormAttachment(100, 0);
    wWidgetsComposite.setLayoutData(fdWidgetsComposite);

    guiCompositeWidgets = new GuiCompositeWidgets(manager.getVariables());
    guiCompositeWidgets.createCompositeWidgets(
        metadata, null, wWidgetsComposite, GUI_WIDGETS_PARENT_ID, lastControl);
    guiCompositeWidgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            setChanged();
          }
        });

    setWidgetsContent();
    resetChanged();
    wName.addModifyListener(e -> setChanged());
  }

  @Override
  public void setWidgetsContent() {
    SparkCatalog meta = this.getMetadata();
    wName.setText(Const.NVL(meta.getName(), ""));
    guiCompositeWidgets.setWidgetsContents(metadata, wWidgetsComposite, GUI_WIDGETS_PARENT_ID);
  }

  @Override
  public void getWidgetsContent(SparkCatalog meta) {
    meta.setName(wName.getText());
    guiCompositeWidgets.getWidgetsContents(metadata, GUI_WIDGETS_PARENT_ID);
  }

  @Override
  public boolean setFocus() {
    if (wName == null || wName.isDisposed()) {
      return false;
    }
    return wName.setFocus();
  }
}
