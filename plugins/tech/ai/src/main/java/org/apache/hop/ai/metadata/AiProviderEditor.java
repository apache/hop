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

package org.apache.hop.ai.metadata;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hop.ai.engine.AiModelCatalog;
import org.apache.hop.ai.provider.IAiProvider;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;

/** Metadata editor for {@link AiProvider}. */
public class AiProviderEditor extends MetadataEditor<AiProvider> {

  private static final Class<?> PKG = AiProvider.class;

  private Composite parent;
  private TextVar wName;
  private Combo wProviderType;
  private GuiCompositeWidgets widgets;
  private ScrolledComposite wScrolled;
  private Composite wContent;
  private final AtomicBoolean busyChangingType = new AtomicBoolean(false);

  public AiProviderEditor(HopGui hopGui, MetadataManager<AiProvider> manager, AiProvider metadata) {
    super(hopGui, manager, metadata);
  }

  @Override
  public void createControl(Composite parent) {
    this.parent = parent;
    PropsUi props = PropsUi.getInstance();
    int margin = PropsUi.getMargin();
    int middle = props.getMiddlePct();

    wName =
        createNameField(
            parent, BaseMessages.getString(PKG, "AiProviderEditor.Name.Label"), middle, margin * 2);

    Label wlType = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlType);
    wlType.setText(BaseMessages.getString(PKG, "AiProviderEditor.Type.Label"));
    FormData fdlType = new FormData();
    fdlType.top = new FormAttachment(wName, margin * 2);
    fdlType.left = new FormAttachment(0, 0);
    fdlType.right = new FormAttachment(middle, -margin);
    wlType.setLayoutData(fdlType);

    wProviderType = new Combo(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.READ_ONLY);
    wProviderType.setItems(AiProviderPlugins.names());
    PropsUi.setLook(wProviderType);
    FormData fdType = new FormData();
    fdType.top = new FormAttachment(wlType, 0, SWT.CENTER);
    fdType.left = new FormAttachment(middle, 0);
    fdType.right = new FormAttachment(100, 0);
    wProviderType.setLayoutData(fdType);

    wScrolled = new ScrolledComposite(parent, SWT.V_SCROLL);
    FormData fdScrolled = new FormData();
    fdScrolled.left = new FormAttachment(0, 0);
    fdScrolled.right = new FormAttachment(100, 0);
    fdScrolled.top = new FormAttachment(wProviderType, 15);
    fdScrolled.bottom = new FormAttachment(100, 0);
    wScrolled.setLayoutData(fdScrolled);
    wScrolled.setExpandHorizontal(true);
    wScrolled.setExpandVertical(true);

    wContent = new Composite(wScrolled, SWT.NONE);
    PropsUi.setLook(wContent);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 0;
    contentLayout.marginHeight = 0;
    wContent.setLayout(contentLayout);
    wScrolled.setContent(wContent);

    widgets = new GuiCompositeWidgets(manager.getVariables());
    widgets.createCompositeWidgets(
        getMetadata(), null, wContent, AiProvider.GUI_WIDGETS_PARENT_ID, null);

    wScrolled.addListener(SWT.Resize, e -> relayoutScrolledContent());

    setWidgetsContent();

    wName.addListener(SWT.Modify, e -> setChanged());
    wProviderType.addListener(SWT.Modify, e -> changeProviderType());
    widgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            setChanged();
          }
        });
  }

  private void changeProviderType() {
    if (busyChangingType.get()) {
      return;
    }
    busyChangingType.set(true);
    try {
      AiProvider meta = getMetadata();
      widgets.getWidgetsContents(meta, AiProvider.GUI_WIDGETS_PARENT_ID);
      String selected = wProviderType.getText();
      if (selected != null && !selected.isEmpty()) {
        meta.setProviderType(selected);
      }
      applyModelNameChoices(meta.getModelNameChoices(null, null));
      widgets.setWidgetsContents(meta, wContent, AiProvider.GUI_WIDGETS_PARENT_ID);
      updateVisibility();
      setChanged();
    } catch (HopException e) {
      new ErrorDialog(
          parent.getShell(),
          BaseMessages.getString(PKG, "AiProviderEditor.TypeError.Title"),
          BaseMessages.getString(PKG, "AiProviderEditor.TypeError.Message"),
          e);
    } finally {
      busyChangingType.set(false);
    }
  }

  private void updateVisibility() {
    if (widgets == null) {
      return;
    }
    IAiProvider provider = getMetadata().getProvider();
    Set<String> hidden = new HashSet<>();
    if (provider != null && !provider.requiresApiKey()) {
      hidden.add(AiProvider.WIDGET_API_KEY);
    }
    widgets.setWidgetsHidden(getMetadata(), hidden);
    relayoutScrolledContent();
  }

  private void relayoutScrolledContent() {
    if (wScrolled == null || wScrolled.isDisposed() || wContent == null || wContent.isDisposed()) {
      return;
    }
    wContent.layout(true, true);
    Rectangle client = wScrolled.getClientArea();
    int width = Math.max(client.width, 1);
    Point size = wContent.computeSize(width, SWT.DEFAULT);
    wScrolled.setMinWidth(width);
    wScrolled.setMinHeight(size.y);
    wContent.setSize(width, size.y);
  }

  @Override
  public void setWidgetsContent() {
    AiProvider meta = getMetadata();
    wName.setText(Const.NVL(meta.getName(), ""));
    if (meta.getProvider() == null && wProviderType.getItemCount() > 0) {
      try {
        meta.setProviderType(wProviderType.getItem(0));
      } catch (HopException e) {
        // Leave the combo empty; Test will explain.
      }
    }
    if (meta.getPluginName() != null) {
      wProviderType.setText(meta.getPluginName());
    }
    widgets.setWidgetsContents(meta, wContent, AiProvider.GUI_WIDGETS_PARENT_ID);
    updateVisibility();
  }

  @Override
  public void getWidgetsContent(AiProvider meta) {
    meta.setName(wName.getText());
    widgets.getWidgetsContents(meta, AiProvider.GUI_WIDGETS_PARENT_ID);
    String selected = wProviderType.getText();
    if (selected != null && !selected.isEmpty()) {
      try {
        if (meta.getProvider() == null || !selected.equals(meta.getPluginName())) {
          meta.setProviderType(selected);
        }
      } catch (HopException e) {
        throw new HopRuntimeException(e);
      }
    }
  }

  @Override
  public Button[] createButtonsForButtonBar(Composite composite) {
    Button wbRefresh = new Button(composite, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbRefresh);
    wbRefresh.setText(BaseMessages.getString(PKG, "AiProviderEditor.RefreshModels.Label"));
    wbRefresh.addListener(SWT.Selection, e -> refreshModels());
    Button wbTest = new Button(composite, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbTest);
    wbTest.setText(BaseMessages.getString(PKG, "AiProviderEditor.Test.Label"));
    wbTest.addListener(SWT.Selection, e -> test());
    return new Button[] {wbRefresh, wbTest};
  }

  public void refreshModels() {
    try {
      AiProvider meta = new AiProvider(getMetadata());
      getWidgetsContent(meta);
      List<String> names =
          new ArrayList<>(AiModelCatalog.listModelNames(meta, manager.getVariables()));
      String current = Const.NVL(meta.getModelName(), "");
      if (!Utils.isEmpty(current) && names.stream().noneMatch(current::equals)) {
        names.add(0, current);
      }
      applyModelNameChoices(names);
      widgets.setWidgetsContents(meta, wContent, AiProvider.GUI_WIDGETS_PARENT_ID);
      MessageBox box = new MessageBox(parent.getShell(), SWT.ICON_INFORMATION | SWT.OK);
      box.setText(BaseMessages.getString(PKG, "AiProviderEditor.RefreshModels.Success.Title"));
      box.setMessage(
          BaseMessages.getString(
              PKG,
              "AiProviderEditor.RefreshModels.Success.Message",
              Integer.toString(names.size())));
      box.open();
    } catch (Exception e) {
      new ErrorDialog(
          parent.getShell(),
          BaseMessages.getString(PKG, "AiProviderEditor.RefreshModels.Error.Title"),
          BaseMessages.getString(PKG, "AiProviderEditor.RefreshModels.Error.Message"),
          e);
    }
  }

  private void applyModelNameChoices(List<String> names) {
    if (widgets == null || names == null) {
      return;
    }
    widgets.setComboValues(AiProvider.WIDGET_MODEL_NAME, names.toArray(String[]::new));
  }

  public void test() {
    try {
      AiProvider meta = new AiProvider(getMetadata());
      getWidgetsContent(meta);
      String message = meta.test(manager.getVariables());
      MessageBox box = new MessageBox(parent.getShell(), SWT.ICON_INFORMATION | SWT.OK);
      box.setText(BaseMessages.getString(PKG, "AiProviderEditor.Test.Success.Title"));
      box.setMessage(message);
      box.open();
    } catch (Exception e) {
      new ErrorDialog(
          parent.getShell(),
          BaseMessages.getString(PKG, "AiProviderEditor.Test.Error.Title"),
          BaseMessages.getString(PKG, "AiProviderEditor.Test.Error.Message"),
          e);
    }
  }
}
