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

package org.apache.hop.pipeline.transforms.maskfields;

import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;

/** Editor for a {@link MaskingPattern}. */
public class MaskingPatternEditor extends MetadataEditor<MaskingPattern> {

  private static final Class<?> PKG = MaskingPattern.class;

  private GuiCompositeWidgets widgets;
  private ScrolledComposite wScrolled;
  private Composite wContent;
  private TextVar wName;

  public MaskingPatternEditor(
      HopGui hopGui, MetadataManager<MaskingPattern> manager, MaskingPattern metadata) {
    super(hopGui, manager, metadata);
  }

  @Override
  public void createControl(Composite parent) {
    PropsUi props = PropsUi.getInstance();
    int margin = PropsUi.getMargin();
    int middle = props.getMiddlePct();

    wName =
        createNameField(
            parent,
            BaseMessages.getString(PKG, "MaskingPatternEditor.Name.Label"),
            middle,
            margin * 2);

    wScrolled = new ScrolledComposite(parent, SWT.V_SCROLL);
    FormData fdScrolled = new FormData();
    fdScrolled.left = new FormAttachment(0, 0);
    fdScrolled.right = new FormAttachment(100, 0);
    fdScrolled.top = new FormAttachment(wName, 15);
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
        getMetadata(), null, wContent, MaskingPattern.GUI_WIDGETS_PARENT_ID, null);
    wScrolled.addListener(SWT.Resize, e -> relayoutScrolledContent());

    setWidgetsContent();
    wName.addListener(SWT.Modify, e -> setChanged());
    widgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            setChanged();
            updateVisibility();
          }
        });
  }

  @Override
  public void setWidgetsContent() {
    MaskingPattern meta = getMetadata();
    wName.setText(Const.NVL(meta.getName(), ""));
    widgets.setWidgetsContents(meta, wContent, MaskingPattern.GUI_WIDGETS_PARENT_ID);
    updateVisibility();
  }

  @Override
  public void getWidgetsContent(MaskingPattern meta) {
    meta.setName(wName.getText());
    widgets.getWidgetsContents(meta, MaskingPattern.GUI_WIDGETS_PARENT_ID);
  }

  private void updateVisibility() {
    if (widgets == null) {
      return;
    }
    MaskingValueSource source =
        readEnum(MaskingPattern.WIDGET_VALUE_SOURCE, MaskingValueSource.class);
    if (source == null) {
      source = MaskingValueSource.SYNTHETIC;
    }
    MaskingToken token = readEnum(MaskingPattern.WIDGET_TOKEN, MaskingToken.class);
    if (token == null) {
      token = MaskingToken.SEQUENCE;
    }
    MaskingStorage storage = readEnum(MaskingPattern.WIDGET_STORAGE, MaskingStorage.class);
    if (storage == null) {
      storage = MaskingStorage.NONE;
    }

    Set<String> hidden = new HashSet<>();
    boolean synthetic = source == MaskingValueSource.SYNTHETIC;
    boolean removal =
        source == MaskingValueSource.SET_NULL || source == MaskingValueSource.SET_EMPTY;
    if (!synthetic) {
      hidden.add(MaskingPattern.WIDGET_TOKEN);
      hidden.add(MaskingPattern.WIDGET_PREFIX);
      hidden.add(MaskingPattern.WIDGET_SUFFIX);
      hidden.add(MaskingPattern.WIDGET_SEQUENCE_START);
    } else if (token != MaskingToken.SEQUENCE) {
      hidden.add(MaskingPattern.WIDGET_SEQUENCE_START);
    }
    if (removal) {
      hidden.add(MaskingPattern.WIDGET_STORAGE);
      hidden.add(MaskingPattern.WIDGET_CONNECTION);
      hidden.add(MaskingPattern.WIDGET_SCHEMA);
      hidden.add(MaskingPattern.WIDGET_TABLE);
    } else if (storage != MaskingStorage.DATABASE) {
      hidden.add(MaskingPattern.WIDGET_CONNECTION);
      hidden.add(MaskingPattern.WIDGET_SCHEMA);
      hidden.add(MaskingPattern.WIDGET_TABLE);
    }
    widgets.setWidgetsHidden(getMetadata(), hidden);
    relayoutScrolledContent();
  }

  private <E extends Enum<E>> E readEnum(String widgetId, Class<E> type) {
    Control control = widgets.getWidgetsMap().get(widgetId);
    String text = null;
    if (control instanceof Combo combo) {
      text = combo.getText();
    } else if (control instanceof ComboVar comboVar) {
      text = comboVar.getText();
    }
    if (StringUtils.isNotEmpty(text)) {
      if (IEnumHasCodeAndDescription.class.isAssignableFrom(type)) {
        @SuppressWarnings("unchecked")
        Class<? extends IEnumHasCodeAndDescription> coded =
            (Class<? extends IEnumHasCodeAndDescription>) type;
        @SuppressWarnings("unchecked")
        E byDescription = (E) IEnumHasCodeAndDescription.lookupDescription(coded, text, null);
        if (byDescription != null) {
          return byDescription;
        }
      }
      try {
        return Enum.valueOf(type, text);
      } catch (IllegalArgumentException e) {
        return null;
      }
    }
    return null;
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
}
