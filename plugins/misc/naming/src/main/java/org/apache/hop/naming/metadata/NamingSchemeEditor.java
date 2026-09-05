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

package org.apache.hop.naming.metadata;

import org.apache.hop.core.Const;
import org.apache.hop.core.naming.NamingSchemeKinds;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.naming.engine.NamingEngine;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

public class NamingSchemeEditor extends MetadataEditor<NamingScheme> {

  private static final Class<?> PKG = NamingScheme.class;
  private static final String PREVIEW_USER_EDITED = "userEdited";

  private TextVar wName;
  private GuiCompositeWidgets widgets;
  private Composite widgetsParent;
  private Text wPreviewInput;
  private Text wPreviewOutput;

  public NamingSchemeEditor(
      HopGui hopGui, MetadataManager<NamingScheme> manager, NamingScheme metadata) {
    super(hopGui, manager, metadata);
  }

  @Override
  public void createControl(Composite parent) {
    PropsUi props = PropsUi.getInstance();
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin() + 2;

    wName =
        createNameField(
            parent, BaseMessages.getString(PKG, "NamingSchemeEditor.Name.Label"), middle, margin);
    wName.addModifyListener(e -> setChanged());

    widgets = new GuiCompositeWidgets(hopGui.getVariables());
    widgets.registerExtraGroup(
        BaseMessages.getString(PKG, "NamingSchemeEditor.Group.Preview"),
        "20",
        null,
        this::addPreview);
    widgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {
            updatePreview();
          }

          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            setChanged();
            if (NamingScheme.WIDGET_TYPE.equals(widgetId)) {
              maybeResetPreviewSample();
            }
            updatePreview();
          }
        });
    widgetsParent = parent;
    widgets.createCompositeWidgets(
        getMetadata(), null, parent, NamingScheme.GUI_PLUGIN_ELEMENT_PARENT_ID, wName);
    setWidgetsContent();
  }

  private void addPreview(Composite parent) {
    PropsUi props = PropsUi.getInstance();
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    Label wlPreviewInput = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlPreviewInput);
    wlPreviewInput.setText(BaseMessages.getString(PKG, "NamingSchemeEditor.PreviewInput.Label"));
    FormData fdlPreviewInput = new FormData();
    fdlPreviewInput.top = new FormAttachment(0, margin);
    fdlPreviewInput.left = new FormAttachment(0, 0);
    fdlPreviewInput.right = new FormAttachment(middle, -margin);
    wlPreviewInput.setLayoutData(fdlPreviewInput);
    wPreviewInput = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPreviewInput);
    wPreviewInput.setText(defaultPreviewSample(getMetadata().getType()));
    FormData fdPreviewInput = new FormData();
    fdPreviewInput.top = new FormAttachment(wlPreviewInput, 0, SWT.CENTER);
    fdPreviewInput.left = new FormAttachment(middle, 0);
    fdPreviewInput.right = new FormAttachment(100, 0);
    wPreviewInput.setLayoutData(fdPreviewInput);
    wPreviewInput.addModifyListener(
        e -> {
          wPreviewInput.setData(PREVIEW_USER_EDITED, Boolean.TRUE);
          updatePreview();
        });

    Label wlPreviewOutput = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlPreviewOutput);
    wlPreviewOutput.setText(BaseMessages.getString(PKG, "NamingSchemeEditor.PreviewOutput.Label"));
    FormData fdlPreviewOutput = new FormData();
    fdlPreviewOutput.top = new FormAttachment(wPreviewInput, margin);
    fdlPreviewOutput.left = new FormAttachment(0, 0);
    fdlPreviewOutput.right = new FormAttachment(middle, -margin);
    wlPreviewOutput.setLayoutData(fdlPreviewOutput);
    wPreviewOutput = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wPreviewOutput);
    FormData fdPreviewOutput = new FormData();
    fdPreviewOutput.top = new FormAttachment(wlPreviewOutput, 0, SWT.CENTER);
    fdPreviewOutput.left = new FormAttachment(middle, 0);
    fdPreviewOutput.right = new FormAttachment(100, 0);
    wPreviewOutput.setLayoutData(fdPreviewOutput);
  }

  private void maybeResetPreviewSample() {
    if (wPreviewInput == null || wPreviewInput.isDisposed()) {
      return;
    }
    if (Boolean.TRUE.equals(wPreviewInput.getData(PREVIEW_USER_EDITED))) {
      return;
    }
    wPreviewInput.setText(defaultPreviewSample(currentKindCode()));
    wPreviewInput.setData(PREVIEW_USER_EDITED, Boolean.FALSE);
  }

  private static String defaultPreviewSample(String kind) {
    if (NamingSchemeKinds.isFile(kind)) {
      return "/data/Order ID.csv";
    }
    if (NamingSchemeKinds.isFolder(kind)) {
      return "/data/My Folder";
    }
    return "Order ID";
  }

  private String currentKindCode() {
    String display = widgetText(NamingScheme.WIDGET_TYPE);
    if (display.isEmpty()) {
      return Const.NVL(getMetadata().getType(), NamingSchemeType.GENERAL.getCode());
    }
    return NamingSchemeType.codeFromDisplay(display);
  }

  private String widgetText(String widgetId) {
    if (widgets == null) {
      return "";
    }
    Control control = widgets.getWidgetsMap().get(widgetId);
    if (control instanceof Text text) {
      return text.getText();
    }
    if (control instanceof TextVar textVar) {
      return textVar.getText();
    }
    if (control instanceof Combo combo) {
      return combo.getText();
    }
    if (control instanceof ComboVar comboVar) {
      return comboVar.getText();
    }
    return "";
  }

  private void updatePreview() {
    if (wPreviewOutput == null || wPreviewOutput.isDisposed()) {
      return;
    }
    NamingScheme preview = new NamingScheme();
    fillSchemeFromWidgets(preview);
    String input = wPreviewInput != null ? wPreviewInput.getText() : "";
    wPreviewOutput.setText(Const.NVL(NamingEngine.apply(preview, input, preview.getType()), ""));
  }

  private void fillSchemeFromWidgets(NamingScheme scheme) {
    if (wName != null && !wName.isDisposed()) {
      scheme.setName(wName.getText());
    }
    if (widgets != null) {
      widgets.getWidgetsContents(scheme, NamingScheme.GUI_PLUGIN_ELEMENT_PARENT_ID);
      convertDisplaysToCodes(scheme);
    }
  }

  private static void convertDisplaysToCodes(NamingScheme scheme) {
    scheme.setType(NamingSchemeType.codeFromDisplay(scheme.getType()));
    scheme.setCaseStyle(NamingCaseStyle.fromLabel(scheme.getCaseStyle()).getCode());
    scheme.setWordSeparator(NamingWordSeparator.fromLabel(scheme.getWordSeparator()).getCode());
  }

  @Override
  public void setWidgetsContent() {
    NamingScheme scheme = getMetadata();
    if (wName != null && !wName.isDisposed()) {
      wName.setText(Const.NVL(scheme.getName(), ""));
    }
    if (widgets == null || widgetsParent == null) {
      return;
    }
    String type = scheme.getType();
    String caseStyle = scheme.getCaseStyle();
    String wordSeparator = scheme.getWordSeparator();
    scheme.setType(NamingSchemeType.displayFromCode(type));
    scheme.setCaseStyle(NamingCaseStyle.fromCode(caseStyle).getLabel());
    scheme.setWordSeparator(NamingWordSeparator.fromCode(wordSeparator).getLabel());
    try {
      widgets.setWidgetsContents(scheme, widgetsParent, NamingScheme.GUI_PLUGIN_ELEMENT_PARENT_ID);
    } finally {
      scheme.setType(type);
      scheme.setCaseStyle(caseStyle);
      scheme.setWordSeparator(wordSeparator);
    }
    updatePreview();
  }

  @Override
  public void getWidgetsContent(NamingScheme scheme) {
    fillSchemeFromWidgets(scheme);
  }

  @Override
  public boolean setFocus() {
    if (wName == null || wName.isDisposed()) {
      return false;
    }
    return wName.setFocus();
  }
}
