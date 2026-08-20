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
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.naming.engine.NamingEngine;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

public class NamingSchemeEditor extends MetadataEditor<NamingScheme> {

  private static final Class<?> PKG = NamingScheme.class;

  private TextVar wName;
  private Text wDescription;
  private Combo wType;
  private Combo wCaseStyle;
  private Combo wWordSeparator;
  private Text wExtraDelimiters;
  private Button wRemoveSpecial;
  private Button wCollapseSeparators;
  private Button wTrimEdges;
  private Text wPrefix;
  private Text wSuffix;
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
    Control lastControl = wName;

    // Description
    Label wlDescription = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlDescription);
    wlDescription.setText(BaseMessages.getString(PKG, "NamingSchemeEditor.Description.Label"));
    FormData fdlDescription = new FormData();
    fdlDescription.top = new FormAttachment(lastControl, margin);
    fdlDescription.left = new FormAttachment(0, 0);
    fdlDescription.right = new FormAttachment(middle, -margin);
    wlDescription.setLayoutData(fdlDescription);
    wDescription = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDescription);
    FormData fdDescription = new FormData();
    fdDescription.top = new FormAttachment(wlDescription, 0, SWT.CENTER);
    fdDescription.left = new FormAttachment(middle, 0);
    fdDescription.right = new FormAttachment(100, 0);
    wDescription.setLayoutData(fdDescription);
    lastControl = wDescription;

    // Type
    Label wlType = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlType);
    wlType.setText(BaseMessages.getString(PKG, "NamingSchemeEditor.Type.Label"));
    FormData fdlType = new FormData();
    fdlType.top = new FormAttachment(lastControl, margin);
    fdlType.left = new FormAttachment(0, 0);
    fdlType.right = new FormAttachment(middle, -margin);
    wlType.setLayoutData(fdlType);
    wType = new Combo(parent, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER | SWT.LEFT);
    PropsUi.setLook(wType);
    wType.setItems(NamingSchemeType.getPluginLabels());
    FormData fdType = new FormData();
    fdType.top = new FormAttachment(wlType, 0, SWT.CENTER);
    fdType.left = new FormAttachment(middle, 0);
    fdType.right = new FormAttachment(100, 0);
    wType.setLayoutData(fdType);
    lastControl = wType;

    // Case style
    Label wlCaseStyle = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlCaseStyle);
    wlCaseStyle.setText(BaseMessages.getString(PKG, "NamingSchemeEditor.CaseStyle.Label"));
    FormData fdlCaseStyle = new FormData();
    fdlCaseStyle.top = new FormAttachment(lastControl, margin);
    fdlCaseStyle.left = new FormAttachment(0, 0);
    fdlCaseStyle.right = new FormAttachment(middle, -margin);
    wlCaseStyle.setLayoutData(fdlCaseStyle);
    wCaseStyle = new Combo(parent, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER | SWT.LEFT);
    PropsUi.setLook(wCaseStyle);
    wCaseStyle.setItems(NamingCaseStyle.getLabels());
    FormData fdCaseStyle = new FormData();
    fdCaseStyle.top = new FormAttachment(wlCaseStyle, 0, SWT.CENTER);
    fdCaseStyle.left = new FormAttachment(middle, 0);
    fdCaseStyle.right = new FormAttachment(100, 0);
    wCaseStyle.setLayoutData(fdCaseStyle);
    lastControl = wCaseStyle;

    // Word separator
    Label wlWordSeparator = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlWordSeparator);
    wlWordSeparator.setText(BaseMessages.getString(PKG, "NamingSchemeEditor.WordSeparator.Label"));
    FormData fdlWordSeparator = new FormData();
    fdlWordSeparator.top = new FormAttachment(lastControl, margin);
    fdlWordSeparator.left = new FormAttachment(0, 0);
    fdlWordSeparator.right = new FormAttachment(middle, -margin);
    wlWordSeparator.setLayoutData(fdlWordSeparator);
    wWordSeparator = new Combo(parent, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER | SWT.LEFT);
    PropsUi.setLook(wWordSeparator);
    wWordSeparator.setItems(NamingWordSeparator.getLabels());
    FormData fdWordSeparator = new FormData();
    fdWordSeparator.top = new FormAttachment(wlWordSeparator, 0, SWT.CENTER);
    fdWordSeparator.left = new FormAttachment(middle, 0);
    fdWordSeparator.right = new FormAttachment(100, 0);
    wWordSeparator.setLayoutData(fdWordSeparator);
    lastControl = wWordSeparator;

    // Extra delimiters
    Label wlExtra = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlExtra);
    wlExtra.setText(BaseMessages.getString(PKG, "NamingSchemeEditor.ExtraDelimiters.Label"));
    FormData fdlExtra = new FormData();
    fdlExtra.top = new FormAttachment(lastControl, margin);
    fdlExtra.left = new FormAttachment(0, 0);
    fdlExtra.right = new FormAttachment(middle, -margin);
    wlExtra.setLayoutData(fdlExtra);
    wExtraDelimiters = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wExtraDelimiters);
    FormData fdExtra = new FormData();
    fdExtra.top = new FormAttachment(wlExtra, 0, SWT.CENTER);
    fdExtra.left = new FormAttachment(middle, 0);
    fdExtra.right = new FormAttachment(100, 0);
    wExtraDelimiters.setLayoutData(fdExtra);
    lastControl = wExtraDelimiters;

    // Remove special characters
    Label wlRemoveSpecial = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlRemoveSpecial);
    wlRemoveSpecial.setText(
        BaseMessages.getString(PKG, "NamingSchemeEditor.RemoveSpecialCharacters.Label"));
    FormData fdlRemoveSpecial = new FormData();
    fdlRemoveSpecial.top = new FormAttachment(lastControl, margin);
    fdlRemoveSpecial.left = new FormAttachment(0, 0);
    fdlRemoveSpecial.right = new FormAttachment(middle, -margin);
    wlRemoveSpecial.setLayoutData(fdlRemoveSpecial);
    wRemoveSpecial = new Button(parent, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wRemoveSpecial);
    FormData fdRemoveSpecial = new FormData();
    fdRemoveSpecial.top = new FormAttachment(wlRemoveSpecial, 0, SWT.CENTER);
    fdRemoveSpecial.left = new FormAttachment(middle, 0);
    fdRemoveSpecial.right = new FormAttachment(100, 0);
    wRemoveSpecial.setLayoutData(fdRemoveSpecial);
    lastControl = wlRemoveSpecial;

    // Collapse separators
    Label wlCollapse = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlCollapse);
    wlCollapse.setText(BaseMessages.getString(PKG, "NamingSchemeEditor.CollapseSeparators.Label"));
    FormData fdlCollapse = new FormData();
    fdlCollapse.top = new FormAttachment(lastControl, margin);
    fdlCollapse.left = new FormAttachment(0, 0);
    fdlCollapse.right = new FormAttachment(middle, -margin);
    wlCollapse.setLayoutData(fdlCollapse);
    wCollapseSeparators = new Button(parent, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wCollapseSeparators);
    FormData fdCollapse = new FormData();
    fdCollapse.top = new FormAttachment(wlCollapse, 0, SWT.CENTER);
    fdCollapse.left = new FormAttachment(middle, 0);
    fdCollapse.right = new FormAttachment(100, 0);
    wCollapseSeparators.setLayoutData(fdCollapse);
    lastControl = wlCollapse;

    // Trim edge separators
    Label wlTrim = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlTrim);
    wlTrim.setText(BaseMessages.getString(PKG, "NamingSchemeEditor.TrimEdgeSeparators.Label"));
    FormData fdlTrim = new FormData();
    fdlTrim.top = new FormAttachment(lastControl, margin);
    fdlTrim.left = new FormAttachment(0, 0);
    fdlTrim.right = new FormAttachment(middle, -margin);
    wlTrim.setLayoutData(fdlTrim);
    wTrimEdges = new Button(parent, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wTrimEdges);
    FormData fdTrim = new FormData();
    fdTrim.top = new FormAttachment(wlTrim, 0, SWT.CENTER);
    fdTrim.left = new FormAttachment(middle, 0);
    fdTrim.right = new FormAttachment(100, 0);
    wTrimEdges.setLayoutData(fdTrim);
    lastControl = wlTrim;

    // Prefix
    Label wlPrefix = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlPrefix);
    wlPrefix.setText(BaseMessages.getString(PKG, "NamingSchemeEditor.Prefix.Label"));
    FormData fdlPrefix = new FormData();
    fdlPrefix.top = new FormAttachment(lastControl, margin);
    fdlPrefix.left = new FormAttachment(0, 0);
    fdlPrefix.right = new FormAttachment(middle, -margin);
    wlPrefix.setLayoutData(fdlPrefix);
    wPrefix = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPrefix);
    FormData fdPrefix = new FormData();
    fdPrefix.top = new FormAttachment(wlPrefix, 0, SWT.CENTER);
    fdPrefix.left = new FormAttachment(middle, 0);
    fdPrefix.right = new FormAttachment(100, 0);
    wPrefix.setLayoutData(fdPrefix);
    lastControl = wPrefix;

    // Suffix
    Label wlSuffix = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlSuffix);
    wlSuffix.setText(BaseMessages.getString(PKG, "NamingSchemeEditor.Suffix.Label"));
    FormData fdlSuffix = new FormData();
    fdlSuffix.top = new FormAttachment(lastControl, margin);
    fdlSuffix.left = new FormAttachment(0, 0);
    fdlSuffix.right = new FormAttachment(middle, -margin);
    wlSuffix.setLayoutData(fdlSuffix);
    wSuffix = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSuffix);
    FormData fdSuffix = new FormData();
    fdSuffix.top = new FormAttachment(wlSuffix, 0, SWT.CENTER);
    fdSuffix.left = new FormAttachment(middle, 0);
    fdSuffix.right = new FormAttachment(100, 0);
    wSuffix.setLayoutData(fdSuffix);
    lastControl = wSuffix;

    // Preview input
    Label wlPreviewInput = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlPreviewInput);
    wlPreviewInput.setText(BaseMessages.getString(PKG, "NamingSchemeEditor.PreviewInput.Label"));
    FormData fdlPreviewInput = new FormData();
    fdlPreviewInput.top = new FormAttachment(lastControl, margin * 2);
    fdlPreviewInput.left = new FormAttachment(0, 0);
    fdlPreviewInput.right = new FormAttachment(middle, -margin);
    wlPreviewInput.setLayoutData(fdlPreviewInput);
    wPreviewInput = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPreviewInput);
    wPreviewInput.setText("Order ID");
    FormData fdPreviewInput = new FormData();
    fdPreviewInput.top = new FormAttachment(wlPreviewInput, 0, SWT.CENTER);
    fdPreviewInput.left = new FormAttachment(middle, 0);
    fdPreviewInput.right = new FormAttachment(100, 0);
    wPreviewInput.setLayoutData(fdPreviewInput);
    lastControl = wPreviewInput;

    // Preview output (read-only)
    Label wlPreviewOutput = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlPreviewOutput);
    wlPreviewOutput.setText(BaseMessages.getString(PKG, "NamingSchemeEditor.PreviewOutput.Label"));
    FormData fdlPreviewOutput = new FormData();
    fdlPreviewOutput.top = new FormAttachment(lastControl, margin);
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

    setWidgetsContent();
    updatePreview();

    wName.addModifyListener(e -> setChanged());
    wDescription.addModifyListener(e -> setChanged());
    wType.addModifyListener(e -> setChanged());
    wCaseStyle.addModifyListener(
        e -> {
          setChanged();
          updatePreview();
        });
    wWordSeparator.addModifyListener(
        e -> {
          setChanged();
          updatePreview();
        });
    wExtraDelimiters.addModifyListener(
        e -> {
          setChanged();
          updatePreview();
        });
    wRemoveSpecial.addListener(
        SWT.Selection,
        e -> {
          setChanged();
          updatePreview();
        });
    wCollapseSeparators.addListener(
        SWT.Selection,
        e -> {
          setChanged();
          updatePreview();
        });
    wTrimEdges.addListener(
        SWT.Selection,
        e -> {
          setChanged();
          updatePreview();
        });
    wPrefix.addModifyListener(
        e -> {
          setChanged();
          updatePreview();
        });
    wSuffix.addModifyListener(
        e -> {
          setChanged();
          updatePreview();
        });
    wPreviewInput.addModifyListener(e -> updatePreview());
  }

  private void updatePreview() {
    if (wPreviewOutput == null || wPreviewOutput.isDisposed()) {
      return;
    }
    NamingScheme preview = new NamingScheme();
    fillSchemeFromWidgets(preview);
    String input = wPreviewInput != null ? wPreviewInput.getText() : "";
    wPreviewOutput.setText(Const.NVL(NamingEngine.apply(preview, input), ""));
  }

  private void fillSchemeFromWidgets(NamingScheme scheme) {
    scheme.setName(wName.getText());
    scheme.setDescription(wDescription.getText());
    scheme.setType(NamingSchemeType.codeFromDisplay(wType.getText()));
    scheme.setCaseStyle(NamingCaseStyle.fromLabel(wCaseStyle.getText()).getCode());
    scheme.setWordSeparator(NamingWordSeparator.fromLabel(wWordSeparator.getText()).getCode());
    scheme.setExtraDelimiters(wExtraDelimiters.getText());
    scheme.setRemoveSpecialCharacters(wRemoveSpecial.getSelection());
    scheme.setCollapseRepeatedSeparators(wCollapseSeparators.getSelection());
    scheme.setTrimEdgeSeparators(wTrimEdges.getSelection());
    scheme.setPrefix(wPrefix.getText());
    scheme.setSuffix(wSuffix.getText());
  }

  @Override
  public void setWidgetsContent() {
    NamingScheme scheme = getMetadata();
    wName.setText(Const.NVL(scheme.getName(), ""));
    wDescription.setText(Const.NVL(scheme.getDescription(), ""));
    wType.setText(NamingSchemeType.displayFromCode(scheme.getType()));
    wCaseStyle.setText(NamingCaseStyle.fromCode(scheme.getCaseStyle()).getLabel());
    wWordSeparator.setText(NamingWordSeparator.fromCode(scheme.getWordSeparator()).getLabel());
    wExtraDelimiters.setText(Const.NVL(scheme.getExtraDelimiters(), ""));
    wRemoveSpecial.setSelection(scheme.isRemoveSpecialCharacters());
    wCollapseSeparators.setSelection(scheme.isCollapseRepeatedSeparators());
    wTrimEdges.setSelection(scheme.isTrimEdgeSeparators());
    wPrefix.setText(Const.NVL(scheme.getPrefix(), ""));
    wSuffix.setText(Const.NVL(scheme.getSuffix(), ""));
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
