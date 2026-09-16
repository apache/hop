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
package org.apache.hop.pipeline.transforms.chunker;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transforms.chunker.chunking.ChunkingStrategyType;
import org.apache.hop.pipeline.transforms.chunker.document.ContentType;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.LabelTextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class TextChunkerDialog extends BaseTransformDialog {

  private static final Class<?> PKG = TextChunkerMeta.class;

  private final TextChunkerMeta input;

  private ComboVar wInputField;
  private ComboVar wSourceDocumentIdField;
  private LabelTextVar wOutputChunkField;
  private CCombo wChunkingStrategy;
  private CCombo wContentType;
  private ComboVar wContentTypeField;
  private LabelTextVar wChunkSize;
  private LabelTextVar wChunkOverlap;
  private Button wIncludeMetadata;
  private LabelTextVar wChunkIndexField;
  private LabelTextVar wChunkStartPosField;
  private LabelTextVar wDocumentIdField;
  private LabelTextVar wChunkCountField;

  public TextChunkerDialog(
      Shell parent,
      IVariables variables,
      TextChunkerMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    Control lastControl = createShell(BaseMessages.getString(PKG, "TextChunkerDialog.Shell.Title"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    wInputField = new ComboVar(variables, shell, SWT.BORDER | SWT.READ_ONLY);
    lastControl = addFieldCombo(lastControl, "TextChunker.inputField", wInputField);

    wSourceDocumentIdField = new ComboVar(variables, shell, SWT.BORDER | SWT.READ_ONLY);
    lastControl =
        addFieldCombo(lastControl, "TextChunker.sourceDocumentIdField", wSourceDocumentIdField);

    wOutputChunkField =
        new LabelTextVar(
            variables,
            shell,
            BaseMessages.getString(PKG, "TextChunker.outputChunkField.Label"),
            BaseMessages.getString(PKG, "TextChunker.outputChunkField.Tooltip"));
    PropsUi.setLook(wOutputChunkField);
    wOutputChunkField.addModifyListener(lsMod);
    FormData fdOutput = new FormData();
    fdOutput.left = new FormAttachment(0, 0);
    fdOutput.top = new FormAttachment(lastControl, margin);
    fdOutput.right = new FormAttachment(100, 0);
    wOutputChunkField.setLayoutData(fdOutput);
    lastControl = wOutputChunkField;

    Label wlStrategy = new Label(shell, SWT.RIGHT);
    wlStrategy.setText(BaseMessages.getString(PKG, "TextChunker.chunkingStrategy.Label"));
    PropsUi.setLook(wlStrategy);
    FormData fdlStrategy = new FormData();
    fdlStrategy.left = new FormAttachment(0, 0);
    fdlStrategy.right = new FormAttachment(middle, -margin);
    fdlStrategy.top = new FormAttachment(lastControl, margin);
    wlStrategy.setLayoutData(fdlStrategy);
    wChunkingStrategy = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wChunkingStrategy);
    wChunkingStrategy.setItems(
        new String[] {
          ChunkingStrategyType.CHARACTER.name(),
          ChunkingStrategyType.PARAGRAPH.name(),
          ChunkingStrategyType.STRUCTURE.name()
        });
    FormData fdStrategy = new FormData();
    fdStrategy.left = new FormAttachment(middle, 0);
    fdStrategy.top = new FormAttachment(lastControl, margin);
    fdStrategy.right = new FormAttachment(100, 0);
    wChunkingStrategy.setLayoutData(fdStrategy);
    wChunkingStrategy.addModifyListener(lsMod);
    wChunkingStrategy.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            updateStructureFieldsEnabled();
          }
        });
    lastControl = wChunkingStrategy;

    Label wlContentType = new Label(shell, SWT.RIGHT);
    wlContentType.setText(BaseMessages.getString(PKG, "TextChunker.contentType.Label"));
    wlContentType.setToolTipText(BaseMessages.getString(PKG, "TextChunker.contentType.Tooltip"));
    PropsUi.setLook(wlContentType);
    FormData fdlContentType = new FormData();
    fdlContentType.left = new FormAttachment(0, 0);
    fdlContentType.right = new FormAttachment(middle, -margin);
    fdlContentType.top = new FormAttachment(lastControl, margin);
    wlContentType.setLayoutData(fdlContentType);
    wContentType = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wContentType);
    wContentType.setItems(
        new String[] {
          ContentType.AUTO.name(),
          ContentType.PLAIN.name(),
          ContentType.MARKDOWN.name(),
          ContentType.ASCIIDOC.name(),
          ContentType.PIPELINE.name(),
          ContentType.WORKFLOW.name(),
          ContentType.METADATA.name()
        });
    FormData fdContentType = new FormData();
    fdContentType.left = new FormAttachment(middle, 0);
    fdContentType.top = new FormAttachment(lastControl, margin);
    fdContentType.right = new FormAttachment(100, 0);
    wContentType.setLayoutData(fdContentType);
    wContentType.addModifyListener(lsMod);
    lastControl = wContentType;

    wContentTypeField = new ComboVar(variables, shell, SWT.BORDER | SWT.READ_ONLY);
    lastControl = addFieldCombo(lastControl, "TextChunker.contentTypeField", wContentTypeField);

    wChunkSize =
        new LabelTextVar(
            variables,
            shell,
            BaseMessages.getString(PKG, "TextChunker.chunkSize.Label"),
            BaseMessages.getString(PKG, "TextChunker.chunkSize.Tooltip"));
    PropsUi.setLook(wChunkSize);
    wChunkSize.addModifyListener(lsMod);
    FormData fdSize = new FormData();
    fdSize.left = new FormAttachment(0, 0);
    fdSize.top = new FormAttachment(lastControl, margin);
    fdSize.right = new FormAttachment(100, 0);
    wChunkSize.setLayoutData(fdSize);
    lastControl = wChunkSize;

    wChunkOverlap =
        new LabelTextVar(
            variables,
            shell,
            BaseMessages.getString(PKG, "TextChunker.chunkOverlap.Label"),
            BaseMessages.getString(PKG, "TextChunker.chunkOverlap.Tooltip"));
    PropsUi.setLook(wChunkOverlap);
    wChunkOverlap.addModifyListener(lsMod);
    FormData fdOverlap = new FormData();
    fdOverlap.left = new FormAttachment(0, 0);
    fdOverlap.top = new FormAttachment(lastControl, margin);
    fdOverlap.right = new FormAttachment(100, 0);
    wChunkOverlap.setLayoutData(fdOverlap);
    lastControl = wChunkOverlap;

    wIncludeMetadata = new Button(shell, SWT.CHECK);
    wIncludeMetadata.setText(BaseMessages.getString(PKG, "TextChunker.includeMetadata.Label"));
    wIncludeMetadata.setToolTipText(
        BaseMessages.getString(PKG, "TextChunker.includeMetadata.Tooltip"));
    PropsUi.setLook(wIncludeMetadata);
    FormData fdMeta = new FormData();
    fdMeta.left = new FormAttachment(middle, 0);
    fdMeta.top = new FormAttachment(lastControl, margin);
    fdMeta.right = new FormAttachment(100, 0);
    wIncludeMetadata.setLayoutData(fdMeta);
    wIncludeMetadata.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            updateMetadataEnabled();
          }
        });
    lastControl = wIncludeMetadata;

    wChunkIndexField =
        new LabelTextVar(
            variables,
            shell,
            BaseMessages.getString(PKG, "TextChunker.chunkIndexField.Label"),
            BaseMessages.getString(PKG, "TextChunker.chunkIndexField.Tooltip"));
    PropsUi.setLook(wChunkIndexField);
    wChunkIndexField.addModifyListener(lsMod);
    FormData fdIndex = new FormData();
    fdIndex.left = new FormAttachment(0, 0);
    fdIndex.top = new FormAttachment(lastControl, margin);
    fdIndex.right = new FormAttachment(100, 0);
    wChunkIndexField.setLayoutData(fdIndex);

    wChunkStartPosField =
        new LabelTextVar(
            variables,
            shell,
            BaseMessages.getString(PKG, "TextChunker.chunkStartPosField.Label"),
            BaseMessages.getString(PKG, "TextChunker.chunkStartPosField.Tooltip"));
    PropsUi.setLook(wChunkStartPosField);
    wChunkStartPosField.addModifyListener(lsMod);
    FormData fdStart = new FormData();
    fdStart.left = new FormAttachment(0, 0);
    fdStart.top = new FormAttachment(wChunkIndexField, margin);
    fdStart.right = new FormAttachment(100, 0);
    wChunkStartPosField.setLayoutData(fdStart);

    wDocumentIdField =
        new LabelTextVar(
            variables,
            shell,
            BaseMessages.getString(PKG, "TextChunker.documentIdField.Label"),
            BaseMessages.getString(PKG, "TextChunker.documentIdField.Tooltip"));
    PropsUi.setLook(wDocumentIdField);
    wDocumentIdField.addModifyListener(lsMod);
    FormData fdDoc = new FormData();
    fdDoc.left = new FormAttachment(0, 0);
    fdDoc.top = new FormAttachment(wChunkStartPosField, margin);
    fdDoc.right = new FormAttachment(100, 0);
    wDocumentIdField.setLayoutData(fdDoc);

    wChunkCountField =
        new LabelTextVar(
            variables,
            shell,
            BaseMessages.getString(PKG, "TextChunker.chunkCountField.Label"),
            BaseMessages.getString(PKG, "TextChunker.chunkCountField.Tooltip"));
    PropsUi.setLook(wChunkCountField);
    wChunkCountField.addModifyListener(lsMod);
    FormData fdCount = new FormData();
    fdCount.left = new FormAttachment(0, 0);
    fdCount.top = new FormAttachment(wDocumentIdField, margin);
    fdCount.right = new FormAttachment(100, 0);
    wChunkCountField.setLayoutData(fdCount);
    fdCount.bottom = new FormAttachment(wOk, -margin * 2);
    wChunkCountField.setLayoutData(fdCount);

    getData();
    updateMetadataEnabled();
    updateStructureFieldsEnabled();
    loading = false;
    input.setChanged(changed);
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private Control addFieldCombo(Control previous, String labelKey, ComboVar combo) {
    Label label = new Label(shell, SWT.RIGHT);
    label.setText(BaseMessages.getString(PKG, labelKey + ".Label"));
    label.setToolTipText(BaseMessages.getString(PKG, labelKey + ".Tooltip"));
    PropsUi.setLook(label);
    FormData fdl = new FormData();
    fdl.left = new FormAttachment(0, 0);
    fdl.right = new FormAttachment(middle, -margin);
    fdl.top = new FormAttachment(previous, margin);
    label.setLayoutData(fdl);
    PropsUi.setLook(combo);
    combo.addModifyListener(lsMod);
    combo.addFocusListener(
        new FocusAdapter() {
          @Override
          public void focusGained(FocusEvent e) {
            populateInputFields();
          }
        });
    FormData fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(previous, margin);
    fd.right = new FormAttachment(100, 0);
    combo.setLayoutData(fd);
    return combo;
  }

  private void populateInputFields() {
    try {
      String current = wInputField.getText();
      wInputField.removeAll();
      wSourceDocumentIdField.removeAll();
      wContentTypeField.removeAll();
      IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (row != null) {
        wInputField.setItems(row.getFieldNames());
        wSourceDocumentIdField.setItems(row.getFieldNames());
        wContentTypeField.setItems(row.getFieldNames());
        if (!Utils.isEmpty(current)) {
          wInputField.setText(current);
        }
      }
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "TextChunkerDialog.GetFields.Error.Title"),
          BaseMessages.getString(PKG, "TextChunkerDialog.GetFields.Error.Message"),
          e);
    }
  }

  private void updateStructureFieldsEnabled() {
    boolean structure = ChunkingStrategyType.STRUCTURE.name().equals(wChunkingStrategy.getText());
    wContentType.setEnabled(structure);
    wContentTypeField.setEnabled(structure);
  }

  private void updateMetadataEnabled() {
    boolean enabled = wIncludeMetadata.getSelection();
    wChunkIndexField.setEnabled(enabled);
    wChunkStartPosField.setEnabled(enabled);
    wDocumentIdField.setEnabled(enabled);
    wChunkCountField.setEnabled(enabled);
  }

  private void getData() {
    wInputField.setText(Const.NVL(input.getInputField(), ""));
    wSourceDocumentIdField.setText(Const.NVL(input.getSourceDocumentIdField(), ""));
    wOutputChunkField.setText(Const.NVL(input.getOutputChunkField(), ""));
    wChunkingStrategy.setText(
        input.getChunkingStrategy() != null
            ? input.getChunkingStrategy().name()
            : ChunkingStrategyType.CHARACTER.name());
    wContentType.setText(
        input.getContentType() != null ? input.getContentType().name() : ContentType.AUTO.name());
    wContentTypeField.setText(Const.NVL(input.getContentTypeField(), ""));
    wChunkSize.setText(Integer.toString(input.getChunkSize()));
    wChunkOverlap.setText(Integer.toString(input.getChunkOverlap()));
    wIncludeMetadata.setSelection(input.isIncludeMetadata());
    wChunkIndexField.setText(Const.NVL(input.getChunkIndexField(), ""));
    wChunkStartPosField.setText(Const.NVL(input.getChunkStartPosField(), ""));
    wDocumentIdField.setText(Const.NVL(input.getDocumentIdField(), ""));
    wChunkCountField.setText(Const.NVL(input.getChunkCountField(), ""));
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }
    transformName = wTransformName.getText();
    input.setInputField(wInputField.getText());
    input.setSourceDocumentIdField(wSourceDocumentIdField.getText());
    input.setOutputChunkField(wOutputChunkField.getText());
    input.setChunkingStrategy(ChunkingStrategyType.fromString(wChunkingStrategy.getText()));
    input.setContentType(ContentType.fromString(wContentType.getText()));
    input.setContentTypeField(wContentTypeField.getText());
    input.setChunkSize(Const.toInt(wChunkSize.getText(), 1000));
    input.setChunkOverlap(Const.toInt(wChunkOverlap.getText(), 0));
    input.setIncludeMetadata(wIncludeMetadata.getSelection());
    input.setChunkIndexField(wChunkIndexField.getText());
    input.setChunkStartPosField(wChunkStartPosField.getText());
    input.setDocumentIdField(wDocumentIdField.getText());
    input.setChunkCountField(wChunkCountField.getText());
    dispose();
  }
}
