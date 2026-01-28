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

package org.apache.hop.pipeline.transforms.stanford.nlp.simple;

import static org.apache.hop.core.util.Utils.isEmpty;
import static org.apache.hop.i18n.BaseMessages.getString;
import static org.eclipse.swt.SWT.BORDER;
import static org.eclipse.swt.SWT.CHECK;
import static org.eclipse.swt.SWT.CURSOR_WAIT;
import static org.eclipse.swt.SWT.LEFT;
import static org.eclipse.swt.SWT.READ_ONLY;
import static org.eclipse.swt.SWT.RIGHT;
import static org.eclipse.swt.SWT.SINGLE;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class StanfordSimpleNlpDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = StanfordSimpleNlpDialog.class; // For Translator
  private final StanfordSimpleNlpMeta input;
  private boolean gotPreviousFields = false;
  private CCombo wCorpusFieldName;
  private Button wIncludePartOfSpeech;
  private Button wParallelism;
  private Label wlOutputFieldNamePrefix;
  private FormData fdlOutputFieldNamePrefix, fdOutputFieldNamePrefix;
  private Text wOutputFieldNamePrefix;

  public StanfordSimpleNlpDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (StanfordSimpleNlpMeta) in;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "StanfordSimpleNlpDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    // CorpusFieldName field
    Label wlCorpusFieldName = new Label(shell, RIGHT);
    wlCorpusFieldName.setText(getString(PKG, "StanfordSimpleNlpDialog.CorpusFieldName.Label"));
    PropsUi.setLook(wlCorpusFieldName);
    FormData fdlCorpusFieldName = new FormData();
    fdlCorpusFieldName.left = new FormAttachment(0, 0);
    fdlCorpusFieldName.right = new FormAttachment(middle, -margin);
    fdlCorpusFieldName.top = new FormAttachment(wSpacer, margin);
    wlCorpusFieldName.setLayoutData(fdlCorpusFieldName);

    wCorpusFieldName = new CCombo(shell, BORDER | READ_ONLY);
    PropsUi.setLook(wCorpusFieldName);
    wCorpusFieldName.addModifyListener(lsMod);
    FormData fdCorpusFieldName = new FormData();
    fdCorpusFieldName.left = new FormAttachment(middle, 0);
    fdCorpusFieldName.top = new FormAttachment(wSpacer, margin);
    fdCorpusFieldName.right = new FormAttachment(100, -margin);
    wCorpusFieldName.setLayoutData(fdCorpusFieldName);
    wCorpusFieldName.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {}

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), CURSOR_WAIT);
            shell.setCursor(busy);
            get();
            shell.setCursor(null);
            busy.dispose();
          }
        });

    // OutputFieldNamePrefix field
    wlOutputFieldNamePrefix = new Label(shell, RIGHT);
    wlOutputFieldNamePrefix.setText(
        getString(PKG, "StanfordSimpleNlpDialog.OutputFieldNamePrefix.Label"));
    PropsUi.setLook(wlOutputFieldNamePrefix);
    fdlOutputFieldNamePrefix = new FormData();
    fdlOutputFieldNamePrefix.left = new FormAttachment(0, 0);
    fdlOutputFieldNamePrefix.right = new FormAttachment(middle, -margin);
    fdlOutputFieldNamePrefix.top = new FormAttachment(wCorpusFieldName, margin);
    wlOutputFieldNamePrefix.setLayoutData(fdlOutputFieldNamePrefix);
    wOutputFieldNamePrefix = new Text(shell, SINGLE | LEFT | BORDER);
    wOutputFieldNamePrefix.setText("" + input.getOutputFieldNamePrefix());
    PropsUi.setLook(wOutputFieldNamePrefix);
    wOutputFieldNamePrefix.addModifyListener(lsMod);
    fdOutputFieldNamePrefix = new FormData();
    fdOutputFieldNamePrefix.left = new FormAttachment(middle, 0);
    fdOutputFieldNamePrefix.top = new FormAttachment(wCorpusFieldName, margin);
    fdOutputFieldNamePrefix.right = new FormAttachment(100, 0);
    wOutputFieldNamePrefix.setLayoutData(fdOutputFieldNamePrefix);

    // includePartOfSpeech
    Label wlIncludePartOfSpeech = new Label(shell, RIGHT);
    wlIncludePartOfSpeech.setText(
        getString(PKG, "StanfordSimpleNlpDialog.IncludePartOfSpeech.Label"));
    PropsUi.setLook(wlIncludePartOfSpeech);
    FormData fdlIncludePartOfSpeech = new FormData();
    fdlIncludePartOfSpeech.left = new FormAttachment(0, 0);
    fdlIncludePartOfSpeech.top = new FormAttachment(wOutputFieldNamePrefix, margin);
    fdlIncludePartOfSpeech.right = new FormAttachment(middle, -margin);
    wlIncludePartOfSpeech.setLayoutData(fdlIncludePartOfSpeech);

    wIncludePartOfSpeech = new Button(shell, CHECK);
    wIncludePartOfSpeech.setSelection(input.isIncludePartOfSpeech());
    PropsUi.setLook(wIncludePartOfSpeech);
    wIncludePartOfSpeech.setToolTipText(
        getString(PKG, "StanfordSimpleNlpDialog.IncludePartOfSpeech.Tooltip"));
    FormData fdIncludePartOfSpeech = new FormData();
    fdIncludePartOfSpeech.left = new FormAttachment(middle, -margin);
    fdIncludePartOfSpeech.top = new FormAttachment(wOutputFieldNamePrefix, margin);
    fdIncludePartOfSpeech.right = new FormAttachment(100, 0);
    wIncludePartOfSpeech.setLayoutData(fdIncludePartOfSpeech);
    wIncludePartOfSpeech.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // parallelism
    Label wlParallelism = new Label(shell, RIGHT);
    wlParallelism.setText(getString(PKG, "StanfordSimpleNlpDialog.Parallelism.Label"));
    PropsUi.setLook(wlParallelism);
    FormData fdlParallelism = new FormData();
    fdlParallelism.left = new FormAttachment(0, 0);
    fdlParallelism.top = new FormAttachment(wIncludePartOfSpeech, margin);
    fdlParallelism.right = new FormAttachment(middle, -margin);
    wlParallelism.setLayoutData(fdlParallelism);

    wParallelism = new Button(shell, CHECK);
    wParallelism.setSelection(input.isParallelism());
    PropsUi.setLook(wParallelism);
    wParallelism.setToolTipText(getString(PKG, "StanfordSimpleNlpDialog.Parallelism.Tooltip"));
    FormData fdParallelism = new FormData();
    fdParallelism.left = new FormAttachment(middle, -margin);
    fdParallelism.top = new FormAttachment(wIncludePartOfSpeech, margin);
    fdParallelism.right = new FormAttachment(100, 0);
    wParallelism.setLayoutData(fdParallelism);
    wParallelism.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    getData();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (input.getCorpusField() != null) {
      wCorpusFieldName.setText(input.getCorpusField());
    }
    if (input.isIncludePartOfSpeech()) {
      wIncludePartOfSpeech.setEnabled(input.isIncludePartOfSpeech());
    }
    if (input.isParallelism()) {
      wParallelism.setEnabled(input.isParallelism());
    }

    if (input.getOutputFieldNamePrefix() != null) {
      wOutputFieldNamePrefix.setText(input.getOutputFieldNamePrefix());
    }
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (isEmpty(wTransformName.getText())) {
      return;
    }

    input.setCorpusField(wCorpusFieldName.getText());
    input.setIncludePartOfSpeech(wIncludePartOfSpeech.getSelection());
    input.setParallelism(wParallelism.getSelection());
    input.setOutputFieldNamePrefix(wOutputFieldNamePrefix.getText());

    transformName = wTransformName.getText(); // return value

    dispose();
  }

  private void get() {
    if (!gotPreviousFields) {
      try {
        String corpusField = null;

        if (wCorpusFieldName.getText() != null) {
          corpusField = wCorpusFieldName.getText();
        }
        wCorpusFieldName.removeAll();

        IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
        if (r != null) {
          wCorpusFieldName.setItems(r.getFieldNames());
        }
        if (corpusField != null) {
          wCorpusFieldName.setText(corpusField);
        }
        gotPreviousFields = true;
      } catch (HopException ke) {
        new ErrorDialog(
            shell,
            getString(PKG, "StanfordSimpleNlpDialog.FailedToGetFields.DialogTitle"),
            getString(PKG, "StanfordSimpleNlpDialog.FailedToGetFields.DialogMessage"),
            ke);
      }
    }
  }
}
