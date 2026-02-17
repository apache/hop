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

package org.apache.hop.pipeline.transforms.joinrows;

import java.util.List;
import org.apache.hop.core.Condition;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ConditionEditor;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class JoinRowsDialog extends BaseTransformDialog {
  private static final Class<?> PKG = JoinRowsMeta.class;

  private TextVar wSortDir;

  private Text wPrefix;

  private Text wCache;

  private CCombo wMainTransform;

  private ConditionEditor wCondition;

  private final JoinRowsMeta input;
  private final Condition condition;

  private Condition backupCondition;

  public JoinRowsDialog(
      Shell parent, IVariables variables, JoinRowsMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
    condition = input.getCondition();
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "JoinRowsDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();
    backupCondition = condition.clone();

    Control lastControl = wSpacer;

    // Connection line
    Label wlSortDir = new Label(shell, SWT.RIGHT);
    wlSortDir.setText(BaseMessages.getString(PKG, "JoinRowsDialog.TempDir.Label"));
    PropsUi.setLook(wlSortDir);
    FormData fdlSortDir = new FormData();
    fdlSortDir.left = new FormAttachment(0, 0);
    fdlSortDir.right = new FormAttachment(middle, -margin);
    fdlSortDir.top = new FormAttachment(lastControl, margin);
    wlSortDir.setLayoutData(fdlSortDir);

    Button wbSortDir = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbSortDir);
    wbSortDir.setText(BaseMessages.getString(PKG, "JoinRowsDialog.Browse.Button"));
    FormData fdbSortDir = new FormData();
    fdbSortDir.right = new FormAttachment(100, 0);
    fdbSortDir.top = new FormAttachment(wSpacer, margin);
    wbSortDir.setLayoutData(fdbSortDir);

    wSortDir = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wSortDir.setText(BaseMessages.getString(PKG, "JoinRowsDialog.Temp.Label"));
    PropsUi.setLook(wSortDir);
    wSortDir.addModifyListener(lsMod);
    FormData fdSortDir = new FormData();
    fdSortDir.left = new FormAttachment(middle, 0);
    fdSortDir.top = new FormAttachment(wSpacer, margin);
    fdSortDir.right = new FormAttachment(wbSortDir, -margin);
    wSortDir.setLayoutData(fdSortDir);

    wbSortDir.addListener(
        SWT.Selection, e -> BaseDialog.presentDirectoryDialog(shell, wSortDir, variables));

    // Whenever something changes, set the tooltip to the expanded version:
    wSortDir.addModifyListener(e -> wSortDir.setToolTipText(variables.resolve(wSortDir.getText())));

    // Table line...
    Label wlPrefix = new Label(shell, SWT.RIGHT);
    wlPrefix.setText(BaseMessages.getString(PKG, "JoinRowsDialog.TempFilePrefix.Label"));
    PropsUi.setLook(wlPrefix);
    FormData fdlPrefix = new FormData();
    fdlPrefix.left = new FormAttachment(0, 0);
    fdlPrefix.right = new FormAttachment(middle, -margin);
    fdlPrefix.top = new FormAttachment(wbSortDir, margin);
    wlPrefix.setLayoutData(fdlPrefix);
    wPrefix = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPrefix);
    wPrefix.addModifyListener(lsMod);
    FormData fdPrefix = new FormData();
    fdPrefix.left = new FormAttachment(middle, 0);
    fdPrefix.top = new FormAttachment(wbSortDir, margin);
    fdPrefix.right = new FormAttachment(100, 0);
    wPrefix.setLayoutData(fdPrefix);
    wPrefix.setText(BaseMessages.getString(PKG, "JoinRowsDialog.Prefix.Label"));

    // ICache size...
    Label wlCache = new Label(shell, SWT.RIGHT);
    wlCache.setText(BaseMessages.getString(PKG, "JoinRowsDialog.Cache.Label"));
    PropsUi.setLook(wlCache);
    FormData fdlCache = new FormData();
    fdlCache.left = new FormAttachment(0, 0);
    fdlCache.right = new FormAttachment(middle, -margin);
    fdlCache.top = new FormAttachment(wPrefix, margin);
    wlCache.setLayoutData(fdlCache);
    wCache = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wCache);
    wCache.addModifyListener(lsMod);
    FormData fdCache = new FormData();
    fdCache.left = new FormAttachment(middle, 0);
    fdCache.top = new FormAttachment(wPrefix, margin);
    fdCache.right = new FormAttachment(100, 0);
    wCache.setLayoutData(fdCache);

    // Read date from...
    Label wlMainTransform = new Label(shell, SWT.RIGHT);
    wlMainTransform.setText(BaseMessages.getString(PKG, "JoinRowsDialog.MainTransform.Label"));
    PropsUi.setLook(wlMainTransform);
    FormData fdlMainTransform = new FormData();
    fdlMainTransform.left = new FormAttachment(0, 0);
    fdlMainTransform.right = new FormAttachment(middle, -margin);
    fdlMainTransform.top = new FormAttachment(wCache, margin);
    wlMainTransform.setLayoutData(fdlMainTransform);
    wMainTransform = new CCombo(shell, SWT.BORDER);
    PropsUi.setLook(wMainTransform);

    List<TransformMeta> prevTransforms =
        pipelineMeta.findPreviousTransforms(pipelineMeta.findTransform(transformName));
    for (TransformMeta transformMeta : prevTransforms) {
      wMainTransform.add(transformMeta.getName());
    }

    wMainTransform.addModifyListener(lsMod);
    FormData fdMainTransform = new FormData();
    fdMainTransform.left = new FormAttachment(middle, 0);
    fdMainTransform.top = new FormAttachment(wCache, margin);
    fdMainTransform.right = new FormAttachment(100, 0);
    wMainTransform.setLayoutData(fdMainTransform);

    // Condition widget...
    Label wlCondition = new Label(shell, SWT.NONE);
    wlCondition.setText(BaseMessages.getString(PKG, "JoinRowsDialog.Condition.Label"));
    PropsUi.setLook(wlCondition);
    FormData fdlCondition = new FormData();
    fdlCondition.left = new FormAttachment(0, 0);
    fdlCondition.top = new FormAttachment(wMainTransform, margin);
    wlCondition.setLayoutData(fdlCondition);

    IRowMeta inputfields = null;
    try {
      inputfields = pipelineMeta.getPrevTransformFields(variables, transformName);
    } catch (HopException ke) {
      inputfields = new RowMeta();
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "JoinRowsDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "JoinRowsDialog.FailedToGetFields.DialogMessage"),
          ke);
    }

    wCondition = new ConditionEditor(shell, SWT.BORDER, condition, inputfields);

    FormData fdCondition = new FormData();
    fdCondition.left = new FormAttachment(0, 0);
    fdCondition.top = new FormAttachment(wlCondition, margin);
    fdCondition.right = new FormAttachment(100, 0);
    fdCondition.bottom = new FormAttachment(wOk, -margin);
    fdCondition.height = 200;
    wCondition.setLayoutData(fdCondition);
    wCondition.addModifyListener(lsMod);

    getData();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    // Sync from hops (rename, insert-in-the-middle) so main transform is up to date
    input.searchInfoAndTargetTransforms(pipelineMeta.getTransforms());

    if (input.getPrefix() != null) {
      wPrefix.setText(input.getPrefix());
    }
    if (input.getDirectory() != null) {
      wSortDir.setText(input.getDirectory());
    }
    wCache.setText("" + input.getCacheSize());
    if (input.getMainTransform() != null) {
      wMainTransform.setText(input.getLookupTransformName());
    }
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    input.setCondition(backupCondition);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    if (wCondition.getLevel() > 0) {
      wCondition.goUp();
    } else {
      transformName = wTransformName.getText(); // return value

      input.setPrefix(wPrefix.getText());
      input.setDirectory(wSortDir.getText());
      input.setCacheSize(Const.toInt(wCache.getText(), -1));
      input.setMainTransform(pipelineMeta.findTransform(wMainTransform.getText()));
      input.setMainTransformName(wMainTransform.getText());

      dispose();
    }
  }
}
