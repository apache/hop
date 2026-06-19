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
package org.apache.hop.pipeline.transforms.vcardinput;

import static org.apache.hop.core.util.Utils.isEmpty;

import java.util.List;
import org.apache.hop.core.Props;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transforms.vcard.VCardFieldDiscovery;
import org.apache.hop.pipeline.transforms.vcard.VCardFieldMapping;
import org.apache.hop.pipeline.transforms.vcard.VCardMappingDialogUtil;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

public class VCardInputDialog extends BaseTransformDialog implements ITransformDialog {

  private static final Class<?> PKG = VCardInputDialog.class;
  private final VCardInputMeta input;
  private CTabFolder wTabFolder;
  private TableView wMappings;
  private VCardInputFileDialogSection fileSection;
  private ModifyListener lsMod;

  public VCardInputDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (VCardInputMeta) in;
  }

  @Override
  public String open() {
    PropsUi props = PropsUi.getInstance();
    int margin = PropsUi.getMargin();

    lsMod = e -> input.setChanged();
    Control lastControl = createShell(BaseMessages.getString(PKG, "VCardInputDialog.Shell.Title"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancelDialog()).build();

    wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    CTabItem wFileTab = new CTabItem(wTabFolder, SWT.NONE);
    wFileTab.setFont(GuiResource.getInstance().getFontDefault());
    wFileTab.setText(BaseMessages.getString(PKG, "VCardInputDialog.FileTab.Label"));
    Composite wFileComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wFileComp);
    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = PropsUi.getFormMargin();
    fileLayout.marginHeight = PropsUi.getFormMargin();
    wFileComp.setLayout(fileLayout);
    wFileTab.setControl(wFileComp);

    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setFont(GuiResource.getInstance().getFontDefault());
    wFieldsTab.setText(BaseMessages.getString(PKG, "VCardInputDialog.FieldsTab.Label"));
    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wFieldsComp);
    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = PropsUi.getFormMargin();
    fieldsLayout.marginHeight = PropsUi.getFormMargin();
    wFieldsComp.setLayout(fieldsLayout);
    wFieldsTab.setControl(wFieldsComp);

    fileSection =
        new VCardInputFileDialogSection(shell, variables, pipelineMeta, transformName, lsMod);
    fileSection.build(wFileComp);

    Label wlMappings = new Label(wFieldsComp, SWT.LEFT);
    wlMappings.setText(BaseMessages.getString(PKG, "VCardInputDialog.Mappings.Label"));
    PropsUi.setLook(wlMappings);
    FormData fdlMappings = new FormData();
    fdlMappings.left = new FormAttachment(0, 0);
    fdlMappings.top = new FormAttachment(0, 0);
    wlMappings.setLayoutData(fdlMappings);

    Button wGetFields = new Button(wFieldsComp, SWT.PUSH);
    wGetFields.setText(BaseMessages.getString(PKG, "System.Button.GetFields"));
    wGetFields.setToolTipText(BaseMessages.getString(PKG, "System.Tooltip.GetFields"));
    wGetFields.addListener(SWT.Selection, e -> getFieldsFromFiles());
    setButtonPositions(new Button[] {wGetFields}, margin, null);

    ColumnInfo[] columns = VCardMappingDialogUtil.createMappingColumns();
    wMappings =
        new TableView(
            variables,
            wFieldsComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            Math.max(1, input.getFieldMappings().size()),
            lsMod,
            props);
    PropsUi.setLook(wMappings);
    FormData fdMappings = new FormData();
    fdMappings.left = new FormAttachment(0, 0);
    fdMappings.right = new FormAttachment(100, 0);
    fdMappings.top = new FormAttachment(wlMappings, margin);
    fdMappings.bottom = new FormAttachment(wGetFields, -margin);
    wMappings.setLayoutData(fdMappings);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, margin);
    fdTabFolder.right = new FormAttachment(100, -margin);
    fdTabFolder.top = new FormAttachment(lastControl, margin);
    fdTabFolder.bottom = new FormAttachment(wOk, -margin);
    wTabFolder.setLayoutData(fdTabFolder);

    wTabFolder.setSelection(0);

    getData();
    input.setChanged(changed);
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancelDialog());
    return transformName;
  }

  private void getFieldsFromFiles() {
    VCardInputMeta probe = new VCardInputMeta();
    fileSection.save(probe);
    if (probe.getFileInput().isAcceptingFilenames()) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      mb.setMessage(
          BaseMessages.getString(PKG, "VCardInputDialog.GetFields.AcceptFilenames.Message"));
      mb.setText(BaseMessages.getString(PKG, "VCardInputDialog.GetFields.DialogTitle"));
      mb.open();
      return;
    }
    try {
      FileInputList files = probe.getFileInputList(variables);
      if (files.nrOfFiles() == 0) {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(BaseMessages.getString(PKG, "VCardInputDialog.GetFields.NoFiles"));
        mb.setText(BaseMessages.getString(PKG, "VCardInputDialog.GetFields.DialogTitle"));
        mb.open();
        return;
      }
      List<VCardFieldMapping> mappings =
          VCardFieldDiscovery.discoverFromFiles(files, probe.getEncoding());
      if (mappings.isEmpty()) {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
        mb.setMessage(BaseMessages.getString(PKG, "VCardInputDialog.GetFields.NoProperties"));
        mb.setText(BaseMessages.getString(PKG, "VCardInputDialog.GetFields.DialogTitle"));
        mb.open();
        return;
      }
      wMappings.clearAll();
      VCardMappingDialogUtil.loadMappings(wMappings, mappings);
      wMappings.removeEmptyRows();
      wMappings.setRowNums();
      wMappings.optWidth(true);
      input.setChanged();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "VCardInputDialog.GetFields.DialogTitle"),
          BaseMessages.getString(PKG, "VCardInputDialog.GetFields.Failed", e.getMessage()),
          e);
    }
  }

  private void getData() {
    fileSection.load(input);
    wMappings.clearAll();
    VCardMappingDialogUtil.loadMappings(wMappings, input.getFieldMappings());
    wMappings.removeEmptyRows();
    wMappings.setRowNums();
    wMappings.optWidth(true);
  }

  private void ok() {
    if (isEmpty(wTransformName.getText())) {
      return;
    }
    transformName = wTransformName.getText();
    fileSection.save(input);
    input.setFieldMappings(VCardMappingDialogUtil.saveMappings(wMappings));
    input.setChanged();
    dispose();
  }

  private void cancelDialog() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }
}
