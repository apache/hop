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

package org.apache.hop.pipeline.transforms.xml.advancedxmloutput;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/** Dialog: File, Content, and XML Tree tabs. */
public class AdvancedXmlOutputDialog extends BaseTransformDialog {

  private static final Class<?> PKG = AdvancedXmlOutputMeta.class;

  private static final AdvancedXmlOutputMeta.XmlOutputOperation[] OPERATION_ORDER = {
    AdvancedXmlOutputMeta.XmlOutputOperation.WRITE_TO_FILE,
    AdvancedXmlOutputMeta.XmlOutputOperation.OUTPUT_VALUE,
    AdvancedXmlOutputMeta.XmlOutputOperation.BOTH
  };

  private final AdvancedXmlOutputMeta input;

  // File tab
  private Label wlOperation;
  private CCombo wOperationType;
  private Label wlOutputXmlField;
  private TextVar wOutputXmlField;
  private Button wIncludeInputFieldsInOutput;
  private TextVar wFilename;
  private Button wbFilename;
  private TextVar wExtension;
  private Button wAddTransformnr;
  private Button wAddDate;
  private Button wAddTime;
  private Button wSpecifyFormat;
  private TextVar wDateTimeFormat;
  private Text wSplitEvery;
  private Button wZipped;
  private Button wDoNotOpenAtInit;
  private Button wDoNotCreateEmptyFile;
  private Button wAddToResult;
  private CCombo wEncoding;
  private Button wShowFiles;

  // Content tab
  private Button wCompactFile;
  private Button wBlankLineAfterDecl;
  private Button wCreateEmptyElement;
  private Button wCreateAttributeIfNull;
  private Button wCreateAttributeIfUnmapped;
  private Button wTrim;
  private TextVar wDefaultDecimal;
  private TextVar wDefaultGroup;
  private Button wGenerateXsd;
  private TextVar wDoctypeRoot;
  private TextVar wDoctypeSystem;
  private TextVar wDoctypePublic;
  private TextVar wXslHref;
  private TextVar wXslType;

  // Tree designer tab
  private XmlTreeDesigner wTreeDesigner;

  private final List<String> inputFieldNames = new ArrayList<>();
  private boolean encodingsLoaded = false;

  public AdvancedXmlOutputDialog(
      Shell parent,
      IVariables variables,
      AdvancedXmlOutputMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    this.input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.Shell.Title"));
    changed = input.hasChanged();

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ScrolledComposite wScrolledComposite =
        new ScrolledComposite(shell, SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(wScrolledComposite);
    FormData fdSc = new FormData();
    fdSc.left = new FormAttachment(0, 0);
    fdSc.top = new FormAttachment(wSpacer, 0);
    fdSc.right = new FormAttachment(100, 0);
    fdSc.bottom = new FormAttachment(wOk, -margin);
    wScrolledComposite.setLayoutData(fdSc);
    wScrolledComposite.setLayout(new FillLayout());
    wScrolledComposite.setExpandHorizontal(true);
    wScrolledComposite.setExpandVertical(true);

    Composite mainComposite = new Composite(wScrolledComposite, SWT.NONE);
    PropsUi.setLook(mainComposite);
    mainComposite.setLayout(props.createFormLayout());

    Label wContentTop = new Label(mainComposite, SWT.NONE);
    wContentTop.setLayoutData(new FormData(0, 0));

    CTabFolder wTabFolder = new CTabFolder(mainComposite, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    addFileTab(wTabFolder, lsMod, margin, middle);
    addContentTab(wTabFolder, lsMod, margin, middle);
    addTreeTab(wTabFolder, margin);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.top = new FormAttachment(wContentTop, margin);
    fdTabFolder.bottom = new FormAttachment(100, -margin);
    wTabFolder.setLayoutData(fdTabFolder);

    wScrolledComposite.setContent(mainComposite);
    mainComposite.pack();
    wScrolledComposite.setMinSize(mainComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));

    FormData fdComp = new FormData();
    fdComp.left = new FormAttachment(0, 0);
    fdComp.top = new FormAttachment(0, 0);
    fdComp.right = new FormAttachment(100, 0);
    fdComp.bottom = new FormAttachment(100, 0);
    mainComposite.setLayoutData(fdComp);

    mainComposite.pack();

    wTabFolder.setSelection(0);

    populateInputFieldsAsync();

    getData();
    setSpecifyFormatVisibility();

    input.setChanged(changed);

    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  // ---------------------------------------------------------------------------
  // File tab
  // ---------------------------------------------------------------------------

  private void addFileTab(CTabFolder tabFolder, ModifyListener lsMod, int margin, int middle) {
    CTabItem tab = new CTabItem(tabFolder, SWT.NONE);
    tab.setFont(GuiResource(shell));
    tab.setText(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.FileTab.Title"));

    Composite comp = new Composite(tabFolder, SWT.NONE);
    PropsUi.setLook(comp);
    FormLayout fl = new FormLayout();
    fl.marginWidth = 3;
    fl.marginHeight = 3;
    comp.setLayout(fl);

    wlOperation = new Label(comp, SWT.RIGHT);
    wlOperation.setText(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.OperationType.Label"));
    PropsUi.setLook(wlOperation);
    FormData fdWlOp = new FormData();
    fdWlOp.left = new FormAttachment(0, 0);
    fdWlOp.right = new FormAttachment(middle, -margin);
    fdWlOp.top = new FormAttachment(0, margin);
    wlOperation.setLayoutData(fdWlOp);

    wOperationType = new CCombo(comp, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wOperationType);
    for (AdvancedXmlOutputMeta.XmlOutputOperation op : OPERATION_ORDER) {
      wOperationType.add(op.getDescription());
    }
    FormData fdOp = new FormData();
    fdOp.left = new FormAttachment(middle, 0);
    fdOp.right = new FormAttachment(100, 0);
    fdOp.top = new FormAttachment(0, margin);
    wOperationType.setLayoutData(fdOp);
    wOperationType.addListener(
        SWT.Selection,
        e -> {
          updateFileWidgetsForOperation();
          lsMod.modifyText(null);
        });

    wlOutputXmlField = new Label(comp, SWT.RIGHT);
    wlOutputXmlField.setText(
        BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.OutputXmlField.Label"));
    PropsUi.setLook(wlOutputXmlField);
    FormData fdWlOut = new FormData();
    fdWlOut.left = new FormAttachment(0, 0);
    fdWlOut.right = new FormAttachment(middle, -margin);
    fdWlOut.top = new FormAttachment(wOperationType, margin);
    wlOutputXmlField.setLayoutData(fdWlOut);

    wOutputXmlField = new TextVar(variables, comp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wOutputXmlField);
    wOutputXmlField.addModifyListener(lsMod);
    FormData fdOutF = new FormData();
    fdOutF.left = new FormAttachment(middle, 0);
    fdOutF.right = new FormAttachment(100, 0);
    fdOutF.top = new FormAttachment(wOperationType, margin);
    wOutputXmlField.setLayoutData(fdOutF);

    wIncludeInputFieldsInOutput =
        addCheckbox(comp, wOutputXmlField, "IncludeInputFieldsInOutput", lsMod, middle, margin);

    // Filename row
    Label lblFn = new Label(comp, SWT.RIGHT);
    lblFn.setText(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.Filename.Label"));
    PropsUi.setLook(lblFn);
    FormData fdLblFn = new FormData();
    fdLblFn.left = new FormAttachment(0, 0);
    fdLblFn.top = new FormAttachment(wIncludeInputFieldsInOutput, margin);
    fdLblFn.right = new FormAttachment(middle, -margin);
    lblFn.setLayoutData(fdLblFn);

    wbFilename = new Button(comp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbFilename);
    wbFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdBtn = new FormData();
    fdBtn.right = new FormAttachment(100, 0);
    fdBtn.top = new FormAttachment(wIncludeInputFieldsInOutput, margin);
    wbFilename.setLayoutData(fdBtn);

    wFilename = new TextVar(variables, comp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilename);
    wFilename.addModifyListener(lsMod);
    FormData fdFn = new FormData();
    fdFn.left = new FormAttachment(middle, 0);
    fdFn.top = new FormAttachment(wIncludeInputFieldsInOutput, margin);
    fdFn.right = new FormAttachment(wbFilename, -margin);
    wFilename.setLayoutData(fdFn);

    wbFilename.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                true,
                shell,
                wFilename,
                variables,
                new String[] {"*.xml", "*"},
                new String[] {
                  BaseMessages.getString(PKG, "System.FileType.XMLFiles"),
                  BaseMessages.getString(PKG, "System.FileType.AllFiles")
                },
                true));

    wExtension =
        addLabeledTextVar(
            comp,
            wFilename,
            "Extension",
            "AdvancedXMLOutputDialog.Extension.Label",
            lsMod,
            middle,
            margin);

    Label lblEnc = new Label(comp, SWT.RIGHT);
    lblEnc.setText(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.Encoding.Label"));
    PropsUi.setLook(lblEnc);
    FormData fdLblEnc = new FormData();
    fdLblEnc.left = new FormAttachment(0, 0);
    fdLblEnc.right = new FormAttachment(middle, -margin);
    fdLblEnc.top = new FormAttachment(wExtension, margin);
    lblEnc.setLayoutData(fdLblEnc);
    wEncoding = new CCombo(comp, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wEncoding);
    wEncoding.addModifyListener(lsMod);
    FormData fdEnc = new FormData();
    fdEnc.left = new FormAttachment(middle, 0);
    fdEnc.right = new FormAttachment(100, 0);
    fdEnc.top = new FormAttachment(wExtension, margin);
    wEncoding.setLayoutData(fdEnc);
    wEncoding.addFocusListener(
        new org.eclipse.swt.events.FocusAdapter() {
          @Override
          public void focusGained(org.eclipse.swt.events.FocusEvent e) {
            ensureEncodingsLoaded();
          }
        });

    wAddTransformnr = addCheckbox(comp, wEncoding, "AddTransformnr", lsMod, middle, margin);
    wAddDate = addCheckbox(comp, wAddTransformnr, "AddDate", lsMod, middle, margin);
    wAddTime = addCheckbox(comp, wAddDate, "AddTime", lsMod, middle, margin);
    wSpecifyFormat = addCheckbox(comp, wAddTime, "SpecifyFormat", lsMod, middle, margin);
    wSpecifyFormat.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            setSpecifyFormatVisibility();
          }
        });

    wDateTimeFormat =
        addLabeledTextVar(
            comp,
            wSpecifyFormat,
            "DateTimeFormat",
            "AdvancedXMLOutputDialog.DateTimeFormat.Label",
            lsMod,
            middle,
            margin);

    Label lblSplit = new Label(comp, SWT.RIGHT);
    lblSplit.setText(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.SplitEvery.Label"));
    PropsUi.setLook(lblSplit);
    FormData fdLblSplit = new FormData();
    fdLblSplit.left = new FormAttachment(0, 0);
    fdLblSplit.right = new FormAttachment(middle, -margin);
    fdLblSplit.top = new FormAttachment(wDateTimeFormat, margin);
    lblSplit.setLayoutData(fdLblSplit);
    wSplitEvery = new Text(comp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSplitEvery);
    wSplitEvery.addModifyListener(lsMod);
    FormData fdSplit = new FormData();
    fdSplit.left = new FormAttachment(middle, 0);
    fdSplit.right = new FormAttachment(100, 0);
    fdSplit.top = new FormAttachment(wDateTimeFormat, margin);
    wSplitEvery.setLayoutData(fdSplit);

    wZipped = addCheckbox(comp, wSplitEvery, "Zipped", lsMod, middle, margin);
    wDoNotOpenAtInit = addCheckbox(comp, wZipped, "DoNotOpenAtInit", lsMod, middle, margin);
    wDoNotCreateEmptyFile =
        addCheckbox(comp, wDoNotOpenAtInit, "DoNotCreateEmptyFile", lsMod, middle, margin);
    wAddToResult = addCheckbox(comp, wDoNotCreateEmptyFile, "AddToResult", lsMod, middle, margin);

    wShowFiles = new Button(comp, SWT.PUSH);
    wShowFiles.setText(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.ShowFiles.Button"));
    PropsUi.setLook(wShowFiles);
    FormData fdShow = new FormData();
    fdShow.left = new FormAttachment(middle, 0);
    fdShow.top = new FormAttachment(wAddToResult, margin * 2);
    wShowFiles.setLayoutData(fdShow);
    wShowFiles.addListener(SWT.Selection, e -> showFilesPreview());

    FormData fdComp = new FormData();
    fdComp.left = new FormAttachment(0, 0);
    fdComp.top = new FormAttachment(0, 0);
    fdComp.right = new FormAttachment(100, 0);
    fdComp.bottom = new FormAttachment(100, 0);
    comp.setLayoutData(fdComp);

    comp.layout();
    tab.setControl(comp);
    updateFileWidgetsForOperation();
  }

  private void updateFileWidgetsForOperation() {
    if (wOperationType == null || wOperationType.isDisposed()) {
      return;
    }
    int idx = wOperationType.getSelectionIndex();
    if (idx < 0) {
      idx = 0;
    }
    boolean needFile = idx == 0 || idx == 2;
    boolean needField = idx == 1 || idx == 2;
    wlOutputXmlField.setEnabled(needField);
    wOutputXmlField.setEnabled(needField);
    if (wIncludeInputFieldsInOutput != null && !wIncludeInputFieldsInOutput.isDisposed()) {
      wIncludeInputFieldsInOutput.setEnabled(needField);
    }
    wFilename.setEnabled(needFile);
    wbFilename.setEnabled(needFile);
    wExtension.setEnabled(needFile);
    wAddTransformnr.setEnabled(needFile);
    wSpecifyFormat.setEnabled(needFile);
    wSplitEvery.setEnabled(needFile || needField);
    wZipped.setEnabled(needFile);
    wDoNotOpenAtInit.setEnabled(needFile);
    wDoNotCreateEmptyFile.setEnabled(needFile);
    wAddToResult.setEnabled(needFile);
    if (wShowFiles != null && !wShowFiles.isDisposed()) {
      wShowFiles.setEnabled(needFile);
    }
    setSpecifyFormatVisibility();
  }

  /** Pops up a dialog with up to a handful of sample filenames built from the current settings. */
  private void showFilesPreview() {
    XmlFileOutputSupport snapshot = new XmlFileOutputSupport();
    snapshot.setFileName(wFilename.getText());
    snapshot.setExtension(wExtension.getText());
    snapshot.setSplitEvery(parsePositiveInt(wSplitEvery.getText(), 0));
    snapshot.setTransformNrInFilename(wAddTransformnr.getSelection());
    snapshot.setDateInFilename(wAddDate.getSelection());
    snapshot.setTimeInFilename(wAddTime.getSelection());
    snapshot.setSpecifyFormat(wSpecifyFormat.getSelection());
    snapshot.setDateTimeFormat(wDateTimeFormat.getText());
    snapshot.setZipped(wZipped.getSelection());

    String[] files = snapshot.previewFilenames(variables);
    if (files == null || files.length == 0) {
      org.apache.hop.ui.core.dialog.MessageBox box =
          new org.apache.hop.ui.core.dialog.MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.ShowFiles.Title"));
      box.setMessage(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.ShowFiles.Empty"));
      box.open();
      return;
    }
    org.apache.hop.ui.core.dialog.EnterSelectionDialog d =
        new org.apache.hop.ui.core.dialog.EnterSelectionDialog(
            shell,
            files,
            BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.ShowFiles.Title"),
            BaseMessages.getString(
                PKG, "AdvancedXMLOutputDialog.ShowFiles.Message", String.valueOf(files.length)));
    d.setViewOnly();
    d.open();
  }

  // ---------------------------------------------------------------------------
  // Content tab
  // ---------------------------------------------------------------------------

  private void addContentTab(CTabFolder tabFolder, ModifyListener lsMod, int margin, int middle) {
    CTabItem tab = new CTabItem(tabFolder, SWT.NONE);
    tab.setFont(GuiResource(shell));
    tab.setText(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.ContentTab.Title"));

    Composite comp = new Composite(tabFolder, SWT.NONE);
    PropsUi.setLook(comp);
    FormLayout fl = new FormLayout();
    fl.marginWidth = 3;
    fl.marginHeight = 3;
    comp.setLayout(fl);

    wCompactFile = addCheckbox(comp, null, "CompactFile", lsMod, middle, margin);
    wBlankLineAfterDecl =
        addCheckbox(comp, wCompactFile, "BlankLineAfterDecl", lsMod, middle, margin);
    wCreateEmptyElement =
        addCheckbox(comp, wBlankLineAfterDecl, "CreateEmptyElement", lsMod, middle, margin);
    wCreateAttributeIfNull =
        addCheckbox(comp, wCreateEmptyElement, "CreateAttributeIfNull", lsMod, middle, margin);
    wCreateAttributeIfUnmapped =
        addCheckbox(
            comp, wCreateAttributeIfNull, "CreateAttributeIfUnmapped", lsMod, middle, margin);
    wTrim = addCheckbox(comp, wCreateAttributeIfUnmapped, "Trim", lsMod, middle, margin);

    wDefaultDecimal =
        addLabeledTextVar(
            comp,
            wTrim,
            "DefaultDecimal",
            "AdvancedXMLOutputDialog.DefaultDecimal.Label",
            lsMod,
            middle,
            margin);
    wDefaultGroup =
        addLabeledTextVar(
            comp,
            wDefaultDecimal,
            "DefaultGroup",
            "AdvancedXMLOutputDialog.DefaultGroup.Label",
            lsMod,
            middle,
            margin);

    wGenerateXsd = addCheckbox(comp, wDefaultGroup, "GenerateXsd", lsMod, middle, margin);

    wDoctypeRoot =
        addLabeledTextVar(
            comp,
            wGenerateXsd,
            "DoctypeRoot",
            "AdvancedXMLOutputDialog.DoctypeRoot.Label",
            lsMod,
            middle,
            margin);
    wDoctypeSystem =
        addLabeledTextVar(
            comp,
            wDoctypeRoot,
            "DoctypeSystem",
            "AdvancedXMLOutputDialog.DoctypeSystem.Label",
            lsMod,
            middle,
            margin);
    wDoctypePublic =
        addLabeledTextVar(
            comp,
            wDoctypeSystem,
            "DoctypePublic",
            "AdvancedXMLOutputDialog.DoctypePublic.Label",
            lsMod,
            middle,
            margin);

    wXslHref =
        addLabeledTextVar(
            comp,
            wDoctypePublic,
            "XslHref",
            "AdvancedXMLOutputDialog.XslHref.Label",
            lsMod,
            middle,
            margin);
    wXslType =
        addLabeledTextVar(
            comp,
            wXslHref,
            "XslType",
            "AdvancedXMLOutputDialog.XslType.Label",
            lsMod,
            middle,
            margin);

    FormData fdComp = new FormData();
    fdComp.left = new FormAttachment(0, 0);
    fdComp.top = new FormAttachment(0, 0);
    fdComp.right = new FormAttachment(100, 0);
    fdComp.bottom = new FormAttachment(100, 0);
    comp.setLayoutData(fdComp);

    comp.layout();
    tab.setControl(comp);
  }

  // ---------------------------------------------------------------------------
  // Tree tab
  // ---------------------------------------------------------------------------

  private void addTreeTab(CTabFolder tabFolder, int margin) {
    CTabItem tab = new CTabItem(tabFolder, SWT.NONE);
    tab.setFont(GuiResource(shell));
    tab.setText(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.TreeTab.Title"));

    Composite comp = new Composite(tabFolder, SWT.NONE);
    PropsUi.setLook(comp);
    FormLayout fl = new FormLayout();
    fl.marginWidth = 3;
    fl.marginHeight = 3;
    comp.setLayout(fl);

    wTreeDesigner = new XmlTreeDesigner(comp, SWT.NONE, variables);
    PropsUi.setLook(wTreeDesigner);
    FormData fdTd = new FormData();
    fdTd.left = new FormAttachment(0, 0);
    fdTd.top = new FormAttachment(0, 0);
    fdTd.right = new FormAttachment(100, 0);
    fdTd.bottom = new FormAttachment(100, 0);
    wTreeDesigner.setLayoutData(fdTd);

    wTreeDesigner.setChangeListener(input::setChanged);
    wTreeDesigner.setGetFieldsListener(e -> reloadInputFieldsBlocking());

    FormData fdComp = new FormData();
    fdComp.left = new FormAttachment(0, 0);
    fdComp.top = new FormAttachment(0, 0);
    fdComp.right = new FormAttachment(100, 0);
    fdComp.bottom = new FormAttachment(100, 0);
    comp.setLayoutData(fdComp);

    comp.layout();
    tab.setControl(comp);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Adds a "label : TextVar" row anchored below {@code below}.
   *
   * @param messageKey i18n key for the label, or {@code null} to skip translation lookup
   */
  private TextVar addLabeledTextVar(
      Composite parent,
      Control below,
      String idHint,
      String messageKey,
      ModifyListener lsMod,
      int middle,
      int margin) {
    Label lbl = new Label(parent, SWT.RIGHT);
    String labelKey =
        messageKey != null ? messageKey : "AdvancedXMLOutputDialog." + idHint + ".Label";
    lbl.setText(BaseMessages.getString(PKG, labelKey));
    PropsUi.setLook(lbl);
    FormData fdLbl = new FormData();
    fdLbl.left = new FormAttachment(0, 0);
    fdLbl.right = new FormAttachment(middle, -margin);
    fdLbl.top = below == null ? new FormAttachment(0, margin) : new FormAttachment(below, margin);
    lbl.setLayoutData(fdLbl);

    TextVar tv = new TextVar(variables, parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(tv);
    tv.addModifyListener(lsMod);
    FormData fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    fd.top = below == null ? new FormAttachment(0, margin) : new FormAttachment(below, margin);
    tv.setLayoutData(fd);
    return tv;
  }

  private Button addCheckbox(
      Composite parent,
      Control below,
      String idHint,
      ModifyListener lsMod,
      int middle,
      int margin) {
    Label lbl = new Label(parent, SWT.RIGHT);
    lbl.setText(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog." + idHint + ".Label"));
    PropsUi.setLook(lbl);
    FormData fdLbl = new FormData();
    fdLbl.left = new FormAttachment(0, 0);
    fdLbl.right = new FormAttachment(middle, -margin);
    fdLbl.top = below == null ? new FormAttachment(0, margin) : new FormAttachment(below, margin);
    lbl.setLayoutData(fdLbl);

    Button b = new Button(parent, SWT.CHECK);
    PropsUi.setLook(b);
    FormData fdB = new FormData();
    fdB.left = new FormAttachment(middle, 0);
    fdB.right = new FormAttachment(100, 0);
    fdB.top = below == null ? new FormAttachment(0, margin) : new FormAttachment(below, margin);
    b.setLayoutData(fdB);
    b.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            if (lsMod != null) {
              lsMod.modifyText(null);
            }
          }
        });
    return b;
  }

  /** Workaround to keep tab fonts consistent across the dialog. */
  private static org.eclipse.swt.graphics.Font GuiResource(Shell shell) {
    return shell.getFont();
  }

  // ---------------------------------------------------------------------------
  // Specify-format <-> add-date/time conditional logic
  // ---------------------------------------------------------------------------

  private void setSpecifyFormatVisibility() {
    int idx =
        wOperationType != null && !wOperationType.isDisposed()
            ? wOperationType.getSelectionIndex()
            : 0;
    if (idx < 0) {
      idx = 0;
    }
    boolean needFile = idx == 0 || idx == 2;
    boolean specify = wSpecifyFormat != null && wSpecifyFormat.getSelection();
    if (wDateTimeFormat != null) {
      wDateTimeFormat.setEnabled(needFile && specify);
    }
    if (wAddDate != null) {
      wAddDate.setEnabled(needFile && !specify);
    }
    if (wAddTime != null) {
      wAddTime.setEnabled(needFile && !specify);
    }
  }

  private void ensureEncodingsLoaded() {
    if (encodingsLoaded || wEncoding.isDisposed()) {
      return;
    }
    encodingsLoaded = true;
    List<String> encs = new ArrayList<>(Charset.availableCharsets().keySet());
    java.util.Collections.sort(encs);
    String current = wEncoding.getText();
    for (String e : encs) {
      wEncoding.add(e);
    }
    if (!Utils.isEmpty(current)) {
      int idx = wEncoding.indexOf(current);
      if (idx >= 0) {
        wEncoding.select(idx);
      } else {
        wEncoding.setText(current);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Input fields lookup (asynchronous, like the existing transform dialogs)
  // ---------------------------------------------------------------------------

  private void populateInputFieldsAsync() {
    Runnable r =
        () -> {
          TransformMeta tm = pipelineMeta.findTransform(transformName);
          if (tm == null) {
            return;
          }
          try {
            IRowMeta row = pipelineMeta.getPrevTransformFields(variables, tm);
            inputFieldNames.clear();
            if (row != null) {
              for (int i = 0; i < row.size(); i++) {
                inputFieldNames.add(row.getValueMeta(i).getName());
              }
            }
            Display.getDefault().asyncExec(this::pushInputFieldsToDesigner);
          } catch (HopException e) {
            logError(BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.ErrorGettingFields"), e);
          }
        };
    new Thread(r, "AdvancedXMLOutput-FieldLookup").start();
  }

  /** Synchronous re-fetch triggered by the "Get fields" button. */
  private void reloadInputFieldsBlocking() {
    TransformMeta tm = pipelineMeta.findTransform(transformName);
    if (tm == null) {
      return;
    }
    try {
      IRowMeta row = pipelineMeta.getPrevTransformFields(variables, tm);
      inputFieldNames.clear();
      if (row != null) {
        for (int i = 0; i < row.size(); i++) {
          inputFieldNames.add(row.getValueMeta(i).getName());
        }
      }
      pushInputFieldsToDesigner();
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.ErrorGettingFields.Title"),
          BaseMessages.getString(PKG, "AdvancedXMLOutputDialog.ErrorGettingFields"),
          e);
    }
  }

  private void pushInputFieldsToDesigner() {
    if (wTreeDesigner != null && !wTreeDesigner.isDisposed()) {
      wTreeDesigner.setInputFields(new ArrayList<>(inputFieldNames));
    }
  }

  // ---------------------------------------------------------------------------
  // Meta <-> dialog data binding
  // ---------------------------------------------------------------------------

  /** Copies the data from the {@link AdvancedXmlOutputMeta} into the dialog widgets. */
  public void getData() {
    XmlFileOutputSupport f = input.getFileSupport();
    if (f == null) {
      f = new XmlFileOutputSupport();
      input.setFileSupport(f);
    }
    wFilename.setText(Const.NVL(f.getFileName(), ""));
    wExtension.setText(Const.NVL(f.getExtension(), "xml"));
    wOperationType.select(0);
    AdvancedXmlOutputMeta.XmlOutputOperation currentOp = input.getOperationType();
    for (int i = 0; i < OPERATION_ORDER.length; i++) {
      if (OPERATION_ORDER[i] == currentOp) {
        wOperationType.select(i);
        break;
      }
    }
    wOutputXmlField.setText(Const.NVL(input.getOutputXmlField(), "outputXml"));
    wIncludeInputFieldsInOutput.setSelection(input.isIncludeInputFieldsInOutput());
    wAddTransformnr.setSelection(f.isTransformNrInFilename());
    wAddDate.setSelection(f.isDateInFilename());
    wAddTime.setSelection(f.isTimeInFilename());
    wSpecifyFormat.setSelection(f.isSpecifyFormat());
    wDateTimeFormat.setText(Const.NVL(f.getDateTimeFormat(), ""));
    wSplitEvery.setText(f.getSplitEvery() > 0 ? Integer.toString(f.getSplitEvery()) : "");
    wZipped.setSelection(f.isZipped());
    wDoNotOpenAtInit.setSelection(f.isDoNotOpenNewFileInit());
    wDoNotCreateEmptyFile.setSelection(f.isDoNotCreateEmptyFile());
    wAddToResult.setSelection(f.isAddToResultFilenames());

    String enc = Const.NVL(input.getEncoding(), "UTF-8");
    wEncoding.setText(enc);

    wCompactFile.setSelection(input.isCompactFile());
    wBlankLineAfterDecl.setSelection(input.isBlankLineAfterXmlDeclaration());
    wCreateEmptyElement.setSelection(input.isCreateEmptyElement());
    wCreateAttributeIfNull.setSelection(input.isCreateAttributeIfNull());
    wCreateAttributeIfUnmapped.setSelection(input.isCreateAttributeIfUnmapped());
    wTrim.setSelection(input.isTrimValues());
    wDefaultDecimal.setText(Const.NVL(input.getDefaultDecimalSeparator(), ""));
    wDefaultGroup.setText(Const.NVL(input.getDefaultGroupingSeparator(), ""));
    wGenerateXsd.setSelection(input.isGenerateXsd());
    wDoctypeRoot.setText(Const.NVL(input.getDoctypeRootElement(), ""));
    wDoctypeSystem.setText(Const.NVL(input.getDoctypeSystemId(), ""));
    wDoctypePublic.setText(Const.NVL(input.getDoctypePublicId(), ""));
    wXslHref.setText(Const.NVL(input.getXslStylesheetHref(), ""));
    wXslType.setText(Const.NVL(input.getXslStylesheetType(), ""));

    XmlNode root =
        input.getRootNode() != null ? new XmlNode(input.getRootNode()) : defaultRootNode();
    wTreeDesigner.setRootNode(root);
    updateFileWidgetsForOperation();
  }

  private static XmlNode defaultRootNode() {
    XmlNode root = new XmlNode("Rows", XmlNode.NodeKind.Element);
    XmlNode loop = new XmlNode("Row", XmlNode.NodeKind.Element);
    loop.setLoop(true);
    root.addChild(loop);
    return root;
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

    int oxi = wOperationType.getSelectionIndex();
    input.setOperationType(
        oxi >= 0 && oxi < OPERATION_ORDER.length
            ? OPERATION_ORDER[oxi]
            : AdvancedXmlOutputMeta.XmlOutputOperation.WRITE_TO_FILE);
    input.setOutputXmlField(wOutputXmlField.getText());
    input.setIncludeInputFieldsInOutput(wIncludeInputFieldsInOutput.getSelection());

    XmlFileOutputSupport f = input.getFileSupport();
    if (f == null) {
      f = new XmlFileOutputSupport();
      input.setFileSupport(f);
    }
    f.setFileName(wFilename.getText());
    f.setExtension(wExtension.getText());
    f.setTransformNrInFilename(wAddTransformnr.getSelection());
    f.setDateInFilename(wAddDate.getSelection());
    f.setTimeInFilename(wAddTime.getSelection());
    f.setSpecifyFormat(wSpecifyFormat.getSelection());
    f.setDateTimeFormat(wDateTimeFormat.getText());
    f.setSplitEvery(parsePositiveInt(wSplitEvery.getText(), 0));
    f.setZipped(wZipped.getSelection());
    f.setDoNotOpenNewFileInit(wDoNotOpenAtInit.getSelection());
    f.setDoNotCreateEmptyFile(wDoNotCreateEmptyFile.getSelection());
    f.setAddToResultFilenames(wAddToResult.getSelection());

    input.setEncoding(wEncoding.getText());
    input.setCompactFile(wCompactFile.getSelection());
    input.setBlankLineAfterXmlDeclaration(wBlankLineAfterDecl.getSelection());
    input.setCreateEmptyElement(wCreateEmptyElement.getSelection());
    input.setCreateAttributeIfNull(wCreateAttributeIfNull.getSelection());
    input.setCreateAttributeIfUnmapped(wCreateAttributeIfUnmapped.getSelection());
    input.setTrimValues(wTrim.getSelection());
    input.setDefaultDecimalSeparator(wDefaultDecimal.getText());
    input.setDefaultGroupingSeparator(wDefaultGroup.getText());
    input.setGenerateXsd(wGenerateXsd.getSelection());
    input.setDoctypeRootElement(wDoctypeRoot.getText());
    input.setDoctypeSystemId(wDoctypeSystem.getText());
    input.setDoctypePublicId(wDoctypePublic.getText());
    input.setXslStylesheetHref(wXslHref.getText());
    input.setXslStylesheetType(wXslType.getText());

    XmlNode designed = wTreeDesigner.getRootNode();
    input.setRootNode(designed != null ? new XmlNode(designed) : null);

    input.setChanged();
    dispose();
  }

  private static int parsePositiveInt(String s, int dflt) {
    if (s == null || s.isBlank()) {
      return dflt;
    }
    try {
      int v = Integer.parseInt(s.trim());
      return v < 0 ? dflt : v;
    } catch (NumberFormatException e) {
      return dflt;
    }
  }
}
