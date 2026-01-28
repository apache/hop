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

package org.apache.hop.pipeline.transforms.fuzzymatch;

import static org.apache.hop.pipeline.transforms.fuzzymatch.FuzzyMatchMeta.Algorithm.DAMERAU_LEVENSHTEIN;
import static org.apache.hop.pipeline.transforms.fuzzymatch.FuzzyMatchMeta.Algorithm.DOUBLE_METAPHONE;
import static org.apache.hop.pipeline.transforms.fuzzymatch.FuzzyMatchMeta.Algorithm.JARO;
import static org.apache.hop.pipeline.transforms.fuzzymatch.FuzzyMatchMeta.Algorithm.JARO_WINKLER;
import static org.apache.hop.pipeline.transforms.fuzzymatch.FuzzyMatchMeta.Algorithm.LEVENSHTEIN;
import static org.apache.hop.pipeline.transforms.fuzzymatch.FuzzyMatchMeta.Algorithm.METAPHONE;
import static org.apache.hop.pipeline.transforms.fuzzymatch.FuzzyMatchMeta.Algorithm.NEEDLEMAN_WUNSH;
import static org.apache.hop.pipeline.transforms.fuzzymatch.FuzzyMatchMeta.Algorithm.PAIR_SIMILARITY;
import static org.apache.hop.pipeline.transforms.fuzzymatch.FuzzyMatchMeta.Algorithm.REFINED_SOUNDEX;
import static org.apache.hop.pipeline.transforms.fuzzymatch.FuzzyMatchMeta.Algorithm.SOUNDEX;
import static org.apache.hop.pipeline.transforms.fuzzymatch.FuzzyMatchMeta.Algorithm.getDescriptions;
import static org.apache.hop.pipeline.transforms.fuzzymatch.FuzzyMatchMeta.Algorithm.lookupDescription;

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
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class FuzzyMatchDialog extends BaseTransformDialog {
  private static final Class<?> PKG = FuzzyMatchMeta.class;

  private CCombo wTransform;

  private CCombo wAlgorithm;

  private ComboVar wMainStreamField;

  private ComboVar wLookupField;

  private ColumnInfo[] ciReturn;
  private Label wlReturn;
  private TableView wReturn;

  private TextVar wMatchField;

  private Label wlValueField;
  private TextVar wValueField;

  private Label wlCaseSensitive;
  private Button wCaseSensitive;

  private Label wlGetCloserValue;
  private Button wGetCloserValue;

  private Label wlMinValue;
  private TextVar wMinValue;

  private Label wlMaxValue;
  private TextVar wMaxValue;

  private Label wlSeparator;
  private TextVar wSeparator;

  private Button wGetLookup;

  private final FuzzyMatchMeta input;
  private boolean gotPreviousFields = false;
  private boolean gotLookupFields = false;

  public FuzzyMatchDialog(
      Shell parent, IVariables variables, FuzzyMatchMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "FuzzyMatchDialog.Shell.Title"));

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

    Composite wContent = new Composite(wScrolledComposite, SWT.NONE);
    PropsUi.setLook(wContent);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = PropsUi.getFormMargin();
    contentLayout.marginHeight = PropsUi.getFormMargin();
    wContent.setLayout(contentLayout);

    Label wContentTop = new Label(wContent, SWT.NONE);
    wContentTop.setLayoutData(new FormData(0, 0));

    CTabFolder wTabFolder = new CTabFolder(wContent, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF General TAB ///
    // ////////////////////////
    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setFont(GuiResource.getInstance().getFontDefault());
    wGeneralTab.setText(BaseMessages.getString(PKG, "FuzzyMatchDialog.General.Tab"));

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wGeneralComp);

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout(generalLayout);

    // /////////////////////////////////
    // START OF Lookup Fields GROUP
    // /////////////////////////////////

    Group wLookupGroup = new Group(wGeneralComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wLookupGroup);
    wLookupGroup.setText(BaseMessages.getString(PKG, "FuzzyMatchDialog.Group.Lookup.Label"));

    FormLayout lookupGroupLayout = new FormLayout();
    lookupGroupLayout.marginWidth = 10;
    lookupGroupLayout.marginHeight = 10;
    wLookupGroup.setLayout(lookupGroupLayout);

    // Source transform line...
    Label wlTransform = new Label(wLookupGroup, SWT.RIGHT);
    wlTransform.setText(BaseMessages.getString(PKG, "FuzzyMatchDialog.SourceTransform.Label"));
    PropsUi.setLook(wlTransform);
    FormData fdlTransform = new FormData();
    fdlTransform.left = new FormAttachment(0, 0);
    fdlTransform.right = new FormAttachment(middle, -margin);
    fdlTransform.top = new FormAttachment(wSpacer, margin);
    wlTransform.setLayoutData(fdlTransform);
    wTransform = new CCombo(wLookupGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTransform);

    List<TransformMeta> transforms =
        pipelineMeta.findPreviousTransforms(pipelineMeta.findTransform(transformName), true);
    for (TransformMeta transformMeta : transforms) {
      wTransform.add(transformMeta.getName());
    }

    wTransform.addListener(SWT.Selection, e -> setComboBoxesLookup());

    FormData fdTransform = new FormData();
    fdTransform.left = new FormAttachment(middle, 0);
    fdTransform.top = new FormAttachment(wSpacer, margin);
    fdTransform.right = new FormAttachment(100, 0);
    wTransform.setLayoutData(fdTransform);

    // LookupField
    Label wlLookupField = new Label(wLookupGroup, SWT.RIGHT);
    wlLookupField.setText(BaseMessages.getString(PKG, "FuzzyMatchDialog.wlLookupField.Label"));
    PropsUi.setLook(wlLookupField);
    FormData fdlLookupField = new FormData();
    fdlLookupField.left = new FormAttachment(0, 0);
    fdlLookupField.top = new FormAttachment(wTransform, margin);
    fdlLookupField.right = new FormAttachment(middle, -margin);
    wlLookupField.setLayoutData(fdlLookupField);

    wLookupField = new ComboVar(variables, wLookupGroup, SWT.BORDER | SWT.READ_ONLY);
    wLookupField.setEditable(true);
    PropsUi.setLook(wLookupField);
    FormData fdLookupField = new FormData();
    fdLookupField.left = new FormAttachment(middle, 0);
    fdLookupField.top = new FormAttachment(wTransform, margin);
    fdLookupField.right = new FormAttachment(100, -margin);
    wLookupField.setLayoutData(fdLookupField);
    wLookupField.addListener(SWT.FocusIn, e -> setLookupField());

    FormData fdLookupGroup = new FormData();
    fdLookupGroup.left = new FormAttachment(0, margin);
    fdLookupGroup.top = new FormAttachment(wSpacer, margin);
    fdLookupGroup.right = new FormAttachment(100, -margin);
    wLookupGroup.setLayoutData(fdLookupGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF Lookup GROUP
    // ///////////////////////////////////////////////////////////

    // /////////////////////////////////
    // START OF MainStream Fields GROUP
    // /////////////////////////////////

    Group wMainStreamGroup = new Group(wGeneralComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wMainStreamGroup);
    wMainStreamGroup.setText(
        BaseMessages.getString(PKG, "FuzzyMatchDialog.Group.MainStreamGroup.Label"));

    FormLayout mainStreamGroupLayout = new FormLayout();
    mainStreamGroupLayout.marginWidth = 10;
    mainStreamGroupLayout.marginHeight = 10;
    wMainStreamGroup.setLayout(mainStreamGroupLayout);

    // MainStreamFieldName field
    Label wlMainStreamField = new Label(wMainStreamGroup, SWT.RIGHT);
    wlMainStreamField.setText(
        BaseMessages.getString(PKG, "FuzzyMatchDialog.wlMainStreamField.Label"));
    PropsUi.setLook(wlMainStreamField);
    FormData fdlMainStreamField = new FormData();
    fdlMainStreamField.left = new FormAttachment(0, 0);
    fdlMainStreamField.top = new FormAttachment(wLookupGroup, margin);
    fdlMainStreamField.right = new FormAttachment(middle, -margin);
    wlMainStreamField.setLayoutData(fdlMainStreamField);

    wMainStreamField = new ComboVar(variables, wMainStreamGroup, SWT.BORDER | SWT.READ_ONLY);
    wMainStreamField.setEditable(true);
    PropsUi.setLook(wMainStreamField);
    FormData fdMainStreamField = new FormData();
    fdMainStreamField.left = new FormAttachment(middle, 0);
    fdMainStreamField.top = new FormAttachment(wLookupGroup, margin);
    fdMainStreamField.right = new FormAttachment(100, -margin);
    wMainStreamField.setLayoutData(fdMainStreamField);
    wMainStreamField.addListener(SWT.FocusIn, e -> setMainStreamField());

    FormData fdMainStreamGroup = new FormData();
    fdMainStreamGroup.left = new FormAttachment(0, margin);
    fdMainStreamGroup.top = new FormAttachment(wLookupGroup, margin);
    fdMainStreamGroup.right = new FormAttachment(100, -margin);
    wMainStreamGroup.setLayoutData(fdMainStreamGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF MainStream GROUP
    // ///////////////////////////////////////////////////////////

    // /////////////////////////////////
    // START OF Settings Fields GROUP
    // /////////////////////////////////

    Group wSettingsGroup = new Group(wGeneralComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wSettingsGroup);
    wSettingsGroup.setText(
        BaseMessages.getString(PKG, "FuzzyMatchDialog.Group.SettingsGroup.Label"));

    FormLayout settingsGroupLayout = new FormLayout();
    settingsGroupLayout.marginWidth = 10;
    settingsGroupLayout.marginHeight = 10;
    wSettingsGroup.setLayout(settingsGroupLayout);

    // Algorithm
    Label wlAlgorithm = new Label(wSettingsGroup, SWT.RIGHT);
    wlAlgorithm.setText(BaseMessages.getString(PKG, "FuzzyMatchDialog.Algorithm.Label"));
    PropsUi.setLook(wlAlgorithm);
    FormData fdlAlgorithm = new FormData();
    fdlAlgorithm.left = new FormAttachment(0, 0);
    fdlAlgorithm.right = new FormAttachment(middle, -margin);
    fdlAlgorithm.top = new FormAttachment(wMainStreamGroup, margin);
    wlAlgorithm.setLayoutData(fdlAlgorithm);

    wAlgorithm = new CCombo(wSettingsGroup, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wAlgorithm);
    FormData fdAlgorithm = new FormData();
    fdAlgorithm.left = new FormAttachment(middle, 0);
    fdAlgorithm.top = new FormAttachment(wMainStreamGroup, margin);
    fdAlgorithm.right = new FormAttachment(100, -margin);
    wAlgorithm.setLayoutData(fdAlgorithm);
    wAlgorithm.setItems(getDescriptions());
    wAlgorithm.addListener(SWT.Selection, e -> activeAlgorithm());

    // Is case-sensitive
    wlCaseSensitive = new Label(wSettingsGroup, SWT.RIGHT);
    wlCaseSensitive.setText(BaseMessages.getString(PKG, "FuzzyMatchDialog.caseSensitive.Label"));
    PropsUi.setLook(wlCaseSensitive);
    FormData fdlCaseSensitive = new FormData();
    fdlCaseSensitive.left = new FormAttachment(0, 0);
    fdlCaseSensitive.top = new FormAttachment(wAlgorithm, margin);
    fdlCaseSensitive.right = new FormAttachment(middle, -margin);
    wlCaseSensitive.setLayoutData(fdlCaseSensitive);

    wCaseSensitive = new Button(wSettingsGroup, SWT.CHECK);
    PropsUi.setLook(wCaseSensitive);
    wCaseSensitive.setToolTipText(
        BaseMessages.getString(PKG, "FuzzyMatchDialog.caseSensitive.Tooltip"));
    FormData fdcaseSensitive = new FormData();
    fdcaseSensitive.left = new FormAttachment(middle, 0);
    fdcaseSensitive.top = new FormAttachment(wlCaseSensitive, 0, SWT.CENTER);
    wCaseSensitive.setLayoutData(fdcaseSensitive);

    // Is get closer value
    wlGetCloserValue = new Label(wSettingsGroup, SWT.RIGHT);
    wlGetCloserValue.setText(BaseMessages.getString(PKG, "FuzzyMatchDialog.getCloserValue.Label"));
    PropsUi.setLook(wlGetCloserValue);
    FormData fdlGetCloserValue = new FormData();
    fdlGetCloserValue.left = new FormAttachment(0, 0);
    fdlGetCloserValue.top = new FormAttachment(wCaseSensitive, margin);
    fdlGetCloserValue.right = new FormAttachment(middle, -margin);
    wlGetCloserValue.setLayoutData(fdlGetCloserValue);

    wGetCloserValue = new Button(wSettingsGroup, SWT.CHECK);
    PropsUi.setLook(wGetCloserValue);
    wGetCloserValue.setToolTipText(
        BaseMessages.getString(PKG, "FuzzyMatchDialog.getCloserValue.Tooltip"));
    FormData fdgetCloserValue = new FormData();
    fdgetCloserValue.left = new FormAttachment(middle, 0);
    fdgetCloserValue.top = new FormAttachment(wlGetCloserValue, 0, SWT.CENTER);
    wGetCloserValue.setLayoutData(fdgetCloserValue);
    wGetCloserValue.addListener(SWT.Selection, e -> activeGetCloserValue());

    wlMinValue = new Label(wSettingsGroup, SWT.RIGHT);
    wlMinValue.setText(BaseMessages.getString(PKG, "FuzzyMatchDialog.minValue.Label"));
    PropsUi.setLook(wlMinValue);
    FormData fdlminValue = new FormData();
    fdlminValue.left = new FormAttachment(0, 0);
    fdlminValue.top = new FormAttachment(wGetCloserValue, margin);
    fdlminValue.right = new FormAttachment(middle, -margin);
    wlMinValue.setLayoutData(fdlminValue);
    wMinValue = new TextVar(variables, wSettingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMinValue);
    wMinValue.setToolTipText(BaseMessages.getString(PKG, "FuzzyMatchDialog.minValue.Tooltip"));
    FormData fdminValue = new FormData();
    fdminValue.left = new FormAttachment(middle, 0);
    fdminValue.top = new FormAttachment(wGetCloserValue, margin);
    fdminValue.right = new FormAttachment(100, 0);
    wMinValue.setLayoutData(fdminValue);

    wlMaxValue = new Label(wSettingsGroup, SWT.RIGHT);
    wlMaxValue.setText(BaseMessages.getString(PKG, "FuzzyMatchDialog.maxValue.Label"));
    PropsUi.setLook(wlMaxValue);
    FormData fdlmaxValue = new FormData();
    fdlmaxValue.left = new FormAttachment(0, 0);
    fdlmaxValue.top = new FormAttachment(wMinValue, margin);
    fdlmaxValue.right = new FormAttachment(middle, -margin);
    wlMaxValue.setLayoutData(fdlmaxValue);
    wMaxValue = new TextVar(variables, wSettingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMaxValue);
    wMaxValue.setToolTipText(BaseMessages.getString(PKG, "FuzzyMatchDialog.maxValue.Tooltip"));
    FormData fdmaxValue = new FormData();
    fdmaxValue.left = new FormAttachment(middle, 0);
    fdmaxValue.top = new FormAttachment(wMinValue, margin);
    fdmaxValue.right = new FormAttachment(100, 0);
    wMaxValue.setLayoutData(fdmaxValue);

    wlSeparator = new Label(wSettingsGroup, SWT.RIGHT);
    wlSeparator.setText(BaseMessages.getString(PKG, "FuzzyMatchDialog.separator.Label"));
    PropsUi.setLook(wlSeparator);
    FormData fdlSeparator = new FormData();
    fdlSeparator.left = new FormAttachment(0, 0);
    fdlSeparator.top = new FormAttachment(wMaxValue, margin);
    fdlSeparator.right = new FormAttachment(middle, -margin);
    wlSeparator.setLayoutData(fdlSeparator);
    wSeparator = new TextVar(variables, wSettingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSeparator);
    FormData fdSeparator = new FormData();
    fdSeparator.left = new FormAttachment(middle, 0);
    fdSeparator.top = new FormAttachment(wMaxValue, margin);
    fdSeparator.right = new FormAttachment(100, 0);
    wSeparator.setLayoutData(fdSeparator);

    FormData fdSettingsGroup = new FormData();
    fdSettingsGroup.left = new FormAttachment(0, margin);
    fdSettingsGroup.top = new FormAttachment(wMainStreamGroup, margin);
    fdSettingsGroup.right = new FormAttachment(100, -margin);
    wSettingsGroup.setLayoutData(fdSettingsGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF Settings GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment(0, 0);
    fdGeneralComp.top = new FormAttachment(0, 0);
    fdGeneralComp.right = new FormAttachment(100, 0);
    fdGeneralComp.bottom = new FormAttachment(100, 0);
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);

    // ///////////////////////////////////////////////////////////
    // / END OF General TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF Fields TAB ///
    // ////////////////////////
    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setFont(GuiResource.getInstance().getFontDefault());
    wFieldsTab.setText(BaseMessages.getString(PKG, "FuzzyMatchDialog.Fields.Tab"));

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wFieldsComp);

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = 3;
    fieldsLayout.marginHeight = 3;
    wFieldsComp.setLayout(fieldsLayout);

    // /////////////////////////////////
    // START OF OutputFields Fields GROUP
    // /////////////////////////////////

    Group wOutputFieldsGroup = new Group(wFieldsComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wOutputFieldsGroup);
    wOutputFieldsGroup.setText(
        BaseMessages.getString(PKG, "FuzzyMatchDialog.Group.OutputFieldsGroup.Label"));

    FormLayout outputFieldsGroupLayout = new FormLayout();
    outputFieldsGroupLayout.marginWidth = 10;
    outputFieldsGroupLayout.marginHeight = 10;
    wOutputFieldsGroup.setLayout(outputFieldsGroupLayout);

    Label wlMatchField = new Label(wOutputFieldsGroup, SWT.RIGHT);
    wlMatchField.setText(BaseMessages.getString(PKG, "FuzzyMatchDialog.MatchField.Label"));
    PropsUi.setLook(wlMatchField);
    FormData fdlMatchField = new FormData();
    fdlMatchField.left = new FormAttachment(0, 0);
    fdlMatchField.top = new FormAttachment(wSettingsGroup, margin);
    fdlMatchField.right = new FormAttachment(middle, -margin);
    wlMatchField.setLayoutData(fdlMatchField);
    wMatchField = new TextVar(variables, wOutputFieldsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMatchField);
    FormData fdMatchField = new FormData();
    fdMatchField.left = new FormAttachment(middle, 0);
    fdMatchField.top = new FormAttachment(wSettingsGroup, margin);
    fdMatchField.right = new FormAttachment(100, 0);
    wMatchField.setLayoutData(fdMatchField);

    wlValueField = new Label(wOutputFieldsGroup, SWT.RIGHT);
    wlValueField.setText(BaseMessages.getString(PKG, "FuzzyMatchDialog.valueField.Label"));
    PropsUi.setLook(wlValueField);
    FormData fdlValueField = new FormData();
    fdlValueField.left = new FormAttachment(0, 0);
    fdlValueField.top = new FormAttachment(wMatchField, margin);
    fdlValueField.right = new FormAttachment(middle, -margin);
    wlValueField.setLayoutData(fdlValueField);
    wValueField = new TextVar(variables, wOutputFieldsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wValueField);
    wValueField.setToolTipText(BaseMessages.getString(PKG, "FuzzyMatchDialog.valueField.Tooltip"));
    FormData fdValueField = new FormData();
    fdValueField.left = new FormAttachment(middle, 0);
    fdValueField.top = new FormAttachment(wMatchField, margin);
    fdValueField.right = new FormAttachment(100, 0);
    wValueField.setLayoutData(fdValueField);

    FormData fdOutputFieldsGroup = new FormData();
    fdOutputFieldsGroup.left = new FormAttachment(0, margin);
    fdOutputFieldsGroup.top = new FormAttachment(wSettingsGroup, margin);
    fdOutputFieldsGroup.right = new FormAttachment(100, -margin);
    wOutputFieldsGroup.setLayoutData(fdOutputFieldsGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF OutputFields GROUP
    // ///////////////////////////////////////////////////////////

    // THE UPDATE/INSERT TABLE
    wlReturn = new Label(wFieldsComp, SWT.NONE);
    wlReturn.setText(BaseMessages.getString(PKG, "FuzzyMatchDialog.ReturnFields.Label"));
    PropsUi.setLook(wlReturn);
    FormData fdlReturn = new FormData();
    fdlReturn.left = new FormAttachment(0, 0);
    fdlReturn.top = new FormAttachment(wOutputFieldsGroup, margin);
    wlReturn.setLayoutData(fdlReturn);

    wGetLookup = new Button(wFieldsComp, SWT.PUSH);
    wGetLookup.setText(BaseMessages.getString(PKG, "FuzzyMatchDialog.GetLookupFields.Button"));
    FormData fdlGetLookup = new FormData();
    fdlGetLookup.top = new FormAttachment(wlReturn, margin);
    fdlGetLookup.right = new FormAttachment(100, 0);
    wGetLookup.setLayoutData(fdlGetLookup);
    wGetLookup.addListener(SWT.Selection, e -> getlookup());

    int upInsCols = 2;
    int upInsRows = input.getLookupValues().size();

    ciReturn = new ColumnInfo[upInsCols];
    ciReturn[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "FuzzyMatchDialog.ColumnInfo.FieldReturn"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciReturn[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "FuzzyMatchDialog.ColumnInfo.NewName"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);

    wReturn =
        new TableView(
            variables,
            wFieldsComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciReturn,
            upInsRows,
            null,
            props);

    FormData fdReturn = new FormData();
    fdReturn.left = new FormAttachment(0, 0);
    fdReturn.top = new FormAttachment(wlReturn, margin);
    fdReturn.right = new FormAttachment(wGetLookup, -margin);
    fdReturn.bottom = new FormAttachment(100, -3 * margin);
    wReturn.setLayoutData(fdReturn);

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment(0, 0);
    fdFieldsComp.top = new FormAttachment(0, 0);
    fdFieldsComp.right = new FormAttachment(100, 0);
    fdFieldsComp.bottom = new FormAttachment(100, 0);
    wFieldsComp.setLayoutData(fdFieldsComp);

    wFieldsComp.layout();
    wFieldsTab.setControl(wFieldsComp);

    // ///////////////////////////////////////////////////////////
    // / END OF Fields TAB
    // ///////////////////////////////////////////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wContentTop, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(100, -50);
    wTabFolder.setLayoutData(fdTabFolder);

    wTabFolder.setSelection(0);

    wScrolledComposite.setContent(wContent);
    wContent.pack();
    wScrolledComposite.setMinSize(wContent.computeSize(SWT.DEFAULT, SWT.DEFAULT));

    getData();
    setComboBoxesLookup();
    activeAlgorithm();
    activeGetCloserValue();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (isDebug()) {
      logDebug(BaseMessages.getString(PKG, "FuzzyMatchDialog.Log.GettingKeyInfo"));
    }

    if (input.getAlgorithm() != null) {
      wAlgorithm.setText(input.getAlgorithm().getDescription());
    }
    wMainStreamField.setText(Const.NVL(input.getMainStreamField(), ""));
    wLookupField.setText(Const.NVL(input.getLookupField(), ""));
    wCaseSensitive.setSelection(input.isCaseSensitive());
    wGetCloserValue.setSelection(input.isCloserValue());
    wMinValue.setText(Const.NVL(input.getMinimalValue(), ""));
    wMaxValue.setText(Const.NVL(input.getMaximalValue(), ""));
    wMatchField.setText(Const.NVL(input.getOutputMatchField(), ""));
    wValueField.setText(Const.NVL(input.getOutputValueField(), ""));
    wSeparator.setText(Const.NVL(input.getSeparator(), ""));

    for (int i = 0; i < input.getLookupValues().size(); i++) {
      FuzzyMatchMeta.FMLookupValue lookupValue = input.getLookupValues().get(i);
      TableItem item = wReturn.table.getItem(i);
      item.setText(1, Const.NVL(lookupValue.getName(), ""));
      item.setText(2, Const.NVL(lookupValue.getRename(), ""));
    }

    IStream infoStream = input.getTransformIOMeta().getInfoStreams().get(0);
    wTransform.setText(Const.NVL(infoStream.getTransformName(), ""));

    wReturn.optimizeTableView();
  }

  private void cancel() {
    transformName = null;
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    input.setMainStreamField(wMainStreamField.getText());
    input.setLookupTransformName(wTransform.getText());
    input.setLookupField(wLookupField.getText());

    input.setAlgorithm(lookupDescription(wAlgorithm.getText()));
    input.setCaseSensitive(wCaseSensitive.getSelection());
    input.setCloserValue(wGetCloserValue.getSelection());
    input.setMaximalValue(wMaxValue.getText());
    input.setMinimalValue(wMinValue.getText());

    input.setOutputMatchField(wMatchField.getText());
    input.setOutputValueField(wValueField.getText());
    input.setSeparator(wSeparator.getText());

    input.getLookupValues().clear();
    for (TableItem item : wReturn.getNonEmptyItems()) {
      FuzzyMatchMeta.FMLookupValue lookupValue = new FuzzyMatchMeta.FMLookupValue();
      lookupValue.setName(item.getText(1));
      lookupValue.setRename(item.getText(2));
      input.getLookupValues().add(lookupValue);
    }

    transformName = wTransformName.getText(); // return value
    input.setChanged();
    dispose();
  }

  private void setMainStreamField() {
    if (!gotPreviousFields) {
      String field = wMainStreamField.getText();
      try {
        wMainStreamField.removeAll();

        IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
        if (r != null) {
          wMainStreamField.setItems(r.getFieldNames());
        }
      } catch (HopException ke) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "FuzzyMatchDialog.FailedToGetFields.DialogTitle"),
            BaseMessages.getString(PKG, "FuzzyMatchDialog.FailedToGetFields.DialogMessage"),
            ke);
      }
      if (field != null) {
        wMainStreamField.setText(field);
      }
      gotPreviousFields = true;
    }
  }

  private void setLookupField() {
    if (!gotLookupFields) {
      String field = wLookupField.getText();
      try {
        wLookupField.removeAll();

        IRowMeta r = pipelineMeta.getTransformFields(variables, wTransform.getText());
        if (r != null) {
          wLookupField.setItems(r.getFieldNames());
        }
      } catch (HopException ke) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "FuzzyMatchDialog.FailedToGetLookupFields.DialogTitle"),
            BaseMessages.getString(PKG, "FuzzyMatchDialog.FailedToGetLookupFields.DialogMessage"),
            ke);
      }
      if (field != null) {
        wLookupField.setText(field);
      }
      gotLookupFields = true;
    }
  }

  private void activeGetCloserValue() {
    FuzzyMatchMeta.Algorithm algorithm = lookupDescription(wAlgorithm.getText());

    boolean enableRange =
        (algorithm == LEVENSHTEIN
                || algorithm == NEEDLEMAN_WUNSH
                || algorithm == DAMERAU_LEVENSHTEIN
                || algorithm == JARO
                || algorithm == JARO_WINKLER
                || algorithm == PAIR_SIMILARITY)
            && !wGetCloserValue.getSelection();

    wlSeparator.setEnabled(enableRange);
    wSeparator.setEnabled(enableRange);
    wlValueField.setEnabled(wGetCloserValue.getSelection());
    wValueField.setEnabled(wGetCloserValue.getSelection());

    activeAddFields();
  }

  private void activeAddFields() {
    FuzzyMatchMeta.Algorithm algorithm = lookupDescription(wAlgorithm.getText());

    boolean activate =
        wGetCloserValue.getSelection()
            || algorithm == DOUBLE_METAPHONE
            || algorithm == SOUNDEX
            || algorithm == REFINED_SOUNDEX
            || algorithm == METAPHONE;

    wlReturn.setEnabled(activate);
    wReturn.setEnabled(activate);
    wGetLookup.setEnabled(activate);
  }

  private void activeAlgorithm() {
    FuzzyMatchMeta.Algorithm algorithm = lookupDescription(wAlgorithm.getText());

    boolean enable =
        (algorithm == LEVENSHTEIN
            || algorithm == NEEDLEMAN_WUNSH
            || algorithm == DAMERAU_LEVENSHTEIN
            || algorithm == JARO
            || algorithm == JARO_WINKLER
            || algorithm == PAIR_SIMILARITY);

    wlGetCloserValue.setEnabled(enable);
    wGetCloserValue.setEnabled(enable);
    wlMinValue.setEnabled(enable);
    wMinValue.setEnabled(enable);
    wlMaxValue.setEnabled(enable);
    wMaxValue.setEnabled(enable);

    if (algorithm == JARO || algorithm == JARO_WINKLER || algorithm == PAIR_SIMILARITY) {
      if (Const.toDouble(variables.resolve(wMinValue.getText()), 0) > 1) {
        wMinValue.setText(String.valueOf(1));
      }
      if (Const.toDouble(variables.resolve(wMaxValue.getText()), 0) > 1) {
        wMaxValue.setText(String.valueOf(1));
      }
    }

    boolean enableCaseSensitive = (algorithm == LEVENSHTEIN || algorithm == DAMERAU_LEVENSHTEIN);
    wlCaseSensitive.setEnabled(enableCaseSensitive);
    wCaseSensitive.setEnabled(enableCaseSensitive);
    activeGetCloserValue();
  }

  private void getlookup() {
    try {
      String transformFrom = wTransform.getText();
      if (!Utils.isEmpty(transformFrom)) {
        IRowMeta r = pipelineMeta.getTransformFields(variables, transformFrom);
        if (r != null && !r.isEmpty()) {
          BaseTransformDialog.getFieldsFromPrevious(
              r, wReturn, 1, new int[] {1}, new int[] {4}, -1, -1, null);
        } else {
          MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
          mb.setMessage(
              BaseMessages.getString(PKG, "FuzzyMatchDialog.CouldNotFindFields.DialogMessage"));
          mb.setText(
              BaseMessages.getString(PKG, "FuzzyMatchDialog.CouldNotFindFields.DialogTitle"));
          mb.open();
        }
      } else {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(
            BaseMessages.getString(PKG, "FuzzyMatchDialog.TransformNameRequired.DialogMessage"));
        mb.setText(
            BaseMessages.getString(PKG, "FuzzyMatchDialog.TransformNameRequired.DialogTitle"));
        mb.open();
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "FuzzyMatchDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "FuzzyMatchDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  protected void setComboBoxesLookup() {
    Runnable fieldLoader =
        () -> {
          TransformMeta lookupTransformMeta = pipelineMeta.findTransform(wTransform.getText());
          if (lookupTransformMeta != null) {
            try {
              IRowMeta row = pipelineMeta.getTransformFields(variables, lookupTransformMeta);
              List<String> lookupFields = new ArrayList<>();
              // Remember these fields...
              for (int i = 0; i < row.size(); i++) {
                lookupFields.add(row.getValueMeta(i).getName());
              }

              // Something was changed in the row.
              //
              String[] fieldNames = ConstUi.sortFieldNames(lookupFields);
              // return fields
              ciReturn[0].setComboValues(fieldNames);
            } catch (HopException e) {
              logError(
                  "It was not possible to retrieve the list of fields for transform ["
                      + wTransform.getText()
                      + "]!");
            }
          }
        };
    shell.getDisplay().asyncExec(fieldLoader);
  }
}
