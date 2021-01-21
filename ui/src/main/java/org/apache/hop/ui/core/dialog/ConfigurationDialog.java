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

package org.apache.hop.ui.core.dialog;

import org.apache.hop.IExecutionConfiguration;
import org.apache.hop.base.AbstractMeta;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.HelpUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class ConfigurationDialog extends Dialog {

  protected AbstractMeta abstractMeta;
  protected IExecutionConfiguration configuration;
  protected TableView wVariables;
  protected boolean retval;
  protected Shell shell;
  protected PropsUi props;
  protected Label wlLogLevel;
  protected Group gDetails;
  protected CCombo wLogLevel;
  protected Button wClearLog;
  protected int margin;
  protected Composite composite;
  protected Control wRunConfigurationControl;

  private TableView wParams;
  private Display display;
  private Shell parent;
  private Button wOk;
  private Button wCancel;
  protected FormData fdDetails;
  private FormData fdTabFolder;
  private CTabFolder tabFolder;
  private Button alwaysShowOption;

  protected HopGui hopGui;

  public ConfigurationDialog(
      Shell parent, IExecutionConfiguration configuration, AbstractMeta meta) {
    super(parent);
    this.parent = parent;
    this.configuration = configuration;
    this.abstractMeta = meta;

    this.hopGui = HopGui.getInstance();

    // Fill the parameters, maybe do this in another place?
    Map<String, String> params = configuration.getParametersMap();
    params.clear();
    String[] paramNames = meta.listParameters();
    for (String name : paramNames) {
      params.put(name, "");
    }

    props = PropsUi.getInstance();
    margin = props.getMargin();
  }

  protected void getInfoVariables() {
    Map<String, String> map = new HashMap<>();
    int nrNonEmptyVariables = wVariables.nrNonEmpty();
    for (int i = 0; i < nrNonEmptyVariables; i++) {
      TableItem tableItem = wVariables.getNonEmpty(i);
      String varName = tableItem.getText(1);
      String varValue = tableItem.getText(2);

      if (!Utils.isEmpty(varName)) {
        map.put(varName, varValue);
      }
    }
    configuration.setVariablesMap(map);
  }

  /** Get the parameters from the dialog. */
  protected void getInfoParameters() {
    Map<String, String> map = new HashMap<>();
    int nrNonEmptyVariables = wParams.nrNonEmpty();
    for (int i = 0; i < nrNonEmptyVariables; i++) {
      TableItem tableItem = wParams.getNonEmpty(i);
      String paramName = tableItem.getText(1);
      String defaultValue = tableItem.getText(2);
      String paramValue = tableItem.getText(3);

      if (Utils.isEmpty(paramValue)) {
        paramValue = Const.NVL(defaultValue, "");
      }

      map.put(paramName, paramValue);
    }
    configuration.setParametersMap(map);
  }

  protected void ok() {
    abstractMeta.setAlwaysShowRunOptions(alwaysShowOption.getSelection());
    abstractMeta.setShowDialog(alwaysShowOption.getSelection());
    if (Const.isOSX()) {
      // OSX bug workaround.
      wVariables.applyOSXChanges();
      wParams.applyOSXChanges();
    }
    if (getInfo()) {
      retval = true;
      dispose();
    }
  }

  private void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  protected void cancel() {
    dispose();
  }

  /** @return True if all is OK. Returns false if there is an error in the configuration */
  public abstract boolean getInfo();

  protected void getParamsData() {
    wParams.clearAll(false);
    ArrayList<String> paramNames = new ArrayList<>(configuration.getParametersMap().keySet());
    Collections.sort(paramNames);

    for (int i = 0; i < paramNames.size(); i++) {
      String paramName = paramNames.get(i);
      String paramValue = configuration.getParametersMap().get(paramName);
      String defaultValue;
      try {
        defaultValue = abstractMeta.getParameterDefault(paramName);
      } catch (UnknownParamException e) {
        defaultValue = "";
      }

      String description;
      try {
        description = abstractMeta.getParameterDescription(paramName);
      } catch (UnknownParamException e) {
        description = "";
      }

      TableItem tableItem = new TableItem(wParams.table, SWT.NONE);
      tableItem.setText(1, paramName);
      tableItem.setText(2, Const.NVL(defaultValue, ""));
      tableItem.setText(3, Const.NVL(paramValue, ""));
      tableItem.setText(4, Const.NVL(description, ""));
    }
    wParams.removeEmptyRows();
    wParams.setRowNums();
    wParams.optWidth(true);
  }

  /** @param configuration the configuration to set */
  public void setConfiguration(IExecutionConfiguration configuration) {
    this.configuration = configuration;
  }

  protected void mainLayout(String shellTitle, Image img) {
    display = parent.getDisplay();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX);
    props.setLook(shell);
    shell.setImage(img);
    shell.setText(shellTitle);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN * 2;
    formLayout.marginHeight = Const.FORM_MARGIN * 2;
    shell.setLayout(formLayout);
  }

  protected void optionsSectionLayout(Class<?> PKG, String prefix) {
    gDetails = new Group(shell, SWT.SHADOW_ETCHED_IN);
    gDetails.setText(BaseMessages.getString(PKG, prefix + ".DetailsGroup.Label"));
    props.setLook(gDetails);

    // The layout
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN * 2;
    formLayout.marginHeight = Const.FORM_MARGIN * 2;
    gDetails.setLayout(formLayout);
    fdDetails = new FormData();
    fdDetails.top = new FormAttachment(wRunConfigurationControl, 15);
    fdDetails.right = new FormAttachment(100, 0);
    fdDetails.left = new FormAttachment(0, 0);
    gDetails.setBackground(shell.getBackground()); // the default looks ugly
    gDetails.setLayoutData(fdDetails);

    optionsSectionControls();
  }

  protected void parametersSectionLayout(Class<?> PKG, String prefix) {

    tabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(tabFolder, Props.WIDGET_STYLE_TAB);
    fdTabFolder = new FormData();
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(gDetails, 15);
    fdTabFolder.bottom = new FormAttachment(alwaysShowOption, -15);
    tabFolder.setLayoutData(fdTabFolder);

    // Parameters
    CTabItem tbtmParameters = new CTabItem(tabFolder, SWT.NONE);
    tbtmParameters.setText(BaseMessages.getString(PKG, prefix + ".Params.Label"));
    Composite parametersComposite = new Composite(tabFolder, SWT.NONE);
    props.setLook(parametersComposite);

    parametersComposite.setLayout(new FormLayout());
    tbtmParameters.setControl(parametersComposite);

    ColumnInfo[] cParams = {
      new ColumnInfo(
          BaseMessages.getString(PKG, prefix + ".ParamsColumn.Argument"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true), // TransformName
      new ColumnInfo(
          BaseMessages.getString(PKG, prefix + ".ParamsColumn.Default"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true), // Preview size
      new ColumnInfo(
          BaseMessages.getString(PKG, prefix + ".ParamsColumn.Value"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          false), // Preview size
      new ColumnInfo(
          BaseMessages.getString(PKG, prefix + ".ParamsColumn.Description"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true), // Preview size
    };

    String[] namedParams = abstractMeta.listParameters();
    int nrParams = namedParams.length;
    wParams =
        new TableView(
            hopGui.getVariables(),
            parametersComposite,
            SWT.FULL_SELECTION | SWT.MULTI,
            cParams,
            nrParams,
            false,
            null,
            props,
            false);
    FormData fdParams = new FormData();
    fdParams.top = new FormAttachment(0, 0);
    fdParams.right = new FormAttachment(100, 0);
    fdParams.bottom = new FormAttachment(100, 0);
    fdParams.left = new FormAttachment(0, 0);
    wParams.setLayoutData(fdParams);

    tabFolder.setSelection(0);

    // Variables
    CTabItem tbtmVariables = new CTabItem(tabFolder, SWT.NONE);
    tbtmVariables.setText(BaseMessages.getString(PKG, prefix + ".Variables.Label"));

    Composite variablesComposite = new Composite(tabFolder, SWT.NONE);
    props.setLook(variablesComposite);
    variablesComposite.setLayout(new FormLayout());
    tbtmVariables.setControl(variablesComposite);

    ColumnInfo[] cVariables = {
      new ColumnInfo(
          BaseMessages.getString(PKG, prefix + ".VariablesColumn.Argument"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          false), // TransformName
      new ColumnInfo(
          BaseMessages.getString(PKG, prefix + ".VariablesColumn.Value"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          false), // Preview size
    };

    int nrVariables =
        configuration.getVariablesMap() != null ? configuration.getVariablesMap().size() : 0;
    wVariables =
        new TableView(
            hopGui.getVariables(),
            variablesComposite,
            SWT.FULL_SELECTION | SWT.MULTI,
            cVariables,
            nrVariables,
            false,
            null,
            props,
            false);

    FormData fdVariables = new FormData();
    fdVariables.top = new FormAttachment(0, 0);
    fdVariables.right = new FormAttachment(100, 0);
    fdVariables.bottom = new FormAttachment(100, 0);
    fdVariables.left = new FormAttachment(0, 0);

    wVariables.setLayoutData(fdVariables);
  }

  protected void buttonsSectionLayout(
      String alwaysShowOptionLabel,
      String alwaysShowOptionTooltip,
      final String docTitle,
      final String docUrl,
      final String docHeader) {

    // Bottom buttons and separator

    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString("System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());

    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString("System.Button.Launch"));
    wOk.addListener(SWT.Selection, e -> ok());

    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    Button wbHelp = new Button(shell, SWT.NONE);
    wbHelp.setImage(GuiResource.getInstance().getImageHelpWeb());
    wbHelp.setText(BaseMessages.getString("System.Button.Help"));
    wbHelp.setToolTipText(BaseMessages.getString("System.Tooltip.Help"));
    FormData fdbHelp = new FormData();
    fdbHelp.bottom = new FormAttachment(100, 0);
    fdbHelp.left = new FormAttachment(0, 0);
    wbHelp.setLayoutData(fdbHelp);
    wbHelp.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent evt) {
            HelpUtils.openHelpDialog(parent.getShell(), docTitle, docUrl, docHeader);
          }
        });

    Label separator = new Label(shell, SWT.SEPARATOR | SWT.HORIZONTAL);
    FormData fdSeparator = new FormData();
    fdSeparator.right = new FormAttachment(100, 0);
    fdSeparator.left = new FormAttachment(0, 0);
    fdSeparator.bottom = new FormAttachment(wOk, -2 * margin);
    separator.setLayoutData(fdSeparator);

    alwaysShowOption = new Button(shell, SWT.CHECK);
    alwaysShowOption.setText(alwaysShowOptionLabel);
    alwaysShowOption.setToolTipText(alwaysShowOptionTooltip);
    props.setLook(alwaysShowOption);
    alwaysShowOption.setSelection(abstractMeta.isAlwaysShowRunOptions());

    FormData fdAlwaysShowOption = new FormData();
    fdAlwaysShowOption.left = new FormAttachment(0, 0);
    fdAlwaysShowOption.bottom = new FormAttachment(separator, -15);
    alwaysShowOption.setLayoutData(fdAlwaysShowOption);
  }

  protected void openDialog() {

    BaseTransformDialog.setSize(shell);

    // Set the focus on the OK button
    shell.setDefaultButton(wOk);

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
  }

  protected abstract void optionsSectionControls();
}
