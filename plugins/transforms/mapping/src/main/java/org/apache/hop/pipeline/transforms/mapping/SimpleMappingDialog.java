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

package org.apache.hop.pipeline.transforms.mapping;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterMappingDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ColumnsResizer;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;
import java.util.List;

public class SimpleMappingDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = SimpleMappingMeta.class; // For Translator

  private SimpleMappingMeta mappingMeta;

  private Label wlPath;
  private TextVar wPath;

  private Button wbBrowse;

  private CTabFolder wTabFolder;

  private PipelineMeta mappingPipelineMeta = null;

  protected boolean transModified;

  private ModifyListener lsMod;

  private int middle;

  private int margin;

  private MappingParameters mappingParameters;

  private MappingIODefinition inputMapping;

  private MappingIODefinition outputMapping;

  private interface ApplyChanges {
    public void applyChanges();
  }

  private class MappingParametersTab implements ApplyChanges {
    private TableView wMappingParameters;

    private MappingParameters parameters;

    private Button wInheritAll;

    public MappingParametersTab(
        TableView wMappingParameters, Button wInheritAll, MappingParameters parameters) {
      this.wMappingParameters = wMappingParameters;
      this.wInheritAll = wInheritAll;
      this.parameters = parameters;
    }

    public void applyChanges() {

      int nrLines = wMappingParameters.nrNonEmpty();
      String[] variables = new String[nrLines];
      String[] inputFields = new String[nrLines];
      parameters.setVariable(variables);
      parameters.setInputField(inputFields);
      // CHECKSTYLE:Indentation:OFF
      for (int i = 0; i < nrLines; i++) {
        TableItem item = wMappingParameters.getNonEmpty(i);
        parameters.getVariable()[i] = item.getText(1);
        parameters.getInputField()[i] = item.getText(2);
      }
      parameters.setInheritingAllVariables(wInheritAll.getSelection());
    }
  }

  private class MappingDefinitionTab implements ApplyChanges {
    private MappingIODefinition definition;

    private TableView wFieldMappings;

    public MappingDefinitionTab(MappingIODefinition definition, TableView fieldMappings) {
      super();
      this.definition = definition;
      wFieldMappings = fieldMappings;
    }

    public void applyChanges() {
      // The grid
      //
      int nrLines = wFieldMappings.nrNonEmpty();
      definition.getValueRenames().clear();
      for (int i = 0; i < nrLines; i++) {
        TableItem item = wFieldMappings.getNonEmpty(i);
        definition.getValueRenames().add(new MappingValueRename(item.getText(1), item.getText(2)));
      }
    }
  }

  private List<ApplyChanges> changeList;

  public SimpleMappingDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname) {
    super(parent, variables, (BaseTransformMeta) in, tr, sname);
    mappingMeta = (SimpleMappingMeta) in;
    transModified = false;

    // Make a copy for our own purposes...
    // This allows us to change everything directly in the classes with
    // listeners.
    // Later we need to copy it to the input class on ok()
    //
    mappingParameters = (MappingParameters) mappingMeta.getMappingParameters().clone();
    inputMapping = (MappingIODefinition) mappingMeta.getInputMapping().clone();
    outputMapping = (MappingIODefinition) mappingMeta.getOutputMapping().clone();

    changeList = new ArrayList<>();
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    props.setLook(shell);
    setShellImage(shell, mappingMeta);

    lsMod = e -> mappingMeta.setChanged();
    changed = mappingMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 15;
    formLayout.marginHeight = 15;

    margin = props.getMargin();
    middle = props.getMiddlePct();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "SimpleMappingDialog.Shell.Title"));

    // Buttons at the bottom
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    Label wicon = new Label(shell, SWT.RIGHT);
    wicon.setImage(getImage());
    FormData fdlicon = new FormData();
    fdlicon.top = new FormAttachment(0, 0);
    fdlicon.right = new FormAttachment(100, 0);
    wicon.setLayoutData(fdlicon);
    props.setLook(wicon);

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "SimpleMappingDialog.TransformName.Label"));
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.top = new FormAttachment(0, 0);
    wlTransformName.setLayoutData(fdlTransformName);

    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.right = new FormAttachment(100, 0);
    fdTransformName.left = new FormAttachment(0, 0);
    fdTransformName.top = new FormAttachment(wlTransformName, 5);
    wTransformName.setLayoutData(fdTransformName);

    Label spacer = new Label(shell, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdSpacer = new FormData();
    fdSpacer.left = new FormAttachment(0, 0);
    fdSpacer.top = new FormAttachment(wTransformName, 15);
    fdSpacer.right = new FormAttachment(100, 0);
    spacer.setLayoutData(fdSpacer);

    wlPath = new Label(shell, SWT.LEFT);
    props.setLook(wlPath);
    wlPath.setText(BaseMessages.getString(PKG, "SimpleMappingDialog.Pipeline.Label"));
    FormData fdlTransformation = new FormData();
    fdlTransformation.left = new FormAttachment(0, 0);
    fdlTransformation.top = new FormAttachment(spacer, 20);
    fdlTransformation.right = new FormAttachment(50, 0);
    wlPath.setLayoutData(fdlTransformation);

    wbBrowse = new Button(shell, SWT.PUSH);
    props.setLook(wbBrowse);
    wbBrowse.setText(BaseMessages.getString(PKG, "SimpleMappingDialog.Browse.Label"));
    FormData fdBrowse = new FormData();
    fdBrowse.right = new FormAttachment(100, 0);
    fdBrowse.top = new FormAttachment(wlPath, Const.isOSX() ? 0 : 5);
    wbBrowse.setLayoutData(fdBrowse);
    wbBrowse.addListener(SWT.Selection, e -> selectFilePipeline());

    wPath = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPath);
    FormData fdTransformation = new FormData();
    fdTransformation.left = new FormAttachment(0, 0);
    fdTransformation.top = new FormAttachment(wlPath, 5);
    fdTransformation.right = new FormAttachment(wbBrowse, -5);
    wPath.setLayoutData(fdTransformation);

    //
    // Add a tab folder for the parameters and various input and output
    // streams
    //
    wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);
    wTabFolder.setUnselectedCloseVisible(true);

    Label hSpacer = new Label(shell, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdhSpacer = new FormData();
    fdhSpacer.left = new FormAttachment(0, 0);
    fdhSpacer.bottom = new FormAttachment(wCancel, -15);
    fdhSpacer.right = new FormAttachment(100, 0);
    hSpacer.setLayoutData(fdhSpacer);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wPath, 20);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(hSpacer, -15);
    wTabFolder.setLayoutData(fdTabFolder);

    // Add listeners
    lsDef =
        new SelectionAdapter() {
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };

    wTransformName.addSelectionListener(lsDef);
    wPath.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    // Set the shell size, based upon previous time...
    setSize(shell, 670, 690);

    getData();
    mappingMeta.setChanged(changed);
    wTabFolder.setSelection(0);

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return transformName;
  }

  protected Image getImage() {
    return SwtSvgImageUtil.getImage(
        shell.getDisplay(),
        getClass().getClassLoader(),
        "MAP.svg",
        ConstUi.ICON_SIZE,
        ConstUi.ICON_SIZE);
  }

  private void selectFilePipeline() {

    try {
      HopPipelineFileType fileType = new HopPipelineFileType();
      String filename =
          BaseDialog.presentFileDialog(
              false, shell, fileType.getFilterExtensions(), fileType.getFilterNames(), true);
      if (filename != null) {
        loadPipelineFile(filename);
        wPath.setText(filename);
      }
    } catch (HopException ex) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "SimpleMappingDialog.ErrorLoadingPipeline.DialogTitle"),
          BaseMessages.getString(PKG, "SimpleMappingDialog.ErrorLoadingPipeline.DialogMessage"),
          ex);
    }
  }

  private void loadPipelineFile(String fname) throws HopException {
    mappingPipelineMeta =
        new PipelineMeta(variables.resolve(fname), metadataProvider, true, variables);
    mappingPipelineMeta.clearChanged();
  }

  void loadPipeline() throws HopException {
    String filename = wPath.getText();
    if (Utils.isEmpty(filename)) {
      return;
    }
    if (!filename.endsWith(".hpl")) {
      filename = filename + ".hpl";
      wPath.setText(filename);
    }
    loadPipelineFile(filename);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wPath.setText(Const.NVL(mappingMeta.getFilename(), ""));

    addParametersTab(mappingParameters);
    wTabFolder.setSelection(0);

    addInputMappingDefinitionTab(inputMapping, 0);
    addOutputMappingDefinitionTab(outputMapping, 1);

    try {
      loadPipeline();
    } catch (Throwable t) {
      // Ignore errors
    }

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void addOutputMappingDefinitionTab(MappingIODefinition definition, int index) {
    addMappingDefinitionTab(
        definition,
        index + 1,
        BaseMessages.getString(PKG, "SimpleMappingDialog.OutputTab.Title"),
        BaseMessages.getString(PKG, "SimpleMappingDialog.OutputTab.Tooltip"),
        BaseMessages.getString(PKG, "SimpleMappingDialog.OutputTab.column.SourceField"),
        BaseMessages.getString(PKG, "SimpleMappingDialog.OutputTab.column.TargetField"),
        false);
  }

  private void addInputMappingDefinitionTab(MappingIODefinition definition, int index) {
    addMappingDefinitionTab(
        definition,
        index + 1,
        BaseMessages.getString(PKG, "SimpleMappingDialog.InputTab.Title"),
        BaseMessages.getString(PKG, "SimpleMappingDialog.InputTab.Tooltip"),
        BaseMessages.getString(PKG, "SimpleMappingDialog.InputTab.column.SourceField"),
        BaseMessages.getString(PKG, "SimpleMappingDialog.InputTab.column.TargetField"),
        true);
  }

  private void addParametersTab(final MappingParameters parameters) {

    CTabItem wParametersTab = new CTabItem(wTabFolder, SWT.NONE);
    wParametersTab.setText(BaseMessages.getString(PKG, "SimpleMappingDialog.Parameters.Title"));
    wParametersTab.setToolTipText(
        BaseMessages.getString(PKG, "SimpleMappingDialog.Parameters.Tooltip"));

    Composite wParametersComposite = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wParametersComposite);

    FormLayout parameterTabLayout = new FormLayout();
    parameterTabLayout.marginWidth = 15;
    parameterTabLayout.marginHeight = 15;
    wParametersComposite.setLayout(parameterTabLayout);

    // Add a checkbox: inherit all variables...
    //
    Button wInheritAll = new Button(wParametersComposite, SWT.CHECK);
    wInheritAll.setText(BaseMessages.getString(PKG, "SimpleMappingDialog.Parameters.InheritAll"));
    props.setLook(wInheritAll);
    FormData fdInheritAll = new FormData();
    fdInheritAll.bottom = new FormAttachment(100, 0);
    fdInheritAll.left = new FormAttachment(0, 0);
    fdInheritAll.right = new FormAttachment(100, -30);
    wInheritAll.setLayoutData(fdInheritAll);
    wInheritAll.setSelection(parameters.isInheritingAllVariables());

    // Now add a tableview with the 2 columns to specify: input and output
    // fields for the source and target transforms.
    //
    ColumnInfo[] colinfo =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "SimpleMappingDialog.Parameters.column.Variable"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SimpleMappingDialog.Parameters.column.ValueOrField"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
        };
    colinfo[1].setUsingVariables(true);

    final TableView wMappingParameters =
        new TableView(
            variables,
            wParametersComposite,
            SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER,
            colinfo,
            parameters.getVariable().length,
            false,
            lsMod,
            props,
            false);
    props.setLook(wMappingParameters);
    FormData fdMappings = new FormData();
    fdMappings.left = new FormAttachment(0, 0);
    fdMappings.right = new FormAttachment(100, 0);
    fdMappings.top = new FormAttachment(0, 0);
    fdMappings.bottom = new FormAttachment(wInheritAll, -10);
    wMappingParameters.setLayoutData(fdMappings);
    wMappingParameters.getTable().addListener(SWT.Resize, new ColumnsResizer(0, 50, 50));

    for (int i = 0; i < parameters.getVariable().length; i++) {
      TableItem tableItem = wMappingParameters.table.getItem(i);
      tableItem.setText(1, parameters.getVariable()[i]);
      tableItem.setText(2, parameters.getInputField()[i]);
    }
    wMappingParameters.setRowNums();
    wMappingParameters.optWidth(true);

    FormData fdParametersComposite = new FormData();
    fdParametersComposite.left = new FormAttachment(0, 0);
    fdParametersComposite.top = new FormAttachment(0, 0);
    fdParametersComposite.right = new FormAttachment(100, 0);
    fdParametersComposite.bottom = new FormAttachment(100, 0);
    wParametersComposite.setLayoutData(fdParametersComposite);

    wParametersComposite.layout();
    wParametersTab.setControl(wParametersComposite);

    changeList.add(new MappingParametersTab(wMappingParameters, wInheritAll, parameters));
  }

  public static String[] getMappingTransforms(
      PipelineMeta mappingPipelineMeta, boolean mappingInput) {
    List<TransformMeta> transforms = new ArrayList<>();
    for (TransformMeta transformMeta : mappingPipelineMeta.getTransforms()) {
      if (mappingInput && transformMeta.getTransformPluginId().equals("MappingInput")) {
        transforms.add(transformMeta);
      }
      if (!mappingInput && transformMeta.getTransformPluginId().equals("MappingOutput")) {
        transforms.add(transformMeta);
      }
    }
    String[] transformnames = new String[transforms.size()];
    for (int i = 0; i < transformnames.length; i++) {
      transformnames[i] = transforms.get(i).getName();
    }

    return transformnames;
  }

  public IRowMeta getFieldsFromTransform(boolean parent, boolean input) throws HopException {
    if (input) {
      // INPUT
      //
      if (parent) {
        return pipelineMeta.getPrevTransformFields(variables, transformMeta);
      } else {
        if (mappingPipelineMeta == null) {
          throw new HopException(
              BaseMessages.getString(PKG, "SimpleMappingDialog.Exception.NoMappingSpecified"));
        }
        TransformMeta mappingInputTransformMeta =
            mappingPipelineMeta.findMappingInputTransform(null);
        return mappingPipelineMeta.getTransformFields(variables, mappingInputTransformMeta);
      }
    } else {
      // OUTPUT
      //
      TransformMeta mappingOutputTransformMeta =
          mappingPipelineMeta.findMappingOutputTransform(null);
      return mappingPipelineMeta.getTransformFields(variables, mappingOutputTransformMeta);
    }
  }

  private void addMappingDefinitionTab(
      final MappingIODefinition definition,
      int index,
      final String tabTitle,
      final String tabTooltip,
      String sourceColumnLabel,
      String targetColumnLabel,
      final boolean input) {

    final CTabItem wTab;
    if (index >= wTabFolder.getItemCount()) {
      wTab = new CTabItem(wTabFolder, SWT.CLOSE);
    } else {
      wTab = new CTabItem(wTabFolder, SWT.CLOSE, index);
    }
    setMappingDefinitionTabNameAndToolTip(wTab, tabTitle, tabTooltip, definition, input);

    Composite wInputComposite = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wInputComposite);

    FormLayout tabLayout = new FormLayout();
    tabLayout.marginWidth = 15;
    tabLayout.marginHeight = 15;
    wInputComposite.setLayout(tabLayout);

    // Now add a table view with the 2 columns to specify: input and output
    // fields for the source and target transforms.
    //
    final Button wbEnterMapping = new Button(wInputComposite, SWT.PUSH);
    props.setLook(wbEnterMapping);
    if (input) {
      wbEnterMapping.setText(
          BaseMessages.getString(PKG, "SimpleMappingDialog.button.EnterMapping"));
    } else {
      wbEnterMapping.setText(BaseMessages.getString(PKG, "SimpleMappingDialog.button.GetFields"));
    }
    FormData fdbEnterMapping = new FormData();
    fdbEnterMapping.bottom = new FormAttachment(100);
    fdbEnterMapping.right = new FormAttachment(100);
    wbEnterMapping.setLayoutData(fdbEnterMapping);

    ColumnInfo[] colinfo =
        new ColumnInfo[] {
          new ColumnInfo(sourceColumnLabel, ColumnInfo.COLUMN_TYPE_TEXT, false, false),
          new ColumnInfo(targetColumnLabel, ColumnInfo.COLUMN_TYPE_TEXT, false, false),
        };
    final TableView wFieldMappings =
        new TableView(
            variables,
            wInputComposite,
            SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER,
            colinfo,
            1,
            false,
            lsMod,
            props,
            false);
    props.setLook(wFieldMappings);
    FormData fdMappings = new FormData();
    fdMappings.left = new FormAttachment(0);
    fdMappings.right = new FormAttachment(100);
    fdMappings.top = new FormAttachment(0);
    fdMappings.bottom = new FormAttachment(wbEnterMapping, -10);
    wFieldMappings.setLayoutData(fdMappings);
    wFieldMappings.getTable().addListener(SWT.Resize, new ColumnsResizer(0, 50, 50));

    for (MappingValueRename valueRename : definition.getValueRenames()) {
      TableItem tableItem = new TableItem(wFieldMappings.table, SWT.NONE);
      tableItem.setText(1, Const.NVL(valueRename.getSourceValueName(), ""));
      tableItem.setText(2, Const.NVL(valueRename.getTargetValueName(), ""));
    }
    wFieldMappings.removeEmptyRows();
    wFieldMappings.setRowNums();
    wFieldMappings.optWidth(true);

    wbEnterMapping.addSelectionListener(
        new SelectionAdapter() {

          @Override
          public void widgetSelected(SelectionEvent arg0) {
            try {
              if (input) {
                // INPUT
                //
                IRowMeta sourceRowMeta = getFieldsFromTransform(true, input);
                IRowMeta targetRowMeta = getFieldsFromTransform(false, input);
                String[] sourceFields = sourceRowMeta.getFieldNames();
                String[] targetFields = targetRowMeta.getFieldNames();

                EnterMappingDialog dialog =
                    new EnterMappingDialog(shell, sourceFields, targetFields);
                List<SourceToTargetMapping> mappings = dialog.open();
                if (mappings != null) {
                  // first clear the dialog...
                  wFieldMappings.clearAll(false);

                  //
                  definition.getValueRenames().clear();

                  // Now add the new values...
                  for (int i = 0; i < mappings.size(); i++) {
                    SourceToTargetMapping mapping = mappings.get(i);
                    TableItem item = new TableItem(wFieldMappings.table, SWT.NONE);
                    item.setText(1, mapping.getSourceString(sourceFields));
                    item.setText(2, mapping.getTargetString(targetFields));

                    String source = input ? item.getText(1) : item.getText(2);
                    String target = input ? item.getText(2) : item.getText(1);
                    definition.getValueRenames().add(new MappingValueRename(source, target));
                  }
                  wFieldMappings.removeEmptyRows();
                  wFieldMappings.setRowNums();
                  wFieldMappings.optWidth(true);
                }
              } else {
                // OUTPUT
                //
                IRowMeta sourceRowMeta = getFieldsFromTransform(true, input);
                BaseTransformDialog.getFieldsFromPrevious(
                    sourceRowMeta,
                    wFieldMappings,
                    1,
                    new int[] {
                      1,
                    },
                    new int[] {},
                    -1,
                    -1,
                    null);
              }
            } catch (HopException e) {
              new ErrorDialog(
                  shell,
                  BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
                  BaseMessages.getString(
                      PKG,
                      "SimpleMappingDialog.Exception.ErrorGettingMappingSourceAndTargetFields",
                      e.toString()),
                  e);
            }
          }
        });

    if (input) {
      Button wRenameOutput = new Button(wInputComposite, SWT.CHECK);
      props.setLook(wRenameOutput);
      wRenameOutput.setText(
          BaseMessages.getString(PKG, "SimpleMappingDialog.input.RenamingOnOutput"));
      FormData fdRenameOutput = new FormData();
      fdRenameOutput.top = new FormAttachment(wFieldMappings, 10);
      fdRenameOutput.left = new FormAttachment(0, 0);
      wRenameOutput.setLayoutData(fdRenameOutput);

      wRenameOutput.setSelection(definition.isRenamingOnOutput());
      wRenameOutput.addSelectionListener(
          new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent event) {
              // flip the switch
              definition.setRenamingOnOutput(!definition.isRenamingOnOutput());
            }
          });
    }

    FormData fdParametersComposite = new FormData();
    fdParametersComposite.left = new FormAttachment(0, 0);
    fdParametersComposite.top = new FormAttachment(0, 0);
    fdParametersComposite.right = new FormAttachment(100, 0);
    fdParametersComposite.bottom = new FormAttachment(100, 0);
    wInputComposite.setLayoutData(fdParametersComposite);

    wInputComposite.layout();
    wTab.setControl(wInputComposite);

    final ApplyChanges applyChanges = new MappingDefinitionTab(definition, wFieldMappings);
    changeList.add(applyChanges);

    wTabFolder.setSelection(wTab);
  }

  private void setMappingDefinitionTabNameAndToolTip(
      CTabItem wTab,
      String tabTitle,
      String tabTooltip,
      MappingIODefinition definition,
      boolean input) {

    String transformName;
    if (input) {
      transformName = definition.getInputTransformName();
    } else {
      transformName = definition.getOutputTransformName();
    }
    String description = definition.getDescription();

    if (Utils.isEmpty(transformName)) {
      wTab.setText(tabTitle);
    } else {
      wTab.setText(tabTitle + " : " + transformName);
    }
    String tooltip = tabTooltip;
    if (!Utils.isEmpty(transformName)) {
      tooltip += Const.CR + Const.CR + transformName;
    }
    if (!Utils.isEmpty(description)) {
      tooltip += Const.CR + Const.CR + description;
    }
    wTab.setToolTipText(tooltip);
  }

  private void cancel() {
    transformName = null;
    mappingMeta.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    try {
      loadPipeline();
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "SimpleMappingDialog.ErrorLoadingSpecifiedPipeline.Title"),
          BaseMessages.getString(PKG, "SimpleMappingDialog.ErrorLoadingSpecifiedPipeline.Message"),
          e);
      return;
    }

    mappingMeta.setFilename(wPath.getText());

    // Load the information on the tabs, optionally do some
    // verifications...
    //
    collectInformation();

    mappingMeta.setMappingParameters(mappingParameters);
    mappingMeta.setInputMapping(inputMapping);
    mappingMeta.setOutputMapping(outputMapping);
    mappingMeta.setChanged(true);

    dispose();
  }

  private void collectInformation() {
    for (ApplyChanges applyChanges : changeList) {
      applyChanges.applyChanges(); // collect information from all
      // tabs...
    }
  }
}
