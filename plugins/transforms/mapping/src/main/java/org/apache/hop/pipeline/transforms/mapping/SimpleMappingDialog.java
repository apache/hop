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

package org.apache.hop.pipeline.transforms.mapping;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engines.local.LocalPipelineRunConfiguration;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterMappingDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ColumnsResizer;
import org.apache.hop.ui.core.widget.ComboVar;
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
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class SimpleMappingDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = SimpleMappingMeta.class; // For Translator

  private SimpleMappingMeta mappingMeta;

  private TextVar wPath;

  private ComboVar wRunConfig;

  private CTabFolder wTabFolder;

  private PipelineMeta mappingPipelineMeta = null;

  protected boolean transModified;

  private ModifyListener lsMod;

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

    @Override
    public void applyChanges() {

      parameters.getVariableMappings().clear();
      for (TableItem item : wMappingParameters.getNonEmptyItems()) {
        parameters
            .getVariableMappings()
            .add(new MappingVariableMapping(item.getText(1), item.getText(2)));
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

    @Override
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

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    PropsUi.setLook(shell);
    setShellImage(shell, mappingMeta);

    lsMod = e -> mappingMeta.setChanged();
    changed = mappingMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 15;
    formLayout.marginHeight = 15;

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

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
    PropsUi.setLook(wicon);

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "SimpleMappingDialog.TransformName.Label"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.top = new FormAttachment(wicon, 0, SWT.CENTER);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.right = new FormAttachment(wicon, -margin);
    fdTransformName.left = new FormAttachment(wlTransformName, margin);
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    wTransformName.setLayoutData(fdTransformName);

    Label spacer = new Label(shell, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdSpacer = new FormData();
    fdSpacer.left = new FormAttachment(0, 0);
    fdSpacer.top = new FormAttachment(wicon, 0);
    fdSpacer.right = new FormAttachment(100, 0);
    spacer.setLayoutData(fdSpacer);

    Label wlPath = new Label(shell, SWT.RIGHT);
    PropsUi.setLook(wlPath);
    wlPath.setText(BaseMessages.getString(PKG, "SimpleMappingDialog.Pipeline.Label"));
    FormData fdlTransformation = new FormData();
    fdlTransformation.left = new FormAttachment(0, 0);
    fdlTransformation.top = new FormAttachment(spacer, 20);
    fdlTransformation.right = new FormAttachment(middle, -margin);
    wlPath.setLayoutData(fdlTransformation);

    Button wbBrowse = new Button(shell, SWT.PUSH);
    PropsUi.setLook(wbBrowse);
    wbBrowse.setText(BaseMessages.getString(PKG, "SimpleMappingDialog.Browse.Label"));
    FormData fdBrowse = new FormData();
    fdBrowse.right = new FormAttachment(100, 0);
    fdBrowse.top = new FormAttachment(wlPath, 0, SWT.CENTER);
    wbBrowse.setLayoutData(fdBrowse);
    wbBrowse.addListener(SWT.Selection, e -> selectFilePipeline());

    wPath = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPath);
    FormData fdTransformation = new FormData();
    fdTransformation.left = new FormAttachment(wlPath, margin);
    fdTransformation.top = new FormAttachment(wlPath, 0, SWT.CENTER);
    fdTransformation.right = new FormAttachment(wbBrowse, -margin);
    wPath.setLayoutData(fdTransformation);

    // The run configuration
    //
    Label wlRunConfig = new Label(shell, SWT.RIGHT);
    PropsUi.setLook(wlRunConfig);
    wlRunConfig.setText(BaseMessages.getString(PKG, "SimpleMappingDialog.RunConfig.Label"));
    FormData fdlRunConfig = new FormData();
    fdlRunConfig.left = new FormAttachment(0, 0);
    fdlRunConfig.top = new FormAttachment(wPath, margin);
    fdlRunConfig.right = new FormAttachment(middle, -margin);
    wlRunConfig.setLayoutData(fdlRunConfig);
    wRunConfig = new ComboVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wRunConfig);
    FormData fdRunConfig = new FormData();
    fdRunConfig.left = new FormAttachment(wlRunConfig, margin);
    fdRunConfig.top = new FormAttachment(wlRunConfig, 0, SWT.CENTER);
    fdRunConfig.right = new FormAttachment(wbBrowse, -margin);
    wRunConfig.setLayoutData(fdRunConfig);

    //
    // Add a tab folder for the parameters and various input and output
    // streams
    //
    wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);
    wTabFolder.setUnselectedCloseVisible(true);

    Label hSpacer = new Label(shell, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdhSpacer = new FormData();
    fdhSpacer.left = new FormAttachment(0, 0);
    fdhSpacer.bottom = new FormAttachment(wCancel, -15);
    fdhSpacer.right = new FormAttachment(100, 0);
    hSpacer.setLayoutData(fdhSpacer);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wRunConfig, 2 * margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(hSpacer, -15);
    wTabFolder.setLayoutData(fdTabFolder);

    getData();
    mappingMeta.setChanged(changed);
    wTabFolder.setSelection(0);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

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
    mappingPipelineMeta = new PipelineMeta(variables.resolve(fname), metadataProvider, variables);
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

    // Prepare a regexp checker to see if the pipeline name contains a variable
    // boolean containsVar = Pattern.matches("^[/\\w]*(\\$\\{\\w+})[/.\\w]*", mappingMeta.getFilename());
    Pattern p = Pattern.compile("^[/\\w]*(\\$\\{\\w+})[/.\\w]*");
    Matcher m = p.matcher(mappingMeta.getFilename());

    if (!m.lookingAt()) {
      try {
        loadPipeline();
      } catch (Throwable t) {
        // Ignore errors
      }
    }
    try {
      // Load the run configurations.
      List<PipelineRunConfiguration> runConfigs =
          metadataProvider.getSerializer(PipelineRunConfiguration.class).loadAll();
      for (PipelineRunConfiguration runConfig : runConfigs) {
        if (runConfig.getEngineRunConfiguration() instanceof LocalPipelineRunConfiguration) {
          wRunConfig.add(runConfig.getName());
        }
      }
      wRunConfig.setText(Const.NVL(mappingMeta.getRunConfigurationName(), ""));
    } catch (Exception e) {
      LogChannel.UI.logError("Error loading pipeline run configurations", e);
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
    wParametersTab.setFont(GuiResource.getInstance().getFontDefault());
    wParametersTab.setText(BaseMessages.getString(PKG, "SimpleMappingDialog.Parameters.Title"));
    wParametersTab.setToolTipText(
        BaseMessages.getString(PKG, "SimpleMappingDialog.Parameters.Tooltip"));

    Composite wParametersComposite = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wParametersComposite);

    FormLayout parameterTabLayout = new FormLayout();
    parameterTabLayout.marginWidth = 15;
    parameterTabLayout.marginHeight = 15;
    wParametersComposite.setLayout(parameterTabLayout);

    // Add a checkbox: inherit all variables...
    //
    Button wInheritAll = new Button(wParametersComposite, SWT.CHECK);
    wInheritAll.setText(BaseMessages.getString(PKG, "SimpleMappingDialog.Parameters.InheritAll"));
    PropsUi.setLook(wInheritAll);
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
            parameters.getVariableMappings().size(),
            false,
            lsMod,
            props,
            false);
    PropsUi.setLook(wMappingParameters);
    FormData fdMappings = new FormData();
    fdMappings.left = new FormAttachment(0, 0);
    fdMappings.right = new FormAttachment(100, 0);
    fdMappings.top = new FormAttachment(0, 0);
    fdMappings.bottom = new FormAttachment(wInheritAll, -10);
    wMappingParameters.setLayoutData(fdMappings);
    wMappingParameters.getTable().addListener(SWT.Resize, new ColumnsResizer(0, 50, 50));

    for (int i = 0; i < parameters.getVariableMappings().size(); i++) {
      MappingVariableMapping mapping = parameters.getVariableMappings().get(i);
      TableItem tableItem = wMappingParameters.table.getItem(i);
      tableItem.setText(1, mapping.getName());
      if (!Utils.isEmpty(mapping.getValue())) {
        tableItem.setText(2, mapping.getValue());
      }
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
    String[] transformNames = new String[transforms.size()];
    for (int i = 0; i < transformNames.length; i++) {
      transformNames[i] = transforms.get(i).getName();
    }

    return transformNames;
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
    wTab.setFont(GuiResource.getInstance().getFontDefault());
    setMappingDefinitionTabNameAndToolTip(wTab, tabTitle, tabTooltip, definition, input);

    Composite wInputComposite = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wInputComposite);

    FormLayout tabLayout = new FormLayout();
    tabLayout.marginWidth = 15;
    tabLayout.marginHeight = 15;
    wInputComposite.setLayout(tabLayout);

    // Now add a table view with the 2 columns to specify: input and output
    // fields for the source and target transforms.
    //
    final Button wbEnterMapping = new Button(wInputComposite, SWT.PUSH);
    PropsUi.setLook(wbEnterMapping);
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
    PropsUi.setLook(wFieldMappings);
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
      PropsUi.setLook(wRenameOutput);
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
    String pipelinePath = wPath.getText();
    // Prepare a regexp checker to see if the pipeline name contains a variable
    Pattern p = Pattern.compile("^[/\\w]*(\\$\\{\\w+})[/.\\w]*");
    Matcher m = p.matcher(mappingMeta.getFilename());

    if (!m.lookingAt()) {
      try {
        loadPipeline();
      } catch (HopException e) {
        new ErrorDialog(
                shell,
                BaseMessages.getString(PKG, "SimpleMappingDialog.ErrorLoadingSpecifiedPipeline.Title"),
                BaseMessages.getString(
                        PKG, "SimpleMappingDialog.ErrorLoadingSpecifiedPipeline.Message"),
                e);
        return;
      }
    }

    mappingMeta.setFilename(pipelinePath);
    mappingMeta.setRunConfigurationName(wRunConfig.getText());

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
