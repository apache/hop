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

package org.apache.hop.pipeline.transforms.multimapping;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
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
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engines.local.LocalPipelineRunConfiguration;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mapping.MappingParameters;
import org.apache.hop.pipeline.transforms.mapping.MappingTransforms;
import org.apache.hop.pipeline.transforms.mapping.MappingVariableMapping;
import org.apache.hop.pipeline.transforms.mapping.SimpleMappingMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterMappingDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ColumnsResizer;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabFolder2Adapter;
import org.eclipse.swt.custom.CTabFolderEvent;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class MultiMappingDialog extends BaseTransformDialog {
  private static final Class<?> PKG = MultiMappingMeta.class;

  private final MultiMappingMeta mappingMeta;
  private final MultiIOMappings workingMappings;

  private TextVar wPath;
  private ComboVar wRunConfig;
  private CTabFolder wTabFolder;
  private Button wbAddInput;
  private Button wbAddOutput;

  private PipelineMeta mappingPipelineMeta;
  private ModifyListener lsMod;
  private final List<Runnable> changeList = new ArrayList<>();

  public MultiMappingDialog(
      Shell parent,
      IVariables variables,
      MultiMappingMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    this.mappingMeta = transformMeta;
    this.workingMappings = new MultiIOMappings(transformMeta.getIoMappings());
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "MultiMappingDialog.Shell.Title"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    lsMod = e -> mappingMeta.setChanged();
    changed = mappingMeta.hasChanged();

    Label wlPath = new Label(shell, SWT.RIGHT);
    PropsUi.setLook(wlPath);
    wlPath.setText(BaseMessages.getString(PKG, "MultiMappingDialog.Pipeline.Label"));
    FormData fdlPath = new FormData();
    fdlPath.left = new FormAttachment(0, 0);
    fdlPath.top = new FormAttachment(wSpacer, margin);
    fdlPath.right = new FormAttachment(middle, -margin);
    wlPath.setLayoutData(fdlPath);

    Button wbEdit = new Button(shell, SWT.PUSH);
    PropsUi.setLook(wbEdit);
    wbEdit.setText(BaseMessages.getString(PKG, "MultiMappingDialog.Edit.Label"));
    FormData fdEdit = new FormData();
    fdEdit.right = new FormAttachment(100, 0);
    fdEdit.top = new FormAttachment(wlPath, 0, SWT.CENTER);
    wbEdit.setLayoutData(fdEdit);
    wbEdit.addListener(SWT.Selection, e -> editPipeline());

    Button wbNew = new Button(shell, SWT.PUSH);
    PropsUi.setLook(wbNew);
    wbNew.setText(BaseMessages.getString(PKG, "MultiMappingDialog.New.Label"));
    FormData fdNew = new FormData();
    fdNew.right = new FormAttachment(wbEdit, -margin);
    fdNew.top = new FormAttachment(wlPath, 0, SWT.CENTER);
    wbNew.setLayoutData(fdNew);
    wbNew.addListener(SWT.Selection, e -> newPipeline());

    Button wbBrowse = new Button(shell, SWT.PUSH);
    PropsUi.setLook(wbBrowse);
    wbBrowse.setText(BaseMessages.getString(PKG, "MultiMappingDialog.Browse.Label"));
    FormData fdBrowse = new FormData();
    fdBrowse.right = new FormAttachment(wbNew, -margin);
    fdBrowse.top = new FormAttachment(wlPath, 0, SWT.CENTER);
    wbBrowse.setLayoutData(fdBrowse);
    wbBrowse.addListener(SWT.Selection, e -> selectFilePipeline());

    wPath = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPath);
    FormData fdPath = new FormData();
    fdPath.left = new FormAttachment(wlPath, margin);
    fdPath.top = new FormAttachment(wlPath, 0, SWT.CENTER);
    fdPath.right = new FormAttachment(wbBrowse, -margin);
    wPath.setLayoutData(fdPath);

    Label wlRunConfig = new Label(shell, SWT.RIGHT);
    PropsUi.setLook(wlRunConfig);
    wlRunConfig.setText(BaseMessages.getString(PKG, "MultiMappingDialog.RunConfig.Label"));
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

    wbAddOutput = new Button(shell, SWT.PUSH);
    PropsUi.setLook(wbAddOutput);
    wbAddOutput.setText(BaseMessages.getString(PKG, "MultiMappingDialog.button.AddOutput"));
    FormData fdAddOutput = new FormData();
    fdAddOutput.right = new FormAttachment(100, 0);
    fdAddOutput.top = new FormAttachment(wRunConfig, margin);
    wbAddOutput.setLayoutData(fdAddOutput);
    wbAddOutput.addListener(
        SWT.Selection, e -> addOutputTab(new MultiMappingOutputDefinition(), true));

    wbAddInput = new Button(shell, SWT.PUSH);
    PropsUi.setLook(wbAddInput);
    wbAddInput.setText(BaseMessages.getString(PKG, "MultiMappingDialog.button.AddInput"));
    FormData fdAddInput = new FormData();
    fdAddInput.right = new FormAttachment(wbAddOutput, -margin);
    fdAddInput.top = new FormAttachment(wRunConfig, margin);
    wbAddInput.setLayoutData(fdAddInput);
    wbAddInput.addListener(
        SWT.Selection, e -> addInputTab(new MultiMappingInputDefinition(), true));

    wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);
    wTabFolder.setUnselectedCloseVisible(true);
    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wbAddInput, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -margin);
    wTabFolder.setLayoutData(fdTabFolder);
    wTabFolder.addCTabFolder2Listener(
        new CTabFolder2Adapter() {
          @Override
          public void close(CTabFolderEvent event) {
            CTabItem item = (CTabItem) event.item;
            if (item.getData() == null) {
              event.doit = false;
              return;
            }
            MessageBox box = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION);
            box.setText(
                BaseMessages.getString(
                    PKG, "MultiMappingDialog.CloseDefinitionTabAreYouSure.Title"));
            box.setMessage(
                BaseMessages.getString(
                    PKG, "MultiMappingDialog.CloseDefinitionTabAreYouSure.Message"));
            if (box.open() != SWT.YES) {
              event.doit = false;
              return;
            }
            Object data = item.getData();
            if (data instanceof MultiMappingInputDefinition definition) {
              workingMappings.getInputMappings().remove(definition);
            } else if (data instanceof MultiMappingOutputDefinition definition) {
              workingMappings.getOutputMappings().remove(definition);
            }
            mappingMeta.setChanged();
          }
        });

    getData();
    mappingMeta.setChanged(changed);
    wTabFolder.setSelection(0);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
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
          BaseMessages.getString(PKG, "MultiMappingDialog.ErrorLoadingPipeline.DialogTitle"),
          BaseMessages.getString(PKG, "MultiMappingDialog.ErrorLoadingPipeline.DialogMessage"),
          ex);
    }
  }

  private void newPipeline() {
    try {
      HopPipelineFileType fileType = new HopPipelineFileType();
      String filename =
          BaseDialog.presentFileDialog(
              false, shell, fileType.getFilterExtensions(), fileType.getFilterNames(), true);
      if (Utils.isEmpty(filename)) {
        return;
      }
      if (!filename.endsWith(".hpl")) {
        filename = filename + ".hpl";
      }
      PipelineMeta newMeta = new PipelineMeta();
      newMeta.setFilename(filename);
      try (OutputStream outputStream = HopVfs.getOutputStream(filename, false)) {
        outputStream.write(newMeta.getXml(variables).getBytes(StandardCharsets.UTF_8));
      }
      loadPipelineFile(filename);
      wPath.setText(filename);
      mappingMeta.setChanged();
    } catch (Exception ex) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MultiMappingDialog.ErrorLoadingPipeline.DialogTitle"),
          BaseMessages.getString(PKG, "MultiMappingDialog.ErrorLoadingPipeline.DialogMessage"),
          ex);
    }
  }

  private void editPipeline() {
    try {
      if (Utils.isEmpty(wPath.getText())) {
        return;
      }
      HopGui.getInstance().fileDelegate.fileOpen(variables.resolve(wPath.getText()));
    } catch (Exception ex) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MultiMappingDialog.ErrorLoadingPipeline.DialogTitle"),
          BaseMessages.getString(PKG, "MultiMappingDialog.ErrorLoadingPipeline.DialogMessage"),
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

  public void getData() {
    wPath.setText(Const.NVL(mappingMeta.getFilename(), ""));
    addParametersTab(workingMappings.getMappingParameters());
    for (MultiMappingInputDefinition definition : workingMappings.getInputMappings()) {
      addInputTab(definition, false);
    }
    for (MultiMappingOutputDefinition definition : workingMappings.getOutputMappings()) {
      addOutputTab(definition, false);
    }
    wTabFolder.setSelection(0);

    Pattern p = Pattern.compile("^[/\\w]*(\\$\\{\\w+})[/.\\w]*");
    Matcher m = p.matcher(Const.NVL(mappingMeta.getFilename(), ""));
    if (!m.lookingAt()) {
      try {
        loadPipeline();
      } catch (Exception e) {
        // Ignore errors while populating the dialog
      }
    }
    try {
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
  }

  private void addParametersTab(MappingParameters parameters) {
    CTabItem wParametersTab = new CTabItem(wTabFolder, SWT.NONE);
    wParametersTab.setFont(GuiResource.getInstance().getFontDefault());
    wParametersTab.setText(BaseMessages.getString(PKG, "MultiMappingDialog.Parameters.Title"));
    wParametersTab.setToolTipText(
        BaseMessages.getString(PKG, "MultiMappingDialog.Parameters.Tooltip"));

    Composite composite = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(composite);
    FormLayout layout = new FormLayout();
    layout.marginWidth = 15;
    layout.marginHeight = 15;
    composite.setLayout(layout);

    Button wInheritAll = new Button(composite, SWT.CHECK);
    PropsUi.setLook(wInheritAll);
    wInheritAll.setText(BaseMessages.getString(PKG, "System.Parameters.PassParentValues.Label"));
    wInheritAll.setToolTipText(
        BaseMessages.getString(PKG, "System.Parameters.PassParentValues.Tooltip"));
    FormData fdInherit = new FormData();
    fdInherit.left = new FormAttachment(0, 0);
    fdInherit.bottom = new FormAttachment(100, 0);
    wInheritAll.setLayoutData(fdInherit);
    wInheritAll.setSelection(parameters.isInheritingAllVariables());

    ColumnInfo[] colinfo =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "MultiMappingDialog.Parameters.column.Variable"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MultiMappingDialog.Parameters.column.ValueOrField"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
        };
    colinfo[1].setUsingVariables(true);
    TableView wMappingParameters =
        new TableView(
            variables,
            composite,
            SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER,
            colinfo,
            Math.max(parameters.getVariableMappings().size(), 1),
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
      tableItem.setText(1, Const.NVL(mapping.getName(), ""));
      tableItem.setText(2, Const.NVL(mapping.getValue(), ""));
    }
    wMappingParameters.setRowNums();
    wMappingParameters.optWidth(true);

    wParametersTab.setControl(composite);
    changeList.add(
        () -> {
          parameters.getVariableMappings().clear();
          for (TableItem item : wMappingParameters.getNonEmptyItems()) {
            parameters
                .getVariableMappings()
                .add(new MappingVariableMapping(item.getText(1), item.getText(2)));
          }
          parameters.setInheritingAllVariables(wInheritAll.getSelection());
        });
  }

  private void addInputTab(MultiMappingInputDefinition definition, boolean addToModel) {
    if (addToModel) {
      workingMappings.getInputMappings().add(definition);
      mappingMeta.setChanged();
    }
    CTabItem tab = new CTabItem(wTabFolder, SWT.CLOSE);
    tab.setFont(GuiResource.getInstance().getFontDefault());
    tab.setData(definition);
    tab.setText(
        tabTitle(
            BaseMessages.getString(PKG, "MultiMappingDialog.InputTab.Title"),
            definition.getInputTransformName()));

    Composite composite = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(composite);
    FormLayout layout = new FormLayout();
    layout.marginWidth = 15;
    layout.marginHeight = 15;
    composite.setLayout(layout);

    Label wlSource = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlSource);
    wlSource.setText(
        BaseMessages.getString(PKG, "MultiMappingDialog.InputTab.label.InputSourceTransformName"));
    FormData fdlSource = new FormData();
    fdlSource.left = new FormAttachment(0, 0);
    fdlSource.top = new FormAttachment(0, 0);
    fdlSource.right = new FormAttachment(middle, -margin);
    wlSource.setLayoutData(fdlSource);
    CCombo wSource = new CCombo(composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSource);
    wSource.setItems(pipelineMeta.getPrevTransformNames(transformMeta));
    wSource.setText(Const.NVL(definition.getInputTransformName(), ""));
    FormData fdSource = new FormData();
    fdSource.left = new FormAttachment(wlSource, margin);
    fdSource.top = new FormAttachment(wlSource, 0, SWT.CENTER);
    fdSource.right = new FormAttachment(100, 0);
    wSource.setLayoutData(fdSource);

    Label wlTarget = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlTarget);
    wlTarget.setText(
        BaseMessages.getString(PKG, "MultiMappingDialog.InputTab.label.OutputTargetTransformName"));
    FormData fdlTarget = new FormData();
    fdlTarget.left = new FormAttachment(0, 0);
    fdlTarget.top = new FormAttachment(wSource, margin);
    fdlTarget.right = new FormAttachment(middle, -margin);
    wlTarget.setLayoutData(fdlTarget);
    CCombo wTarget = new CCombo(composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTarget);
    wTarget.setItems(childTransformNames(true));
    wTarget.setText(Const.NVL(definition.getOutputTransformName(), ""));
    FormData fdTarget = new FormData();
    fdTarget.left = new FormAttachment(wlTarget, margin);
    fdTarget.top = new FormAttachment(wlTarget, 0, SWT.CENTER);
    fdTarget.right = new FormAttachment(100, 0);
    wTarget.setLayoutData(fdTarget);

    Label wlDesc = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlDesc);
    wlDesc.setText(BaseMessages.getString(PKG, "MultiMappingDialog.InputTab.label.Description"));
    FormData fdlDesc = new FormData();
    fdlDesc.left = new FormAttachment(0, 0);
    fdlDesc.top = new FormAttachment(wTarget, margin);
    fdlDesc.right = new FormAttachment(middle, -margin);
    wlDesc.setLayoutData(fdlDesc);
    org.eclipse.swt.widgets.Text wDesc =
        new org.eclipse.swt.widgets.Text(composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDesc);
    wDesc.setText(Const.NVL(definition.getDescription(), ""));
    FormData fdDesc = new FormData();
    fdDesc.left = new FormAttachment(wlDesc, margin);
    fdDesc.top = new FormAttachment(wlDesc, 0, SWT.CENTER);
    fdDesc.right = new FormAttachment(100, 0);
    wDesc.setLayoutData(fdDesc);

    Button wMain = new Button(composite, SWT.CHECK);
    PropsUi.setLook(wMain);
    wMain.setText(BaseMessages.getString(PKG, "MultiMappingDialog.input.MainDataPath"));
    wMain.setSelection(definition.isMainDataPath());
    FormData fdMain = new FormData();
    fdMain.left = new FormAttachment(middle, margin);
    fdMain.top = new FormAttachment(wDesc, margin);
    wMain.setLayoutData(fdMain);

    Button wRename = new Button(composite, SWT.CHECK);
    PropsUi.setLook(wRename);
    wRename.setText(BaseMessages.getString(PKG, "MultiMappingDialog.input.RenamingOnOutput"));
    wRename.setSelection(definition.isRenamingOnOutput());
    FormData fdRename = new FormData();
    fdRename.left = new FormAttachment(middle, margin);
    fdRename.top = new FormAttachment(wMain, margin);
    wRename.setLayoutData(fdRename);

    Button wbEnterMapping = new Button(composite, SWT.PUSH);
    PropsUi.setLook(wbEnterMapping);
    wbEnterMapping.setText(BaseMessages.getString(PKG, "MultiMappingDialog.button.EnterMapping"));
    FormData fdbEnter = new FormData();
    fdbEnter.bottom = new FormAttachment(100);
    fdbEnter.right = new FormAttachment(100);
    wbEnterMapping.setLayoutData(fdbEnter);

    ColumnInfo[] colinfo =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "MultiMappingDialog.InputTab.column.SourceField"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MultiMappingDialog.InputTab.column.TargetField"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
        };
    TableView wFieldMappings =
        new TableView(
            variables,
            composite,
            SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER,
            colinfo,
            1,
            false,
            lsMod,
            props,
            false);
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.top = new FormAttachment(wRename, margin);
    fdFields.bottom = new FormAttachment(wbEnterMapping, -10);
    wFieldMappings.setLayoutData(fdFields);
    populateRenames(wFieldMappings, definition.getValueRenames());

    wbEnterMapping.addListener(
        SWT.Selection,
        e -> {
          try {
            loadPipeline();
            IRowMeta sourceRowMeta =
                Utils.isEmpty(wSource.getText())
                    ? pipelineMeta.getPrevTransformFields(variables, transformMeta)
                    : pipelineMeta.getTransformFields(
                        variables, pipelineMeta.findTransform(wSource.getText()));
            TransformMeta mappingInput =
                SimpleMappingMeta.findMappingInputTransform(mappingPipelineMeta, wTarget.getText());
            IRowMeta targetRowMeta =
                mappingPipelineMeta.getTransformFields(variables, mappingInput);
            EnterMappingDialog dialog =
                new EnterMappingDialog(
                    shell, sourceRowMeta.getFieldNames(), targetRowMeta.getFieldNames());
            List<SourceToTargetMapping> mappings = dialog.open();
            if (mappings != null) {
              wFieldMappings.clearAll(false);
              for (SourceToTargetMapping mapping : mappings) {
                TableItem item = new TableItem(wFieldMappings.table, SWT.NONE);
                item.setText(1, mapping.getSourceString(sourceRowMeta.getFieldNames()));
                item.setText(2, mapping.getTargetString(targetRowMeta.getFieldNames()));
              }
              wFieldMappings.removeEmptyRows();
              wFieldMappings.setRowNums();
              wFieldMappings.optWidth(true);
            }
          } catch (Exception ex) {
            new ErrorDialog(
                shell,
                BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
                BaseMessages.getString(
                    PKG,
                    "MultiMappingDialog.Exception.ErrorGettingMappingSourceAndTargetFields",
                    ex.toString()),
                ex);
          }
        });

    tab.setControl(composite);
    wTabFolder.setSelection(tab);
    changeList.add(
        () -> {
          definition.setInputTransformName(wSource.getText());
          definition.setOutputTransformName(wTarget.getText());
          definition.setDescription(wDesc.getText());
          definition.setMainDataPath(wMain.getSelection());
          definition.setRenamingOnOutput(wRename.getSelection());
          definition.getValueRenames().clear();
          for (TableItem item : wFieldMappings.getNonEmptyItems()) {
            definition
                .getValueRenames()
                .add(new MultiMappingInputRename(item.getText(1), item.getText(2)));
          }
        });
  }

  private void addOutputTab(MultiMappingOutputDefinition definition, boolean addToModel) {
    if (addToModel) {
      workingMappings.getOutputMappings().add(definition);
      mappingMeta.setChanged();
    }
    CTabItem tab = new CTabItem(wTabFolder, SWT.CLOSE);
    tab.setFont(GuiResource.getInstance().getFontDefault());
    tab.setData(definition);
    tab.setText(
        tabTitle(
            BaseMessages.getString(PKG, "MultiMappingDialog.OutputTab.Title"),
            definition.getOutputTransformName()));

    Composite composite = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(composite);
    FormLayout layout = new FormLayout();
    layout.marginWidth = 15;
    layout.marginHeight = 15;
    composite.setLayout(layout);

    Label wlSource = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlSource);
    wlSource.setText(
        BaseMessages.getString(PKG, "MultiMappingDialog.OutputTab.label.InputSourceTransformName"));
    FormData fdlSource = new FormData();
    fdlSource.left = new FormAttachment(0, 0);
    fdlSource.top = new FormAttachment(0, 0);
    fdlSource.right = new FormAttachment(middle, -margin);
    wlSource.setLayoutData(fdlSource);
    CCombo wSource = new CCombo(composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSource);
    wSource.setItems(childTransformNames(false));
    wSource.setText(Const.NVL(definition.getInputTransformName(), ""));
    FormData fdSource = new FormData();
    fdSource.left = new FormAttachment(wlSource, margin);
    fdSource.top = new FormAttachment(wlSource, 0, SWT.CENTER);
    fdSource.right = new FormAttachment(100, 0);
    wSource.setLayoutData(fdSource);

    Label wlTarget = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlTarget);
    wlTarget.setText(
        BaseMessages.getString(
            PKG, "MultiMappingDialog.OutputTab.label.OutputTargetTransformName"));
    FormData fdlTarget = new FormData();
    fdlTarget.left = new FormAttachment(0, 0);
    fdlTarget.top = new FormAttachment(wSource, margin);
    fdlTarget.right = new FormAttachment(middle, -margin);
    wlTarget.setLayoutData(fdlTarget);
    CCombo wTarget = new CCombo(composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTarget);
    wTarget.setItems(pipelineMeta.getNextTransformNames(transformMeta));
    wTarget.setText(Const.NVL(definition.getOutputTransformName(), ""));
    FormData fdTarget = new FormData();
    fdTarget.left = new FormAttachment(wlTarget, margin);
    fdTarget.top = new FormAttachment(wlTarget, 0, SWT.CENTER);
    fdTarget.right = new FormAttachment(100, 0);
    wTarget.setLayoutData(fdTarget);

    Label wlDesc = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlDesc);
    wlDesc.setText(BaseMessages.getString(PKG, "MultiMappingDialog.OutputTab.label.Description"));
    FormData fdlDesc = new FormData();
    fdlDesc.left = new FormAttachment(0, 0);
    fdlDesc.top = new FormAttachment(wTarget, margin);
    fdlDesc.right = new FormAttachment(middle, -margin);
    wlDesc.setLayoutData(fdlDesc);
    org.eclipse.swt.widgets.Text wDesc =
        new org.eclipse.swt.widgets.Text(composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDesc);
    wDesc.setText(Const.NVL(definition.getDescription(), ""));
    FormData fdDesc = new FormData();
    fdDesc.left = new FormAttachment(wlDesc, margin);
    fdDesc.top = new FormAttachment(wlDesc, 0, SWT.CENTER);
    fdDesc.right = new FormAttachment(100, 0);
    wDesc.setLayoutData(fdDesc);

    Button wMain = new Button(composite, SWT.CHECK);
    PropsUi.setLook(wMain);
    wMain.setText(BaseMessages.getString(PKG, "MultiMappingDialog.input.MainDataPath"));
    wMain.setSelection(definition.isMainDataPath());
    FormData fdMain = new FormData();
    fdMain.left = new FormAttachment(middle, margin);
    fdMain.top = new FormAttachment(wDesc, margin);
    wMain.setLayoutData(fdMain);

    Button wbGetFields = new Button(composite, SWT.PUSH);
    PropsUi.setLook(wbGetFields);
    wbGetFields.setText(BaseMessages.getString(PKG, "MultiMappingDialog.button.GetFields"));
    FormData fdbGet = new FormData();
    fdbGet.bottom = new FormAttachment(100);
    fdbGet.right = new FormAttachment(100);
    wbGetFields.setLayoutData(fdbGet);

    ColumnInfo[] colinfo =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "MultiMappingDialog.OutputTab.column.SourceField"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "MultiMappingDialog.OutputTab.column.TargetField"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
        };
    TableView wFieldMappings =
        new TableView(
            variables,
            composite,
            SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER,
            colinfo,
            1,
            false,
            lsMod,
            props,
            false);
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.top = new FormAttachment(wMain, margin);
    fdFields.bottom = new FormAttachment(wbGetFields, -10);
    wFieldMappings.setLayoutData(fdFields);
    populateOutputRenames(wFieldMappings, definition.getValueRenames());

    wbGetFields.addListener(
        SWT.Selection,
        e -> {
          try {
            loadPipeline();
            TransformMeta mappingOutput =
                SimpleMappingMeta.findMappingOutputTransform(
                    mappingPipelineMeta, wSource.getText());
            IRowMeta sourceRowMeta =
                mappingPipelineMeta.getTransformFields(variables, mappingOutput);
            BaseTransformDialog.getFieldsFromPrevious(
                sourceRowMeta,
                wFieldMappings,
                1,
                new int[] {1},
                new int[] {},
                -1,
                -1,
                (tableItem, v) -> {
                  tableItem.setText(2, tableItem.getText(1));
                  return true;
                });
          } catch (Exception ex) {
            new ErrorDialog(
                shell,
                BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
                BaseMessages.getString(
                    PKG,
                    "MultiMappingDialog.Exception.ErrorGettingMappingSourceAndTargetFields",
                    ex.toString()),
                ex);
          }
        });

    tab.setControl(composite);
    wTabFolder.setSelection(tab);
    changeList.add(
        () -> {
          definition.setInputTransformName(wSource.getText());
          definition.setOutputTransformName(wTarget.getText());
          definition.setDescription(wDesc.getText());
          definition.setMainDataPath(wMain.getSelection());
          definition.getValueRenames().clear();
          for (TableItem item : wFieldMappings.getNonEmptyItems()) {
            definition
                .getValueRenames()
                .add(new MultiMappingOutputRename(item.getText(1), item.getText(2)));
          }
        });
  }

  private void populateRenames(TableView view, List<MultiMappingInputRename> renames) {
    for (MultiMappingInputRename rename : renames) {
      TableItem item = new TableItem(view.table, SWT.NONE);
      item.setText(1, Const.NVL(rename.getSourceValueName(), ""));
      item.setText(2, Const.NVL(rename.getTargetValueName(), ""));
    }
    view.removeEmptyRows();
    view.setRowNums();
    view.optWidth(true);
  }

  private void populateOutputRenames(TableView view, List<MultiMappingOutputRename> renames) {
    for (MultiMappingOutputRename rename : renames) {
      TableItem item = new TableItem(view.table, SWT.NONE);
      item.setText(1, Const.NVL(rename.getSourceValueName(), ""));
      item.setText(2, Const.NVL(rename.getTargetValueName(), ""));
    }
    view.removeEmptyRows();
    view.setRowNums();
    view.optWidth(true);
  }

  private String[] childTransformNames(boolean mappingInput) {
    if (mappingPipelineMeta == null) {
      try {
        loadPipeline();
      } catch (Exception e) {
        return new String[0];
      }
    }
    if (mappingPipelineMeta == null) {
      return new String[0];
    }
    List<TransformMeta> transforms =
        mappingInput
            ? MappingTransforms.findMappingInputMetas(mappingPipelineMeta)
            : MappingTransforms.findMappingOutputMetas(mappingPipelineMeta);
    return transforms.stream().map(TransformMeta::getName).toArray(String[]::new);
  }

  private String tabTitle(String base, String transformName) {
    if (Utils.isEmpty(transformName)) {
      return base;
    }
    return base + " : " + transformName;
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
    if (Utils.isEmpty(wPath.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "MultiMappingDialog.FilenameMissing.Header"));
      mb.setMessage(BaseMessages.getString(PKG, "MultiMappingDialog.FilenameMissing.Message"));
      mb.open();
      return;
    }
    if (isSelfReferencing()) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "MultiMappingDialog.SelfReference.Header"));
      mb.setMessage(BaseMessages.getString(PKG, "MultiMappingDialog.SelfReference.Message"));
      mb.open();
      return;
    }

    transformName = wTransformName.getText();
    mappingMeta.setFilename(wPath.getText());
    mappingMeta.setRunConfigurationName(wRunConfig.getText());

    Pattern p = Pattern.compile("^[/\\w]*(\\$\\{\\w+})[/.\\w]*");
    Matcher m = p.matcher(mappingMeta.getFilename());
    if (!m.lookingAt()) {
      try {
        loadPipeline();
      } catch (HopException e) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "MultiMappingDialog.ErrorLoadingSpecifiedPipeline.Title"),
            BaseMessages.getString(PKG, "MultiMappingDialog.ErrorLoadingSpecifiedPipeline.Message"),
            e);
        return;
      }
    }

    for (Runnable applyChanges : changeList) {
      applyChanges.run();
    }
    mappingMeta.setIoMappings(workingMappings);
    mappingMeta.resetTransformIoMeta();
    mappingMeta.setChanged(true);
    dispose();
  }

  private boolean isSelfReferencing() {
    return variables.resolve(wPath.getText()).equals(variables.resolve(pipelineMeta.getFilename()));
  }
}
