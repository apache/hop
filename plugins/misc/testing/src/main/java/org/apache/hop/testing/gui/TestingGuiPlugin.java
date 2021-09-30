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

package org.apache.hop.testing.gui;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.action.GuiContextActionFilter;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.testing.*;
import org.apache.hop.testing.util.DataSetConst;
import org.apache.hop.testing.xp.PipelineMetaModifier;
import org.apache.hop.testing.xp.WriteToDataSetExtensionPoint;
import org.apache.hop.ui.core.dialog.EnterMappingDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.SelectRowDialog;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.pipeline.context.HopGuiPipelineTransformContext;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.testing.EditRowsDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.MessageBox;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@GuiPlugin
public class TestingGuiPlugin {
  public static final String ACTION_ID_PIPELINE_GRAPH_TRANSFORM_DEFINE_INPUT_DATA_SET =
      "pipeline-graph-transform-20200-define-input-data-set";
  public static final String ACTION_ID_PIPELINE_GRAPH_TRANSFORM_CLEAR_INPUT_DATA_SET =
      "pipeline-graph-transform-20210-clear-input-data-set";
  public static final String ACTION_ID_PIPELINE_GRAPH_TRANSFORM_DEFINE_GOLDEN_DATA_SET =
      "pipeline-graph-transform-20220-define-golden-data-set";
  public static final String ACTION_ID_PIPELINE_GRAPH_TRANSFORM_CLEAR_GOLDEN_DATA_SET =
      "pipeline-graph-transform-20240-clear-golden-data-set";
  public static final String ACTION_ID_PIPELINE_GRAPH_TRANSFORM_ENABLE_TWEAK_REMOVE_TRANSFORM =
      "pipeline-graph-transform-20800-enable-tweak-remove-transform";
  public static final String ACTION_ID_PIPELINE_GRAPH_TRANSFORM_DISABLE_TWEAK_REMOVE_TRANSFORM =
      "pipeline-graph-transform-20810-disable-tweak-remove-transform";
  public static final String ACTION_ID_PIPELINE_GRAPH_TRANSFORM_ENABLE_TWEAK_BYPASS_TRANSFORM =
      "pipeline-graph-transform-20820-enable-tweak-bypass-transform";
  public static final String ACTION_ID_PIPELINE_GRAPH_TRANSFORM_DISABLE_TWEAK_BYPASS_TRANSFORM =
      "pipeline-graph-transform-20830-disable-tweak-bypass-transform";
  protected static Class<?> PKG = TestingGuiPlugin.class; // For Translator

  public static final String ID_TOOLBAR_ITEM_UNIT_TEST_EDIT =
      "HopGuiPipelineGraph-ToolBar-20015-unit-test-edit";
  public static final String ID_TOOLBAR_ITEM_UNIT_TESTS_CREATE =
      "HopGuiPipelineGraph-ToolBar-20020-unit-tests-create";
  public static final String ID_TOOLBAR_ITEM_UNIT_TEST_DETACH =
      "HopGuiPipelineGraph-ToolBar-20030-unit-test-detach";
  public static final String ID_TOOLBAR_ITEM_UNIT_TESTS_DELETE =
      "HopGuiPipelineGraph-ToolBar-20050-unit-tests-delete";

  public static final String ID_TOOLBAR_UNIT_TESTS_LABEL =
      "HopGuiPipelineGraph-ToolBar-20000-unit-tests-label";
  public static final String ID_TOOLBAR_UNIT_TESTS_COMBO =
      "HopGuiPipelineGraph-ToolBar-20010-unit-tests-combo";

  private static TestingGuiPlugin instance = null;

  public TestingGuiPlugin() {}

  public static TestingGuiPlugin getInstance() {
    if (instance == null) {
      instance = new TestingGuiPlugin();
    }
    return instance;
  }

  public static String validateDataSet(
      DataSet dataSet, String previousName, List<String> setNames) {

    String message = null;

    String newName = dataSet.getName();
    if (StringUtil.isEmpty(newName)) {
      message = BaseMessages.getString(PKG, "TestingGuiPlugin.DataSet.NoNameSpecified.Message");
    } else if (!StringUtil.isEmpty(previousName) && !previousName.equals(newName)) {
      message =
          BaseMessages.getString(
              PKG, "TestingGuiPlugin.DataSet.RenamingOfADataSetsNotSupported.Message");
    } else {
      if (StringUtil.isEmpty(previousName) && Const.indexOfString(newName, setNames) >= 0) {
        message =
            BaseMessages.getString(
                PKG, "TestingGuiPlugin.DataSet.ADataSetWithNameExists.Message", newName);
      }
    }

    return message;
  }

  /** We set an input data set */
  @GuiContextAction(
      id = ACTION_ID_PIPELINE_GRAPH_TRANSFORM_DEFINE_INPUT_DATA_SET,
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::TestingGuiPlugin.ContextAction.SetInputDataset.Name",
      tooltip = "i18n::TestingGuiPlugin.ContextAction.SetInputDataset.Tooltip",
      image = "set-input-dataset.svg",
      category = "i18n::TestingGuiPlugin.Category",
      categoryOrder = "8")
  public void setInputDataSet(HopGuiPipelineTransformContext context) {
    HopGui hopGui = HopGui.getInstance();
    IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();
    IVariables variables = context.getPipelineGraph().getVariables();
    PipelineMeta pipelineMeta = context.getPipelineMeta();
    TransformMeta transformMeta = context.getTransformMeta();

    if (checkTestPresent(hopGui, pipelineMeta)) {
      return;
    }
    PipelineUnitTest unitTest = getCurrentUnitTest(pipelineMeta);

    try {

      IHopMetadataSerializer<DataSet> setSerializer = metadataProvider.getSerializer(DataSet.class);
      List<String> setNames = setSerializer.listObjectNames();
      Collections.sort(setNames);
      EnterSelectionDialog esd =
          new EnterSelectionDialog(
              hopGui.getShell(),
              setNames.toArray(new String[setNames.size()]),
              BaseMessages.getString(PKG, "TestingGuiPlugin.ContextAction.SetInputDataset.Header"),
              BaseMessages.getString(
                  PKG, "TestingGuiPlugin.ContextAction.SetInputDataset.Message"));
      String setName = esd.open();
      if (setName != null) {
        DataSet dataSet = setSerializer.load(setName);
        boolean changed =
            setInputDataSetOnTransform(
                variables, metadataProvider, pipelineMeta, transformMeta, unitTest, dataSet);
        if (changed) {
          context.getPipelineGraph().updateGui();
        }
      }
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(
              PKG, "TestingGuiPlugin.ContextAction.SetInputDataset.Error.Header"),
          BaseMessages.getString(
              PKG, "TestingGuiPlugin.ContextAction.SetInputDataset.Error.Message"),
          e);
    }
  }

  private boolean setInputDataSetOnTransform(
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      PipelineUnitTest unitTest,
      DataSet dataSet)
      throws HopPluginException, HopValueException, HopException {
    HopGui hopGui = HopGui.getInstance();

    // Now we need to map the fields from the input data set to the transform...
    //
    IRowMeta setFields = dataSet.getSetRowMeta();
    IRowMeta transformFields;
    try {
      transformFields = pipelineMeta.getTransformFields(variables, transformMeta);
    } catch (HopTransformException e) {
      // Driver or input problems...
      //
      transformFields = new RowMeta();
    }
    if (transformFields.isEmpty()) {
      transformFields = setFields.clone();
    }

    String[] transformFieldNames = transformFields.getFieldNames();
    String[] setFieldNames = setFields.getFieldNames();

    EnterMappingDialog mappingDialog =
        new EnterMappingDialog(hopGui.getShell(), setFieldNames, transformFieldNames);
    List<SourceToTargetMapping> mappings = mappingDialog.open();
    if (mappings == null) {
      return false;
    }

    // Ask about the sort order...
    // Show the mapping as well as an order column
    //
    IRowMeta sortMeta = new RowMeta();
    sortMeta.addValueMeta(
        new ValueMetaString(
            BaseMessages.getString(PKG, "TestingGuiPlugin.SortOrder.Column.SetField")));
    List<Object[]> sortData = new ArrayList<>();
    for (String setFieldName : setFieldNames) {
      sortData.add(new Object[] {setFieldName});
    }
    EditRowsDialog orderDialog =
        new EditRowsDialog(
            hopGui.getShell(),
            SWT.NONE,
            BaseMessages.getString(PKG, "TestingGuiPlugin.SortOrder.Title"),
            BaseMessages.getString(PKG, "TestingGuiPlugin.SortOrder.Message"),
            sortMeta,
            sortData);
    List<Object[]> orderMappings = orderDialog.open();
    if (orderMappings == null) {
      return false;
    }

    // Modify the test
    //

    // Remove other crap on the transform...
    //
    unitTest.removeInputAndGoldenDataSets(transformMeta.getName());

    PipelineUnitTestSetLocation inputLocation = new PipelineUnitTestSetLocation();
    unitTest.getInputDataSets().add(inputLocation);

    inputLocation.setTransformName(transformMeta.getName());
    inputLocation.setDataSetName(dataSet.getName());
    List<PipelineUnitTestFieldMapping> fieldMappings = inputLocation.getFieldMappings();
    fieldMappings.clear();

    for (SourceToTargetMapping mapping : mappings) {
      String transformFieldName = mapping.getTargetString(transformFieldNames);
      String setFieldName = mapping.getSourceString(setFieldNames);
      fieldMappings.add(new PipelineUnitTestFieldMapping(transformFieldName, setFieldName));
    }

    List<String> setFieldOrder = new ArrayList<>();
    for (Object[] orderMapping : orderMappings) {
      String setFieldName = sortMeta.getString(orderMapping, 0);
      setFieldOrder.add(setFieldName);
    }
    inputLocation.setFieldOrder(setFieldOrder);

    // Save the unit test...
    //
    saveUnitTest(variables, metadataProvider, unitTest, pipelineMeta);

    transformMeta.setChanged();
    return true;
  }

  /** We set an golden data set on the selected unit test */
  @GuiContextAction(
      id = ACTION_ID_PIPELINE_GRAPH_TRANSFORM_CLEAR_INPUT_DATA_SET,
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Delete,
      name = "i18n::TestingGuiPlugin.ContextAction.ClearInputDataset.Name",
      tooltip = "i18n::TestingGuiPlugin.ContextAction.ClearInputDataset.Message",
      image = "clear-input-dataset.svg",
      category = "i18n::TestingGuiPlugin.Category",
      categoryOrder = "8")
  public void clearInputDataSet(HopGuiPipelineTransformContext context) {
    HopGui hopGui = ((HopGui) HopGui.getInstance());
    PipelineMeta pipelineMeta = context.getPipelineMeta();
    TransformMeta transformMeta = context.getTransformMeta();
    IVariables variables = context.getPipelineGraph().getVariables();

    if (checkTestPresent(hopGui, pipelineMeta)) {
      return;
    }

    try {
      PipelineUnitTest currentUnitTest = getCurrentUnitTest(pipelineMeta);

      PipelineUnitTestSetLocation inputLocation =
          currentUnitTest.findInputLocation(transformMeta.getName());
      if (inputLocation != null) {
        currentUnitTest.getInputDataSets().remove(inputLocation);
      }

      saveUnitTest(variables, hopGui.getMetadataProvider(), currentUnitTest, pipelineMeta);

      context.getPipelineGraph().updateGui();
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(
              PKG, "TestingGuiPlugin.ContextAction.ClearInputDataset.Error.Header"),
          BaseMessages.getString(
              PKG, "TestingGuiPlugin.ContextAction.ClearInputDataset.Error.Message"),
          e);
    }
  }

  private boolean checkTestPresent(HopGui hopGui, PipelineMeta pipelineMeta) {

    PipelineUnitTest activeTest = getCurrentUnitTest(pipelineMeta);
    if (activeTest != null) {
      return false;
    }

    // there is no test defined of selected in the pipeline.
    // Show a warning
    //
    MessageBox box = new MessageBox(hopGui.getShell(), SWT.OK | SWT.ICON_INFORMATION);
    box.setMessage(
        BaseMessages.getString(PKG, "TestingGuiPlugin.ContextAction.CheckTestPresent.Message"));
    box.setText(
        BaseMessages.getString(PKG, "TestingGuiPlugin.ContextAction.CheckTestPresent.Header"));
    box.open();

    return true;
  }

  /** We set an golden data set on the selected unit test */
  @GuiContextAction(
      id = ACTION_ID_PIPELINE_GRAPH_TRANSFORM_DEFINE_GOLDEN_DATA_SET,
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::TestingGuiPlugin.ContextAction.SetGoldenDataset.Name",
      tooltip = "i18n::TestingGuiPlugin.ContextAction.SetGoldenDataset.Tooltip",
      image = "set-golden-dataset.svg",
      category = "i18n::TestingGuiPlugin.Category",
      categoryOrder = "8")
  public void setGoldenDataSet(HopGuiPipelineTransformContext context) {
    PipelineMeta sourcePipelineMeta = context.getPipelineMeta();
    TransformMeta transformMeta = context.getTransformMeta();
    HopGuiPipelineGraph pipelineGraph = context.getPipelineGraph();
    IVariables variables = pipelineGraph.getVariables();
    HopGui hopGui = HopGui.getInstance();
    IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();

    if (checkTestPresent(hopGui, sourcePipelineMeta)) {
      return;
    }
    PipelineUnitTest unitTest = getCurrentUnitTest(sourcePipelineMeta);

    try {
      // Create a copy and modify the pipeline
      // This way we have
      PipelineMetaModifier modifier =
          new PipelineMetaModifier(variables, sourcePipelineMeta, unitTest);
      PipelineMeta pipelineMeta =
          modifier.getTestPipeline(LogChannel.UI, variables, metadataProvider);

      IHopMetadataSerializer<DataSet> setSerializer = metadataProvider.getSerializer(DataSet.class);
      List<String> setNames = setSerializer.listObjectNames();
      Collections.sort(setNames);
      EnterSelectionDialog esd =
          new EnterSelectionDialog(
              hopGui.getShell(),
              setNames.toArray(new String[setNames.size()]),
              BaseMessages.getString(PKG, "TestingGuiPlugin.ContextAction.SetGoldenDataset.Header"),
              BaseMessages.getString(
                  PKG, "TestingGuiPlugin.ContextAction.SetGoldenDataset.Message"));
      String setName = esd.open();
      if (setName != null) {
        DataSet dataSet = setSerializer.load(setName);
        boolean changed =
            setGoldenDataSetOnTransform(
                variables, metadataProvider, pipelineMeta, transformMeta, unitTest, dataSet);
        if (changed) {
          pipelineGraph.updateGui();
        }
      }
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(
              PKG, "TestingGuiPlugin.ContextAction.SetGoldenDataset.Error.Header"),
          BaseMessages.getString(
              PKG, "TestingGuiPlugin.ContextAction.SetGoldenDataset.Error.Message"),
          e);
    }
  }

  private boolean setGoldenDataSetOnTransform(
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      PipelineUnitTest unitTest,
      DataSet dataSet)
      throws HopPluginException, HopValueException, HopException {
    // Now we need to map the fields from the transform to golden data set fields...
    //
    IRowMeta transformFields;
    try {
      transformFields = pipelineMeta.getPrevTransformFields(variables, transformMeta);
    } catch (HopTransformException e) {
      // Ignore error: issues with not being able to get fields because of the unit test
      // running in a different environment.
      //
      transformFields = new RowMeta();
    }
    IRowMeta setFields = dataSet.getSetRowMeta();

    String[] transformFieldNames = transformFields.getFieldNames();
    String[] setFieldNames = setFields.getFieldNames();

    EnterMappingDialog mappingDialog =
        new EnterMappingDialog(HopGui.getInstance().getShell(), transformFieldNames, setFieldNames);
    List<SourceToTargetMapping> mappings = mappingDialog.open();
    if (mappings == null) {
      return false;
    }

    // Ask about the sort order...
    // Show the mapping as well as an order column
    //
    IRowMeta sortMeta = new RowMeta();
    sortMeta.addValueMeta(
        new ValueMetaString(
            BaseMessages.getString(PKG, "TestingGuiPlugin.SortOrder.Column.SetField")));
    List<Object[]> sortData = new ArrayList<>();
    for (String setFieldName : setFieldNames) {
      sortData.add(new Object[] {setFieldName});
    }
    EditRowsDialog orderDialog =
        new EditRowsDialog(
            HopGui.getInstance().getShell(),
            SWT.NONE,
            BaseMessages.getString(PKG, "TestingGuiPlugin.SortOrder.Title"),
            BaseMessages.getString(PKG, "TestingGuiPlugin.SortOrder.Message"),
            sortMeta,
            sortData);
    List<Object[]> orderMappings = orderDialog.open();
    if (orderMappings == null) {
      return false;
    }

    // Modify the test
    //

    // Remove golden locations and input locations on the transform to avoid duplicates
    //
    unitTest.removeInputAndGoldenDataSets(transformMeta.getName());

    PipelineUnitTestSetLocation goldenLocation = new PipelineUnitTestSetLocation();
    unitTest.getGoldenDataSets().add(goldenLocation);

    goldenLocation.setTransformName(transformMeta.getName());
    goldenLocation.setDataSetName(dataSet.getName());
    List<PipelineUnitTestFieldMapping> fieldMappings = goldenLocation.getFieldMappings();
    fieldMappings.clear();

    for (SourceToTargetMapping mapping : mappings) {
      fieldMappings.add(
          new PipelineUnitTestFieldMapping(
              mapping.getSourceString(transformFieldNames),
              mapping.getTargetString(setFieldNames)));
    }

    List<String> setFieldOrder = new ArrayList<>();
    for (Object[] orderMapping : orderMappings) {
      setFieldOrder.add(sortMeta.getString(orderMapping, 0));
    }
    goldenLocation.setFieldOrder(setFieldOrder);

    // Save the unit test...
    //
    saveUnitTest(variables, metadataProvider, unitTest, pipelineMeta);

    transformMeta.setChanged();

    return true;
  }

  /** We set an golden data set on the selected unit test */
  @GuiContextAction(
      id = ACTION_ID_PIPELINE_GRAPH_TRANSFORM_CLEAR_GOLDEN_DATA_SET,
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Delete,
      name = "i18n::TestingGuiPlugin.ContextAction.ClearGoldenDataset.Name",
      tooltip = "i18n::TestingGuiPlugin.ContextAction.ClearGoldenDataset.Tooltip",
      image = "clear-golden-dataset.svg",
      category = "i18n::TestingGuiPlugin.Category",
      categoryOrder = "8")
  public void clearGoldenDataSet(HopGuiPipelineTransformContext context) {
    HopGui hopGui = HopGui.getInstance();
    PipelineMeta pipelineMeta = context.getPipelineMeta();
    TransformMeta transformMeta = context.getTransformMeta();
    IVariables variables = context.getPipelineGraph().getVariables();

    if (checkTestPresent(hopGui, pipelineMeta)) {
      return;
    }

    try {
      PipelineUnitTest currentUnitTest = getCurrentUnitTest(pipelineMeta);

      PipelineUnitTestSetLocation goldenLocation =
          currentUnitTest.findGoldenLocation(transformMeta.getName());
      if (goldenLocation != null) {
        currentUnitTest.getGoldenDataSets().remove(goldenLocation);
      }

      saveUnitTest(variables, hopGui.getMetadataProvider(), currentUnitTest, pipelineMeta);
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(
              PKG, "TestingGuiPlugin.ContextAction.ClearGoldenDataset.Error.Header"),
          BaseMessages.getString(
              PKG, "TestingGuiPlugin.ContextAction.ClearGoldenDataset.Error.Message"),
          e);
    }
    pipelineMeta.setChanged();
    context.getPipelineGraph().updateGui();
  }

  @GuiContextActionFilter(parentId = HopGuiPipelineTransformContext.CONTEXT_ID)
  public boolean filterTestingActions(
      String contextActionId, HopGuiPipelineTransformContext context) {
    PipelineUnitTest currentTest = getCurrentUnitTest(context.getPipelineMeta());

    // Input & golden data set handling
    //
    if (ACTION_ID_PIPELINE_GRAPH_TRANSFORM_DEFINE_INPUT_DATA_SET.equals(contextActionId)) {
      return currentTest != null
          && currentTest.findInputLocation(context.getTransformMeta().getName()) == null;
    }
    if (ACTION_ID_PIPELINE_GRAPH_TRANSFORM_CLEAR_INPUT_DATA_SET.equals(contextActionId)) {
      return currentTest != null
          && currentTest.findInputLocation(context.getTransformMeta().getName()) != null;
    }
    if (ACTION_ID_PIPELINE_GRAPH_TRANSFORM_DEFINE_GOLDEN_DATA_SET.equals(contextActionId)) {
      return currentTest != null
          && currentTest.findGoldenLocation(context.getTransformMeta().getName()) == null;
    }
    if (ACTION_ID_PIPELINE_GRAPH_TRANSFORM_CLEAR_GOLDEN_DATA_SET.equals(contextActionId)) {
      return currentTest != null
          && currentTest.findGoldenLocation(context.getTransformMeta().getName()) != null;
    }

    // Tweaks
    //
    PipelineUnitTestTweak tweak = null;
    if (currentTest != null) {
      tweak = currentTest.findTweak(context.getTransformMeta().getName());
    }
    if (ACTION_ID_PIPELINE_GRAPH_TRANSFORM_ENABLE_TWEAK_REMOVE_TRANSFORM.equals(contextActionId)) {
      return currentTest != null && tweak == null;
    }
    if (ACTION_ID_PIPELINE_GRAPH_TRANSFORM_DISABLE_TWEAK_REMOVE_TRANSFORM.equals(contextActionId)) {
      return currentTest != null
          && tweak != null
          && tweak.getTweak() == PipelineTweak.REMOVE_TRANSFORM;
    }
    if (ACTION_ID_PIPELINE_GRAPH_TRANSFORM_ENABLE_TWEAK_BYPASS_TRANSFORM.equals(contextActionId)) {
      return currentTest != null && tweak == null;
    }
    if (ACTION_ID_PIPELINE_GRAPH_TRANSFORM_DISABLE_TWEAK_BYPASS_TRANSFORM.equals(contextActionId)) {
      return currentTest != null
          && tweak != null
          && tweak.getTweak() == PipelineTweak.BYPASS_TRANSFORM;
    }

    return true;
  }

  /** Create a new data set with the output from */
  @GuiContextAction(
      id = "pipeline-graph-transform-20400-create-data-set-from-transform",
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Delete,
      name = "i18n::TestingGuiPlugin.ContextAction.CreateDataset.Name",
      tooltip = "i18n::TestingGuiPlugin.ContextAction.CreateDataset.Tooltip",
      image = "create-dataset.svg",
      category = "i18n::TestingGuiPlugin.Category",
      categoryOrder = "8")
  public void createDataSetFromTransform(HopGuiPipelineTransformContext context) {
    HopGui hopGui = HopGui.getInstance();
    IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();
    IVariables variables = context.getPipelineGraph().getVariables();

    TransformMeta transformMeta = context.getTransformMeta();
    PipelineMeta pipelineMeta = context.getPipelineMeta();

    try {
      DataSet dataSet = new DataSet();

      IRowMeta rowMeta = pipelineMeta.getTransformFields(variables, transformMeta);
      for (int i = 0; i < rowMeta.size(); i++) {
        IValueMeta valueMeta = rowMeta.getValueMeta(i);
        String setFieldName = valueMeta.getName();
        DataSetField field =
            new DataSetField(
                setFieldName,
                valueMeta.getType(),
                valueMeta.getLength(),
                valueMeta.getPrecision(),
                valueMeta.getComments(),
                valueMeta.getFormatMask());
        dataSet.getFields().add(field);
      }

      MetadataManager<DataSet> manager =
          new MetadataManager<>(hopGui.getVariables(), hopGui.getMetadataProvider(), DataSet.class);
      if (manager.newMetadata(dataSet) != null) {

        PipelineUnitTest unitTest = getCurrentUnitTest(pipelineMeta);
        if (unitTest == null) {
          return;
        }

        // Now that the data set is created and we have an active unit test, perhaps the user wants
        // to use it on the transform?
        //
        MessageBox box =
            new MessageBox(hopGui.getShell(), SWT.YES | SWT.NO | SWT.CANCEL | SWT.ICON_QUESTION);
        box.setText(
            BaseMessages.getString(
                PKG, "TestingGuiPlugin.ContextAction.CreateDataset.DatasetType.Header"));
        box.setMessage(
            BaseMessages.getString(
                    PKG,
                    "TestingGuiPlugin.ContextAction.CreateDataset.DatasetType.Message",
                    dataSet.getName(),
                    transformMeta.getName())
                + Const.CR
                + BaseMessages.getString(
                    PKG, "TestingGuiPlugin.ContextAction.CreateDataset.DatasetType.Answer1")
                + Const.CR
                + BaseMessages.getString(
                    PKG, "TestingGuiPlugin.ContextAction.CreateDataset.DatasetType.Answer2")
                + Const.CR
                + BaseMessages.getString(
                    PKG, "TestingGuiPlugin.ContextAction.CreateDataset.DatasetType.Answer3")
                + Const.CR);
        int answer = box.open();
        if ((answer & SWT.YES) != 0) {
          // set the new data set as an input
          //
          setInputDataSetOnTransform(
              variables, metadataProvider, pipelineMeta, transformMeta, unitTest, dataSet);
        }
        if ((answer & SWT.NO) != 0) {
          setGoldenDataSetOnTransform(
              variables, metadataProvider, pipelineMeta, transformMeta, unitTest, dataSet);
        }
      }
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "TestingGuiPlugin.ContextAction.CreateDataset.Error.Header"),
          BaseMessages.getString(PKG, "TestingGuiPlugin.ContextAction.CreateDataset.Error.Message"),
          e);
    }
  }

  /**
   * Ask which data set to write to Ask for the mapping between the output row and the data set
   * field Start the pipeline and capture the output of the transform, write to the database table
   * backing the data set.
   */
  @GuiContextAction(
      id = "pipeline-graph-transform-20500-write-transform-data-to-set",
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Create,
      name = "i18n::TestingGuiPlugin.ContextAction.Run.Name",
      tooltip = "i18n::TestingGuiPlugin.ContextAction.Run.Tooltip",
      image = "write-to-dataset.svg",
      category = "i18n::TestingGuiPlugin.Category",
      categoryOrder = "8")
  public void writeTransformDataToDataSet(HopGuiPipelineTransformContext context) {
    HopGui hopGui = HopGui.getInstance();
    IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();
    IVariables variables = context.getPipelineGraph().getVariables();

    TransformMeta transformMeta = context.getTransformMeta();
    PipelineMeta pipelineMeta = context.getPipelineMeta();

    if (pipelineMeta.hasChanged()) {
      MessageBox box = new MessageBox(hopGui.getShell(), SWT.OK | SWT.ICON_INFORMATION);
      box.setText(
          BaseMessages.getString(
              PKG, "TestingGuiPlugin.ContextAction.Run.SavePipelineDialog.Header"));
      box.setMessage(
          BaseMessages.getString(
              PKG, "TestingGuiPlugin.ContextAction.Run.SavePipelineDialog.Message"));
      box.open();
      return;
    }

    try {
      IHopMetadataSerializer<DataSet> setSerializer = metadataProvider.getSerializer(DataSet.class);

      // Ask which data set to write to
      //
      List<String> setNames = setSerializer.listObjectNames();
      Collections.sort(setNames);
      EnterSelectionDialog esd =
          new EnterSelectionDialog(
              hopGui.getShell(),
              setNames.toArray(new String[setNames.size()]),
              BaseMessages.getString(PKG, "TestingGuiPlugin.ContextAction.Run.ActionList.Item1"),
              BaseMessages.getString(PKG, "TestingGuiPlugin.ContextAction.Run.ActionList.Item2"));
      String setName = esd.open();
      if (setName == null) {
        return;
      }

      DataSet dataSet = setSerializer.load(setName);

      String[] setFields = new String[dataSet.getFields().size()];
      for (int i = 0; i < setFields.length; i++) {
        setFields[i] = dataSet.getFields().get(i).getFieldName();
      }

      IRowMeta rowMeta = pipelineMeta.getTransformFields(variables, transformMeta);
      String[] transformFields = new String[rowMeta.size()];
      for (int i = 0; i < rowMeta.size(); i++) {
        IValueMeta valueMeta = rowMeta.getValueMeta(i);
        transformFields[i] = valueMeta.getName();
      }

      // Ask for the mapping between the output row and the data set field
      //
      EnterMappingDialog mappingDialog =
          new EnterMappingDialog(hopGui.getShell(), transformFields, setFields);
      List<SourceToTargetMapping> mapping = mappingDialog.open();
      if (mapping == null) {
        return;
      }

      // Run the pipeline.  We want to use the standard HopGui runFile() method
      // So we need to leave the source to target mapping list somewhere so it can be picked up
      // later.
      // For now we'll leave it where we need it.
      //
      WriteToDataSetExtensionPoint.transformsMap.put(pipelineMeta.getName(), transformMeta);
      WriteToDataSetExtensionPoint.mappingsMap.put(pipelineMeta.getName(), mapping);
      WriteToDataSetExtensionPoint.setsMap.put(pipelineMeta.getName(), dataSet);

      // Signal to the pipeline xp plugin to inject data into some data set
      //
      variables.setVariable(DataSetConst.VAR_WRITE_TO_DATASET, "Y");

      // Start the pipeline
      //
      context.getPipelineGraph().start();

    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "TestingGuiPlugin.ContextAction.Run.Error.Header"),
          BaseMessages.getString(PKG, "TestingGuiPlugin.ContextAction.Run.Error.Message"),
          e);
    }
  }

  private void saveUnitTest(
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      PipelineUnitTest unitTest,
      PipelineMeta pipelineMeta)
      throws HopException {
    unitTest.setRelativeFilename(variables, pipelineMeta.getFilename());
    metadataProvider.getSerializer(PipelineUnitTest.class).save(unitTest);
  }

  /** Clear the current unit test from the active pipeline... */
  @GuiToolbarElement(
      root = HopGuiPipelineGraph.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = ID_TOOLBAR_ITEM_UNIT_TEST_DETACH,
      toolTip = "i18n::TestingGuiPlugin.ToolbarElement.UnitTest.Detach.Tooltip",
      image = "Test_tube_icon_detach.svg")
  public void detachUnitTest() {
    HopGui hopGui = HopGui.getInstance();
    HopGuiPipelineGraph pipelineGraph = HopGui.getActivePipelineGraph();
    if (pipelineGraph == null) {
      return;
    }

    try {
      PipelineMeta pipelineMeta = getActivePipelineMeta();
      if (pipelineMeta == null) {
        return;
      }

      // Remove
      //
      Map<String, Object> stateMap = getStateMap(pipelineMeta);
      if (stateMap != null) {
        stateMap.remove(DataSetConst.STATE_KEY_ACTIVE_UNIT_TEST);
      }

      // Clear the combo box
      //
      Combo combo = getUnitTestsCombo();
      if (combo != null) {
        combo.setText("");
      }

      // Update the GUI
      //
      pipelineGraph.updateGui();
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "TestingGuiPlugin.ToolbarElement.Detach.Error.Header"),
          BaseMessages.getString(PKG, "TestingGuiPlugin.ToolbarElement.Detach.Error.Message"),
          e);
    }
  }

  @GuiToolbarElement(
      root = HopGuiPipelineGraph.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = ID_TOOLBAR_ITEM_UNIT_TEST_EDIT,
      toolTip = "i18n::TestingGuiPlugin.ToolbarElement.UnitTest.Edit.Tooltip",
      image = "Test_tube_icon_edit.svg",
      separator = true)
  public void editUnitTest() {
    HopGui hopGui = HopGui.getInstance();
    PipelineMeta pipelineMeta = getActivePipelineMeta();
    if (pipelineMeta == null) {
      return;
    }
    PipelineUnitTest unitTest = getCurrentUnitTest(pipelineMeta);

    MetadataManager<PipelineUnitTest> manager =
        new MetadataManager<>(
            hopGui.getVariables(), hopGui.getMetadataProvider(), PipelineUnitTest.class);
    if (manager.editMetadata(unitTest.getName())) {
      // Activate the test
      refreshUnitTestsList();
      selectUnitTest(pipelineMeta, unitTest);
    }
  }

  @GuiToolbarElement(
      root = HopGuiPipelineGraph.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = ID_TOOLBAR_ITEM_UNIT_TESTS_CREATE,
      toolTip = "i18n::TestingGuiPlugin.ToolbarElement.UnitTest.Create.Tooltip",
      image = "Test_tube_icon_create.svg",
      separator = true)
  public void createUnitTest() {
    HopGui hopGui = HopGui.getInstance();
    PipelineMeta pipelineMeta = getActivePipelineMeta();
    if (pipelineMeta == null) {
      return;
    }

    MetadataManager<PipelineUnitTest> manager =
        new MetadataManager<>(
            hopGui.getVariables(), hopGui.getMetadataProvider(), PipelineUnitTest.class);
    PipelineUnitTest test = manager.newMetadata();
    if (test != null) {
      // Activate the test
      refreshUnitTestsList();
      selectUnitTest(pipelineMeta, test);
    }
  }

  @GuiToolbarElement(
      root = HopGuiPipelineGraph.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = ID_TOOLBAR_ITEM_UNIT_TESTS_DELETE,
      toolTip = "i18n::TestingGuiPlugin.ToolbarElement.UnitTest.Delete.Tooltip",
      image = "Test_tube_icon_delete.svg",
      separator = true)
  public void deleteUnitTest() {
    HopGui hopGui = HopGui.getInstance();
    PipelineMeta pipelineMeta = getActivePipelineMeta();
    if (pipelineMeta == null) {
      return;
    }
    Combo combo = getUnitTestsCombo();
    if (combo == null) {
      return;
    }
    if (StringUtils.isEmpty(combo.getText())) {
      return;
    }

    // Load the test, delete it after confirmation
    //
    try {
      IHopMetadataSerializer<PipelineUnitTest> testSerializer =
          hopGui.getMetadataProvider().getSerializer(PipelineUnitTest.class);
      PipelineUnitTest pipelineUnitTest = testSerializer.load(combo.getText());
      if (pipelineUnitTest == null) {
        return; // doesn't exist
      }

      MessageBox box = new MessageBox(hopGui.getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
      box.setMessage(
          BaseMessages.getString(
              PKG,
              "TestingGuiPlugin.ToolbarElement.Delete.Confirmation.Message",
              pipelineUnitTest.getName()));
      box.setText(
          BaseMessages.getString(
              PKG, "TestingGuiPlugin.ToolbarElement.Delete.Confirmation.Header"));
      int answer = box.open();
      if ((answer & SWT.YES) == 0) {
        return;
      }

      // First detach it.
      //
      detachUnitTest();

      // Then delete it.
      //
      testSerializer.delete(pipelineUnitTest.getName());

      refreshUnitTestsList();
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "TestingGuiPlugin.ToolbarElement.Delete.Error.Header"),
          BaseMessages.getString(PKG, "TestingGuiPlugin.ToolbarElement.Delete.Error.Message"),
          e);
    }
  }

  private Combo getUnitTestsCombo() {
    HopGuiPipelineGraph pipelineGraph = HopGui.getActivePipelineGraph();
    if (pipelineGraph == null) {
      return null;
    }
    Control control =
        pipelineGraph.getToolBarWidgets().getWidgetsMap().get(ID_TOOLBAR_UNIT_TESTS_COMBO);
    if ((control != null) && (control instanceof Combo)) {
      Combo combo = (Combo) control;
      return combo;
    }
    return null;
  }

  public static void refreshUnitTestsList() {
    HopGuiPipelineGraph pipelineGraph = HopGui.getActivePipelineGraph();
    if (pipelineGraph == null) {
      return;
    }
    pipelineGraph.getToolBarWidgets().refreshComboItemList(ID_TOOLBAR_UNIT_TESTS_COMBO);
  }

  public static void selectUnitTestInList(String name) {
    HopGuiPipelineGraph pipelineGraph = HopGui.getActivePipelineGraph();
    if (pipelineGraph == null) {
      return;
    }
    pipelineGraph.getToolBarWidgets().selectComboItem(ID_TOOLBAR_UNIT_TESTS_COMBO, name);
  }

  @GuiToolbarElement(
      root = HopGuiPipelineGraph.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = ID_TOOLBAR_UNIT_TESTS_LABEL,
      type = GuiToolbarElementType.LABEL,
      label = "i18n::TestingGuiPlugin.ToolbarElement.UnitTest.Label",
      toolTip = "i18n::TestingGuiPlugin.ToolbarElement.UnitTest.Tooltip",
      separator = true)
  public void editPipelineUnitTest() {
    HopGui hopGui = HopGui.getInstance();
    Combo combo = getUnitTestsCombo();
    if (combo == null) {
      return;
    }
    String unitTestName = combo.getText();
    try {
      MetadataManager<PipelineUnitTest> manager =
          new MetadataManager<>(
              hopGui.getVariables(), hopGui.getMetadataProvider(), PipelineUnitTest.class);
      if (manager.editMetadata(unitTestName)) {
        refreshUnitTestsList();
        selectUnitTestInList(unitTestName);
      }
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "TestingGuiPlugin.ToolbarElement.UnitTest.Error.Header"),
          BaseMessages.getString(
              PKG, "TestingGuiPlugin.ToolbarElement.UnitTest.Error.Message", unitTestName),
          e);
    }
  }

  /**
   * Get the active pipeline. If we don't have an active one, return null
   *
   * @return The active pipeline or null
   */
  public static final PipelineMeta getActivePipelineMeta() {
    IHopFileTypeHandler handler = HopGui.getInstance().getActiveFileTypeHandler();

    // These conditions shouldn't ever happen but let's make sure...
    //
    if (handler == null || handler.getSubject() == null) {
      return null;
    }
    Object subject = handler.getSubject();
    if (!(subject instanceof PipelineMeta)) {
      return null;
    }

    // On with the program
    //
    PipelineMeta pipelineMeta = (PipelineMeta) subject;
    return pipelineMeta;
  }

  /**
   * Called by the Combo in the toolbar
   *
   * @param log
   * @param metadataProvider
   * @param instanceId instance ID of the GuiToolbarWidgets instance
   * @return
   * @throws Exception
   */
  public List<String> getUnitTestsList(
      ILogChannel log, IHopMetadataProvider metadataProvider, String instanceId) throws Exception {

    // Grab the relevant variables
    //
    Object guiPluginObject =
        GuiRegistry.getInstance()
            .findGuiPluginObject(
                HopGui.getInstance().getId(), HopGuiPipelineGraph.class.getName(), instanceId);
    if (guiPluginObject == null) {
      return Collections.emptyList();
    }
    if (!(guiPluginObject instanceof HopGuiPipelineGraph)) {
      return Collections.emptyList();
    }

    HopGuiPipelineGraph pipelineGraph = (HopGuiPipelineGraph) guiPluginObject;
    IVariables variables = pipelineGraph.getVariables();

    // Get the active pipeline, match it...
    //
    List<String> names;
    PipelineMeta pipelineMeta = pipelineGraph.getPipelineMeta();
    IHopMetadataSerializer<PipelineUnitTest> testSerializer =
        metadataProvider.getSerializer(PipelineUnitTest.class);
    if (pipelineMeta == null) {
      names = testSerializer.listObjectNames();
    } else {
      List<PipelineUnitTest> tests = testSerializer.loadAll();
      names = new ArrayList<>();
      for (PipelineUnitTest test : tests) {
        if (test.matchesPipelineFilename(variables, pipelineMeta.getFilename())) {
          names.add(test.getName());
        }
      }
    }
    Collections.sort(names);
    return names;
  }

  @GuiToolbarElement(
      root = HopGuiPipelineGraph.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = ID_TOOLBAR_UNIT_TESTS_COMBO,
      type = GuiToolbarElementType.COMBO,
      comboValuesMethod = "getUnitTestsList",
      extraWidth = 200,
      toolTip = "i18n::TestingGuiPlugin.ToolbarElement.GetUnitTestList.Tooltip")
  public void selectUnitTest() {

    HopGui hopGui = HopGui.getInstance();
    try {
      PipelineMeta pipelineMeta = getActivePipelineMeta();
      if (pipelineMeta == null) {
        return;
      }
      IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();

      Combo combo = getUnitTestsCombo();
      if (combo == null) {
        return;
      }

      IHopMetadataSerializer<PipelineUnitTest> testSerializer =
          metadataProvider.getSerializer(PipelineUnitTest.class);

      String testName = combo.getText();
      if (testName != null) {
        PipelineUnitTest unitTest = testSerializer.load(testName);
        if (unitTest == null) {
          throw new HopException(
              BaseMessages.getString(
                  PKG, "TestingGuiPlugin.ToolbarElement.GetUnitTestList.Exception", testName));
        }

        selectUnitTest(pipelineMeta, unitTest);

        // Update the pipeline graph
        //
        hopGui.getActiveFileTypeHandler().updateGui();
      }
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(
              PKG, "TestingGuiPlugin.ToolbarElement.GetUnitTestList.Error.Header"),
          BaseMessages.getString(
              PKG, "TestingGuiPlugin.ToolbarElement.GetUnitTestList.Error.Message"),
          e);
    }
  }

  public static final void selectUnitTest(PipelineMeta pipelineMeta, PipelineUnitTest unitTest) {
    Map<String, Object> stateMap = getStateMap(pipelineMeta);
    if (stateMap == null) {
      // Can't select since we don't find the tab
    }
    stateMap.put(DataSetConst.STATE_KEY_ACTIVE_UNIT_TEST, unitTest);
    selectUnitTestInList(unitTest.getName());
  }

  public static final Map<String, Object> getStateMap(PipelineMeta pipelineMeta) {
    for (TabItemHandler item : HopGui.getDataOrchestrationPerspective().getItems()) {
      if (item.getTypeHandler().getSubject().equals(pipelineMeta)) {
        HopGuiPipelineGraph pipelineGraph = (HopGuiPipelineGraph) item.getTypeHandler();
        return pipelineGraph.getStateMap();
      }
    }
    return null;
  }

  public static final PipelineUnitTest getCurrentUnitTest(PipelineMeta pipelineMeta) {
    // When rendering a pipeline on a server status page we never have a current unit test
    //
    if ("Server".equalsIgnoreCase(Const.getHopPlatformRuntime())) {
      return null;
    }
    Map<String, Object> stateMap = getStateMap(pipelineMeta);
    if (stateMap == null) {
      return null;
    }

    PipelineUnitTest test =
        (PipelineUnitTest) stateMap.get(DataSetConst.STATE_KEY_ACTIVE_UNIT_TEST);
    return test;
  }

  @GuiContextAction(
      id = ACTION_ID_PIPELINE_GRAPH_TRANSFORM_ENABLE_TWEAK_REMOVE_TRANSFORM,
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::TestingGuiPlugin.ContextAction.RemoveFromTest.Name",
      tooltip = "i18n::TestingGuiPlugin.ContextAction.RemoveFromTest.Tooltip",
      image = "Test_tube_icon.svg",
      category = "i18n::TestingGuiPlugin.Category",
      categoryOrder = "8")
  public void enableTweakRemoveTransformInUnitTest(HopGuiPipelineTransformContext context) {
    IVariables variables = context.getPipelineGraph().getVariables();
    tweakRemoveTransformInUnitTest(
        variables, context.getPipelineMeta(), context.getTransformMeta(), true);
  }

  @GuiContextAction(
      id = ACTION_ID_PIPELINE_GRAPH_TRANSFORM_DISABLE_TWEAK_REMOVE_TRANSFORM,
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::TestingGuiPlugin.ContextAction.IncludeInTest.Name",
      tooltip = "i18n::TestingGuiPlugin.ContextAction.IncludeInTest.Tooltip",
      image = "Test_tube_icon.svg",
      category = "i18n::TestingGuiPlugin.Category",
      categoryOrder = "8")
  public void disableTweakRemoveTransformInUnitTest(HopGuiPipelineTransformContext context) {
    tweakRemoveTransformInUnitTest(
        context.getPipelineGraph().getVariables(),
        context.getPipelineMeta(),
        context.getTransformMeta(),
        false);
  }

  public void tweakRemoveTransformInUnitTest(
      IVariables variables,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      boolean enable) {
    tweakUnitTestTransform(
        variables, pipelineMeta, transformMeta, PipelineTweak.REMOVE_TRANSFORM, enable);
  }

  @GuiContextAction(
      id = ACTION_ID_PIPELINE_GRAPH_TRANSFORM_ENABLE_TWEAK_BYPASS_TRANSFORM,
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::TestingGuiPlugin.ContextAction.BypassInTest.Name",
      tooltip = "i18n::TestingGuiPlugin.ContextAction.BypassInTest.Tooltip",
      image = "Test_tube_icon.svg",
      category = "i18n::TestingGuiPlugin.Category",
      categoryOrder = "8")
  public void enableTweakBypassTransformInUnitTest(HopGuiPipelineTransformContext context) {
    tweakBypassTransformInUnitTest(
        context.getPipelineGraph().getVariables(),
        context.getPipelineMeta(),
        context.getTransformMeta(),
        true);
  }

  @GuiContextAction(
      id = ACTION_ID_PIPELINE_GRAPH_TRANSFORM_DISABLE_TWEAK_BYPASS_TRANSFORM,
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::TestingGuiPlugin.ContextAction.RemoveBypassInTest.Name",
      tooltip = "i18n::TestingGuiPlugin.ContextAction.RemoveBypassInTest.Tooltip",
      image = "Test_tube_icon.svg",
      category = "i18n::TestingGuiPlugin.Category",
      categoryOrder = "8")
  public void disableTweakBypassTransformInUnitTest(HopGuiPipelineTransformContext context) {
    tweakBypassTransformInUnitTest(
        context.getPipelineGraph().getVariables(),
        context.getPipelineMeta(),
        context.getTransformMeta(),
        false);
  }

  public void tweakBypassTransformInUnitTest(
      IVariables variables,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      boolean enable) {
    tweakUnitTestTransform(
        variables, pipelineMeta, transformMeta, PipelineTweak.BYPASS_TRANSFORM, enable);
  }

  private void tweakUnitTestTransform(
      IVariables variables,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      PipelineTweak tweak,
      boolean enable) {
    HopGui hopGui = HopGui.getInstance();
    IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();
    if (transformMeta == null || pipelineMeta == null) {
      return;
    }
    if (checkTestPresent(hopGui, pipelineMeta)) {
      return;
    }

    try {
      PipelineUnitTest unitTest = getCurrentUnitTest(pipelineMeta);
      PipelineUnitTestTweak unitTestTweak = unitTest.findTweak(transformMeta.getName());
      if (unitTestTweak != null) {
        unitTest.getTweaks().remove(unitTestTweak);
      }
      if (enable) {
        unitTest.getTweaks().add(new PipelineUnitTestTweak(tweak, transformMeta.getName()));
      }

      saveUnitTest(variables, metadataProvider, unitTest, pipelineMeta);

      selectUnitTest(pipelineMeta, unitTest);

      hopGui.getActiveFileTypeHandler().updateGui();
    } catch (Exception exception) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "TestingGuiPlugin.TweakUnitTestTransform.Error.Header"),
          BaseMessages.getString(
              PKG,
              "TestingGuiPlugin.TweakUnitTestTransform.Error.Message",
              transformMeta.getName(),
              tweak.name()),
          exception);
    }
  }

  /** List all unit tests which are defined And allow the user to select one */
  public RowMetaAndData selectUnitTestFromAllTests() {
    HopGui hopGui = HopGui.getInstance();
    IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("Unit test"));
    rowMeta.addValueMeta(new ValueMetaString("Description"));
    rowMeta.addValueMeta(new ValueMetaString("Filename"));

    List<RowMetaAndData> rows = new ArrayList<>();

    try {
      IHopMetadataSerializer<PipelineUnitTest> testSerializer =
          metadataProvider.getSerializer(PipelineUnitTest.class);

      List<String> testNames = testSerializer.listObjectNames();
      for (String testName : testNames) {
        PipelineUnitTest unitTest = testSerializer.load(testName);

        Object[] row = RowDataUtil.allocateRowData(rowMeta.size());
        row[0] = testName;
        row[1] = unitTest.getDescription();
        row[2] = unitTest.getPipelineFilename();

        rows.add(new RowMetaAndData(rowMeta, row));
      }

      // Now show a selection dialog...
      //
      SelectRowDialog dialog =
          new SelectRowDialog(
              hopGui.getShell(), new Variables(), SWT.DIALOG_TRIM | SWT.MAX | SWT.RESIZE, rows);
      RowMetaAndData selection = dialog.open();
      if (selection != null) {
        return selection;
      }
      return null;
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "TestingGuiPlugin.SelectUnitTestFromAllTests.Error.Header"),
          BaseMessages.getString(PKG, "TestingGuiPlugin.SelectUnitTestFromAllTests.Error.Message"),
          e);
      return null;
    }
  }

  public void openUnitTestPipeline(IVariables variables) {
    try {
      HopGui hopGui = HopGui.getInstance();
      IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();
      RowMetaAndData selection = selectUnitTestFromAllTests();
      if (selection != null) {
        String filename = selection.getString(2, null);
        if (StringUtils.isNotEmpty(filename)) {
          // Load the unit test...
          //
          String unitTestName = selection.getString(0, null);
          PipelineUnitTest targetTest =
              metadataProvider.getSerializer(PipelineUnitTest.class).load(unitTestName);

          if (targetTest != null) {
            String completeFilename = targetTest.calculateCompletePipelineFilename(variables);
            hopGui.fileDelegate.fileOpen(completeFilename);

            PipelineMeta pipelineMeta = getActivePipelineMeta();
            if (pipelineMeta != null) {
              switchUnitTest(targetTest, pipelineMeta);
            }
          }
        } else {
          throw new HopException("No filename found in the selected test");
        }
      }
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          BaseMessages.getString(PKG, "TestingGuiPlugin.OpenUnitTestPipeline.Error.Header"),
          BaseMessages.getString(PKG, "TestingGuiPlugin.OpenUnitTestPipeline.Error.Message"),
          e);
    }
  }

  public void switchUnitTest(PipelineUnitTest targetTest, PipelineMeta pipelineMeta) {
    try {
      TestingGuiPlugin.getInstance().detachUnitTest();
      TestingGuiPlugin.selectUnitTest(pipelineMeta, targetTest);
    } catch (Exception exception) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          BaseMessages.getString(
              PKG, "ShowUnitTestMenuExtensionPoint.ErrorSwitchingUnitTest.Title"),
          BaseMessages.getString(
              PKG,
              "ShowUnitTestMenuExtensionPoint.ErrorSwitchingUnitTest.Message",
              targetTest.getName()),
          exception);
    }
    HopGui.getInstance().getActiveFileTypeHandler().updateGui();
  }

  public static List<PipelineUnitTest> findPipelineUnitTest(
      IVariables variables, PipelineMeta pipelineMeta, IHopMetadataProvider metadataProvider) {
    List<PipelineUnitTest> tests = new ArrayList<>();

    try {

      IHopMetadataSerializer<PipelineUnitTest> testSerializer =
          metadataProvider.getSerializer(PipelineUnitTest.class);

      if (StringUtils.isNotEmpty(pipelineMeta.getFilename())) {

        List<PipelineUnitTest> allTests = testSerializer.loadAll();
        for (PipelineUnitTest test : allTests) {
          // Match the pipeline reference filename
          //
          if (test.matchesPipelineFilename(variables, pipelineMeta.getFilename())) {
            tests.add(test);
          }
        }
      }
    } catch (Exception exception) {
      new ErrorDialog(
          HopGui.getInstance().getShell(),
          BaseMessages.getString(
              PKG, "ShowUnitTestMenuExtensionPoint.ErrorFindingUnitTestsForPipeline.Title"),
          BaseMessages.getString(
              PKG, "ShowUnitTestMenuExtensionPoint.ErrorFindingUnitTestsForPipeline.Message"),
          exception);
    }
    return tests;
  }
}
