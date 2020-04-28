/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.testing.gui;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
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
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.persist.MetaStoreFactory;
import org.apache.hop.metastore.stores.delegate.DelegatingMetaStore;
import org.apache.hop.metastore.util.HopDefaults;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.testing.DataSet;
import org.apache.hop.testing.DataSetField;
import org.apache.hop.testing.PipelineTweak;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.PipelineUnitTestFieldMapping;
import org.apache.hop.testing.PipelineUnitTestSetLocation;
import org.apache.hop.testing.PipelineUnitTestTweak;
import org.apache.hop.testing.TestType;
import org.apache.hop.testing.util.DataSetConst;
import org.apache.hop.testing.xp.PipelineMetaModifier;
import org.apache.hop.testing.xp.WriteToDataSetExtensionPoint;
import org.apache.hop.ui.core.dialog.EnterMappingDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.SelectRowDialog;
import org.apache.hop.ui.core.metastore.MetaStoreManager;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.pipeline.context.HopGuiPipelineTransformContext;
import org.apache.hop.ui.testing.DataSetDialog;
import org.apache.hop.ui.testing.EditRowsDialog;
import org.apache.hop.ui.testing.PipelineUnitTestDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.MessageBox;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@GuiPlugin
public class TestingGuiPlugin {
  protected static Class<?> PKG = TestingGuiPlugin.class; // for i18n

  public static final String ID_TOOLBAR_UNIT_TESTS_LABEL = "HopGuiPipelineGraph-ToolBar-20000-unit-tests-label";
  public static final String ID_TOOLBAR_UNIT_TESTS_COMBO = "HopGuiPipelineGraph-ToolBar-20010-unit-tests-combo";


  private static TestingGuiPlugin instance = null;

  private Map<PipelineMeta, PipelineUnitTest> activeTests;

  private TestingGuiPlugin() {
    activeTests = new HashMap<>();
  }

  public static TestingGuiPlugin getInstance() {
    if ( instance == null ) {
      instance = new TestingGuiPlugin();
    }
    return instance;
  }


  public static String validateDataSet( DataSet dataSet, String previousName, List<String> setNames ) {

    String message = null;

    String newName = dataSet.getName();
    if ( StringUtil.isEmpty( newName ) ) {
      message = BaseMessages.getString( PKG, "TestingGuiPlugin.DataSet.NoNameSpecified.Message" );
    } else if ( !StringUtil.isEmpty( previousName ) && !previousName.equals( newName ) ) {
      message = BaseMessages.getString( PKG, "TestingGuiPlugin.DataSet.RenamingOfADataSetsNotSupported.Message" );
    } else {
      if ( StringUtil.isEmpty( previousName ) && Const.indexOfString( newName, setNames ) >= 0 ) {
        message = BaseMessages.getString( PKG, "TestingGuiPlugin.DataSet.ADataSetWithNameExists.Message", newName );
      }
    }

    return message;
  }


  /**
   * We set an input data set
   */
  @GuiContextAction(
    id = "pipeline-graph-transform-20200-define-input-data-set",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Set input data set",
    tooltip = "For the active unit test it defines which data to use instead of the output of the transform",
    image = "set-input-dataset.svg"
  )
  public void setInputDataSet( HopGuiPipelineTransformContext context ) {
    HopGui hopGui = HopGui.getInstance();
    IMetaStore metaStore = hopGui.getMetaStore();

    PipelineMeta pipelineMeta = context.getPipelineMeta();
    TransformMeta transformMeta = context.getTransformMeta();

    if ( checkTestPresent( hopGui, pipelineMeta ) ) {
      return;
    }
    PipelineUnitTest unitTest = activeTests.get( pipelineMeta );

    try {

      MetaStoreFactory<DataSet> setFactory = DataSet.createFactory( metaStore );
      List<String> setNames = setFactory.getElementNames();
      Collections.sort( setNames );
      EnterSelectionDialog esd = new EnterSelectionDialog( hopGui.getShell(), setNames.toArray( new String[ setNames.size() ] ), "Select the set", "Select the data set to edit..." );
      String setName = esd.open();
      if ( setName != null ) {
        DataSet dataSet = setFactory.loadElement( setName );

        // Now we need to map the fields from the input data set to the transform...
        //
        IRowMeta setFields = dataSet.getSetRowMeta();
        IRowMeta transformFields;
        try {
          transformFields = pipelineMeta.getTransformFields( transformMeta );
        } catch ( HopTransformException e ) {
          // Driver or input problems...
          //
          transformFields = new RowMeta();
        }
        if ( transformFields.isEmpty() ) {
          transformFields = setFields.clone();
        }

        String[] transformFieldNames = transformFields.getFieldNames();
        String[] setFieldNames = setFields.getFieldNames();

        EnterMappingDialog mappingDialog = new EnterMappingDialog( hopGui.getShell(), setFieldNames, transformFieldNames );
        List<SourceToTargetMapping> mappings = mappingDialog.open();
        if ( mappings == null ) {
          return;
        }

        // Ask about the sort order...
        // Show the mapping as well as an order column
        //
        IRowMeta sortMeta = new RowMeta();
        sortMeta.addValueMeta( new ValueMetaString( BaseMessages.getString( PKG, "TestingGuiPlugin.SortOrder.Column.SetField" ) ) );
        List<Object[]> sortData = new ArrayList<Object[]>();
        for ( String setFieldName : setFieldNames ) {
          sortData.add( new Object[] { setFieldName } );
        }
        EditRowsDialog orderDialog = new EditRowsDialog( hopGui.getShell(), SWT.NONE,
          BaseMessages.getString( PKG, "TestingGuiPlugin.SortOrder.Title" ),
          BaseMessages.getString( PKG, "TestingGuiPlugin.SortOrder.Message" ),
          sortMeta, sortData
        );
        List<Object[]> orderMappings = orderDialog.open();
        if ( orderMappings == null ) {
          return;
        }

        // Modify the test
        //

        // Remove other crap on the transform...
        //
        unitTest.removeInputAndGoldenDataSets( transformMeta.getName() );

        PipelineUnitTestSetLocation inputLocation = new PipelineUnitTestSetLocation();
        unitTest.getInputDataSets().add( inputLocation );

        inputLocation.setTransformName( transformMeta.getName() );
        inputLocation.setDataSetName( dataSet.getName() );
        List<PipelineUnitTestFieldMapping> fieldMappings = inputLocation.getFieldMappings();
        fieldMappings.clear();

        for ( SourceToTargetMapping mapping : mappings ) {
          String transformFieldName = mapping.getTargetString( transformFieldNames );
          String setFieldName = mapping.getSourceString( setFieldNames );
          fieldMappings.add( new PipelineUnitTestFieldMapping( transformFieldName, setFieldName ) );
        }

        List<String> setFieldOrder = new ArrayList<String>();
        for ( Object[] orderMapping : orderMappings ) {
          String setFieldName = sortMeta.getString( orderMapping, 0 );
          setFieldOrder.add( setFieldName );
        }
        inputLocation.setFieldOrder( setFieldOrder );

        // Save the unit test...
        //
        saveUnitTest( metaStore, unitTest, pipelineMeta );

        transformMeta.setChanged();

        context.getPipelineGraph().updateGui();
      }
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error retrieving the list of data set groups", e );
    }
  }


  /**
   * We set an golden data set on the selected unit test
   */
  @GuiContextAction(
    id = "pipeline-graph-transform-20210-clear-input-data-set",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Delete,
    name = "Clear input data set",
    tooltip = "Remove a defined input data set from this transform unit test",
    image = "clear-input-dataset.svg"
  )
  public void clearInputDataSet( HopGuiPipelineTransformContext context ) {
    HopGui hopGui = ( (HopGui) HopGui.getInstance() );
    PipelineMeta pipelineMeta = context.getPipelineMeta();
    TransformMeta transformMeta = context.getTransformMeta();

    if ( checkTestPresent( hopGui, pipelineMeta ) ) {
      return;
    }

    try {
      PipelineUnitTest currentUnitTest = getCurrentUnitTest( pipelineMeta );

      PipelineUnitTestSetLocation inputLocation = currentUnitTest.findInputLocation( transformMeta.getName() );
      if ( inputLocation != null ) {
        currentUnitTest.getInputDataSets().remove( inputLocation );
      }

      saveUnitTest( hopGui.getMetaStore(), currentUnitTest, pipelineMeta );

      context.getPipelineGraph().updateGui();
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error saving unit test", e );
    }
  }

  private boolean checkTestPresent( HopGui hopGui, PipelineMeta pipelineMeta ) {

    PipelineUnitTest activeTest = activeTests.get( pipelineMeta );
    if ( activeTest != null ) {
      return false;
    }

    // there is no test defined of selected in the pipeline.
    // Show a warning
    //
    MessageBox box = new MessageBox( hopGui.getShell(), SWT.OK | SWT.ICON_INFORMATION );
    box.setMessage( "Please create a test-case first by left clicking on the test icon." );
    box.setText( "First create a test-case" );
    box.open();

    return true;
  }

  /**
   * We set an golden data set on the selected unit test
   */
  @GuiContextAction(
    id = "pipeline-graph-transform-20220-define-golden-data-set",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Set golden data set",
    tooltip = "The input to this transform is taken and compared to the golden data set you are selecting.\nThe transform itself is not executed during testing.",
    image = "set-golden-dataset.svg"
  )
  public void setGoldenDataSet( HopGuiPipelineTransformContext context ) {
    PipelineMeta sourcePipelineMeta = context.getPipelineMeta();
    TransformMeta transformMeta = context.getTransformMeta();
    HopGui hopGui = HopGui.getInstance();
    IMetaStore metaStore = hopGui.getMetaStore();

    if ( checkTestPresent( hopGui, sourcePipelineMeta ) ) {
      return;
    }
    PipelineUnitTest unitTest = activeTests.get( sourcePipelineMeta );

    try {
      // Create a copy and modify the pipeline
      // This way we have
      PipelineMetaModifier modifier = new PipelineMetaModifier( sourcePipelineMeta, unitTest );
      PipelineMeta pipelineMeta = modifier.getTestPipeline( LogChannel.UI, sourcePipelineMeta, metaStore );


      MetaStoreFactory<DataSet> setFactory = DataSet.createFactory( metaStore );
      List<String> setNames = setFactory.getElementNames();
      Collections.sort( setNames );
      EnterSelectionDialog esd = new EnterSelectionDialog( hopGui.getShell(), setNames.toArray( new String[ setNames.size() ] ), "Select the golden data set", "Select the golden data set..." );
      String setName = esd.open();
      if ( setName != null ) {
        DataSet dataSet = setFactory.loadElement( setName );

        // Now we need to map the fields from the transform to golden data set fields...
        //
        IRowMeta transformFields;
        try {
          transformFields = pipelineMeta.getPrevTransformFields( transformMeta );
        } catch ( HopTransformException e ) {
          // Ignore error: issues with not being able to get fields because of the unit test
          // running in a different environment.
          //
          transformFields = new RowMeta();
        }
        IRowMeta setFields = dataSet.getSetRowMeta();

        String[] transformFieldNames = transformFields.getFieldNames();
        String[] setFieldNames = setFields.getFieldNames();

        EnterMappingDialog mappingDialog = new EnterMappingDialog( hopGui.getShell(), transformFieldNames, setFieldNames );
        List<SourceToTargetMapping> mappings = mappingDialog.open();
        if ( mappings == null ) {
          return;
        }

        // Ask about the sort order...
        // Show the mapping as well as an order column
        //
        IRowMeta sortMeta = new RowMeta();
        sortMeta.addValueMeta( new ValueMetaString( BaseMessages.getString( PKG, "TestingGuiPlugin.SortOrder.Column.SetField" ) ) );
        List<Object[]> sortData = new ArrayList<Object[]>();
        for ( String setFieldName : setFieldNames ) {
          sortData.add( new Object[] { setFieldName } );
        }
        EditRowsDialog orderDialog = new EditRowsDialog( hopGui.getShell(), SWT.NONE,
          BaseMessages.getString( PKG, "TestingGuiPlugin.SortOrder.Title" ),
          BaseMessages.getString( PKG, "TestingGuiPlugin.SortOrder.Message" ),
          sortMeta, sortData
        );
        List<Object[]> orderMappings = orderDialog.open();
        if ( orderMappings == null ) {
          return;
        }

        // Modify the test
        //

        // Remove golden locations and input locations on the transform to avoid duplicates
        //
        unitTest.removeInputAndGoldenDataSets( transformMeta.getName() );

        PipelineUnitTestSetLocation goldenLocation = new PipelineUnitTestSetLocation();
        unitTest.getGoldenDataSets().add( goldenLocation );

        goldenLocation.setTransformName( transformMeta.getName() );
        goldenLocation.setDataSetName( dataSet.getName() );
        List<PipelineUnitTestFieldMapping> fieldMappings = goldenLocation.getFieldMappings();
        fieldMappings.clear();

        for ( SourceToTargetMapping mapping : mappings ) {
          fieldMappings.add( new PipelineUnitTestFieldMapping(
            mapping.getSourceString( transformFieldNames ),
            mapping.getTargetString( setFieldNames ) ) );
        }

        List<String> setFieldOrder = new ArrayList<String>();
        for ( Object[] orderMapping : orderMappings ) {
          setFieldOrder.add( sortMeta.getString( orderMapping, 0 ) );
        }
        goldenLocation.setFieldOrder( setFieldOrder );

        // Save the unit test...
        //
        saveUnitTest( metaStore, unitTest, pipelineMeta );

        transformMeta.setChanged();

        context.getPipelineGraph().updateGui();
      }
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error retrieving the list of data set groups", e );
    }
  }


  /**
   * We set an golden data set on the selected unit test
   */
  @GuiContextAction(
    id = "pipeline-graph-transform-20240-clear-golden-data-set",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Delete,
    name = "Clear golden data set",
    tooltip = "Remove a defined input data set from this transform unit test",
    image = "clear-golden-dataset.svg"
  )
  public void clearGoldenDataSet( HopGuiPipelineTransformContext context ) {
    HopGui hopGui = HopGui.getInstance();
    PipelineMeta pipelineMeta = context.getPipelineMeta();
    TransformMeta transformMeta = context.getTransformMeta();

    if ( checkTestPresent( hopGui, pipelineMeta ) ) {
      return;
    }

    try {
      PipelineUnitTest currentUnitTest = getCurrentUnitTest( pipelineMeta );

      PipelineUnitTestSetLocation goldenLocation = currentUnitTest.findGoldenLocation( transformMeta.getName() );
      if ( goldenLocation != null ) {
        currentUnitTest.getGoldenDataSets().remove( goldenLocation );
      }

      saveUnitTest( hopGui.getMetaStore(), currentUnitTest, pipelineMeta );
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error saving unit test", e );
    }
    pipelineMeta.setChanged();
    context.getPipelineGraph().updateGui();
  }


  /**
   * Create a new data set with the output from
   */
  @GuiContextAction(
    id = "pipeline-graph-transform-20400-clear-golden-data-set",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Delete,
    name = "Create data set",
    tooltip = "Create an empty dataset with the output fields of this transform ",
    image = "dataset.svg"
  )
  public void createDataSetFromTransform( HopGuiPipelineTransformContext context ) {
    HopGui hopGui = ( (HopGui) HopGui.getInstance() );
    IMetaStore metaStore = hopGui.getMetaStore();

    TransformMeta transformMeta = context.getTransformMeta();
    PipelineMeta pipelineMeta = context.getPipelineMeta();

    try {

      MetaStoreFactory<DataSet> setFactory = DataSet.createFactory( metaStore );

      DataSet dataSet = new DataSet();
      IRowMeta rowMeta = pipelineMeta.getTransformFields( transformMeta );
      for ( int i = 0; i < rowMeta.size(); i++ ) {
        IValueMeta valueMeta = rowMeta.getValueMeta( i );
        String setFieldName = valueMeta.getName();
        String columnName = "field" + i;
        DataSetField field = new DataSetField( setFieldName, columnName, valueMeta.getType(), valueMeta.getLength(),
          valueMeta.getPrecision(), valueMeta.getComments(), valueMeta.getFormatMask() );
        dataSet.getFields().add( field );
      }

      DataSetDialog dataSetDialog = new DataSetDialog( hopGui.getShell(), metaStore, dataSet );
      String dataSetName = dataSetDialog.open();
      if ( dataSetName != null ) {
        setFactory.saveElement( dataSet );
      }
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error creating a new data set", e );
    }
  }

  /**
   * Ask which data set to write to
   * Ask for the mapping between the output row and the data set field
   * Start the pipeline and capture the output of the transform, write to the database table backing the data set.
   */
  @GuiContextAction(
    id = "pipeline-graph-transform-20500-write-transform-data-to-set",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Create,
    name = "Write rows to data set",
    tooltip = "Run the current pipeline and write the data to a data set",
    image = "dataset.svg"
  )
  public void writeTransformDataToDataSet( HopGuiPipelineTransformContext context ) {
    HopGui hopGui = HopGui.getInstance();
    IMetaStore metaStore = hopGui.getMetaStore();

    TransformMeta transformMeta = context.getTransformMeta();
    PipelineMeta pipelineMeta = context.getPipelineMeta();

    if ( pipelineMeta.hasChanged() ) {
      MessageBox box = new MessageBox( hopGui.getShell(), SWT.OK | SWT.ICON_INFORMATION );
      box.setText( "Save pipeline" );
      box.setMessage( "Please save your pipeline first." );
      box.open();
      return;
    }

    try {

      MetaStoreFactory<DataSet> setFactory = DataSet.createFactory( metaStore );

      // Ask which data set to write to
      //
      List<String> setNames = setFactory.getElementNames();
      Collections.sort( setNames );
      EnterSelectionDialog esd = new EnterSelectionDialog( hopGui.getShell(), setNames.toArray( new String[ setNames.size() ] ), "Select the set", "Select the data set to write rows to..." );
      String setName = esd.open();
      if ( setName == null ) {
        return;
      }

      DataSet dataSet = setFactory.loadElement( setName );
      String[] setFields = new String[ dataSet.getFields().size() ];
      for ( int i = 0; i < setFields.length; i++ ) {
        setFields[ i ] = dataSet.getFields().get( i ).getFieldName();
      }

      IRowMeta rowMeta = pipelineMeta.getTransformFields( transformMeta );
      String[] transformFields = new String[ rowMeta.size() ];
      for ( int i = 0; i < rowMeta.size(); i++ ) {
        IValueMeta valueMeta = rowMeta.getValueMeta( i );
        transformFields[ i ] = valueMeta.getName();
      }

      // Ask for the mapping between the output row and the data set field
      //
      EnterMappingDialog mappingDialog = new EnterMappingDialog( hopGui.getShell(), transformFields, setFields );
      List<SourceToTargetMapping> mapping = mappingDialog.open();
      if ( mapping == null ) {
        return;
      }

      // Run the pipeline.  We want to use the standard HopGui runFile() method
      // So we need to leave the source to target mapping list somewhere so it can be picked up later.
      // For now we'll leave it where we need it.
      //
      WriteToDataSetExtensionPoint.transformsMap.put( pipelineMeta.getName(), transformMeta );
      WriteToDataSetExtensionPoint.mappingsMap.put( pipelineMeta.getName(), mapping );
      WriteToDataSetExtensionPoint.setsMap.put( pipelineMeta.getName(), dataSet );

      // Signal to the pipeline xp plugin to inject data into some data set
      //
      pipelineMeta.setVariable( DataSetConst.VAR_WRITE_TO_DATASET, "Y" );

      // Start the pipeline
      //
      context.getPipelineGraph().start();

    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error creating a new data set", e );
    }
  }

  private void saveUnitTest( IMetaStore metaStore, PipelineUnitTest unitTest, PipelineMeta pipelineMeta ) throws MetaStoreException {
    unitTest.setRelativeFilename(pipelineMeta.getFilename());
    PipelineUnitTest.createFactory( metaStore ).saveElement( unitTest );
  }

  /**
   * Clear the current unit test from the active pipeline...
   */
  @GuiToolbarElement(
    root = HopGuiPipelineGraph.GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = "HopGuiPipelineGraph-ToolBar-20030-unit-test-detach",
    toolTip = "Detach the unit test from this pipeline",
    image = "Test_tube_icon_detach.svg"
  )
  public void detachUnitTest() {
    HopGui hopGui = HopGui.getInstance();
    try {
      PipelineMeta pipelineMeta = getActivePipelineMeta();
      if ( pipelineMeta == null ) {
        return;
      }

      // Remove
      //
      activeTests.remove( pipelineMeta );
      pipelineMeta.setVariable( DataSetConst.VAR_RUN_UNIT_TEST, "N" );

      // Clear the combo box
      //
      Combo combo = getUnitTestsCombo();
      if (combo!=null) {
        combo.setText( "" );
      }

      // Update the GUI
      //
      hopGui.getActiveFileTypeHandler().updateGui();
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error detaching unit test", e );
    }
  }

  @GuiToolbarElement(
    root = HopGuiPipelineGraph.GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = "HopGuiPipelineGraph-ToolBar-20020-unit-tests-create",
    toolTip = "Create a new unit test for this pipeline",
    image = "Test_tube_icon_create.svg",
    separator = true
  )
  public void createUnitTest() {
    HopGui hopGui = HopGui.getInstance();
    PipelineMeta pipelineMeta = getActivePipelineMeta();
    if ( pipelineMeta == null ) {
      return;
    }
    // Create a new unit test
    PipelineUnitTest test = new PipelineUnitTest(
      pipelineMeta.getName() + " UNIT",
      "",
      pipelineMeta.getFilename(),
      new ArrayList<>(),
      new ArrayList<>(),
      new ArrayList<>(),
      TestType.UNIT_TEST,
      null,
      new ArrayList<>(),
      false
    );

    PipelineUnitTestDialog dialog = new PipelineUnitTestDialog( hopGui.getShell(), hopGui.getMetaStore(), test );
    String testName = dialog.open();
    if ( testName != null ) {
      try {
        test.setRelativeFilename( pipelineMeta.getFilename() );

        PipelineUnitTest.createFactory( hopGui.getMetaStore() ).saveElement( test );

        // Activate the test
        refreshUnitTestsList();
        selectUnitTest( pipelineMeta, test );
      } catch ( Exception e ) {
        new ErrorDialog( hopGui.getShell(), "Error", "Error saving test '" + test.getName() + "'", e );
      }
    }
  }

  @GuiToolbarElement(
    root = HopGuiPipelineGraph.GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = "HopGuiPipelineGraph-ToolBar-20050-unit-tests-delete",
    toolTip = "Delete the active unit test",
    image = "Test_tube_icon_delete.svg",
    separator = true
  )
  public void deleteUnitTest() {
    HopGui hopGui = HopGui.getInstance();
    PipelineMeta pipelineMeta = getActivePipelineMeta();
    if ( pipelineMeta == null ) {
      return;
    }
    Combo combo = getUnitTestsCombo();
    if ( combo == null ) {
      return;
    }
    if ( StringUtils.isEmpty(combo.getText())) {
      return;
    }

    MetaStoreFactory<PipelineUnitTest> testFactory = PipelineUnitTest.createFactory( hopGui.getMetaStore() );

    // Load the test, delete it after confirmation
    //
    try {
      PipelineUnitTest pipelineUnitTest = testFactory.loadElement( combo.getText() );
      if (pipelineUnitTest==null) {
        return; // doesn't exist
      }

      MessageBox box = new MessageBox( hopGui.getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION );
      box.setMessage( "Are you sure you want to delete test '"+pipelineUnitTest.getName()+"'?" );
      box.setText( "Confirm unit test removal" );
      int answer = box.open();
      if ((answer&SWT.YES)==0) {
        return;
      }

      // First detach it.
      //
      detachUnitTest();

      // Then delete it.
      //
      testFactory.deleteElement( pipelineUnitTest.getName() );

      refreshUnitTestsList();
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error deleting test", e );
    }
  }

  private Combo getUnitTestsCombo() {
    Control control = HopGuiPipelineGraph.getInstance().getToolBarWidgets().getWidgetsMap().get( ID_TOOLBAR_UNIT_TESTS_COMBO );
    if ( ( control != null ) && ( control instanceof Combo ) ) {
      Combo combo = (Combo) control;
      return combo;
    }
    return null;
  }

  public static void refreshUnitTestsList() {
    HopGuiPipelineGraph.getInstance().getToolBarWidgets().refreshComboItemList( ID_TOOLBAR_UNIT_TESTS_COMBO );
  }

  public static void selectUnitTestInList( String name ) {
    HopGuiPipelineGraph.getInstance().getToolBarWidgets().selectComboItem( ID_TOOLBAR_UNIT_TESTS_COMBO, name );
  }

  @GuiToolbarElement(
    root = HopGuiPipelineGraph.GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = ID_TOOLBAR_UNIT_TESTS_LABEL,
    type = GuiToolbarElementType.LABEL,
    label = "  Unit test :",
    toolTip = "Click here to edit the active unit test",
    separator = true
  )
  public void editPipelineUnitTest() {
    HopGui hopGui = HopGui.getInstance();
    Combo combo = getUnitTestsCombo();
    if ( combo == null ) {
      return;
    }
    String unitTestName = combo.getText();
    try {
      MetaStoreManager<PipelineUnitTest> manager = new MetaStoreManager<>( hopGui.getVariables(), hopGui.getMetaStore(), PipelineUnitTest.class );
      if ( manager.editMetadata( unitTestName ) ) {
        refreshUnitTestsList();
        selectUnitTestInList( unitTestName );
      }
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error editing active unit test '" + unitTestName, e );
    }
  }

  /**
   * Get the active pipeline. If we don't have an active one, return null
   *
   * @return The active pipeline or null
   */
  public PipelineMeta getActivePipelineMeta() {
    IHopFileTypeHandler handler = HopGui.getInstance().getActiveFileTypeHandler();

    // These conditions shouldn't ever happen but let's make sure...
    //
    if ( handler == null || handler.getSubject() == null ) {
      return null;
    }
    Object subject = handler.getSubject();
    if ( !( subject instanceof PipelineMeta ) ) {
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
   * @param metaStore
   * @return
   * @throws Exception
   */
  public List<String> getUnitTestsList( ILogChannel log, IMetaStore metaStore ) throws Exception {
    // Get the active pipeline, match it...
    //
    List<String> names;
    PipelineMeta pipelineMeta = getActivePipelineMeta();
    if ( pipelineMeta == null ) {
      names = PipelineUnitTest.createFactory( metaStore ).getElementNames();
    } else {
      List<PipelineUnitTest> tests = PipelineUnitTest.createFactory( metaStore ).getElements();
      names = new ArrayList<>();
      for ( PipelineUnitTest test : tests ) {
        test.initializeVariablesFrom( HopGui.getInstance().getVariables() );
        if ( test.matchesPipelineFilename( pipelineMeta.getFilename() ) ) {
          names.add( test.getName() );
        }
      }
    }
    Collections.sort( names );
    return names;
  }

  @GuiToolbarElement(
    root = HopGuiPipelineGraph.GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = ID_TOOLBAR_UNIT_TESTS_COMBO,
    type = GuiToolbarElementType.COMBO,
    comboValuesMethod = "getUnitTestsList",
    extraWidth = 200,
    toolTip = "Select the active environment"
  )
  public void selectUnitTest() {

    HopGui hopGui = HopGui.getInstance();
    try {
      PipelineMeta pipelineMeta = getActivePipelineMeta();
      if ( pipelineMeta == null ) {
        return;
      }
      IMetaStore metaStore = hopGui.getMetaStore();

      Combo combo = getUnitTestsCombo();
      if ( combo == null ) {
        return;
      }

      String testName = combo.getText();
      if ( testName != null ) {

        PipelineUnitTest unitTest = PipelineUnitTest.createFactory( metaStore ).loadElement( testName );
        if ( unitTest == null ) {
          throw new HopException( "Unit test '" + testName + "' could not be found (deleted)?" );
        }

        selectUnitTest( pipelineMeta, unitTest );

        // Update the pipeline graph
        //
        hopGui.getActiveFileTypeHandler().updateGui();
      }
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error selecting a new pipeline unit test", e );
    }
  }

  public static final void selectUnitTest( PipelineMeta pipelineMeta, PipelineUnitTest unitTest ) {
    getInstance().getActiveTests().put( pipelineMeta, unitTest );
    selectUnitTestInList( unitTest.getName() );
  }

  public static final PipelineUnitTest getCurrentUnitTest( PipelineMeta pipelineMeta ) {
    return getInstance().getActiveTests().get( pipelineMeta );
  }

  @GuiContextAction(
    id = "pipeline-graph-transform-20800-enable-tweak-remove-transform",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Remove from test",
    tooltip = "When this unit test is run, do not include this transform",
    image = "Test_tube_icon.svg"
  )
  public void enableTweakRemoveTransformInUnitTest( HopGuiPipelineTransformContext context ) {
    tweakRemoveTransformInUnitTest( context.getPipelineMeta(), context.getTransformMeta(), true );
  }

  @GuiContextAction(
    id = "pipeline-graph-transform-20810-disable-tweak-remove-transform",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Include in test",
    tooltip = "Run the current pipeline and write the data to a data set",
    image = "Test_tube_icon.svg"
  )
  public void disableTweakRemoveTransformInUnitTest( HopGuiPipelineTransformContext context ) {
    tweakRemoveTransformInUnitTest( context.getPipelineMeta(), context.getTransformMeta(), false );
  }

  public void tweakRemoveTransformInUnitTest( PipelineMeta pipelineMeta, TransformMeta transformMeta, boolean enable ) {
    tweakUnitTestTransform( pipelineMeta, transformMeta, PipelineTweak.REMOVE_TRANSFORM, enable );
  }

  @GuiContextAction(
    id = "pipeline-graph-transform-20820-enable-tweak-bypass-transform",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Bypass in test",
    tooltip = "When this unit test is run, bypass this transform (replace with a dummy)",
    image = "Test_tube_icon.svg"
  )
  public void enableTweakBypassTransformInUnitTest( HopGuiPipelineTransformContext context ) {
    tweakBypassTransformInUnitTest( context.getPipelineMeta(), context.getTransformMeta(), true );
  }

  @GuiContextAction(
    id = "pipeline-graph-transform-20830-disable-tweak-bypass-transform",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Remove bypass in test",
    tooltip = "Do not bypass this transform in the current pipeline during testing",
    image = "Test_tube_icon.svg"
  )
  public void disableTweakBypassTransformInUnitTest( HopGuiPipelineTransformContext context ) {
    tweakBypassTransformInUnitTest( context.getPipelineMeta(), context.getTransformMeta(), false );
  }

  public void tweakBypassTransformInUnitTest( PipelineMeta pipelineMeta, TransformMeta transformMeta, boolean enable ) {
    tweakUnitTestTransform( pipelineMeta, transformMeta, PipelineTweak.BYPASS_TRANSFORM, enable );
  }

  private void tweakUnitTestTransform( PipelineMeta pipelineMeta, TransformMeta transformMeta, PipelineTweak tweak, boolean enable ) {
    HopGui hopGui = HopGui.getInstance();
    IMetaStore metaStore = hopGui.getMetaStore();
    if ( transformMeta == null || pipelineMeta == null ) {
      return;
    }
    if ( checkTestPresent( hopGui, pipelineMeta ) ) {
      return;
    }

    try {
      PipelineUnitTest unitTest = getCurrentUnitTest( pipelineMeta );
      PipelineUnitTestTweak unitTestTweak = unitTest.findTweak( transformMeta.getName() );
      if ( unitTestTweak != null ) {
        unitTest.getTweaks().remove( unitTestTweak );
      }
      if ( enable ) {
        unitTest.getTweaks().add( new PipelineUnitTestTweak( tweak, transformMeta.getName() ) );
      }

      saveUnitTest( metaStore, unitTest, pipelineMeta );

      selectUnitTest( pipelineMeta, unitTest );

      hopGui.getActiveFileTypeHandler().updateGui();
    } catch ( Exception exception ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error tweaking pipeline unit test on transform '" + transformMeta.getName() + "' with operation " + tweak.name(), exception );
    }
  }

  /**
   * List all unit tests which are defined
   * And allow the user to select one
   */
  public RowMetaAndData selectUnitTestFromAllTests() {
    HopGui hopGui = HopGui.getInstance();
    DelegatingMetaStore metaStore = hopGui.getMetaStore();

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "Unit test" ) );
    rowMeta.addValueMeta( new ValueMetaString( "Description" ) );
    rowMeta.addValueMeta( new ValueMetaString( "Filename" ) );

    List<RowMetaAndData> rows = new ArrayList<>();

    try {
      MetaStoreFactory<PipelineUnitTest> testFactory = PipelineUnitTest.createFactory( metaStore );
      List<String> testNames = testFactory.getElementNames();
      for ( String testName : testNames ) {
        PipelineUnitTest unitTest = testFactory.loadElement( testName );
        Object[] row = RowDataUtil.allocateRowData( rowMeta.size() );
        row[ 0 ] = testName;
        row[ 1 ] = unitTest.getDescription();
        row[ 2 ] = unitTest.getPipelineFilename();

        rows.add( new RowMetaAndData( rowMeta, row ) );
      }

      // Now show a selection dialog...
      //
      SelectRowDialog dialog = new SelectRowDialog( hopGui.getShell(), new Variables(), SWT.DIALOG_TRIM | SWT.MAX | SWT.RESIZE, rows );
      RowMetaAndData selection = dialog.open();
      if ( selection != null ) {
        return selection;
      }
      return null;
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error listing/deleting unit test(s)", e );
      return null;
    }
  }

  public void openUnitTestPipeline() {
    try {
      HopGui hopGui = HopGui.getInstance();
      IMetaStore metaStore = hopGui.getMetaStore();
      RowMetaAndData selection = selectUnitTestFromAllTests();
      if ( selection != null ) {
        String filename = selection.getString( 2, null );
        if ( StringUtils.isNotEmpty( filename ) ) {
          // Load the unit test...
          //
          String unitTestName = selection.getString( 0, null );
          PipelineUnitTest targetTest = PipelineUnitTest.createFactory( metaStore ).loadElement( unitTestName );

          if ( targetTest != null ) {

            String completeFilename = targetTest.calculateCompleteFilename();
            hopGui.fileDelegate.fileOpen( completeFilename );

            PipelineMeta pipelineMeta = getActivePipelineMeta();
            if ( pipelineMeta != null ) {
              switchUnitTest( targetTest, pipelineMeta );
            }
          }
        } else {
          throw new HopException( "No filename found in the selected test" );
        }
      }
    } catch ( Exception e ) {
      new ErrorDialog( HopGui.getInstance().getShell(), "Error", "Error opening unit test pipeline", e );
    }
  }

  public void switchUnitTest( PipelineUnitTest targetTest, PipelineMeta pipelineMeta ) {
    try {
      TestingGuiPlugin.getInstance().detachUnitTest();
      TestingGuiPlugin.selectUnitTest( pipelineMeta, targetTest );
    } catch ( Exception exception ) {
      new ErrorDialog( HopGui.getInstance().getShell(),
        BaseMessages.getString( PKG, "ShowUnitTestMenuExtensionPoint.ErrorSwitchingUnitTest.Title" ),
        BaseMessages.getString( PKG, "ShowUnitTestMenuExtensionPoint.ErrorSwitchingUnitTest.Message", targetTest.getName() ),
        exception );
    }
    HopGui.getInstance().getActiveFileTypeHandler().updateGui();
  }

  public static List<PipelineUnitTest> findPipelineUnitTest( PipelineMeta pipelineMeta, IMetaStore metaStore ) {
    MetaStoreFactory<PipelineUnitTest> factory = new MetaStoreFactory<>( PipelineUnitTest.class, metaStore, HopDefaults.NAMESPACE );
    List<PipelineUnitTest> tests = new ArrayList<>();

    try {

      if ( StringUtils.isNotEmpty( pipelineMeta.getFilename() ) ) {

        List<PipelineUnitTest> allTests = factory.getElements();
        for ( PipelineUnitTest test : allTests ) {
          // Match the pipeline reference filename
          //
          if (test.matchesPipelineFilename( pipelineMeta.getFilename() )) {
            tests.add( test );
          }
        }
      }
    } catch ( Exception exception ) {
      new ErrorDialog( HopGui.getInstance().getShell(),
        BaseMessages.getString( PKG, "ShowUnitTestMenuExtensionPoint.ErrorFindingUnitTestsForPipeline.Title" ),
        BaseMessages.getString( PKG, "ShowUnitTestMenuExtensionPoint.ErrorFindingUnitTestsForPipeline.Message" ),
        exception );
    }
    return tests;
  }

  /**
   * Gets activeTests
   *
   * @return value of activeTests
   */
  public Map<PipelineMeta, PipelineUnitTest> getActiveTests() {
    return activeTests;
  }

  /**
   * @param activeTests The activeTests to set
   */
  public void setActiveTests( Map<PipelineMeta, PipelineUnitTest> activeTests ) {
    this.activeTests = activeTests;
  }
}
