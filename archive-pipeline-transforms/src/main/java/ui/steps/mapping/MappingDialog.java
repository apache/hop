/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.ui.pipeline.transforms.mapping;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mapping.MappingIODefinition;
import org.apache.hop.pipeline.transforms.mapping.MappingMeta;
import org.apache.hop.pipeline.transforms.mapping.MappingParameters;
import org.apache.hop.pipeline.transforms.mapping.MappingValueRename;
import org.apache.hop.ui.core.ConstUI;
import org.apache.hop.ui.core.dialog.EnterMappingDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ColumnsResizer;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopui.HopUi;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
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
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MappingDialog extends BaseTransformDialog implements ITransformDialog {
  private static Class<?> PKG = MappingMeta.class; // for i18n purposes, needed by Translator!!

  private MappingMeta mappingMeta;

  private Label wlPath;
  private TextVar wPath;

  private Button wbBrowse;

  private CTabFolder wTabFolder;

  private PipelineMeta mappingPipelineMeta = null;

  protected boolean transModified;

  private ModifyListener lsMod;

  private MappingParameters mappingParameters;

  private List<MappingIODefinition> inputMappings;

  private List<MappingIODefinition> outputMappings;

  private interface ApplyChanges {
    void applyChanges();
  }

  private class MappingParametersTab implements ApplyChanges {
    private TableView wMappingParameters;

    private MappingParameters parameters;

    private Button wInheritAll;

    public MappingParametersTab( TableView wMappingParameters, Button wInheritAll, MappingParameters parameters ) {
      this.wMappingParameters = wMappingParameters;
      this.wInheritAll = wInheritAll;
      this.parameters = parameters;
    }

    public void applyChanges() {

      int nrLines = wMappingParameters.nrNonEmpty();
      String[] variables = new String[ nrLines ];
      String[] inputFields = new String[ nrLines ];
      parameters.setVariable( variables );
      parameters.setInputField( inputFields );
      //CHECKSTYLE:Indentation:OFF
      for ( int i = 0; i < nrLines; i++ ) {
        TableItem item = wMappingParameters.getNonEmpty( i );
        parameters.getVariable()[ i ] = item.getText( 1 );
        parameters.getInputField()[ i ] = item.getText( 2 );
      }
      parameters.setInheritingAllVariables( wInheritAll.getSelection() );
    }
  }

  private class MappingDefinitionTab implements ApplyChanges {
    private MappingIODefinition definition;

    private Text wInputTransform;

    private Text wOutputTransform;

    private Button wMainPath;

    private Text wDescription;

    private TableView wFieldMappings;

    public MappingDefinitionTab( MappingIODefinition definition, Text inputTransform, Text outputTransform, Button mainPath,
                                 Text description, TableView fieldMappings ) {
      super();
      this.definition = definition;
      wInputTransform = inputTransform;
      wOutputTransform = outputTransform;
      wMainPath = mainPath;
      wDescription = description;
      wFieldMappings = fieldMappings;
    }

    public void applyChanges() {

      // The input transform
      definition.setInputTransformName( wInputTransform.getText() );

      // The output transform
      definition.setOutputTransformName( wOutputTransform.getText() );

      // The description
      definition.setDescription( wDescription.getText() );

      // The main path flag
      definition.setMainDataPath( wMainPath.getSelection() );

      // The grid
      //
      int nrLines = wFieldMappings.nrNonEmpty();
      definition.getValueRenames().clear();
      for ( int i = 0; i < nrLines; i++ ) {
        TableItem item = wFieldMappings.getNonEmpty( i );
        definition.getValueRenames().add( new MappingValueRename( item.getText( 1 ), item.getText( 2 ) ) );
      }
    }
  }

  private ApplyChanges parameterChanges;
  private ApplyChanges tabChanges;

  public MappingDialog( Shell parent, Object in, PipelineMeta tr, String sname ) {
    super( parent, (BaseTransformMeta) in, tr, sname );
    mappingMeta = (MappingMeta) in;
    transModified = false;

    // Make a copy for our own purposes...
    // This allows us to change everything directly in the classes with
    // listeners.
    // Later we need to copy it to the input class on ok()
    //
    mappingParameters = (MappingParameters) mappingMeta.getMappingParameters().clone();
    inputMappings = new ArrayList<MappingIODefinition>();
    outputMappings = new ArrayList<MappingIODefinition>();
    for ( int i = 0; i < mappingMeta.getInputMappings().size(); i++ ) {
      inputMappings.add( (MappingIODefinition) mappingMeta.getInputMappings().get( i ).clone() );
    }
    for ( int i = 0; i < mappingMeta.getOutputMappings().size(); i++ ) {
      outputMappings.add( (MappingIODefinition) mappingMeta.getOutputMappings().get( i ).clone() );
    }
  }

  private static int CONST_WIDTH = 200;

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, mappingMeta );

    lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        mappingMeta.setChanged();
      }
    };
    changed = mappingMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 15;
    formLayout.marginHeight = 15;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "MappingDialog.Shell.Title" ) );

    Label wicon = new Label( shell, SWT.RIGHT );
    wicon.setImage( getImage() );
    FormData fdlicon = new FormData();
    fdlicon.top = new FormAttachment( 0, 0 );
    fdlicon.right = new FormAttachment( 100, 0 );
    wicon.setLayoutData( fdlicon );
    props.setLook( wicon );

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "MappingDialog.TransformName.Label" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.top = new FormAttachment( 0, 0 );
    wlTransformName.setLayoutData( fdlTransformName );

    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformName.setText( transformName );
    props.setLook( wTransformName );
    wTransformName.addModifyListener( lsMod );
    fdTransformName = new FormData();
    fdTransformName.width = 250;
    fdTransformName.left = new FormAttachment( 0, 0 );
    fdTransformName.top = new FormAttachment( wlTransformName, 5 );
    wTransformName.setLayoutData( fdTransformName );

    Label spacer = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    FormData fdSpacer = new FormData();
    fdSpacer.left = new FormAttachment( 0, 0 );
    fdSpacer.top = new FormAttachment( wTransformName, 15 );
    fdSpacer.right = new FormAttachment( 100, 0 );
    spacer.setLayoutData( fdSpacer );

    wlPath = new Label( shell, SWT.LEFT );
    props.setLook( wlPath );
    wlPath.setText( BaseMessages.getString( PKG, "MappingDialog.Pipeline.Label" ) );
    FormData fdlPipeline = new FormData();
    fdlPipeline.left = new FormAttachment( 0, 0 );
    fdlPipeline.top = new FormAttachment( spacer, 20 );
    fdlPipeline.right = new FormAttachment( 50, 0 );
    wlPath.setLayoutData( fdlPipeline );

    wPath = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPath );
    FormData fdPipeline = new FormData();
    fdPipeline.left = new FormAttachment( 0, 0 );
    fdPipeline.top = new FormAttachment( wlPath, 5 );
    fdPipeline.width = 350;
    wPath.setLayoutData( fdPipeline );

    wbBrowse = new Button( shell, SWT.PUSH );
    props.setLook( wbBrowse );
    wbBrowse.setText( BaseMessages.getString( PKG, "MappingDialog.Browse.Label" ) );
    FormData fdBrowse = new FormData();
    fdBrowse.left = new FormAttachment( wPath, 5 );
    fdBrowse.top = new FormAttachment( wlPath, Const.isOSX() ? 0 : 5 );
    wbBrowse.setLayoutData( fdBrowse );

    wbBrowse.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        selectFilePipeline();
      }
    } );

    //
    // Add a tab folder for the parameters and various input and output
    // streams
    //
    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setSimple( false );
    wTabFolder.setUnselectedCloseVisible( true );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    FormData fdCancel = new FormData();
    fdCancel.right = new FormAttachment( 100, 0 );
    fdCancel.bottom = new FormAttachment( 100, 0 );
    wCancel.setLayoutData( fdCancel );

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    FormData fdOk = new FormData();
    fdOk.right = new FormAttachment( wCancel, -5 );
    fdOk.bottom = new FormAttachment( 100, 0 );
    wOK.setLayoutData( fdOk );

    Label hSpacer = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    FormData fdhSpacer = new FormData();
    fdhSpacer.left = new FormAttachment( 0, 0 );
    fdhSpacer.bottom = new FormAttachment( wCancel, -15 );
    fdhSpacer.right = new FormAttachment( 100, 0 );
    hSpacer.setLayoutData( fdhSpacer );

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wPath, 20 );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( hSpacer, -15 );
    wTabFolder.setLayoutData( fdTabFolder );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize( shell, 670, 690 );

    getData();
    mappingMeta.setChanged( changed );
    wTabFolder.setSelection( 0 );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  protected Image getImage() {
    return SwtSvgImageUtil
      .getImage( shell.getDisplay(), getClass().getClassLoader(), "MAP.svg", ConstUI.LARGE_ICON_SIZE,
        ConstUI.LARGE_ICON_SIZE );
  }

  private void selectFilePipeline() {
    String curFile = pipelineMeta.environmentSubstitute( wPath.getText() );

    FileObject root = null;

    String parentFolder = null;
    try {
      parentFolder =
        HopVFS.getFileObject( pipelineMeta.environmentSubstitute( pipelineMeta.getFilename() ) ).getParent().toString();
    } catch ( Exception e ) {
      // Take no action
    }

    try {
      root = HopVFS.getFileObject( curFile != null ? curFile : Const.getUserHomeDirectory() );

      VfsFileChooserDialog vfsFileChooser = HopUi.getInstance().getVfsFileChooserDialog( root.getParent(), root );
      FileObject file =
        vfsFileChooser.open(
          shell, null, Const.STRING_PIPELINE_FILTER_EXT, Const.getPipelineFilterNames(),
          VfsFileChooserDialog.VFS_DIALOG_OPEN_FILE );
      if ( file == null ) {
        return;
      }
      String fileName = file.getName().toString();
      if ( fileName != null ) {
        loadPipelineFile( fileName );
        if ( parentFolder != null && fileName.startsWith( parentFolder ) ) {
          fileName = fileName.replace( parentFolder, "${" + Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY + "}" );
        }
        wPath.setText( fileName );
      }
    } catch ( IOException | HopException e ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "MappingDialog.ErrorLoadingPipeline.DialogTitle" ),
        BaseMessages.getString( PKG, "MappingDialog.ErrorLoadingPipeline.DialogMessage" ), e );
    }
  }

  private void loadPipelineFile( String fname ) throws HopException {
    mappingPipelineMeta = new PipelineMeta( pipelineMeta.environmentSubstitute( fname ) );
    mappingPipelineMeta.clearChanged();
  }

  // Method is defined as package-protected in order to be accessible by unit tests
  void loadPipeline() throws HopException {
    String filename = wPath.getText();
    if ( Utils.isEmpty( filename ) ) {
      return;
    }
    if ( !filename.endsWith( ".hpl" ) ) {
      filename = filename + ".hpl";
      wPath.setText( filename );
    }
    loadPipelineFile( filename );

  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wPath.setText( Const.NVL( mappingMeta.getFileName(), "" ) );

    // Add the parameters tab
    addParametersTab( mappingParameters );
    wTabFolder.setSelection( 0 );

    // Now add the input stream tabs: where is our data coming from?
    addInputMappingDefinitionTab();
    addOutputMappingDefinitionTab();

    try {
      loadPipeline();
    } catch ( Throwable t ) {
      // Ignore errors
    }

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void addInputMappingDefinitionTab() {
    addMappingDefinitionTab( inputMappings, BaseMessages.getString( PKG, "MappingDialog.InputTab.Title" ),
      BaseMessages.getString( PKG, "MappingDialog.label.AvailableInputs" ),
      BaseMessages.getString( PKG, "MappingDialog.label.AddInput" ),
      BaseMessages.getString( PKG, "MappingDialog.label.RemoveInput" ),
      BaseMessages.getString( PKG, "MappingDialog.InputTab.label.InputSourceTransformName" ), BaseMessages.getString(
        PKG, "MappingDialog.InputTab.label.OutputTargetTransformName" ), BaseMessages.getString(
        PKG, "MappingDialog.InputTab.label.Description" ), BaseMessages.getString(
        PKG, "MappingDialog.InputTab.column.SourceField" ), BaseMessages.getString(
        PKG, "MappingDialog.InputTab.column.TargetField" ), BaseMessages.getString(
        PKG, "MappingDialog.InputTab.label.NoItems" ), true
    );
  }

  private void addOutputMappingDefinitionTab() {
    addMappingDefinitionTab( outputMappings, BaseMessages.getString( PKG, "MappingDialog.OutputTab.Title" ),
      BaseMessages.getString( PKG, "MappingDialog.label.AvailableOutputs" ),
      BaseMessages.getString( PKG, "MappingDialog.label.AddOutput" ),
      BaseMessages.getString( PKG, "MappingDialog.label.RemoveOutput" ),
      BaseMessages.getString( PKG, "MappingDialog.OutputTab.label.InputSourceTransformName" ), BaseMessages.getString(
        PKG, "MappingDialog.OutputTab.label.OutputTargetTransformName" ), BaseMessages.getString(
        PKG, "MappingDialog.OutputTab.label.Description" ), BaseMessages.getString(
        PKG, "MappingDialog.OutputTab.column.SourceField" ), BaseMessages.getString(
        PKG, "MappingDialog.OutputTab.column.TargetField" ), BaseMessages.getString(
        PKG, "MappingDialog.OutputTab.label.NoItems" ), false );
  }

  private void addParametersTab( final MappingParameters parameters ) {

    CTabItem wParametersTab = new CTabItem( wTabFolder, SWT.NONE );
    wParametersTab.setText( BaseMessages.getString( PKG, "MappingDialog.Parameters.Title" ) );
    wParametersTab.setToolTipText( BaseMessages.getString( PKG, "MappingDialog.Parameters.Tooltip" ) );

    Composite wParametersComposite = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wParametersComposite );

    FormLayout parameterTabLayout = new FormLayout();
    parameterTabLayout.marginWidth = 15;
    parameterTabLayout.marginHeight = 15;
    wParametersComposite.setLayout( parameterTabLayout );

    // Add a checkbox: inherit all variables...
    //
    Button wInheritAll = new Button( wParametersComposite, SWT.CHECK );
    wInheritAll.setText( BaseMessages.getString( PKG, "MappingDialog.Parameters.InheritAll" ) );
    props.setLook( wInheritAll );
    FormData fdInheritAll = new FormData();
    fdInheritAll.bottom = new FormAttachment( 100, 0 );
    fdInheritAll.left = new FormAttachment( 0, 0 );
    fdInheritAll.right = new FormAttachment( 100, -30 );
    wInheritAll.setLayoutData( fdInheritAll );
    wInheritAll.setSelection( parameters.isInheritingAllVariables() );

    // Now add a tableview with the 2 columns to specify: input and output
    // fields for the source and target transforms.
    //
    ColumnInfo[] colinfo =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "MappingDialog.Parameters.column.Variable" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "MappingDialog.Parameters.column.ValueOrField" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, false ), };
    colinfo[ 1 ].setUsingVariables( true );

    final TableView wMappingParameters =
      new TableView(
        pipelineMeta, wParametersComposite, SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER, colinfo, parameters
        .getVariable().length, lsMod, props
      );
    props.setLook( wMappingParameters );
    FormData fdMappings = new FormData();
    fdMappings.left = new FormAttachment( 0, 0 );
    fdMappings.right = new FormAttachment( 100, 0 );
    fdMappings.top = new FormAttachment( 0, 0 );
    fdMappings.bottom = new FormAttachment( wInheritAll, -10 );
    wMappingParameters.setLayoutData( fdMappings );
    wMappingParameters.getTable().addListener( SWT.Resize, new ColumnsResizer( 0, 50, 50 ) );

    for ( int i = 0; i < parameters.getVariable().length; i++ ) {
      TableItem tableItem = wMappingParameters.table.getItem( i );
      tableItem.setText( 1, Const.NVL( parameters.getVariable()[ i ], "" ) );
      tableItem.setText( 2, Const.NVL( parameters.getInputField()[ i ], "" ) );
    }
    wMappingParameters.setRowNums();
    wMappingParameters.optWidth( true );

    FormData fdParametersComposite = new FormData();
    fdParametersComposite.left = new FormAttachment( 0, 0 );
    fdParametersComposite.top = new FormAttachment( 0, 0 );
    fdParametersComposite.right = new FormAttachment( 100, 0 );
    fdParametersComposite.bottom = new FormAttachment( 100, 0 );
    wParametersComposite.setLayoutData( fdParametersComposite );

    wParametersComposite.layout();
    wParametersTab.setControl( wParametersComposite );

    parameterChanges = new MappingParametersTab( wMappingParameters, wInheritAll, parameters );
  }

  protected String selectPipelineTransformName( boolean getPipelineTransform, boolean mappingInput ) {
    String dialogTitle;
    String dialogMessage;
    if ( getPipelineTransform ) {
      dialogTitle = BaseMessages.getString( PKG, "MappingDialog.SelectPipelineTransform.Title" );
      dialogMessage = BaseMessages.getString( PKG, "MappingDialog.SelectPipelineTransform.Message" );
      String[] transformnames;
      if ( mappingInput ) {
        transformnames = pipelineMeta.getPrevTransformNames( transformMeta );
      } else {
        transformnames = pipelineMeta.getNextTransformNames( transformMeta );
      }
      EnterSelectionDialog dialog = new EnterSelectionDialog( shell, transformnames, dialogTitle, dialogMessage );
      return dialog.open();
    } else {
      dialogTitle = BaseMessages.getString( PKG, "MappingDialog.SelectMappingTransform.Title" );
      dialogMessage = BaseMessages.getString( PKG, "MappingDialog.SelectMappingTransform.Message" );

      String[] transformnames = getMappingTransforms( mappingPipelineMeta, mappingInput );
      EnterSelectionDialog dialog = new EnterSelectionDialog( shell, transformnames, dialogTitle, dialogMessage );
      return dialog.open();
    }
  }

  public static String[] getMappingTransforms( PipelineMeta mappingPipelineMeta, boolean mappingInput ) {
    List<TransformMeta> transforms = new ArrayList<TransformMeta>();
    for ( TransformMeta transformMeta : mappingPipelineMeta.getTransforms() ) {
      if ( mappingInput && transformMeta.getTransformPluginId().equals( "MappingInput" ) ) {
        transforms.add( transformMeta );
      }
      if ( !mappingInput && transformMeta.getTransformPluginId().equals( "MappingOutput" ) ) {
        transforms.add( transformMeta );
      }
    }
    String[] transformnames = new String[ transforms.size() ];
    for ( int i = 0; i < transformnames.length; i++ ) {
      transformnames[ i ] = transforms.get( i ).getName();
    }

    return transformnames;

  }

  public IRowMeta getFieldsFromTransform( String transformName, boolean getPipelineTransform, boolean mappingInput ) throws HopException {
    if ( !( mappingInput ^ getPipelineTransform ) ) {
      if ( Utils.isEmpty( transformName ) ) {
        // If we don't have a specified transformName we return the input row
        // metadata
        //
        return pipelineMeta.getPrevTransformFields( this.transformName );
      } else {
        // OK, a fieldname is specified...
        // See if we can find it...
        TransformMeta transformMeta = pipelineMeta.findTransform( transformName );
        if ( transformMeta == null ) {
          throw new HopException( BaseMessages.getString(
            PKG, "MappingDialog.Exception.SpecifiedTransformWasNotFound", transformName ) );
        }
        return pipelineMeta.getTransformFields( transformMeta );
      }

    } else {
      if ( mappingPipelineMeta == null ) {
        throw new HopException( BaseMessages.getString( PKG, "MappingDialog.Exception.NoMappingSpecified" ) );
      }

      if ( Utils.isEmpty( transformName ) ) {
        // If we don't have a specified transformName we select the one and
        // only "mapping input" transform.
        //
        String[] transformnames = getMappingTransforms( mappingPipelineMeta, mappingInput );
        if ( transformnames.length > 1 ) {
          throw new HopException( BaseMessages.getString(
            PKG, "MappingDialog.Exception.OnlyOneMappingInputTransformAllowed", "" + transformnames.length ) );
        }
        if ( transformnames.length == 0 ) {
          throw new HopException( BaseMessages.getString(
            PKG, "MappingDialog.Exception.OneMappingInputTransformRequired", "" + transformnames.length ) );
        }
        return mappingPipelineMeta.getTransformFields( transformnames[ 0 ] );
      } else {
        // OK, a fieldname is specified...
        // See if we can find it...
        TransformMeta transformMeta = mappingPipelineMeta.findTransform( transformName );
        if ( transformMeta == null ) {
          throw new HopException( BaseMessages.getString(
            PKG, "MappingDialog.Exception.SpecifiedTransformWasNotFound", transformName ) );
        }
        return mappingPipelineMeta.getTransformFields( transformMeta );
      }
    }
  }

  private void addMappingDefinitionTab( List<MappingIODefinition> definitions, final String tabTitle, String listLabel,
                                        String addToolTip, String removeToolTip, String inputTransformLabel,
                                        String outputTransformLabel, String descriptionLabel, String sourceColumnLabel,
                                        String targetColumnLabel, String noItemsLabel, final boolean input ) {

    final CTabItem wTab = new CTabItem( wTabFolder, SWT.NONE );
    wTab.setText( tabTitle );

    Composite wInputComposite = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wInputComposite );

    FormLayout tabLayout = new FormLayout();
    tabLayout.marginWidth = 15;
    tabLayout.marginHeight = 15;
    wInputComposite.setLayout( tabLayout );

    Label wAvailableInputs = new Label( wInputComposite, SWT.LEFT );
    props.setLook( wAvailableInputs );
    wAvailableInputs.setText( listLabel );
    FormData fdwAvailableInputs = new FormData();
    fdwAvailableInputs.left = new FormAttachment( 0 );
    fdwAvailableInputs.top = new FormAttachment( 0 );

    Label wRemoveButton = new Label( wInputComposite, SWT.NONE );
    wRemoveButton.setImage( GUIResource.getInstance().getImage("ui/images/generic-delete.svg") );
    wRemoveButton.setToolTipText( removeToolTip );
    props.setLook( wRemoveButton );
    FormData fdwAddInputButton = new FormData();
    fdwAddInputButton.top = new FormAttachment( 0 );
    fdwAddInputButton.right = new FormAttachment( 30 );
    wRemoveButton.setLayoutData( fdwAddInputButton );

    Label wAddButton = new Label( wInputComposite, SWT.NONE );
    wAddButton.setImage( GUIResource.getInstance().getImage("ui/images/Add.svg") );
    wAddButton.setToolTipText( addToolTip );
    props.setLook( wAddButton );
    FormData fdwAddButton = new FormData();
    fdwAddButton.top = new FormAttachment( 0 );
    fdwAddButton.right = new FormAttachment( wRemoveButton, -5 );
    wAddButton.setLayoutData( fdwAddButton );

    org.eclipse.swt.widgets.List wInputList = new org.eclipse.swt.widgets.List( wInputComposite, SWT.BORDER );
    FormData fdwInputList = new FormData();
    fdwInputList.left = new FormAttachment( 0 );
    fdwInputList.top = new FormAttachment( wAvailableInputs, 5 );
    fdwInputList.bottom = new FormAttachment( 100 );
    fdwInputList.right = new FormAttachment( 30 );
    wInputList.setLayoutData( fdwInputList );

    for ( int i = 0; i < definitions.size(); i++ ) {
      String label =
        !Utils.isEmpty( definitions.get( i ).getInputTransformName() ) ? definitions.get( i ).getInputTransformName()
          : tabTitle + ( i > 0 ? String.valueOf( i + 1 ) : "" );
      wInputList.add( label );
    }

    final Label wlNoItems = new Label( wInputComposite, SWT.CENTER );
    wlNoItems.setText( noItemsLabel );
    props.setLook( wlNoItems );
    FormData fdlNoItems = new FormData();
    fdlNoItems.left = new FormAttachment( wInputList, 30 );
    fdlNoItems.right = new FormAttachment( 100 );
    fdlNoItems.top = new FormAttachment( 50 );
    wlNoItems.setLayoutData( fdlNoItems );
    wlNoItems.setVisible( false );

    Composite wFieldsComposite = new Composite( wInputComposite, SWT.NONE );
    props.setLook( wFieldsComposite );
    FormLayout fieldLayout = new FormLayout();
    fieldLayout.marginWidth = 0;
    fieldLayout.marginHeight = 0;
    wFieldsComposite.setLayout( fieldLayout );

    final Button wMainPath = new Button( wFieldsComposite, SWT.CHECK );
    wMainPath.setText( BaseMessages.getString( PKG, "MappingDialog.input.MainDataPath" ) );
    props.setLook( wMainPath );
    FormData fdMainPath = new FormData();
    fdMainPath.top = new FormAttachment( 0 );
    fdMainPath.left = new FormAttachment( 0 );
    wMainPath.setLayoutData( fdMainPath );
    wMainPath.addSelectionListener( new SelectionAdapter() {

      @Override
      public void widgetSelected( SelectionEvent event ) {
        definitions.get( wInputList.getSelectionIndex() )
          .setMainDataPath( !definitions.get( wInputList.getSelectionIndex() ).isMainDataPath() );
      }

    } );

    final Label wlInputTransform = new Label( wFieldsComposite, SWT.RIGHT );
    props.setLook( wlInputTransform );
    wlInputTransform.setText( inputTransformLabel );
    FormData fdlInputTransform = new FormData();
    fdlInputTransform.top = new FormAttachment( wMainPath, 10 );
    fdlInputTransform.left = new FormAttachment( 0 );
    wlInputTransform.setLayoutData( fdlInputTransform );

    // What's the transformName to read from? (empty is OK too)
    //
    final Button wbInputTransform = new Button( wFieldsComposite, SWT.PUSH );
    props.setLook( wbInputTransform );
    wbInputTransform.setText( BaseMessages.getString( PKG, "MappingDialog.button.SourceTransformName" ) );
    FormData fdbInputTransform = new FormData();
    fdbInputTransform.top = new FormAttachment( wlInputTransform, 5 );
    fdbInputTransform.right = new FormAttachment( 100 ); // First one in the
    // left top corner
    wbInputTransform.setLayoutData( fdbInputTransform );

    final Text wInputTransform = new Text( wFieldsComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInputTransform );
    wInputTransform.addModifyListener( lsMod );
    FormData fdInputTransform = new FormData();
    fdInputTransform.top = new FormAttachment( wlInputTransform, 5 );
    fdInputTransform.left = new FormAttachment( 0 ); // To the right of
    // the label
    fdInputTransform.right = new FormAttachment( wbInputTransform, -5 );
    wInputTransform.setLayoutData( fdInputTransform );
    wInputTransform.addFocusListener( new FocusAdapter() {
      @Override
      public void focusLost( FocusEvent event ) {
        definitions.get( wInputList.getSelectionIndex() ).setInputTransformName( wInputTransform.getText() );
        String label = !Utils.isEmpty( wInputTransform.getText() ) ? wInputTransform.getText()
          : tabTitle + ( wInputList.getSelectionIndex() > 0 ? String.valueOf( wInputList.getSelectionIndex() + 1 )
          : "" );
        wInputList.setItem( wInputList.getSelectionIndex(), label );
      }
    } );
    wbInputTransform.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent event ) {
        String transformName = selectPipelineTransformName( input, input );
        if ( transformName != null ) {
          wInputTransform.setText( transformName );
          definitions.get( wInputList.getSelectionIndex() ).setInputTransformName( transformName );
        }
      }
    } );

    // What's the transform name to read from? (empty is OK too)
    //
    final Label wlOutputTransform = new Label( wFieldsComposite, SWT.RIGHT );
    props.setLook( wlOutputTransform );
    wlOutputTransform.setText( outputTransformLabel );
    FormData fdlOutputTransform = new FormData();
    fdlOutputTransform.top = new FormAttachment( wInputTransform, 10 );
    fdlOutputTransform.left = new FormAttachment( 0 );
    wlOutputTransform.setLayoutData( fdlOutputTransform );

    final Button wbOutputTransform = new Button( wFieldsComposite, SWT.PUSH );
    props.setLook( wbOutputTransform );
    wbOutputTransform.setText( BaseMessages.getString( PKG, "MappingDialog.button.SourceTransformName" ) );
    FormData fdbOutputTransform = new FormData();
    fdbOutputTransform.top = new FormAttachment( wlOutputTransform, 5 );
    fdbOutputTransform.right = new FormAttachment( 100 );
    wbOutputTransform.setLayoutData( fdbOutputTransform );

    final Text wOutputTransform = new Text( wFieldsComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wOutputTransform );
    wOutputTransform.addModifyListener( lsMod );
    FormData fdOutputTransform = new FormData();
    fdOutputTransform.top = new FormAttachment( wlOutputTransform, 5 );
    fdOutputTransform.left = new FormAttachment( 0 ); // To the right of
    // the label
    fdOutputTransform.right = new FormAttachment( wbOutputTransform, -5 );
    wOutputTransform.setLayoutData( fdOutputTransform );

    // Allow for a small description
    //
    Label wlDescription = new Label( wFieldsComposite, SWT.RIGHT );
    props.setLook( wlDescription );
    wlDescription.setText( descriptionLabel );
    FormData fdlDescription = new FormData();
    fdlDescription.top = new FormAttachment( wOutputTransform, 5 );
    fdlDescription.left = new FormAttachment( 0 ); // First one in the left
    wlDescription.setLayoutData( fdlDescription );

    final Text wDescription = new Text( wFieldsComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDescription );
    wDescription.addModifyListener( lsMod );
    FormData fdDescription = new FormData();
    fdDescription.top = new FormAttachment( wlDescription, 5 );
    fdDescription.left = new FormAttachment( 0 ); // To the right of
    // the label
    fdDescription.right = new FormAttachment( 100 );
    wDescription.setLayoutData( fdDescription );
    wDescription.addFocusListener( new FocusAdapter() {
      @Override
      public void focusLost( FocusEvent event ) {
        definitions.get( wInputList.getSelectionIndex() ).setDescription( wDescription.getText() );
      }
    } );

    final Button wbEnterMapping = new Button( wFieldsComposite, SWT.PUSH );
    props.setLook( wbEnterMapping );
    wbEnterMapping.setText( BaseMessages.getString( PKG, "MappingDialog.button.EnterMapping" ) );
    FormData fdbEnterMapping = new FormData();
    fdbEnterMapping.bottom = new FormAttachment( 100 );
    fdbEnterMapping.right = new FormAttachment( 100 );
    wbEnterMapping.setLayoutData( fdbEnterMapping );
    wbEnterMapping.setEnabled( input );

    ColumnInfo[] colinfo =
      new ColumnInfo[] {
        new ColumnInfo( sourceColumnLabel, ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
        new ColumnInfo( targetColumnLabel, ColumnInfo.COLUMN_TYPE_TEXT, false, false ), };
    final TableView wFieldMappings =
      new TableView( pipelineMeta, wFieldsComposite, SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER, colinfo, 1, false, lsMod,
        props, false );
    props.setLook( wFieldMappings );
    FormData fdMappings = new FormData();
    fdMappings.top = new FormAttachment( wDescription, 20 );
    fdMappings.bottom = new FormAttachment( wbEnterMapping, -5 );
    fdMappings.left = new FormAttachment( 0 );
    fdMappings.right = new FormAttachment( 100 );
    wFieldMappings.setLayoutData( fdMappings );
    wFieldMappings.getTable().addListener( SWT.Resize, new ColumnsResizer( 0, 50, 50 ) );

    wbEnterMapping.addSelectionListener( new SelectionAdapter() {

      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        try {
          IRowMeta sourceRowMeta = getFieldsFromTransform( wInputTransform.getText(), true, input );
          IRowMeta targetRowMeta = getFieldsFromTransform( wOutputTransform.getText(), false, input );
          String[] sourceFields = sourceRowMeta.getFieldNames();
          String[] targetFields = targetRowMeta.getFieldNames();

          EnterMappingDialog dialog = new EnterMappingDialog( shell, sourceFields, targetFields );
          List<SourceToTargetMapping> mappings = dialog.open();
          if ( mappings != null ) {
            // first clear the dialog...
            wFieldMappings.clearAll( false );

            //
            definitions.get( wInputList.getSelectionIndex() ).getValueRenames().clear();

            // Now add the new values...
            for ( SourceToTargetMapping mapping : mappings ) {
              TableItem item = new TableItem( wFieldMappings.table, SWT.NONE );
              item.setText( 1, mapping.getSourceString( sourceFields ) );
              item.setText( 2, mapping.getTargetString( targetFields ) );

              String source = input ? item.getText( 1 ) : item.getText( 2 );
              String target = input ? item.getText( 2 ) : item.getText( 1 );
              definitions.get( wInputList.getSelectionIndex() ).getValueRenames().add( new MappingValueRename( source, target ) );
            }
            wFieldMappings.removeEmptyRows();
            wFieldMappings.setRowNums();
            wFieldMappings.optWidth( true );
          }
        } catch ( HopException e ) {
          new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.Error.Title" ), BaseMessages.getString(
            PKG, "MappingDialog.Exception.ErrorGettingMappingSourceAndTargetFields", e.toString() ), e );
        }
      }

    } );

    wOutputTransform.addFocusListener( new FocusAdapter() {
      @Override
      public void focusLost( FocusEvent event ) {
        definitions.get( wInputList.getSelectionIndex() ).setOutputTransformName( wOutputTransform.getText() );
        try {
          enableMappingButton( wbEnterMapping, input, wInputTransform.getText(), wOutputTransform.getText() );
        } catch ( HopException e ) {
          // Show the missing/wrong transform name error
          //
          new ErrorDialog( shell, "Error", "Unexpected error", e );
        }
      }
    } );
    wbOutputTransform.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent event ) {
        String transformName = selectPipelineTransformName( !input, input );
        if ( transformName != null ) {
          wOutputTransform.setText( transformName );
          definitions.get( wInputList.getSelectionIndex() ).setOutputTransformName( transformName );
          try {
            enableMappingButton( wbEnterMapping, input, wInputTransform.getText(), wOutputTransform.getText() );
          } catch ( HopException e ) {
            // Show the missing/wrong transformName error
            new ErrorDialog( shell, "Error", "Unexpected error", e );
          }
        }
      }
    } );

    final Button wRenameOutput;
    if ( input ) {
      // Add a checkbox to indicate that all output mappings need to rename
      // the values back...
      //
      wRenameOutput = new Button( wFieldsComposite, SWT.CHECK );
      wRenameOutput.setText( BaseMessages.getString( PKG, "MappingDialog.input.RenamingOnOutput" ) );
      props.setLook( wRenameOutput );
      FormData fdRenameOutput = new FormData();
      fdRenameOutput.top = new FormAttachment( wFieldMappings, 5 );
      fdRenameOutput.left = new FormAttachment( 0 );
      wRenameOutput.setLayoutData( fdRenameOutput );
      wRenameOutput.addSelectionListener( new SelectionAdapter() {
        @Override
        public void widgetSelected( SelectionEvent event ) {
          definitions.get( wInputList.getSelectionIndex() ).setRenamingOnOutput( !definitions.get( wInputList.getSelectionIndex() ).isRenamingOnOutput() );
        }
      } );
    } else {
      wRenameOutput = null;
    }

    FormData fdInputComposite = new FormData();
    fdInputComposite.left = new FormAttachment( 0 );
    fdInputComposite.top = new FormAttachment( 0 );
    fdInputComposite.right = new FormAttachment( 100 );
    fdInputComposite.bottom = new FormAttachment( 100 );
    wInputComposite.setLayoutData( fdInputComposite );

    FormData fdFieldsComposite = new FormData();
    fdFieldsComposite.left = new FormAttachment( wInputList, 30 );
    fdFieldsComposite.right = new FormAttachment( 100 );
    fdFieldsComposite.bottom = new FormAttachment( 100 );
    fdFieldsComposite.top = new FormAttachment( 0 );
    wFieldsComposite.setLayoutData( fdFieldsComposite );

    wInputComposite.layout();
    wTab.setControl( wInputComposite );

    wMainPath.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        setTabFlags( wMainPath, wlInputTransform, wInputTransform, wbInputTransform, wlOutputTransform, wOutputTransform,
          wbOutputTransform, wlDescription, wDescription );
      }
    } );

    wInputList.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        updateFields( definitions.get( wInputList.getSelectionIndex() ), input, wMainPath, wlInputTransform, wInputTransform,
          wbInputTransform, wlOutputTransform, wOutputTransform, wbOutputTransform, wlDescription, wDescription, wFieldMappings,
          wRenameOutput );
      }
    } );

    wAddButton.addMouseListener( new MouseAdapter() {
      @Override public void mouseUp( MouseEvent mouseEvent ) {
        MappingIODefinition definition = new MappingIODefinition();
        definition.setMainDataPath( true );
        definitions.add( definition );
        wInputList.add( tabTitle + ( definitions.size() > 1 ? String.valueOf( definitions.size() ) : "" ) );
        wInputList.select( definitions.size() - 1 );
        updateFields( definitions.get( wInputList.getSelectionIndex() ), input, wMainPath, wlInputTransform, wInputTransform,
          wbInputTransform, wlOutputTransform, wOutputTransform,
          wbOutputTransform, wlDescription, wDescription, wFieldMappings, wRenameOutput );
        wlNoItems.setVisible( false );
        wFieldsComposite.setVisible( true );
        wRemoveButton.setEnabled( true );
      }
    } );

    wRemoveButton.addMouseListener( new MouseAdapter() {
      @Override public void mouseUp( MouseEvent mouseEvent ) {
        MessageBox box = new MessageBox( shell, SWT.YES | SWT.NO );
        box.setText( BaseMessages.getString( PKG, "MappingDialog.CloseDefinitionTabAreYouSure.Title" ) );
        box.setMessage( BaseMessages.getString( PKG, "MappingDialog.CloseDefinitionTabAreYouSure.Message" ) );
        int answer = box.open();
        if ( answer != SWT.YES ) {
          return;
        }

        int index = wInputList.getSelectionIndex();
        definitions.remove( index );
        wInputList.removeAll();
        for ( int i = 0; i < definitions.size(); i++ ) {
          String label =
            !Utils.isEmpty( definitions.get( i ).getInputTransformName() ) ? definitions.get( i ).getInputTransformName()
              : tabTitle + ( i > 0 ? String.valueOf( i + 1 ) : "" );
          wInputList.add( label );
        }
        if ( index > 0 ) {
          wInputList.select( index - 1 );
        } else if ( definitions.size() > 0 ) {
          wInputList.select( index );
        } else {
          index = -1;
        }
        if ( index != -1 ) {
          updateFields( definitions.get( wInputList.getSelectionIndex() ), input, wMainPath, wlInputTransform, wInputTransform,
            wbInputTransform, wlOutputTransform, wOutputTransform,
            wbOutputTransform, wlDescription, wDescription, wFieldMappings, wRenameOutput );
        }
        if ( definitions.size() == 0 ) {
          wlNoItems.setVisible( true );
          wFieldsComposite.setVisible( false );
          wRemoveButton.setEnabled( false );
        }
      }
    } );

    if ( definitions.size() > 0 ) {
      wInputList.select( 0 );
      updateFields( definitions.get( 0 ), input, wMainPath, wlInputTransform, wInputTransform,
        wbInputTransform, wlOutputTransform, wOutputTransform, wbOutputTransform, wlDescription, wDescription, wFieldMappings,
        wRenameOutput );
    } else {
      wlNoItems.setVisible( true );
      wFieldsComposite.setVisible( false );
      wRemoveButton.setEnabled( false );
    }

    setTabFlags( wMainPath, wlInputTransform, wInputTransform, wbInputTransform, wlOutputTransform, wOutputTransform,
      wbOutputTransform, wlDescription, wDescription );

    wTabFolder.setSelection( wTab );

  }

  private void updateFields( MappingIODefinition definition, boolean input, Button wMainPath, Label wlInputTransform,
                             Text wInputTransform, Button wbInputTransform, Label wlOutputTransform, Text wOutputTransform,
                             Button wbOutputTransform, Label wlDescription, Text wDescription, TableView wFieldMappings,
                             Button wRenameOutput ) {
    if ( tabChanges != null ) {
      tabChanges.applyChanges();
    }
    wMainPath.setSelection( definition.isMainDataPath() );
    wInputTransform.setText( Const.NVL( definition.getInputTransformName(), "" ) );
    wOutputTransform.setText( Const.NVL( definition.getOutputTransformName(), "" ) );
    wDescription.setText( Const.NVL( definition.getDescription(), "" ) );
    setTabFlags( wMainPath, wlInputTransform, wInputTransform, wbInputTransform, wlOutputTransform, wOutputTransform, wbOutputTransform,
      wlDescription, wDescription );
    wFieldMappings.removeAll();
    for ( MappingValueRename valueRename : definition.getValueRenames() ) {
      TableItem tableItem = new TableItem( wFieldMappings.table, SWT.NONE );
      tableItem.setText( 1, Const.NVL( valueRename.getSourceValueName(), "" ) );
      tableItem.setText( 2, Const.NVL( valueRename.getTargetValueName(), "" ) );
    }
    wFieldMappings.removeEmptyRows();
    wFieldMappings.setRowNums();
    wFieldMappings.optWidth( true );
    if ( input ) {
      wRenameOutput.setSelection( definition.isRenamingOnOutput() );
    }
    tabChanges =
      new MappingDefinitionTab( definition, wInputTransform, wOutputTransform, wMainPath, wDescription, wFieldMappings );
  }

  private void setTabFlags( Button wMainPath, Label wlInputTransform, Text wInputTransform, Button wbInputTransform,
                            Label wlOutputTransform, Text wOutputTransform, Button wbOutputTransform, Label wlDescription,
                            Text wDescription ) {
    boolean mainPath = wMainPath.getSelection();
    wlInputTransform.setEnabled( !mainPath );
    wInputTransform.setEnabled( !mainPath );
    wbInputTransform.setEnabled( !mainPath );
    wlOutputTransform.setEnabled( !mainPath );
    wOutputTransform.setEnabled( !mainPath );
    wbOutputTransform.setEnabled( !mainPath );
    wlDescription.setEnabled( !mainPath );
    wDescription.setEnabled( !mainPath );
  }

  /**
   * Enables or disables the mapping button. We can only enable it if the target transforms allows a mapping to be made
   * against it.
   *
   * @param button         The button to disable or enable
   * @param input          input or output. If it's true, we keep the button enabled all the time.
   * @param sourceTransformName The mapping output transform
   * @param targetTransformname The target transform to verify
   * @throws HopException
   */
  private void enableMappingButton( final Button button, boolean input, String sourceTransformName, String targetTransformname ) throws HopException {
    if ( input ) {
      return; // nothing to do
    }

    boolean enabled = false;

    if ( mappingPipelineMeta != null ) {
      TransformMeta mappingInputTransform = mappingPipelineMeta.findMappingInputTransform( sourceTransformName );
      if ( mappingInputTransform != null ) {
        TransformMeta mappingOutputTransform = pipelineMeta.findMappingOutputTransform( targetTransformname );
        IRowMeta requiredFields = mappingOutputTransform.getITransform().getRequiredFields( pipelineMeta );
        if ( requiredFields != null && requiredFields.size() > 0 ) {
          enabled = true;
        }
      }
    }

    button.setEnabled( enabled );
  }

  private void cancel() {
    transformName = null;
    mappingMeta.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    try {
      loadPipeline();
    } catch ( HopException e ) {
      new ErrorDialog( shell, BaseMessages.getString(
        PKG, "MappingDialog.ErrorLoadingSpecifiedPipeline.Title" ), BaseMessages.getString(
        PKG, "MappingDialog.ErrorLoadingSpecifiedPipeline.Message" ), e );
      return;
    }

    mappingMeta.setFileName( wPath.getText() );

    // Load the information on the tabs, optionally do some
    // verifications...
    //
    collectInformation();

    mappingMeta.setMappingParameters( mappingParameters );
    mappingMeta.setInputMappings( inputMappings );
    // Set the input transforms for input mappings
    mappingMeta.searchInfoAndTargetTransforms( pipelineMeta.getTransforms() );
    mappingMeta.setOutputMappings( outputMappings );

    mappingMeta.setAllowingMultipleInputs( true );
    mappingMeta.setAllowingMultipleOutputs( true );

    mappingMeta.setChanged( true );

    dispose();
  }

  private void collectInformation() {
    parameterChanges.applyChanges();
    tabChanges.applyChanges();
  }
}
