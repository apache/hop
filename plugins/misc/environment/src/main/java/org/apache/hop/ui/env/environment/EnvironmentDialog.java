package org.apache.hop.ui.env.environment;

import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.env.environment.Environment;
import org.apache.hop.env.environment.EnvironmentVariable;
import org.apache.hop.env.util.EnvironmentUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.metastore.IMetadataDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class EnvironmentDialog extends Dialog implements IMetadataDialog {
  private static Class<?> PKG = EnvironmentDialog.class; // for i18n purposes, needed by Translator2!!

  private final Environment environment;
  private final String environmentName;
  private final String environmentHomeFolder;

  private String returnValue;

  private Shell shell;
  private final PropsUi props;

  private Text wDescription;
  private Text wCompany;
  private Text wDepartment;
  private Text wProject;
  private Text wVersion;

  private TextVar wMetadataBaseFolder;
  private TextVar wUnitTestsBasePath;
  private TextVar wDataSetCsvFolder;
  private Button wEnforceHomeExecution;
  private TableView wVariables;

  private int margin;
  private int middle;

  private IVariables variables;

  public EnvironmentDialog( Shell parent, Environment environment, String environmentName, String environmentHomeFolder, IVariables variables ) {
    super( parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE );

    this.environment = environment;
    this.environmentName = environmentName;
    this.environmentHomeFolder = environmentHomeFolder;

    props = PropsUi.getInstance();

    this.variables = new Variables();
    this.variables.initializeVariablesFrom( null );
    environment.modifyVariables( variables, environmentName, environmentHomeFolder );
  }

  public String open() {

    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE );
    shell.setImage( GuiResource.getInstance().getImageHopUi() );
    props.setLook( shell );

    margin = Const.MARGIN + 2;
    middle = Const.MIDDLE_PCT;

    FormLayout formLayout = new FormLayout();

    shell.setLayout( formLayout );
    shell.setText( "Environment dialog" );

    // Buttons go at the bottom of the dialog
    //
    Button wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOK.addListener( SWT.Selection, event -> ok() );
    Button wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, event -> cancel() );
    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOK, wCancel }, margin * 3, null );

    Label wlDescription = new Label( shell, SWT.RIGHT );
    props.setLook( wlDescription );
    wlDescription.setText( "Description " );
    FormData fdlDescription = new FormData();
    fdlDescription.left = new FormAttachment( 0, 0 );
    fdlDescription.right = new FormAttachment( middle, 0 );
    fdlDescription.top = new FormAttachment( 0, margin );
    wlDescription.setLayoutData( fdlDescription );
    wDescription = new Text( shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT );
    props.setLook( wDescription );
    FormData fdDescription = new FormData();
    fdDescription.left = new FormAttachment( middle, margin );
    fdDescription.right = new FormAttachment( 100, 0 );
    fdDescription.top = new FormAttachment( wlDescription, 0, SWT.CENTER );
    wDescription.setLayoutData( fdDescription );
    Control lastControl = wDescription;

    Label wlCompany = new Label( shell, SWT.RIGHT );
    props.setLook( wlCompany );
    wlCompany.setText( "Company " );
    FormData fdlCompany = new FormData();
    fdlCompany.left = new FormAttachment( 0, 0 );
    fdlCompany.right = new FormAttachment( middle, 0 );
    fdlCompany.top = new FormAttachment( lastControl, margin );
    wlCompany.setLayoutData( fdlCompany );
    wCompany = new Text( shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT );
    props.setLook( wCompany );
    FormData fdCompany = new FormData();
    fdCompany.left = new FormAttachment( middle, margin );
    fdCompany.right = new FormAttachment( 100, 0 );
    fdCompany.top = new FormAttachment( wlCompany, 0, SWT.CENTER );
    wCompany.setLayoutData( fdCompany );
    lastControl = wCompany;

    Label wlDepartment = new Label( shell, SWT.RIGHT );
    props.setLook( wlDepartment );
    wlDepartment.setText( "Department " );
    FormData fdlDepartment = new FormData();
    fdlDepartment.left = new FormAttachment( 0, 0 );
    fdlDepartment.right = new FormAttachment( middle, 0 );
    fdlDepartment.top = new FormAttachment( lastControl, margin );
    wlDepartment.setLayoutData( fdlDepartment );
    wDepartment = new Text( shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT );
    props.setLook( wDepartment );
    FormData fdDepartment = new FormData();
    fdDepartment.left = new FormAttachment( middle, margin );
    fdDepartment.right = new FormAttachment( 100, 0 );
    fdDepartment.top = new FormAttachment( wlDepartment, 0, SWT.CENTER );
    wDepartment.setLayoutData( fdDepartment );
    lastControl = wDepartment;

    Label wlProject = new Label( shell, SWT.RIGHT );
    props.setLook( wlProject );
    wlProject.setText( "Project " );
    FormData fdlProject = new FormData();
    fdlProject.left = new FormAttachment( 0, 0 );
    fdlProject.right = new FormAttachment( middle, 0 );
    fdlProject.top = new FormAttachment( lastControl, margin );
    wlProject.setLayoutData( fdlProject );
    wProject = new Text( shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT );
    props.setLook( wProject );
    FormData fdProject = new FormData();
    fdProject.left = new FormAttachment( middle, margin );
    fdProject.right = new FormAttachment( 100, 0 );
    fdProject.top = new FormAttachment( wlProject, 0, SWT.CENTER );
    wProject.setLayoutData( fdProject );
    lastControl = wProject;

    Label wlVersion = new Label( shell, SWT.RIGHT );
    props.setLook( wlVersion );
    wlVersion.setText( "Version " );
    FormData fdlVersion = new FormData();
    fdlVersion.left = new FormAttachment( 0, 0 );
    fdlVersion.right = new FormAttachment( middle, 0 );
    fdlVersion.top = new FormAttachment( lastControl, margin );
    wlVersion.setLayoutData( fdlVersion );
    wVersion = new Text( shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT );
    props.setLook( wVersion );
    FormData fdVersion = new FormData();
    fdVersion.left = new FormAttachment( middle, margin );
    fdVersion.right = new FormAttachment( 100, 0 );
    fdVersion.top = new FormAttachment( wlVersion, 0, SWT.CENTER );
    wVersion.setLayoutData( fdVersion );
    lastControl = wVersion;

    Label wlMetadataBaseFolder = new Label( shell, SWT.RIGHT );
    props.setLook( wlMetadataBaseFolder );
    wlMetadataBaseFolder.setText( "Metadata base folder (HOP_METADATA_FOLDER)" );
    FormData fdlMetadataBaseFolder = new FormData();
    fdlMetadataBaseFolder.left = new FormAttachment( 0, 0 );
    fdlMetadataBaseFolder.right = new FormAttachment( middle, 0 );
    fdlMetadataBaseFolder.top = new FormAttachment( lastControl, margin );
    wlMetadataBaseFolder.setLayoutData( fdlMetadataBaseFolder );
    wMetadataBaseFolder = new TextVar( variables, shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT );
    props.setLook( wMetadataBaseFolder );
    FormData fdMetadataBaseFolder = new FormData();
    fdMetadataBaseFolder.left = new FormAttachment( middle, margin );
    fdMetadataBaseFolder.right = new FormAttachment( 100, 0 );
    fdMetadataBaseFolder.top = new FormAttachment( wlMetadataBaseFolder, 0, SWT.CENTER );
    wMetadataBaseFolder.setLayoutData( fdMetadataBaseFolder );
    wMetadataBaseFolder.addModifyListener( e -> updateIVariables() );
    lastControl = wMetadataBaseFolder;

    Label wlUnitTestsBasePath = new Label( shell, SWT.RIGHT );
    props.setLook( wlUnitTestsBasePath );
    wlUnitTestsBasePath.setText( "Unit tests base path (UNIT_TESTS_BASE_PATH) " );
    FormData fdlUnitTestsBasePath = new FormData();
    fdlUnitTestsBasePath.left = new FormAttachment( 0, 0 );
    fdlUnitTestsBasePath.right = new FormAttachment( middle, 0 );
    fdlUnitTestsBasePath.top = new FormAttachment( lastControl, margin );
    wlUnitTestsBasePath.setLayoutData( fdlUnitTestsBasePath );
    wUnitTestsBasePath = new TextVar( variables, shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT );
    props.setLook( wUnitTestsBasePath );
    FormData fdUnitTestsBasePath = new FormData();
    fdUnitTestsBasePath.left = new FormAttachment( middle, margin );
    fdUnitTestsBasePath.right = new FormAttachment( 100, 0 );
    fdUnitTestsBasePath.top = new FormAttachment( wlUnitTestsBasePath, 0, SWT.CENTER );
    wUnitTestsBasePath.setLayoutData( fdUnitTestsBasePath );
    wUnitTestsBasePath.addModifyListener( e -> updateIVariables() );
    lastControl = wUnitTestsBasePath;

    Label wlDataSetCsvFolder = new Label( shell, SWT.RIGHT );
    props.setLook( wlDataSetCsvFolder );
    wlDataSetCsvFolder.setText( "Data Sets CSV Folder (DATASETS_BASE_PATH)" );
    FormData fdlDataSetCsvFolder = new FormData();
    fdlDataSetCsvFolder.left = new FormAttachment( 0, 0 );
    fdlDataSetCsvFolder.right = new FormAttachment( middle, 0 );
    fdlDataSetCsvFolder.top = new FormAttachment( lastControl, margin );
    wlDataSetCsvFolder.setLayoutData( fdlDataSetCsvFolder );
    wDataSetCsvFolder = new TextVar( variables, shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT );
    props.setLook( wDataSetCsvFolder );
    FormData fdDataSetCsvFolder = new FormData();
    fdDataSetCsvFolder.left = new FormAttachment( middle, margin );
    fdDataSetCsvFolder.right = new FormAttachment( 100, 0 );
    fdDataSetCsvFolder.top = new FormAttachment( wlDataSetCsvFolder, 0, SWT.CENTER );
    wDataSetCsvFolder.setLayoutData( fdDataSetCsvFolder );
    wDataSetCsvFolder.addModifyListener( e -> updateIVariables() );
    lastControl = wDataSetCsvFolder;

    Label wlEnforceHomeExecution = new Label( shell, SWT.RIGHT );
    props.setLook( wlEnforceHomeExecution );
    wlEnforceHomeExecution.setText( "Enforce executions in environment home? " );
    FormData fdlEnforceHomeExecution = new FormData();
    fdlEnforceHomeExecution.left = new FormAttachment( 0, 0 );
    fdlEnforceHomeExecution.right = new FormAttachment( middle, 0 );
    fdlEnforceHomeExecution.top = new FormAttachment( lastControl, margin );
    wlEnforceHomeExecution.setLayoutData( fdlEnforceHomeExecution );
    wEnforceHomeExecution = new Button( shell, SWT.CHECK | SWT.LEFT );
    props.setLook( wEnforceHomeExecution );
    FormData fdEnforceHomeExecution = new FormData();
    fdEnforceHomeExecution.left = new FormAttachment( middle, margin );
    fdEnforceHomeExecution.right = new FormAttachment( 100, 0 );
    fdEnforceHomeExecution.top = new FormAttachment( wlEnforceHomeExecution, 0, SWT.CENTER );
    wEnforceHomeExecution.setLayoutData( fdEnforceHomeExecution );
    lastControl = wEnforceHomeExecution;

    Label wlVariables = new Label( shell, SWT.RIGHT );
    props.setLook( wlVariables );
    wlVariables.setText( "System variables to set : " );
    FormData fdlVariables = new FormData();
    fdlVariables.left = new FormAttachment( 0, 0 );
    fdlVariables.right = new FormAttachment( middle, 0 );
    fdlVariables.top = new FormAttachment( lastControl, margin );
    wlVariables.setLayoutData( fdlVariables );

    ColumnInfo[] columnInfo = new ColumnInfo[] {
      new ColumnInfo( "Name", ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
      new ColumnInfo( "Value", ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
      new ColumnInfo( "Description (optional information)", ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
    };
    columnInfo[ 0 ].setUsingVariables( true );
    columnInfo[ 1 ].setUsingVariables( true );

    wVariables = new TableView( new Variables(), shell, SWT.NONE, columnInfo, environment.getVariables().size(), null, props );
    props.setLook( wVariables );
    FormData fdVariables = new FormData();
    fdVariables.left = new FormAttachment( 0, 0 );
    fdVariables.right = new FormAttachment( 100, 0 );
    fdVariables.top = new FormAttachment( wlVariables, margin );
    fdVariables.bottom = new FormAttachment( wOK, -margin * 2 );
    wVariables.setLayoutData( fdVariables );
    // lastControl = wVariables;

    // When enter is hit, close the dialog
    //
    wDescription.addListener( SWT.DefaultSelection, ( e ) -> ok() );
    wCompany.addListener( SWT.DefaultSelection, ( e ) -> ok() );
    wDepartment.addListener( SWT.DefaultSelection, ( e ) -> ok() );
    wProject.addListener( SWT.DefaultSelection, ( e ) -> ok() );
    wVersion.addListener( SWT.DefaultSelection, ( e ) -> ok() );
    wMetadataBaseFolder.addListener( SWT.DefaultSelection, ( e ) -> ok() );
    wUnitTestsBasePath.addListener( SWT.DefaultSelection, ( e ) -> ok() );
    wDataSetCsvFolder.addListener( SWT.DefaultSelection, ( e ) -> ok() );

    // Set the shell size, based upon previous time...
    BaseTransformDialog.setSize( shell );

    getData();

    shell.open();

    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }

    return returnValue;
  }

  private void updateIVariables() {
    Environment env = new Environment();
    getInfo( env );
    env.modifyVariables( variables, environmentName, environmentHomeFolder );
  }

  private void ok() {
    getInfo( environment );
    returnValue = environmentName;

    dispose();
  }

  private void cancel() {
    returnValue = null;

    dispose();
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  private void getData() {
    wDescription.setText( Const.NVL( environment.getDescription(), "" ) );
    wCompany.setText( Const.NVL( environment.getCompany(), "" ) );
    wDepartment.setText( Const.NVL( environment.getDepartment(), "" ) );
    wProject.setText( Const.NVL( environment.getProject(), "" ) );
    wVersion.setText( Const.NVL( environment.getVersion(), "" ) );
    wMetadataBaseFolder.setText( Const.NVL( environment.getMetadataBaseFolder(), "" ) );
    wUnitTestsBasePath.setText( Const.NVL( environment.getUnitTestsBasePath(), "" ) );
    wDataSetCsvFolder.setText( Const.NVL( environment.getDataSetsCsvFolder(), "" ) );
    wEnforceHomeExecution.setSelection( environment.isEnforcingExecutionInHome() );
    for ( int i = 0; i < environment.getVariables().size(); i++ ) {
      EnvironmentVariable environmentVariable = environment.getVariables().get( i );
      TableItem item = wVariables.table.getItem( i );
      item.setText( 1, Const.NVL( environmentVariable.getName(), "" ) );
      item.setText( 2, Const.NVL( environmentVariable.getValue(), "" ) );
      item.setText( 3, Const.NVL( environmentVariable.getDescription(), "" ) );
    }
    wVariables.setRowNums();
    wVariables.optWidth( true );
  }

  private void getInfo( Environment env ) {
    env.setDescription( wDescription.getText() );
    env.setCompany( wCompany.getText() );
    env.setDepartment( wDepartment.getText() );
    env.setProject( wProject.getText() );
    env.setVersion( wVersion.getText() );
    env.setMetadataBaseFolder( wMetadataBaseFolder.getText() );
    env.setUnitTestsBasePath( wUnitTestsBasePath.getText() );
    env.setDataSetsCsvFolder( wDataSetCsvFolder.getText() );
    env.setEnforcingExecutionInHome( wEnforceHomeExecution.getSelection() );
    env.getVariables().clear();
    for ( int i = 0; i < wVariables.nrNonEmpty(); i++ ) {
      TableItem item = wVariables.getNonEmpty( i );
      EnvironmentVariable variable = new EnvironmentVariable(
        item.getText( 1 ), // name
        item.getText( 2 ), // value
        item.getText( 3 )  // description
      );
      env.getVariables().add( variable );
    }
  }
}
