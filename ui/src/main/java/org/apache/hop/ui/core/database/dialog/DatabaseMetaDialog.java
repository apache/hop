/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.core.database.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseTestResults;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElements;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.plugins.DatabasePluginType;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.stores.memory.MemoryMetaStore;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.dialog.ShowMessageDialog;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.core.gui.GuiElementWidgets;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.trans.step.BaseStepDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import java.util.Arrays;
import java.util.List;

@GuiPlugin(
  id = "DatabaseConnection",
  description = "This is the dialog for database connection metadata"
)
public class DatabaseMetaDialog extends Dialog {
  private static Class<?> PKG = DatabaseMetaDialog.class; // for i18n purposes, needed by Translator2!!
  private Shell parent;
  private Shell shell;
  private DatabaseMeta databaseMeta;
  private DatabaseMeta workingMeta;
  private final IMetaStore metaStore;

  private CTabFolder wTabFolder;

  private CTabItem wGeneralTab;
  private Composite wGeneralComp;
  private FormData fdGeneralComp;
  private Text wName;
  private ComboVar wConnectionType;
  private ComboVar wAccessType;
  private Composite wDatabaseSpecificComp;
  private GuiElementWidgets guiElementWidgets;

  private CTabItem wAdvancedTab;
  private Composite wAdvancedComp;
  private FormData fdAdvancedComp;
  private Button wSupportsBoolean;
  private Button wSupportsTimestamp;
  private Button wQuoteAll;
  private Button wForceLowercase;
  private Button wForceUppercase;
  private Button wPreserveCase;
  private TextVar wPreferredSchema;
  private TextVar wSQLStatements;

  private CTabItem wOptionsTab;
  private Composite wOptionsComp;
  private FormData fdOptionsComp;
  private TableView wOptions;

  private CTabItem wPoolingTab;
  private Composite wPoolingComp;
  private FormData fdPoolingComp;
  private Button wEnablePooling;
  private TextVar wInitialPoolSize;
  private TextVar wMaximumPoolSize;
  private TableView wPoolingParameters;

  private final PropsUI props;
  private int middle;
  private int margin;

  private String returnValue;

  /**
   * These are always the 3 parameters provided
   *
   * @param parent       The parent shell
   * @param metaStore    The MetaStore to optionally reference external objects with
   * @param databaseMeta The object to edit
   */
  public DatabaseMetaDialog( Shell parent, IMetaStore metaStore, DatabaseMeta databaseMeta ) {
    super( parent, SWT.NONE );
    this.parent = parent;
    this.metaStore = metaStore;
    this.databaseMeta = databaseMeta;
    this.workingMeta = new DatabaseMeta(databaseMeta);
    props = PropsUI.getInstance();
    returnValue=null;
  }

  public String open() {
    // Create a tabbed interface instead of the confusing left hand side options
    // This will make it more conforming the rest.
    //
    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    shell.setImage( GUIResource.getInstance().getImageConnection() );

    middle = props.getMiddlePct();
    margin = Const.MARGIN;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setText( BaseMessages.getString( PKG, "DatabaseDialog.Shell.title" ) );
    shell.setLayout( formLayout );

    // Add buttons at the bottom
    Button wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOK.addListener( SWT.Selection, this::ok );

    Button wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, this::cancel );

    Button wTest = new Button( shell, SWT.PUSH );
    wTest.setText( BaseMessages.getString( PKG, "System.Button.Test" ) );
    wTest.addListener( SWT.Selection, this::test );

    Button[] buttons = new Button[] { wOK, wTest, wCancel };
    BaseStepDialog.positionBottomButtons( shell, buttons, margin, null );

    // Now create the tabs above the buttons...

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );

    addGeneralTab();

    getData();

    // Select the general tab
    //
    wTabFolder.setSelection( 0 );
    FormData fdTabFolder = new FormData(  );
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( 0, 0 );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOK, -margin*3 );
    wTabFolder.setLayoutData( fdTabFolder );

    BaseStepDialog.setSize( shell );

    shell.open();
    Display display = parent.getDisplay();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return returnValue;
  }

  private void addGeneralTab() {

    wGeneralTab = new CTabItem( wTabFolder, SWT.NONE );
    wGeneralTab.setText( "   "+ BaseMessages.getString( PKG, "EnterOptionsDialog.General.Label" ) +"   " );

    wGeneralComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wGeneralComp );

    FormLayout genLayout = new FormLayout();
    genLayout.marginWidth = Const.FORM_MARGIN*2;
    genLayout.marginHeight = Const.FORM_MARGIN*2;
    wGeneralComp.setLayout( genLayout );

    // What's the name
    //
    Label wlName = new Label( wGeneralComp, SWT.RIGHT );
    props.setLook( wlName );
    wlName.setText( BaseMessages.getString( PKG, "DatabaseDialog.label.ConnectionName" ) );
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment( 0, 0 );
    fdlName.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlName.right = new FormAttachment( middle, 0 );
    wlName.setLayoutData( fdlName );
    wName = new Text( wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    FormData fdName = new FormData();
    fdName.top = new FormAttachment( wlName, 0, SWT.CENTER );
    fdName.left = new FormAttachment( middle, margin ); // To the right of the label
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData( fdName );
    Control lastControl = wName;

    // What's the type of database access?
    //
    Label wlConnectionType = new Label( wGeneralComp, SWT.RIGHT );
    props.setLook( wlConnectionType );
    wlConnectionType.setText( BaseMessages.getString( PKG, "DatabaseDialog.label.ConnectionType" ) );
    FormData fdlConnectionType = new FormData();
    fdlConnectionType.top = new FormAttachment( lastControl, margin );
    fdlConnectionType.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlConnectionType.right = new FormAttachment( middle, 0 );
    wlConnectionType.setLayoutData( fdlConnectionType );
    wConnectionType = new ComboVar( databaseMeta, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wConnectionType );
    wConnectionType.setItems( getConnectionTypes() );
    FormData fdConnectionType = new FormData();
    fdConnectionType.top = new FormAttachment( wlConnectionType, 0, SWT.CENTER );
    fdConnectionType.left = new FormAttachment( middle, margin ); // To the right of the label
    fdConnectionType.right = new FormAttachment( 100, 0 );
    wConnectionType.setLayoutData( fdConnectionType );
    lastControl = wConnectionType;
    // TODO: Add listener to refresh the access type specific composite widgets


    // What's the type of database connection?
    //
    Label wlAccessType = new Label( wGeneralComp, SWT.RIGHT );
    props.setLook( wlAccessType );
    wlAccessType.setText( BaseMessages.getString( PKG, "DatabaseDialog.label.AccessMethod" ) );
    FormData fdlAccessType = new FormData();
    fdlAccessType.top = new FormAttachment( lastControl, margin * 2 );
    fdlAccessType.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlAccessType.right = new FormAttachment( middle, -margin );
    wlAccessType.setLayoutData( fdlAccessType );
    wAccessType = new ComboVar( databaseMeta, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wAccessType );
    wAccessType.setItems( DatabaseMeta.dbAccessTypeDesc );
    FormData fdAccessType = new FormData();
    fdAccessType.top = new FormAttachment( wlAccessType, 0, SWT.CENTER );
    fdAccessType.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdAccessType.right = new FormAttachment( 100, 0 );
    wAccessType.setLayoutData( fdAccessType );
    lastControl = wAccessType;

    // Add a composite area
    //
    wDatabaseSpecificComp = new Composite( wGeneralComp, SWT.BACKGROUND );
    props.setLook(wDatabaseSpecificComp);
    wDatabaseSpecificComp.setLayout( new FormLayout() );
    FormData fdDatabaseSpecificComp = new FormData(  );
    fdDatabaseSpecificComp.left = new FormAttachment( 0, 0 );
    fdDatabaseSpecificComp.right = new FormAttachment( 100, 0 );
    fdDatabaseSpecificComp.top = new FormAttachment( lastControl, margin );
    fdDatabaseSpecificComp.bottom = new FormAttachment( 100, 0 );
    wDatabaseSpecificComp.setLayoutData( fdDatabaseSpecificComp );

    // Now add the database plugin specific widgets
    //
    guiElementWidgets = new GuiElementWidgets(databaseMeta);
    guiElementWidgets.createWidgets( workingMeta.getDatabaseInterface(), wDatabaseSpecificComp, DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID, null);

    fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( 0, 0 );
    fdGeneralComp.right = new FormAttachment( 100, 0 );
    fdGeneralComp.bottom = new FormAttachment( 100, 0 );
    wGeneralComp.setLayoutData( fdGeneralComp );

    wGeneralComp.layout();
    wGeneralTab.setControl( wGeneralComp );
  }

  private void ok( Event event ) {
    getInfo( databaseMeta );
    returnValue = databaseMeta.getName();
    dispose();
  }

  private void cancel( Event event ) {
    dispose();
  }

  private void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  private void test( Event event ) {
    testConnection( shell, getInfo(new DatabaseMeta()) );
  }

  /**
   * Copy data from the metadata into the dialog.
   */
  private void getData() {

    System.out.println("DMD getData()  START");
    wName.setText( Const.NVL(workingMeta.getName(), "") );
    wAccessType.setText( Const.NVL(workingMeta.getAccessTypeDesc(), "") );
    wConnectionType.setText( Const.NVL(workingMeta.getPluginId(), "") );

    System.out.println("DMD getData()  getWidgetsContents");
    guiElementWidgets.setWidgetsContents( workingMeta.getDatabaseInterface(), wDatabaseSpecificComp, DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID );

    System.out.println("DMD getData()  END");
  }

  private DatabaseMeta getInfo( DatabaseMeta meta ) {

    meta.setName(wName.getText());
    meta.setDatabaseType( wConnectionType.getText() );
    meta.setAccessType( DatabaseMeta.getAccessType( wAccessType.getText() ) );

    // Get the database specific information
    //
    guiElementWidgets.getWidgetsContents(meta.getDatabaseInterface(), DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);

    return meta;
  }



  /**
   * Test the database connection
   */
  public static final void testConnection( Shell shell, DatabaseMeta databaseMeta ) {
    String[] remarks = databaseMeta.checkParameters();
    if ( remarks.length == 0 ) {
      // Get a "test" report from this database
      DatabaseTestResults databaseTestResults = databaseMeta.testConnectionSuccess();
      String message = databaseTestResults.getMessage();
      boolean success = databaseTestResults.isSuccess();
      String title = success ? BaseMessages.getString( PKG, "DatabaseDialog.DatabaseConnectionTestSuccess.title" )
        : BaseMessages.getString( PKG, "DatabaseDialog.DatabaseConnectionTest.title" );
      if ( success && message.contains( Const.CR ) ) {
        message = message.substring( 0, message.indexOf( Const.CR ) )
          + Const.CR + message.substring( message.indexOf( Const.CR ) );
        message = message.substring( 0, message.lastIndexOf( Const.CR ) );
      }
      ShowMessageDialog msgDialog = new ShowMessageDialog( shell, SWT.ICON_INFORMATION | SWT.OK,
        title, message, message.length() > 300 );
      msgDialog.setType( success ? Const.SHOW_MESSAGE_DIALOG_DB_TEST_SUCCESS
        : Const.SHOW_MESSAGE_DIALOG_DB_TEST_DEFAULT );
      msgDialog.open();
    } else {
      String message = "";
      for ( int i = 0; i < remarks.length; i++ ) {
        message += "    * " + remarks[i] + Const.CR;
      }

      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "DatabaseDialog.ErrorParameters2.title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "DatabaseDialog.ErrorParameters2.description", message ) );
      mb.open();
    }
  }

  private String[] getConnectionTypes() {
    PluginRegistry registry = PluginRegistry.getInstance();
    List<PluginInterface> plugins = registry.getPlugins( DatabasePluginType.class );
    String[] types = new String[plugins.size()];
    for (int i=0;i<types.length;i++) {
      types[i] = plugins.get( i ).getName();
    }
    Arrays.sort(types);
    return types;
  }

  /**
   * Gets databaseMeta
   *
   * @return value of databaseMeta
   */
  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  /**
   * @param databaseMeta The databaseMeta to set
   */
  public void setDatabaseMeta( DatabaseMeta databaseMeta ) {
    this.databaseMeta = databaseMeta;
  }

  /**
   * Gets metaStore
   *
   * @return value of metaStore
   */
  public IMetaStore getMetaStore() {
    return metaStore;
  }

  public static void main( String[] args ) throws HopException {
    System.out.println(">>>>>>>>>>>>>>>> DatabaseMetaDialog START");
    Display display = new Display(  );
    Shell shell = new Shell( display, SWT.MIN | SWT.MAX | SWT.RESIZE );
    // shell.setSize( 500, 500 );
    // shell.open();

    System.out.println(">>>>>>>>>>>>>>>> Main shell opened");

    HopClientEnvironment.init();
    System.out.println(">>>>>>>>>>>>>>>> Hop client environment initialized");

    List<PluginInterface> plugins = PluginRegistry.getInstance().getPlugins( DatabasePluginType.class );
    System.out.println(">>>>>>>>>>>>>>>> Nr of database plugins found: "+plugins.size());

    PropsUI.init( display, Props.TYPE_PROPERTIES_SPOON );
    System.out.println(">>>>>>>>>>>>>>>> PropsUI initialized");

    HopEnvironment.init();
    System.out.println(">>>>>>>>>>>>>>>> Hop Environment initialized");

    IMetaStore metaStore = new MemoryMetaStore();
    DatabaseMeta databaseMeta = new DatabaseMeta("Test", "MYSQL", "Native", "localhost", "samples", "3306", "username", "password");

    System.out.println(">>>>>>>>>>>>>>>> DatabaseMetaDialog created");
    DatabaseMetaDialog dialog = new DatabaseMetaDialog( shell, metaStore, databaseMeta );
    System.out.println(">>>>>>>>>>>>>>>> DatabaseMetaDialog created");
    String name = dialog.open();
    System.out.println(">>>>>>>>>>>>>>>> DatabaseMetaDialog opened, name="+name);

    System.out.println(">>>>>>>>>>>>>>>> DatabaseMetaDialog hostname = "+databaseMeta.getHostname());
    System.out.println(">>>>>>>>>>>>>>>> DatabaseMetaDialog port     = "+databaseMeta.getPort());
    System.out.println(">>>>>>>>>>>>>>>> DatabaseMetaDialog db name  = "+databaseMeta.getDatabaseName());
    System.out.println(">>>>>>>>>>>>>>>> DatabaseMetaDialog username = "+databaseMeta.getUsername());
    System.out.println(">>>>>>>>>>>>>>>> DatabaseMetaDialog password = "+databaseMeta.getPassword());

    // while ( shell != null && !shell.isDisposed() ) {
    //   if ( !display.readAndDispatch() ) {
    //    display.sleep();
    //  }
    // }
    display.dispose();
  }
}
