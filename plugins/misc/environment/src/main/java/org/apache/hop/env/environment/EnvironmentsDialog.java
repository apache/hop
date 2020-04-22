package org.apache.hop.env.environment;

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.List;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.apache.hop.env.config.EnvironmentConfig;
import org.apache.hop.env.config.EnvironmentConfigDialog;
import org.apache.hop.env.config.EnvironmentConfigSingleton;
import org.apache.hop.env.util.Defaults;
import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.persist.MetaStoreFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.util.Collections;

public class EnvironmentsDialog extends Dialog {

  private static Class<?> PKG = EnvironmentsDialog.class; // for i18n purposes, needed by Translator2!!

  private String lastEnvironment;
  private String selectedEnvironment;

  private Shell shell;
  private final PropsUi props;

  private List wEnvironments;

  private int margin;
  private int middle;
  private final MetaStoreFactory<Environment> environmentFactory;

  public EnvironmentsDialog( Shell parent, IMetaStore metaStore ) throws MetaStoreException {
    super( parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE );

    props = PropsUi.getInstance();

    environmentFactory = EnvironmentSingleton.getEnvironmentFactory();

    lastEnvironment = EnvironmentConfigSingleton.getConfig().getLastUsedEnvironment();
    selectedEnvironment = null;
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
    shell.setText( "Environments" );

    Button wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOK.addListener( SWT.Selection, event -> ok() );
    Button wAddDefault = new Button( shell, SWT.PUSH );
    wAddDefault.setText( "Add Default" );
    wAddDefault.addListener( SWT.Selection, event -> addDefault() );
    Button wEditConfig = new Button( shell, SWT.PUSH );
    wEditConfig.setText( "System Config" );
    wEditConfig.addListener( SWT.Selection, event -> editConfiguration() );
    Button wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, event -> cancel() );

    Label wlEnvironments = new Label( shell, SWT.LEFT );
    props.setLook( wlEnvironments );
    wlEnvironments.setText( "Select an Environment to use: " );
    FormData fdlEnvironments = new FormData();
    fdlEnvironments.left = new FormAttachment( 0, 0 );
    fdlEnvironments.top = new FormAttachment( 0, 0 );
    wlEnvironments.setLayoutData( fdlEnvironments );

    // Add some buttons
    //
    Button wbAdd = new Button( shell, SWT.PUSH );
    wbAdd.setImage( GuiResource.getInstance().getImageAdd() );
    wbAdd.setToolTipText( "Add a new configuration" );
    FormData fdbAdd = new FormData();
    fdbAdd.left = new FormAttachment( 0, 0 );
    fdbAdd.top = new FormAttachment( wlEnvironments, margin * 2 );
    wbAdd.setLayoutData( fdbAdd );
    wbAdd.addListener( SWT.Selection, ( e ) -> addEnvironment() );

    Button wbEdit = new Button( shell, SWT.PUSH );
    wbEdit.setImage( GuiResource.getInstance().getImageEdit() );
    wbEdit.setToolTipText( "Edit the selected configuration" );
    FormData fdbEdit = new FormData();
    fdbEdit.left = new FormAttachment( wbAdd, margin );
    fdbEdit.top = new FormAttachment( wlEnvironments, margin * 2 );
    wbEdit.setLayoutData( fdbEdit );
    wbEdit.addListener( SWT.Selection, ( e ) -> editEnvironment() );

    Button wbDelete = new Button( shell, SWT.PUSH );
    wbDelete.setImage( GuiResource.getInstance().getImageDelete() );
    wbDelete.setToolTipText( "Delete the selected configuration after confirmation" );
    FormData fdbDelete = new FormData();
    fdbDelete.left = new FormAttachment( wbEdit, margin * 2 );
    fdbDelete.top = new FormAttachment( wlEnvironments, margin * 2 );
    wbDelete.setLayoutData( fdbDelete );
    wbDelete.addListener( SWT.Selection, ( e ) -> deleteEnvironment() );

    Button wbImport = new Button( shell, SWT.PUSH );
    wbImport.setImage( GuiResource.getInstance().getImageFolder() );
    wbImport.setToolTipText( "Import an environment from a JSON file" );
    FormData fdbImport = new FormData();
    fdbImport.left = new FormAttachment( wbDelete, margin * 2 );
    fdbImport.top = new FormAttachment( wlEnvironments, margin * 2 );
    wbImport.setLayoutData( fdbImport );
    wbImport.addListener( SWT.Selection, ( e ) -> importEnvironment() );

    Button wbExport = new Button( shell, SWT.PUSH );
    wbExport.setImage( GuiResource.getInstance().getImageExport() );
    wbExport.setToolTipText( "Export an environment to a JSON file" );
    FormData fdbExport = new FormData();
    fdbExport.left = new FormAttachment( wbImport, margin * 2 );
    fdbExport.top = new FormAttachment( wlEnvironments, margin * 2 );
    wbExport.setLayoutData( fdbExport );
    wbExport.addListener( SWT.Selection, ( e ) -> exportEnvironment() );


    // Put a label describing config Just above the buttons...
    //
    Label wlClarification = new Label( shell, SWT.LEFT );
    props.setLook( wlClarification );
    String clarification = "Environments are stored in : ";
    if ( StringUtils.isNotEmpty( System.getProperty( Defaults.VARIABLE_ENVIRONMENT_METASTORE_FOLDER ) ) ) {
      clarification += "${" + Defaults.VARIABLE_ENVIRONMENT_METASTORE_FOLDER + "}";
    } else {
      clarification += Defaults.ENVIRONMENT_METASTORE_FOLDER;
    }
    wlClarification.setText( clarification );
    FormData fdlClarification = new FormData();
    fdlClarification.left = new FormAttachment( 0, 0 );
    fdlClarification.right = new FormAttachment( 100, 0 );
    fdlClarification.bottom = new FormAttachment( wOK, -margin * 2 );
    wlClarification.setLayoutData( fdlClarification );

    wEnvironments = new List( shell, SWT.LEFT | SWT.BORDER | SWT.SINGLE );
    props.setLook( wEnvironments );
    FormData fdEnvironments = new FormData();
    fdEnvironments.left = new FormAttachment( 0, 0 );
    fdEnvironments.right = new FormAttachment( 100, 0 );
    fdEnvironments.top = new FormAttachment( wbAdd, margin * 2 );
    fdEnvironments.bottom = new FormAttachment( wlClarification, -margin * 2 );
    wEnvironments.setLayoutData( fdEnvironments );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOK, wAddDefault, wEditConfig, wCancel }, margin, null );

    // Double click on an environment : select it
    //
    wEnvironments.addListener( SWT.DefaultSelection, ( e ) -> ok() );

    getData();

    wEnvironments.setFocus();

    shell.open();

    // Set the shell size, based upon previous time...
    BaseTransformDialog.setSize( shell );


    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }

    return selectedEnvironment;
  }

  private void deleteEnvironment() {
    String selection[] = wEnvironments.getSelection();
    if ( selection.length == 0 ) {
      return;
    }
    String selectedEnvironment = selection[ 0 ];
    MessageBox box = new MessageBox( shell, SWT.ICON_QUESTION | SWT.APPLICATION_MODAL | SWT.YES | SWT.NO );
    box.setText( "Delete environment?" );
    box.setMessage( "Are you sure you want to delete environment '" + selectedEnvironment + "'?" );
    int answer = box.open();
    if ( ( answer & SWT.YES ) == 0 ) {
      return;
    }

    try {
      environmentFactory.deleteElement( selectedEnvironment );
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error deleting environment '" + selectedEnvironment + "'", e );
    } finally {
      refreshEnvironmentsList();
    }
  }

  private void editEnvironment() {
    String selection[] = wEnvironments.getSelection();
    if ( selection.length == 0 ) {
      return;
    }
    String selectedEnvironment = selection[ 0 ];
    try {
      Environment environment = environmentFactory.loadElement( selectedEnvironment );
      EnvironmentDialog environmentDialog = new EnvironmentDialog( shell, environment );
      if ( environmentDialog.open() ) {
        environmentFactory.saveElement( environment );
      }
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error editing environment '" + selectedEnvironment + "'", e );
    } finally {
      refreshEnvironmentsList();
    }
  }

  private void addEnvironment() {
    Environment environment = new Environment();
    environment.applySuggestedSettings();
    environment.setName( "Environment #" + ( wEnvironments.getItemCount() + 1 ) );
    try {
      EnvironmentDialog environmentDialog = new EnvironmentDialog( shell, environment );
      if ( environmentDialog.open() ) {
        environmentFactory.saveElement( environment );
      }
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error adding environment '" + environment.getName() + "'", e );
    } finally {
      refreshEnvironmentsList();
    }
  }

  private void exportEnvironment() {
    String selection[] = wEnvironments.getSelection();
    if ( selection.length == 0 ) {
      return;
    }
    String selectedEnvironment = selection[ 0 ];
    try {
      Environment environment = environmentFactory.loadElement( selectedEnvironment );

      FileDialog dialog = new FileDialog( shell, SWT.SAVE );
      dialog.setFilterNames( new String[] { "JSON files", "All Files (*.*)" } );
      dialog.setFilterExtensions( new String[] { "*.json", "*.*" } );
      dialog.setFileName( environment.getName().toLowerCase().replace( " ", "-" ) + ".json" );
      String filename = dialog.open();
      if ( StringUtils.isNotEmpty( filename ) ) {
        File file = new File( filename );
        if ( file.exists() ) {

          MessageBox box = new MessageBox( shell, SWT.ICON_QUESTION | SWT.APPLICATION_MODAL | SWT.YES | SWT.NO );
          box.setText( "Replace file?" );
          box.setMessage( "Are you sure you want to replace file '" + filename + "'?" );
          int answer = box.open();
          if ( ( answer & SWT.YES ) == 0 ) {
            return;
          }
        }

        // Export to the selected file
        //
        FileOutputStream fos = new FileOutputStream( filename );
        String jsonString = environment.toJsonString();
        fos.write( jsonString.getBytes( "UTF-8" ) );
        fos.flush();
        fos.close();
      }
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error exporting environment '" + selectedEnvironment + "'", e );
    } finally {
      refreshEnvironmentsList();
    }
  }


  private void importEnvironment() {

    try {

      FileDialog dialog = new FileDialog( shell, SWT.OPEN );
      dialog.setFilterNames( new String[] { "JSON files", "All Files (*.*)" } );
      dialog.setFilterExtensions( new String[] { "*.json", "*.*" } );
      String filename = dialog.open();
      if ( StringUtils.isNotEmpty( filename ) ) {

        String environmentJsonString = new String( Files.readAllBytes( new File( filename ).toPath() ), "UTF-8" );
        Environment environment = Environment.fromJsonString( environmentJsonString );

        // Check overwrite
        //
        Environment existing = environmentFactory.loadElement( environment.getName() );
        if ( existing != null ) {
          MessageBox box = new MessageBox( shell, SWT.ICON_QUESTION | SWT.APPLICATION_MODAL | SWT.YES | SWT.NO );
          box.setText( "Replace environment?" );
          box.setMessage( "Are you sure you want to replace environment '" + environment.getName() + "'?" );
          int answer = box.open();
          if ( ( answer & SWT.YES ) == 0 ) {
            return;
          }
        }
        environmentFactory.saveElement( environment );
      }
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error importing environment", e );
    } finally {
      refreshEnvironmentsList();
    }

  }

  private void addDefault() {
    try {
      Environment environment = new Environment();
      environment.applyKettleDefaultSettings();

      EnvironmentDialog environmentDialog = new EnvironmentDialog( shell, environment );
      if ( environmentDialog.open() ) {
        environmentFactory.saveElement( environment );
      }
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error adding Default environment", e );
    } finally {
      refreshEnvironmentsList();
    }
  }

  private void editConfiguration() {
    try {
      EnvironmentConfig config = EnvironmentConfigSingleton.getConfig();
      EnvironmentConfigDialog dialog = new EnvironmentConfigDialog( shell, config );
      if ( dialog.open() ) {
        EnvironmentConfigSingleton.getConfigFactory().saveElement( config );
      }
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error editing configuration", e );
    }
  }

  private void ok() {
    String[] selection = wEnvironments.getSelection();
    if ( selection.length == 0 ) {
      return;
    }
    selectedEnvironment = selection[ 0 ];

    dispose();
  }

  private void cancel() {
    selectedEnvironment = null;

    dispose();
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  private void getData() {
    try {
      refreshEnvironmentsList();
      if ( StringUtils.isNotEmpty( lastEnvironment ) ) {
        wEnvironments.setSelection( new String[] { lastEnvironment } );
      }
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error getting list of environments", e );
    }
  }

  public void refreshEnvironmentsList() {
    try {
      String selected = null;
      if ( wEnvironments.getSelection().length > 0 ) {
        selected = wEnvironments.getSelection()[ 0 ];
      }
      wEnvironments.removeAll();
      java.util.List<String> elementNames = environmentFactory.getElementNames();
      Collections.sort( elementNames );
      for ( String elementName : elementNames ) {
        wEnvironments.add( elementName );
      }
      if ( selected != null ) {
        wEnvironments.setSelection( new String[] { selected } );
      }
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error getting list of environments", e );
    }
  }
}