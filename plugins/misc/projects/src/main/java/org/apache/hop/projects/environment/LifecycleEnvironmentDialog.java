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

package org.apache.hop.projects.environment;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.DescribedVariablesConfigFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.io.File;

public class LifecycleEnvironmentDialog extends Dialog {
  private static final Class<?> PKG = LifecycleEnvironmentDialog.class; // For Translator

  private final LifecycleEnvironment environment;

  private String returnValue;

  private Shell shell;
  private final PropsUi props;

  private Text wName;
  private Combo wPurpose;
  private Combo wProject;
  private TableView wConfigFiles;

  private int margin;
  private int middle;

  private IVariables variables;
  private Button wbSelect;
  private Button wbNew;
  private Button wbEdit;

  private String originalName;

  private boolean needingEnvironmentRefresh;

  public LifecycleEnvironmentDialog( Shell parent, LifecycleEnvironment environment, IVariables variables ) {
    super( parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE );

    this.environment = environment;
    this.variables = variables;

    this.originalName = environment.getName();

    props = PropsUi.getInstance();

    needingEnvironmentRefresh = false;
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
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( "Project Lifecycle Environment dialog" );

    // Buttons go at the bottom of the dialog
    //
    Button wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOK.addListener( SWT.Selection, event -> ok() );
    Button wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, event -> cancel() );
    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOK, wCancel }, margin * 3, null );

    Label wlName = new Label( shell, SWT.RIGHT );
    props.setLook( wlName );
    wlName.setText( "Name " );
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, 0 );
    fdlName.top = new FormAttachment( 0, margin );
    wlName.setLayoutData( fdlName );
    wName = new Text( shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT );
    props.setLook( wName );
    FormData fdName = new FormData();
    fdName.left = new FormAttachment( middle, margin );
    fdName.right = new FormAttachment( 100, 0 );
    fdName.top = new FormAttachment( wlName, 0, SWT.CENTER );
    wName.setLayoutData( fdName );
    Control lastControl = wName;

    Label wlPurpose = new Label( shell, SWT.RIGHT );
    props.setLook( wlPurpose );
    wlPurpose.setText( "Purpose " );
    FormData fdlPurpose = new FormData();
    fdlPurpose.left = new FormAttachment( 0, 0 );
    fdlPurpose.right = new FormAttachment( middle, 0 );
    fdlPurpose.top = new FormAttachment( lastControl, margin );
    wlPurpose.setLayoutData( fdlPurpose );
    wPurpose = new Combo( shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT );
    props.setLook( wPurpose );
    FormData fdPurpose = new FormData();
    fdPurpose.left = new FormAttachment( middle, margin );
    fdPurpose.right = new FormAttachment( 100, 0 );
    fdPurpose.top = new FormAttachment( wlPurpose, 0, SWT.CENTER );
    wPurpose.setLayoutData( fdPurpose );
    wPurpose.addListener( SWT.Modify, e->needingEnvironmentRefresh=true );
    lastControl = wPurpose;

    Label wlProject = new Label( shell, SWT.RIGHT );
    props.setLook( wlProject );
    wlProject.setText( "Project " );
    FormData fdlProject = new FormData();
    fdlProject.left = new FormAttachment( 0, 0 );
    fdlProject.right = new FormAttachment( middle, 0 );
    fdlProject.top = new FormAttachment( lastControl, margin );
    wlProject.setLayoutData( fdlProject );
    wProject = new Combo( shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT );
    props.setLook( wProject );
    FormData fdProject = new FormData();
    fdProject.left = new FormAttachment( middle, margin );
    fdProject.right = new FormAttachment( 100, 0 );
    fdProject.top = new FormAttachment( wlProject, 0, SWT.CENTER );
    wProject.setLayoutData( fdProject );
    wProject.addListener( SWT.Modify, e->needingEnvironmentRefresh=true );
    lastControl = wProject;

    Label wlConfigFiles = new Label( shell, SWT.LEFT );
    props.setLook( wlConfigFiles );
    wlConfigFiles.setText( "Configuration files: " );
    FormData fdlConfigFiles = new FormData();
    fdlConfigFiles.left = new FormAttachment( 0, 0 );
    fdlConfigFiles.right = new FormAttachment( 100, 0 );
    fdlConfigFiles.top = new FormAttachment( lastControl, margin );
    wlConfigFiles.setLayoutData( fdlConfigFiles );

    wbSelect = new Button(shell, SWT.PUSH);
    props.setLook( wbSelect );
    wbSelect.setText( "Select..." );
    FormData fdAdd = new FormData();
    fdAdd.right = new FormAttachment(100, 0);
    fdAdd.top = new FormAttachment(wlConfigFiles, margin);
    wbSelect.setLayoutData( fdAdd );
    wbSelect.addListener( SWT.Selection, this::addConfigFile );

    ColumnInfo[] columnInfo = new ColumnInfo[] {
      new ColumnInfo( "Filename", ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
    };
    columnInfo[ 0 ].setUsingVariables( true );

    wConfigFiles = new TableView( variables, shell, SWT.SINGLE | SWT.BORDER, columnInfo, environment.getConfigurationFiles().size(), null, props );
    props.setLook( wConfigFiles );
    FormData fdConfigFiles = new FormData();
    fdConfigFiles.left = new FormAttachment( 0, 0 );
    fdConfigFiles.right = new FormAttachment( wbSelect, -2*margin );
    fdConfigFiles.top = new FormAttachment( wlConfigFiles, margin );
    fdConfigFiles.bottom = new FormAttachment( wOK, -margin * 2 );
    wConfigFiles.setLayoutData( fdConfigFiles );
    wConfigFiles.table.addListener( SWT.Selection, this::setButtonStates );

    wbNew = new Button(shell, SWT.PUSH);
    props.setLook( wbNew );
    wbNew.setText( "New..." );
    FormData fdNew = new FormData();
    fdNew.left = new FormAttachment(wConfigFiles, 2*margin);
    fdNew.right = new FormAttachment(100, 0);
    fdNew.top = new FormAttachment(wbSelect, margin);
    wbNew.setLayoutData( fdNew );
    wbNew.addListener( SWT.Selection, this::newConfigFile );

    wbEdit = new Button(shell, SWT.PUSH);
    props.setLook( wbEdit );
    wbEdit.setText( "Edit..." );
    FormData fdEdit = new FormData();
    fdEdit.left = new FormAttachment(wConfigFiles, 2*margin);
    fdEdit.right = new FormAttachment(100, 0);
    fdEdit.top = new FormAttachment( wbNew, margin);
    wbEdit.setLayoutData( fdEdit );
    wbEdit.addListener( SWT.Selection, this::editConfigFile );

    // When enter is hit, close the dialog
    //
    wName.addListener( SWT.DefaultSelection, ( e ) -> ok() );
    wPurpose.addListener( SWT.DefaultSelection, ( e ) -> ok() );
    wProject.addListener( SWT.DefaultSelection, ( e ) -> ok() );

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

  private void editConfigFile( Event event ) {
    try {

      int index = wConfigFiles.getSelectionIndex();
      if ( index < 0 ) {
        return;
      }
      String configFilename = wConfigFiles.getItem( index, 1 );
      if ( StringUtils.isEmpty( configFilename ) ) {
        return;
      }
      String realConfigFilename = variables.resolve(configFilename);

      DescribedVariablesConfigFile variablesConfigFile = new DescribedVariablesConfigFile( realConfigFilename );

      File file = new File( realConfigFilename );
      if ( !file.exists() ) {
        MessageBox box = new MessageBox( HopGui.getInstance().getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION );
        box.setText( "Create file?" );
        box.setMessage( "This configuration file doesn't exist.  Do you want to create it?" );
        int anwser = box.open();
        if ( ( anwser & SWT.NO ) != 0 ) {
          return;
        }
      } else {
        variablesConfigFile.readFromFile();
      }

      boolean changed = HopGui.editConfigFile(shell, realConfigFilename, variablesConfigFile, null);
      if (changed) {
        needingEnvironmentRefresh=true;
      }

    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error editing configuration file", e );
    }
  }

  private void addConfigFile( Event event ) {
    String configFile = BaseDialog.presentFileDialog( shell, null, variables,
      new String[] { "*.json", "*"},
      new String[] { "Config JSON files", "All files" },
      true);
    if (configFile!=null) {
      TableItem item = new TableItem( wConfigFiles.table, SWT.NONE );
      item.setText( 1, configFile );
      wConfigFiles.removeEmptyRows();
      wConfigFiles.setRowNums();
      wConfigFiles.optWidth( true );
      wConfigFiles.table.setSelection( item );
      needingEnvironmentRefresh=true;

    }
  }

  private void newConfigFile( Event event ) {
    try {
      // What's the project folder?
      //
      String filename = "environment-conf.json";

      String projectName = wProject.getText();
      if (StringUtils.isNotEmpty( projectName )) {
        ProjectConfig projectConfig = ProjectsConfigSingleton.getConfig().findProjectConfig( projectName );
        if (projectConfig!=null) {
          String environmentName = Const.NVL(wName.getText(), projectName);
          filename = projectConfig.getProjectHome()+File.separator+".."+File.separator+environmentName+"-config.json";
        }
      }
      FileObject fileObject = HopVfs.getFileObject( filename );

      String configFile = BaseDialog.presentFileDialog( shell, null, variables, fileObject,
        new String[] { "*.json", "*"},
        new String[] { "Config JSON files", "All files" },
        true);
      if (configFile!=null) {
        TableItem item = new TableItem( wConfigFiles.table, SWT.NONE );
        item.setText( 1, configFile );
        wConfigFiles.removeEmptyRows();
        wConfigFiles.setRowNums();
        wConfigFiles.optWidth( true );
        wConfigFiles.table.setSelection( item );
        needingEnvironmentRefresh=true;
      }
    } catch(Exception e) {
      new ErrorDialog(HopGui.getInstance().getShell(), "Error", "Error creating new environment configuration file", e);
    }
  }

  private void setButtonStates( Event event ) {
    int index = wConfigFiles.getSelectionIndex();
    wbEdit.setEnabled( index>=0 );
    wbEdit.setGrayed( index<0 );
  }


  private void ok() {

    try {
      String environmentName = wName.getText();
      if (StringUtils.isEmpty( environmentName )) {
        throw new HopException("Please give your environment a name");
      }
      if (StringUtils.isNotEmpty( originalName )) {
        if (!originalName.equals(environmentName)) {
          wName.setText(originalName);
          throw new HopException("Sorry, renaming environment '"+originalName+"' is not supported.");
        }
      }

    getInfo( environment );
    returnValue = environment.getName();

    dispose();
    } catch(Exception e) {
      new ErrorDialog( shell, "Error", "There is a configuration error in the environment", e );
    }
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
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();

    wProject.setItems( config.listProjectConfigNames().toArray( new String[ 0 ] ) );
    wPurpose.setItems( new String[] {
      "Development",
      "Testing",
      "Acceptance",
      "Production",
      "Continuous Integration",
      "Common Build",
    } );

    wName.setText( Const.NVL( environment.getName(), "" ) );
    wPurpose.setText( Const.NVL( environment.getPurpose(), "" ) );
    wProject.setText( Const.NVL( environment.getProjectName(), "" ) );

    for ( int i = 0; i < environment.getConfigurationFiles().size(); i++ ) {
      String configurationFile = environment.getConfigurationFiles().get(i);
      TableItem item = wConfigFiles.table.getItem( i );
      item.setText( 1, Const.NVL( configurationFile, "" ) );
    }
    wConfigFiles.setRowNums();
    wConfigFiles.optWidth( true );

    // Select the first configuration file by default
    // That way you can immediately hit the edit button
    //
    if (!environment.getConfigurationFiles().isEmpty()) {
      wConfigFiles.setSelection( new int[] { 0 } );
    }

  }

  private void getInfo( LifecycleEnvironment env ) {
    env.setName( wName.getText() );
    env.setPurpose( wPurpose.getText() );
    env.setProjectName( wProject.getText() );

    env.getConfigurationFiles().clear();
    for (TableItem item : wConfigFiles.getNonEmptyItems()) {
      env.getConfigurationFiles().add(item.getText(1));
    }
  }

  /**
   * Gets needingEnvironmentRefresh
   *
   * @return value of needingEnvironmentRefresh
   */
  public boolean isNeedingEnvironmentRefresh() {
    return needingEnvironmentRefresh;
  }
}
