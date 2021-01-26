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

package org.apache.hop.workflow.actions.copymoveresultfilenames;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * This dialog allows you to edit the Copy/Move result filenames action settings.
 *
 * @author Samatar
 * @since 26-02-2008
 */
public class ActionCopyMoveResultFilenamesDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionCopyMoveResultFilenames.class; // For Translator

  private Text wName;

  private Button wSpecifyWildcard;

  private Label wlWildcard;
  private TextVar wWildcard;

  private Label wlWildcardExclude;
  private TextVar wWildcardExclude;

  private CCombo wAction;

  private ActionCopyMoveResultFilenames action;
  private Shell shell;

  private boolean changed;

  private Label wlFoldername;
  private Button wbFoldername;
  private TextVar wFoldername;

  private Label wlAddDate;
  private Button wAddDate;

  private Label wlAddTime;
  private Button wAddTime;

  private Label wlSpecifyFormat;
  private Button wSpecifyFormat;

  private Label wlDateTimeFormat;
  private CCombo wDateTimeFormat;

  private CCombo wSuccessCondition;

  private Label wlAddDateBeforeExtension;
  private Button wAddDateBeforeExtension;

  private Label wlNrErrorsLessThan;
  private TextVar wNrErrorsLessThan;

  private Label wlOverwriteFile;
  private Button wOverwriteFile;

  private Label wlCreateDestinationFolder;
  private Button wCreateDestinationFolder;

  private Label wlRemovedSourceFilename;
  private Button wRemovedSourceFilename;

  private Label wlAddDestinationFilename;
  private Button wAddDestinationFilename;

  public ActionCopyMoveResultFilenamesDialog( Shell parent, IAction action,
                                              WorkflowMeta workflowMeta ) {
    super( parent, workflowMeta );
    this.action = (ActionCopyMoveResultFilenames) action;

    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "ActionDeleteResultFilenames.Name.Default" ) );
    }
  }

  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, action );

    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "ActionCopyMoveResultFilenames.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Name line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText( BaseMessages.getString( PKG, "ActionCopyMoveResultFilenames.Name.Label" ) );
    props.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, -margin );
    fdlName.top = new FormAttachment( 0, margin );
    wlName.setLayoutData(fdlName);
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    FormData fdName = new FormData();
    fdName.left = new FormAttachment( middle, 0 );
    fdName.top = new FormAttachment( 0, margin );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData(fdName);

    // Copy or Move
    Label wlAction = new Label(shell, SWT.RIGHT);
    wlAction.setText( BaseMessages.getString( PKG, "ActionCopyMoveResultFilenames.Action.Label" ) );
    props.setLook(wlAction);
    FormData fdlAction = new FormData();
    fdlAction.left = new FormAttachment( 0, 0 );
    fdlAction.right = new FormAttachment( middle, -margin );
    fdlAction.top = new FormAttachment( wName, 2 * margin );
    wlAction.setLayoutData(fdlAction);
    wAction = new CCombo( shell, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wAction.add( BaseMessages.getString( PKG, "ActionCopyMoveResultFilenames.Copy.Label" ) );
    wAction.add( BaseMessages.getString( PKG, "ActionCopyMoveResultFilenames.Move.Label" ) );
    wAction.add( BaseMessages.getString( PKG, "ActionCopyMoveResultFilenames.Delete.Label" ) );
    wAction.select( 0 ); // +1: starts at -1

    props.setLook( wAction );
    FormData fdAction = new FormData();
    fdAction.left = new FormAttachment( middle, 0 );
    fdAction.top = new FormAttachment( wName, 2 * margin );
    fdAction.right = new FormAttachment( 100, 0 );
    wAction.setLayoutData(fdAction);
    wAction.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        enableAction();
        action.setChanged();
      }
    } );

    // Foldername line
    wlFoldername = new Label( shell, SWT.RIGHT );
    wlFoldername.setText( BaseMessages.getString( PKG, "ActionCopyMoveResultFilenames.Foldername.Label" ) );
    props.setLook( wlFoldername );
    FormData fdlFoldername = new FormData();
    fdlFoldername.left = new FormAttachment( 0, 0 );
    fdlFoldername.top = new FormAttachment( wAction, margin );
    fdlFoldername.right = new FormAttachment( middle, -margin );
    wlFoldername.setLayoutData(fdlFoldername);

    wbFoldername = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbFoldername );
    wbFoldername.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbFoldername = new FormData();
    fdbFoldername.right = new FormAttachment( 100, 0 );
    fdbFoldername.top = new FormAttachment( wAction, 0 );
    wbFoldername.setLayoutData(fdbFoldername);

    wFoldername = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFoldername );
    wFoldername.addModifyListener( lsMod );
    FormData fdFoldername = new FormData();
    fdFoldername.left = new FormAttachment( middle, 0 );
    fdFoldername.top = new FormAttachment( wAction, margin );
    fdFoldername.right = new FormAttachment( wbFoldername, -margin );
    wFoldername.setLayoutData(fdFoldername);

    // Whenever something changes, set the tooltip to the expanded version:
    wFoldername.addModifyListener( e -> wFoldername.setToolTipText( variables.resolve( wFoldername.getText() ) ) );

    wbFoldername.addListener( SWT.Selection, e -> BaseDialog.presentDirectoryDialog( shell, wFoldername, variables ) );

    // Create destination folder
    wlCreateDestinationFolder = new Label( shell, SWT.RIGHT );
    wlCreateDestinationFolder.setText( BaseMessages.getString(
      PKG, "ActionCopyMoveResultFilenames.CreateDestinationFolder.Label" ) );
    props.setLook( wlCreateDestinationFolder );
    FormData fdlCreateDestinationFolder = new FormData();
    fdlCreateDestinationFolder.left = new FormAttachment( 0, 0 );
    fdlCreateDestinationFolder.top = new FormAttachment( wFoldername, margin );
    fdlCreateDestinationFolder.right = new FormAttachment( middle, -margin );
    wlCreateDestinationFolder.setLayoutData(fdlCreateDestinationFolder);
    wCreateDestinationFolder = new Button( shell, SWT.CHECK );
    props.setLook( wCreateDestinationFolder );
    wCreateDestinationFolder.setToolTipText( BaseMessages.getString(
      PKG, "ActionCopyMoveResultFilenames.CreateDestinationFolder.Tooltip" ) );
    FormData fdCreateDestinationFolder = new FormData();
    fdCreateDestinationFolder.left = new FormAttachment( middle, 0 );
    fdCreateDestinationFolder.top = new FormAttachment( wFoldername, margin );
    fdCreateDestinationFolder.right = new FormAttachment( 100, 0 );
    wCreateDestinationFolder.setLayoutData(fdCreateDestinationFolder);
    wCreateDestinationFolder.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // Overwrite files
    wlOverwriteFile = new Label( shell, SWT.RIGHT );
    wlOverwriteFile.setText( BaseMessages.getString( PKG, "ActionCopyMoveResultFilenames.OverwriteFile.Label" ) );
    props.setLook( wlOverwriteFile );
    FormData fdlOverwriteFile = new FormData();
    fdlOverwriteFile.left = new FormAttachment( 0, 0 );
    fdlOverwriteFile.top = new FormAttachment( wCreateDestinationFolder, margin );
    fdlOverwriteFile.right = new FormAttachment( middle, -margin );
    wlOverwriteFile.setLayoutData(fdlOverwriteFile);
    wOverwriteFile = new Button( shell, SWT.CHECK );
    props.setLook( wOverwriteFile );
    wOverwriteFile.setToolTipText( BaseMessages.getString(
      PKG, "ActionCopyMoveResultFilenames.OverwriteFile.Tooltip" ) );
    FormData fdOverwriteFile = new FormData();
    fdOverwriteFile.left = new FormAttachment( middle, 0 );
    fdOverwriteFile.top = new FormAttachment( wCreateDestinationFolder, margin );
    fdOverwriteFile.right = new FormAttachment( 100, 0 );
    wOverwriteFile.setLayoutData(fdOverwriteFile);
    wOverwriteFile.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // Remove source filename from result filenames
    wlRemovedSourceFilename = new Label( shell, SWT.RIGHT );
    wlRemovedSourceFilename.setText( BaseMessages.getString(
      PKG, "ActionCopyMoveResultFilenames.RemovedSourceFilename.Label" ) );
    props.setLook( wlRemovedSourceFilename );
    FormData fdlRemovedSourceFilename = new FormData();
    fdlRemovedSourceFilename.left = new FormAttachment( 0, 0 );
    fdlRemovedSourceFilename.top = new FormAttachment( wOverwriteFile, margin );
    fdlRemovedSourceFilename.right = new FormAttachment( middle, -margin );
    wlRemovedSourceFilename.setLayoutData(fdlRemovedSourceFilename);
    wRemovedSourceFilename = new Button( shell, SWT.CHECK );
    props.setLook( wRemovedSourceFilename );
    wRemovedSourceFilename.setToolTipText( BaseMessages.getString(
      PKG, "ActionCopyMoveResultFilenames.RemovedSourceFilename.Tooltip" ) );
    FormData fdRemovedSourceFilename = new FormData();
    fdRemovedSourceFilename.left = new FormAttachment( middle, 0 );
    fdRemovedSourceFilename.top = new FormAttachment( wOverwriteFile, margin );
    fdRemovedSourceFilename.right = new FormAttachment( 100, 0 );
    wRemovedSourceFilename.setLayoutData(fdRemovedSourceFilename);
    wRemovedSourceFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // Add destination filename to result filenames
    wlAddDestinationFilename = new Label( shell, SWT.RIGHT );
    wlAddDestinationFilename.setText( BaseMessages.getString(
      PKG, "ActionCopyMoveResultFilenames.AddDestinationFilename.Label" ) );
    props.setLook( wlAddDestinationFilename );
    FormData fdlAddDestinationFilename = new FormData();
    fdlAddDestinationFilename.left = new FormAttachment( 0, 0 );
    fdlAddDestinationFilename.top = new FormAttachment( wRemovedSourceFilename, margin );
    fdlAddDestinationFilename.right = new FormAttachment( middle, -margin );
    wlAddDestinationFilename.setLayoutData(fdlAddDestinationFilename);
    wAddDestinationFilename = new Button( shell, SWT.CHECK );
    props.setLook( wAddDestinationFilename );
    wAddDestinationFilename.setToolTipText( BaseMessages.getString(
      PKG, "ActionCopyMoveResultFilenames.AddDestinationFilename.Tooltip" ) );
    FormData fdAddDestinationFilename = new FormData();
    fdAddDestinationFilename.left = new FormAttachment( middle, 0 );
    fdAddDestinationFilename.top = new FormAttachment( wRemovedSourceFilename, margin );
    fdAddDestinationFilename.right = new FormAttachment( 100, 0 );
    wAddDestinationFilename.setLayoutData(fdAddDestinationFilename);
    wAddDestinationFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // Create multi-part file?
    wlAddDate = new Label( shell, SWT.RIGHT );
    wlAddDate.setText( BaseMessages.getString( PKG, "ActionCopyMoveResultFilenames.AddDate.Label" ) );
    props.setLook( wlAddDate );
    FormData fdlAddDate = new FormData();
    fdlAddDate.left = new FormAttachment( 0, 0 );
    fdlAddDate.top = new FormAttachment( wAddDestinationFilename, margin );
    fdlAddDate.right = new FormAttachment( middle, -margin );
    wlAddDate.setLayoutData(fdlAddDate);
    wAddDate = new Button( shell, SWT.CHECK );
    props.setLook( wAddDate );
    wAddDate.setToolTipText( BaseMessages.getString( PKG, "ActionCopyMoveResultFilenames.AddDate.Tooltip" ) );
    FormData fdAddDate = new FormData();
    fdAddDate.left = new FormAttachment( middle, 0 );
    fdAddDate.top = new FormAttachment( wlAddDate, 0, SWT.CENTER );
    fdAddDate.right = new FormAttachment( 100, 0 );
    wAddDate.setLayoutData(fdAddDate);
    wAddDate.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
        setAddDateBeforeExtension();
      }
    } );
    // Create multi-part file?
    wlAddTime = new Label( shell, SWT.RIGHT );
    wlAddTime.setText( BaseMessages.getString( PKG, "ActionCopyMoveResultFilenames.AddTime.Label" ) );
    props.setLook( wlAddTime );
    FormData fdlAddTime = new FormData();
    fdlAddTime.left = new FormAttachment( 0, 0 );
    fdlAddTime.top = new FormAttachment( wAddDate, margin );
    fdlAddTime.right = new FormAttachment( middle, -margin );
    wlAddTime.setLayoutData(fdlAddTime);
    wAddTime = new Button( shell, SWT.CHECK );
    props.setLook( wAddTime );
    wAddTime.setToolTipText( BaseMessages.getString( PKG, "ActionCopyMoveResultFilenames.AddTime.Tooltip" ) );
    FormData fdAddTime = new FormData();
    fdAddTime.left = new FormAttachment( middle, 0 );
    fdAddTime.top = new FormAttachment( wlAddTime, 0, SWT.CENTER );
    fdAddTime.right = new FormAttachment( 100, 0 );
    wAddTime.setLayoutData(fdAddTime);
    wAddTime.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
        setAddDateBeforeExtension();
      }
    } );

    // Specify date time format?
    wlSpecifyFormat = new Label( shell, SWT.RIGHT );
    wlSpecifyFormat.setText( BaseMessages.getString( PKG, "ActionCopyMoveResultFilenames.SpecifyFormat.Label" ) );
    props.setLook( wlSpecifyFormat );
    FormData fdlSpecifyFormat = new FormData();
    fdlSpecifyFormat.left = new FormAttachment( 0, 0 );
    fdlSpecifyFormat.top = new FormAttachment( wAddTime, margin );
    fdlSpecifyFormat.right = new FormAttachment( middle, -margin );
    wlSpecifyFormat.setLayoutData(fdlSpecifyFormat);
    wSpecifyFormat = new Button( shell, SWT.CHECK );
    props.setLook( wSpecifyFormat );
    wSpecifyFormat.setToolTipText( BaseMessages.getString(
      PKG, "ActionCopyMoveResultFilenames.SpecifyFormat.Tooltip" ) );
    FormData fdSpecifyFormat = new FormData();
    fdSpecifyFormat.left = new FormAttachment( middle, 0 );
    fdSpecifyFormat.top = new FormAttachment( wAddTime, margin );
    fdSpecifyFormat.right = new FormAttachment( 100, 0 );
    wSpecifyFormat.setLayoutData(fdSpecifyFormat);
    wSpecifyFormat.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
        setDateTimeFormat();
        setAddDateBeforeExtension();
      }
    } );

    // Prepare a list of possible DateTimeFormats...
    String[] dats = Const.getDateFormats();

    // DateTimeFormat
    wlDateTimeFormat = new Label( shell, SWT.RIGHT );
    wlDateTimeFormat
      .setText( BaseMessages.getString( PKG, "ActionCopyMoveResultFilenames.DateTimeFormat.Label" ) );
    props.setLook( wlDateTimeFormat );
    FormData fdlDateTimeFormat = new FormData();
    fdlDateTimeFormat.left = new FormAttachment( 0, 0 );
    fdlDateTimeFormat.top = new FormAttachment( wSpecifyFormat, margin );
    fdlDateTimeFormat.right = new FormAttachment( middle, -margin );
    wlDateTimeFormat.setLayoutData(fdlDateTimeFormat);
    wDateTimeFormat = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    wDateTimeFormat.setEditable( true );
    props.setLook( wDateTimeFormat );
    wDateTimeFormat.addModifyListener( lsMod );
    FormData fdDateTimeFormat = new FormData();
    fdDateTimeFormat.left = new FormAttachment( middle, 0 );
    fdDateTimeFormat.top = new FormAttachment( wSpecifyFormat, margin );
    fdDateTimeFormat.right = new FormAttachment( 100, 0 );
    wDateTimeFormat.setLayoutData(fdDateTimeFormat);
    for (String dat : dats) {
      wDateTimeFormat.add(dat);
    }

    // Add Date before extension?
    wlAddDateBeforeExtension = new Label( shell, SWT.RIGHT );
    wlAddDateBeforeExtension.setText( BaseMessages.getString(
      PKG, "ActionCopyMoveResultFilenames.AddDateBeforeExtension.Label" ) );
    props.setLook( wlAddDateBeforeExtension );
    FormData fdlAddDateBeforeExtension = new FormData();
    fdlAddDateBeforeExtension.left = new FormAttachment( 0, 0 );
    fdlAddDateBeforeExtension.top = new FormAttachment( wDateTimeFormat, margin );
    fdlAddDateBeforeExtension.right = new FormAttachment( middle, -margin );
    wlAddDateBeforeExtension.setLayoutData(fdlAddDateBeforeExtension);
    wAddDateBeforeExtension = new Button( shell, SWT.CHECK );
    props.setLook( wAddDateBeforeExtension );
    wAddDateBeforeExtension.setToolTipText( BaseMessages.getString(
      PKG, "ActionCopyMoveResultFilenames.AddDateBeforeExtension.Tooltip" ) );
    FormData fdAddDateBeforeExtension = new FormData();
    fdAddDateBeforeExtension.left = new FormAttachment( middle, 0 );
    fdAddDateBeforeExtension.top = new FormAttachment( wDateTimeFormat, margin );
    fdAddDateBeforeExtension.right = new FormAttachment( 100, 0 );
    wAddDateBeforeExtension.setLayoutData(fdAddDateBeforeExtension);
    wAddDateBeforeExtension.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
        CheckLimit();
      }
    } );

    // LimitTo grouping?
    // ////////////////////////
    // START OF LimitTo GROUP
    //

    Group wLimitTo = new Group(shell, SWT.SHADOW_NONE);
    props.setLook(wLimitTo);
    wLimitTo.setText( BaseMessages.getString( PKG, "ActionCopyMoveResultFilenames.Group.LimitTo.Label" ) );

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;
    wLimitTo.setLayout( groupLayout );

    // Specify wildcard?
    Label wlSpecifyWildcard = new Label(wLimitTo, SWT.RIGHT);
    wlSpecifyWildcard.setText( BaseMessages.getString(
      PKG, "ActionCopyMoveResultFilenames.SpecifyWildcard.Label" ) );
    props.setLook(wlSpecifyWildcard);
    FormData fdlSpecifyWildcard = new FormData();
    fdlSpecifyWildcard.left = new FormAttachment( 0, 0 );
    fdlSpecifyWildcard.top = new FormAttachment( wAddDateBeforeExtension, margin );
    fdlSpecifyWildcard.right = new FormAttachment( middle, -margin );
    wlSpecifyWildcard.setLayoutData(fdlSpecifyWildcard);
    wSpecifyWildcard = new Button(wLimitTo, SWT.CHECK );
    props.setLook( wSpecifyWildcard );
    wSpecifyWildcard.setToolTipText( BaseMessages.getString(
      PKG, "ActionCopyMoveResultFilenames.SpecifyWildcard.Tooltip" ) );
    FormData fdSpecifyWildcard = new FormData();
    fdSpecifyWildcard.left = new FormAttachment( middle, 0 );
    fdSpecifyWildcard.top = new FormAttachment( wAddDateBeforeExtension, margin );
    fdSpecifyWildcard.right = new FormAttachment( 100, 0 );
    wSpecifyWildcard.setLayoutData(fdSpecifyWildcard);
    wSpecifyWildcard.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
        CheckLimit();
      }
    } );

    // Wildcard line
    wlWildcard = new Label(wLimitTo, SWT.RIGHT );
    wlWildcard.setText( BaseMessages.getString( PKG, "ActionCopyMoveResultFilenames.Wildcard.Label" ) );
    props.setLook( wlWildcard );
    FormData fdlWildcard = new FormData();
    fdlWildcard.left = new FormAttachment( 0, 0 );
    fdlWildcard.top = new FormAttachment( wSpecifyWildcard, margin );
    fdlWildcard.right = new FormAttachment( middle, -margin );
    wlWildcard.setLayoutData(fdlWildcard);
    wWildcard = new TextVar( variables, wLimitTo, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wWildcard.setToolTipText( BaseMessages.getString( PKG, "ActionCopyMoveResultFilenames.Wildcard.Tooltip" ) );
    props.setLook( wWildcard );
    wWildcard.addModifyListener( lsMod );
    FormData fdWildcard = new FormData();
    fdWildcard.left = new FormAttachment( middle, 0 );
    fdWildcard.top = new FormAttachment( wSpecifyWildcard, margin );
    fdWildcard.right = new FormAttachment( 100, -margin );
    wWildcard.setLayoutData(fdWildcard);

    // Whenever something changes, set the tooltip to the expanded version:
    wWildcard.addModifyListener( e -> wWildcard.setToolTipText( variables.resolve( wWildcard.getText() ) ) );

    // wWildcardExclude
    wlWildcardExclude = new Label(wLimitTo, SWT.RIGHT );
    wlWildcardExclude.setText( BaseMessages.getString(
      PKG, "ActionCopyMoveResultFilenames.WildcardExclude.Label" ) );
    props.setLook( wlWildcardExclude );
    FormData fdlWildcardExclude = new FormData();
    fdlWildcardExclude.left = new FormAttachment( 0, 0 );
    fdlWildcardExclude.top = new FormAttachment( wWildcard, margin );
    fdlWildcardExclude.right = new FormAttachment( middle, -margin );
    wlWildcardExclude.setLayoutData(fdlWildcardExclude);
    wWildcardExclude = new TextVar( variables, wLimitTo, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wWildcardExclude.setToolTipText( BaseMessages.getString(
      PKG, "ActionCopyMoveResultFilenames.WildcardExclude.Tooltip" ) );
    props.setLook( wWildcardExclude );
    wWildcardExclude.addModifyListener( lsMod );
    FormData fdWildcardExclude = new FormData();
    fdWildcardExclude.left = new FormAttachment( middle, 0 );
    fdWildcardExclude.top = new FormAttachment( wWildcard, margin );
    fdWildcardExclude.right = new FormAttachment( 100, -margin );
    wWildcardExclude.setLayoutData(fdWildcardExclude);

    // Whenever something changes, set the tooltip to the expanded version:
    wWildcardExclude.addModifyListener( e -> wWildcardExclude.setToolTipText( variables.resolve( wWildcardExclude.getText() ) ) );

    FormData fdLimitTo = new FormData();
    fdLimitTo.left = new FormAttachment( 0, margin );
    fdLimitTo.top = new FormAttachment( wAddDateBeforeExtension, margin );
    fdLimitTo.right = new FormAttachment( 100, -margin );
    wLimitTo.setLayoutData(fdLimitTo);

    // ///////////////////////////////////////////////////////////
    // / END OF LimitTo GROUP
    // ///////////////////////////////////////////////////////////

    // SuccessOngrouping?
    // ////////////////////////
    // START OF SUCCESS ON GROUP///
    // /
    Group wSuccessOn = new Group(shell, SWT.SHADOW_NONE);
    props.setLook(wSuccessOn);
    wSuccessOn.setText( BaseMessages.getString( PKG, "ActionCopyMoveResultFilenames.SuccessOn.Group.Label" ) );

    FormLayout successongroupLayout = new FormLayout();
    successongroupLayout.marginWidth = 10;
    successongroupLayout.marginHeight = 10;

    wSuccessOn.setLayout( successongroupLayout );

    // Success Condition
    Label wlSuccessCondition = new Label(wSuccessOn, SWT.RIGHT);
    wlSuccessCondition.setText( BaseMessages.getString(
      PKG, "ActionCopyMoveResultFilenames.SuccessCondition.Label" ) );
    props.setLook(wlSuccessCondition);
    FormData fdlSuccessCondition = new FormData();
    fdlSuccessCondition.left = new FormAttachment( 0, 0 );
    fdlSuccessCondition.right = new FormAttachment( middle, 0 );
    fdlSuccessCondition.top = new FormAttachment(wLimitTo, 2 * margin );
    wlSuccessCondition.setLayoutData(fdlSuccessCondition);
    wSuccessCondition = new CCombo(wSuccessOn, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wSuccessCondition.add( BaseMessages.getString(
      PKG, "ActionCopyMoveResultFilenames.SuccessWhenAllWorksFine.Label" ) );
    wSuccessCondition
      .add( BaseMessages.getString( PKG, "ActionCopyMoveResultFilenames.SuccessWhenAtLeat.Label" ) );
    wSuccessCondition.add( BaseMessages.getString(
      PKG, "ActionCopyMoveResultFilenames.SuccessWhenErrorsLessThan.Label" ) );
    wSuccessCondition.select( 0 ); // +1: starts at -1

    props.setLook( wSuccessCondition );
    FormData fdSuccessCondition = new FormData();
    fdSuccessCondition.left = new FormAttachment( middle, 0 );
    fdSuccessCondition.top = new FormAttachment(wLimitTo, 2 * margin );
    fdSuccessCondition.right = new FormAttachment( 100, 0 );
    wSuccessCondition.setLayoutData(fdSuccessCondition);
    wSuccessCondition.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        activeSuccessCondition();

      }
    } );

    // Success when number of errors less than
    wlNrErrorsLessThan = new Label(wSuccessOn, SWT.RIGHT );
    wlNrErrorsLessThan.setText( BaseMessages.getString(
      PKG, "ActionCopyMoveResultFilenames.NrErrorsLessThan.Label" ) );
    props.setLook( wlNrErrorsLessThan );
    FormData fdlNrErrorsLessThan = new FormData();
    fdlNrErrorsLessThan.left = new FormAttachment( 0, 0 );
    fdlNrErrorsLessThan.top = new FormAttachment( wSuccessCondition, margin );
    fdlNrErrorsLessThan.right = new FormAttachment( middle, -margin );
    wlNrErrorsLessThan.setLayoutData(fdlNrErrorsLessThan);

    wNrErrorsLessThan =
      new TextVar( variables, wSuccessOn, SWT.SINGLE | SWT.LEFT | SWT.BORDER, BaseMessages.getString(
        PKG, "ActionCopyMoveResultFilenames.NrErrorsLessThan.Tooltip" ) );
    props.setLook( wNrErrorsLessThan );
    wNrErrorsLessThan.addModifyListener( lsMod );
    FormData fdNrErrorsLessThan = new FormData();
    fdNrErrorsLessThan.left = new FormAttachment( middle, 0 );
    fdNrErrorsLessThan.top = new FormAttachment( wSuccessCondition, margin );
    fdNrErrorsLessThan.right = new FormAttachment( 100, -margin );
    wNrErrorsLessThan.setLayoutData(fdNrErrorsLessThan);

    FormData fdSuccessOn = new FormData();
    fdSuccessOn.left = new FormAttachment( 0, margin );
    fdSuccessOn.top = new FormAttachment(wLimitTo, margin );
    fdSuccessOn.right = new FormAttachment( 100, -margin );
    wSuccessOn.setLayoutData(fdSuccessOn);
    // ///////////////////////////////////////////////////////////
    // / END OF Success ON GROUP
    // ///////////////////////////////////////////////////////////

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] {wOk, wCancel}, margin, wSuccessOn);

    // Add listeners
    Listener lsCancel = e -> cancel();
    Listener lsOk = e -> ok();

    wCancel.addListener( SWT.Selection, lsCancel);
    wOk.addListener( SWT.Selection, lsOk);

    SelectionAdapter lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected(SelectionEvent e) {
        ok();
      }
    };

    wName.addSelectionListener(lsDef);
    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    CheckLimit();
    setDateTimeFormat();
    activeSuccessCondition();
    setAddDateBeforeExtension();
    enableAction();
    BaseTransformDialog.setSize( shell );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return action;
  }

  private void setAddDateBeforeExtension() {
    wlAddDateBeforeExtension.setEnabled( wAddDate.getSelection()
      || wAddTime.getSelection() || wSpecifyFormat.getSelection() );
    wAddDateBeforeExtension.setEnabled( wAddDate.getSelection()
      || wAddTime.getSelection() || wSpecifyFormat.getSelection() );
    if ( !wAddDate.getSelection() && !wAddTime.getSelection() && !wSpecifyFormat.getSelection() ) {
      wAddDateBeforeExtension.setSelection( false );
    }
  }

  private void activeSuccessCondition() {
    wlNrErrorsLessThan.setEnabled( wSuccessCondition.getSelectionIndex() != 0 );
    wNrErrorsLessThan.setEnabled( wSuccessCondition.getSelectionIndex() != 0 );
  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  private void CheckLimit() {
    wlWildcard.setEnabled( wSpecifyWildcard.getSelection() );
    wWildcard.setEnabled( wSpecifyWildcard.getSelection() );
    wlWildcardExclude.setEnabled( wSpecifyWildcard.getSelection() );
    wWildcardExclude.setEnabled( wSpecifyWildcard.getSelection() );
  }

  private void setDateTimeFormat() {
    if ( wSpecifyFormat.getSelection() ) {
      wAddDate.setSelection( false );
      wAddTime.setSelection( false );
    }

    wDateTimeFormat.setEnabled( wSpecifyFormat.getSelection() );
    wlDateTimeFormat.setEnabled( wSpecifyFormat.getSelection() );
    wAddDate.setEnabled( !wSpecifyFormat.getSelection() );
    wlAddDate.setEnabled( !wSpecifyFormat.getSelection() );
    wAddTime.setEnabled( !wSpecifyFormat.getSelection() );
    wlAddTime.setEnabled( !wSpecifyFormat.getSelection() );

  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( action.getName() != null ) {
      wName.setText( action.getName() );
    }
    wSpecifyWildcard.setSelection( action.isSpecifyWildcard() );
    if ( action.getWildcard() != null ) {
      wWildcard.setText( action.getWildcard() );
    }
    if ( action.getWildcardExclude() != null ) {
      wWildcardExclude.setText( action.getWildcardExclude() );
    }

    if ( action.getDestinationFolder() != null ) {
      wFoldername.setText( action.getDestinationFolder() );
    }

    if ( action.getNrErrorsLessThan() != null ) {
      wNrErrorsLessThan.setText( action.getNrErrorsLessThan() );
    } else {
      wNrErrorsLessThan.setText( "10" );
    }

    if ( action.getSuccessCondition() != null ) {
      if ( action.getSuccessCondition().equals( action.SUCCESS_IF_AT_LEAST_X_FILES_UN_ZIPPED ) ) {
        wSuccessCondition.select( 1 );
      } else if ( action.getSuccessCondition().equals( action.SUCCESS_IF_ERRORS_LESS ) ) {
        wSuccessCondition.select( 2 );
      } else {
        wSuccessCondition.select( 0 );
      }
    } else {
      wSuccessCondition.select( 0 );
    }

    if ( action.getAction() != null ) {
      if ( action.getAction().equals( "move" ) ) {
        wAction.select( 1 );
      } else if ( action.getAction().equals( "delete" ) ) {
        wAction.select( 2 );
      } else {
        wAction.select( 0 );
      }
    } else {
      wAction.select( 0 );
    }

    if ( action.getDateTimeFormat() != null ) {
      wDateTimeFormat.setText( action.getDateTimeFormat() );
    }

    wAddDate.setSelection( action.isAddDate() );
    wAddTime.setSelection( action.isAddTime() );
    wSpecifyFormat.setSelection( action.isSpecifyFormat() );
    wAddDateBeforeExtension.setSelection( action.isAddDateBeforeExtension() );
    wOverwriteFile.setSelection( action.isOverwriteFile() );
    wCreateDestinationFolder.setSelection( action.isCreateDestinationFolder() );
    wRemovedSourceFilename.setSelection( action.isRemovedSourceFilename() );
    wAddDestinationFilename.setSelection( action.isAddDestinationFilename() );

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    action.setChanged( changed );
    action = null;
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wName.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "System.TransformActionNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "System.ActionNameMissing.Msg" ) );
      mb.open();
      return;
    }
    action.setName( wName.getText() );
    action.setSpecifyWildcard( wSpecifyWildcard.getSelection() );
    action.setWildcard( wWildcard.getText() );
    action.setWildcardExclude( wWildcardExclude.getText() );

    action.setDestinationFolder( wFoldername.getText() );
    action.setNrErrorsLessThan( wNrErrorsLessThan.getText() );

    if ( wSuccessCondition.getSelectionIndex() == 1 ) {
      action.setSuccessCondition( action.SUCCESS_IF_AT_LEAST_X_FILES_UN_ZIPPED );
    } else if ( wSuccessCondition.getSelectionIndex() == 2 ) {
      action.setSuccessCondition( action.SUCCESS_IF_ERRORS_LESS );
    } else {
      action.setSuccessCondition( action.SUCCESS_IF_NO_ERRORS );
    }

    if ( wAction.getSelectionIndex() == 1 ) {
      action.setAction( "move" );
    } else if ( wAction.getSelectionIndex() == 2 ) {
      action.setAction( "delete" );
    } else {
      action.setAction( "copy" );
    }

    action.setAddDate( wAddDate.getSelection() );
    action.setAddTime( wAddTime.getSelection() );
    action.setSpecifyFormat( wSpecifyFormat.getSelection() );
    action.setDateTimeFormat( wDateTimeFormat.getText() );
    action.setAddDateBeforeExtension( wAddDateBeforeExtension.getSelection() );
    action.setOverwriteFile( wOverwriteFile.getSelection() );

    action.setCreateDestinationFolder( wCreateDestinationFolder.getSelection() );
    action.setRemovedSourceFilename( wRemovedSourceFilename.getSelection() );
    action.setAddDestinationFilename( wAddDestinationFilename.getSelection() );

    dispose();
  }

  private void enableAction() {
    boolean copyOrMove = wAction.getSelectionIndex() != 2;

    wlCreateDestinationFolder.setEnabled( copyOrMove );
    wCreateDestinationFolder.setEnabled( copyOrMove );
    wlOverwriteFile.setEnabled( copyOrMove );
    wOverwriteFile.setEnabled( copyOrMove );
    wlRemovedSourceFilename.setEnabled( copyOrMove );
    wRemovedSourceFilename.setEnabled( copyOrMove );
    wlAddDestinationFilename.setEnabled( copyOrMove );
    wAddDestinationFilename.setEnabled( copyOrMove );
    wlAddDate.setEnabled( copyOrMove );
    wAddDate.setEnabled( copyOrMove );
    wlAddTime.setEnabled( copyOrMove );
    wAddTime.setEnabled( copyOrMove );
    wlSpecifyFormat.setEnabled( copyOrMove );
    wSpecifyFormat.setEnabled( copyOrMove );
    wlDateTimeFormat.setEnabled( copyOrMove );
    wDateTimeFormat.setEnabled( copyOrMove );
    wAddDateBeforeExtension.setEnabled( copyOrMove );
    wlAddDateBeforeExtension.setEnabled( copyOrMove );
    wlFoldername.setEnabled( copyOrMove );
    wFoldername.setEnabled( copyOrMove );
    wbFoldername.setEnabled( copyOrMove );
  }
}
