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

package org.apache.hop.workflow.actions.xml.xsdvalidator;

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.PluginDialog;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
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
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;


/**
 * This dialog allows you to edit the XSD Validator job entry settings.
 * 
 * @author Samatar Hassan
 * @since 30-04-2007
 */
@PluginDialog(
        id = "XSD_VALIDATOR",
        image = "org/apache/hop/workflow/actions/xml/XSD.svg",
        pluginType = PluginDialog.PluginType.ACTION,
        documentationUrl = "https://www.project-hop.org/manual/latest/plugins/actions/xsdvalidator.html"
)
public class XsdValidatorDialog extends ActionDialog implements IActionDialog {
  private static Class<?> PKG = XsdValidator.class; // for i18n purposes, needed by Translator2!!

  private static final String[] FILETYPES_XML = new String[] {
    BaseMessages.getString( PKG, "JobEntryXSDValidator.Filetype.Xml" ),
    BaseMessages.getString( PKG, "JobEntryXSDValidator.Filetype.All" ) };

  private static final String[] FILETYPES_XSD = new String[] {
    BaseMessages.getString( PKG, "JobEntryXSDValidator.Filetype.Xsd" ),
    BaseMessages.getString( PKG, "JobEntryXSDValidator.Filetype.All" ) };

  private Label wlName;
  private Text wName;
  private FormData fdlName, fdName;

  private Label wlAllowExternalEntities;
  private Button wAllowExternalEntities;
  private FormData fdlAllowExternalEntities, fdAllowExternalEntities;

  private Label wlxmlFilename;
  private Button wbxmlFilename;
  private TextVar wxmlFilename;
  private FormData fdlxmlFilename, fdbxmlFilename, fdxmlFilename;

  private Label wlxsdFilename;
  private Button wbxsdFilename;
  private TextVar wxsdFilename;
  private FormData fdlxsdFilename, fdbxsdFilename, fdxsdFilename;

  private Button wOK, wCancel;
  private Listener lsOK, lsCancel;

  private XsdValidator jobEntry;
  private Shell shell;

  private SelectionAdapter lsDef;

  private boolean changed;

  public XsdValidatorDialog(Shell parent, IAction jobEntryInt, WorkflowMeta workflowMeta ) {
    super( parent, jobEntryInt, workflowMeta );
    jobEntry = (XsdValidator) jobEntryInt;
    if ( this.jobEntry.getName() == null ) {
      this.jobEntry.setName( BaseMessages.getString( PKG, "JobEntryXSDValidator.Name.Default" ) );
    }
  }

  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE  );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, jobEntry );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        jobEntry.setChanged();
      }
    };
    changed = jobEntry.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "JobEntryXSDValidator.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Name line
    wlName = new Label( shell, SWT.RIGHT );
    wlName.setText( BaseMessages.getString( PKG, "JobEntryXSDValidator.Name.Label" ) );
    props.setLook( wlName );
    fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, -margin );
    fdlName.top = new FormAttachment( 0, margin );
    wlName.setLayoutData( fdlName );
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    fdName = new FormData();
    fdName.left = new FormAttachment( middle, 0 );
    fdName.top = new FormAttachment( 0, margin );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData( fdName );

    // Enable/Disable external entity for XSD validation.
    wlAllowExternalEntities = new Label( shell, SWT.RIGHT );
    wlAllowExternalEntities.setText( BaseMessages.getString( PKG, "JobEntryXSDValidator.AllowExternalEntities.Label" ) );
    props.setLook( wlAllowExternalEntities );
    fdlAllowExternalEntities = new FormData();
    fdlAllowExternalEntities.left = new FormAttachment( 0, 0 );
    fdlAllowExternalEntities.right = new FormAttachment( middle, -margin );
    fdlAllowExternalEntities.top = new FormAttachment( wName, margin );
    wlAllowExternalEntities.setLayoutData( fdlAllowExternalEntities );
    wAllowExternalEntities = new Button( shell, SWT.CHECK );
    props.setLook( wAllowExternalEntities );
    fdAllowExternalEntities = new FormData();
    fdAllowExternalEntities.left = new FormAttachment( middle, 0 );
    fdAllowExternalEntities.top = new FormAttachment( wName, margin );
    fdAllowExternalEntities.right = new FormAttachment( 100, 0 );
    wAllowExternalEntities.setLayoutData( fdAllowExternalEntities );

    wAllowExternalEntities.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        jobEntry.setChanged();
      }
    } );

    // Filename 1 line
    wlxmlFilename = new Label( shell, SWT.RIGHT );
    wlxmlFilename.setText( BaseMessages.getString( PKG, "JobEntryXSDValidator.xmlFilename.Label" ) );
    props.setLook( wlxmlFilename );
    fdlxmlFilename = new FormData();
    fdlxmlFilename.left = new FormAttachment( 0, 0 );
    fdlxmlFilename.top = new FormAttachment( wAllowExternalEntities, margin );
    fdlxmlFilename.right = new FormAttachment( middle, -margin );
    wlxmlFilename.setLayoutData( fdlxmlFilename );
    wbxmlFilename = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbxmlFilename );
    wbxmlFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fdbxmlFilename = new FormData();
    fdbxmlFilename.right = new FormAttachment( 100, 0 );
    fdbxmlFilename.top = new FormAttachment( wAllowExternalEntities, 0 );
    wbxmlFilename.setLayoutData( fdbxmlFilename );
    wxmlFilename = new TextVar( workflowMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wxmlFilename );
    wxmlFilename.addModifyListener( lsMod );
    fdxmlFilename = new FormData();
    fdxmlFilename.left = new FormAttachment( middle, 0 );
    fdxmlFilename.top = new FormAttachment( wAllowExternalEntities, margin );
    fdxmlFilename.right = new FormAttachment( wbxmlFilename, -margin );
    wxmlFilename.setLayoutData( fdxmlFilename );

    // Whenever something changes, set the tooltip to the expanded version:
    wxmlFilename.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        wxmlFilename.setToolTipText( workflowMeta.environmentSubstitute( wxmlFilename.getText() ) );
      }
    } );

    wbxmlFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        FileDialog dialog = new FileDialog( shell, SWT.OPEN );
        dialog.setFilterExtensions( new String[] { "*.xml;*.XML", "*" } );
        if ( wxmlFilename.getText() != null ) {
          dialog.setFileName( workflowMeta.environmentSubstitute( wxmlFilename.getText() ) );
        }
        dialog.setFilterNames( FILETYPES_XML );
        if ( dialog.open() != null ) {
          wxmlFilename.setText( dialog.getFilterPath() + Const.FILE_SEPARATOR + dialog.getFileName() );
        }
      }
    } );

    // Filename 2 line
    wlxsdFilename = new Label( shell, SWT.RIGHT );
    wlxsdFilename.setText( BaseMessages.getString( PKG, "JobEntryXSDValidator.xsdFilename.Label" ) );
    props.setLook( wlxsdFilename );
    fdlxsdFilename = new FormData();
    fdlxsdFilename.left = new FormAttachment( 0, 0 );
    fdlxsdFilename.top = new FormAttachment( wxmlFilename, margin );
    fdlxsdFilename.right = new FormAttachment( middle, -margin );
    wlxsdFilename.setLayoutData( fdlxsdFilename );
    wbxsdFilename = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbxsdFilename );
    wbxsdFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fdbxsdFilename = new FormData();
    fdbxsdFilename.right = new FormAttachment( 100, 0 );
    fdbxsdFilename.top = new FormAttachment( wxmlFilename, 0 );
    wbxsdFilename.setLayoutData( fdbxsdFilename );
    wxsdFilename = new TextVar( workflowMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wxsdFilename );
    wxsdFilename.addModifyListener( lsMod );
    fdxsdFilename = new FormData();
    fdxsdFilename.left = new FormAttachment( middle, 0 );
    fdxsdFilename.top = new FormAttachment( wxmlFilename, margin );
    fdxsdFilename.right = new FormAttachment( wbxsdFilename, -margin );
    wxsdFilename.setLayoutData( fdxsdFilename );

    // Whenever something changes, set the tooltip to the expanded version:
    wxsdFilename.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        wxsdFilename.setToolTipText( workflowMeta.environmentSubstitute( wxsdFilename.getText() ) );
      }
    } );

    wbxsdFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        FileDialog dialog = new FileDialog( shell, SWT.OPEN );
        dialog.setFilterExtensions( new String[] { "*.xsd;*.XSD", "*" } );
        if ( wxsdFilename.getText() != null ) {
          dialog.setFileName( workflowMeta.environmentSubstitute( wxsdFilename.getText() ) );
        }
        dialog.setFilterNames( FILETYPES_XSD );
        if ( dialog.open() != null ) {
          wxsdFilename.setText( dialog.getFilterPath() + Const.FILE_SEPARATOR + dialog.getFileName() );
        }
      }
    } );

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOK, wCancel }, margin, wxsdFilename );

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

    wName.addSelectionListener( lsDef );
    wAllowExternalEntities.addSelectionListener( lsDef );
    wxmlFilename.addSelectionListener( lsDef );
    wxsdFilename.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseTransformDialog.setSize( shell );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return jobEntry;
  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wName.setText( Const.nullToEmpty( jobEntry.getName() ) );
    wAllowExternalEntities.setSelection( jobEntry.isAllowExternalEntities() );
    wxmlFilename.setText( Const.nullToEmpty( jobEntry.getxmlFilename() ) );
    wxsdFilename.setText( Const.nullToEmpty( jobEntry.getxsdFilename() ) );

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    jobEntry.setChanged( changed );
    jobEntry = null;
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wName.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "System.StepJobEntryNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "System.JobEntryNameMissing.Msg" ) );
      mb.open();
      return;
    }
    jobEntry.setName( wName.getText() );
    jobEntry.setAllowExternalEntities( wAllowExternalEntities.getSelection() );
    jobEntry.setxmlFilename( wxmlFilename.getText() );
    jobEntry.setxsdFilename( wxsdFilename.getText() );

    dispose();
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }
}
