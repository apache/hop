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

package org.apache.hop.pipeline.transforms.filelocked;

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.PluginDialog;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusListener;
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
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

@PluginDialog(
        id = "FileLocked",
        image = "filelocked.svg",
        pluginType = PluginDialog.PluginType.TRANSFORM,
        documentationUrl = "http://www.project-hop.org/manual/latest/plugins/transforms/filelocked.html"
)
public class FileLockedDialog extends BaseTransformDialog implements ITransformDialog {
  private static Class<?> PKG = FileLockedMeta.class; // for i18n purposes, needed by Translator!!

  private Label wlFileName;
  private CCombo wFileName;
  private FormData fdlFileName, fdfileName;

  private Label wlResult;
  private TextVar wResult;
  private FormData fdlResult, fdResult;

  private Button wAddResult;
  private FormData fdAddResult, fdlAddResult;
  private Label wlAddResult;

  private FileLockedMeta input;

  private boolean gotPreviousFields = false;

  public FileLockedDialog( Shell parent, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (FileLockedMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        input.setChanged();
      }
    };

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "FileLockedDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "FileLockedDialog.TransformName.Label" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.right = new FormAttachment( middle, -margin );
    fdlTransformName.top = new FormAttachment( 0, margin );
    wlTransformName.setLayoutData( fdlTransformName );
    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformName.setText( transformName );
    props.setLook( wTransformName );
    wTransformName.addModifyListener( lsMod );
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment( middle, 0 );
    fdTransformName.top = new FormAttachment( 0, margin );
    fdTransformName.right = new FormAttachment( 100, 0 );
    wTransformName.setLayoutData( fdTransformName );

    // filename field
    wlFileName = new Label( shell, SWT.RIGHT );
    wlFileName.setText( BaseMessages.getString( PKG, "FileLockedDialog.FileName.Label" ) );
    props.setLook( wlFileName );
    fdlFileName = new FormData();
    fdlFileName.left = new FormAttachment( 0, 0 );
    fdlFileName.right = new FormAttachment( middle, -margin );
    fdlFileName.top = new FormAttachment( wTransformName, margin );
    wlFileName.setLayoutData( fdlFileName );

    wFileName = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    wFileName.setEditable( true );
    props.setLook( wFileName );
    wFileName.addModifyListener( lsMod );
    fdfileName = new FormData();
    fdfileName.left = new FormAttachment( middle, 0 );
    fdfileName.top = new FormAttachment( wTransformName, margin );
    fdfileName.right = new FormAttachment( 100, -margin );
    wFileName.setLayoutData( fdfileName );
    wFileName.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        get();
      }
    } );

    // Result fieldname ...
    wlResult = new Label( shell, SWT.RIGHT );
    wlResult.setText( BaseMessages.getString( PKG, "FileLockedDialog.ResultField.Label" ) );
    props.setLook( wlResult );
    fdlResult = new FormData();
    fdlResult.left = new FormAttachment( 0, 0 );
    fdlResult.right = new FormAttachment( middle, -margin );
    fdlResult.top = new FormAttachment( wFileName, margin * 2 );
    wlResult.setLayoutData( fdlResult );

    wResult = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wResult.setToolTipText( BaseMessages.getString( PKG, "FileLockedDialog.ResultField.Tooltip" ) );
    props.setLook( wResult );
    wResult.addModifyListener( lsMod );
    fdResult = new FormData();
    fdResult.left = new FormAttachment( middle, 0 );
    fdResult.top = new FormAttachment( wFileName, margin * 2 );
    fdResult.right = new FormAttachment( 100, 0 );
    wResult.setLayoutData( fdResult );

    // Add filename to result filenames?
    wlAddResult = new Label( shell, SWT.RIGHT );
    wlAddResult.setText( BaseMessages.getString( PKG, "FileLockedDialog.AddResult.Label" ) );
    props.setLook( wlAddResult );
    fdlAddResult = new FormData();
    fdlAddResult.left = new FormAttachment( 0, 0 );
    fdlAddResult.top = new FormAttachment( wResult, 2 * margin );
    fdlAddResult.right = new FormAttachment( middle, -margin );
    wlAddResult.setLayoutData( fdlAddResult );
    wAddResult = new Button( shell, SWT.CHECK );
    props.setLook( wAddResult );
    wAddResult.setToolTipText( BaseMessages.getString( PKG, "FileLockedDialog.AddResult.Tooltip" ) );
    fdAddResult = new FormData();
    fdAddResult.left = new FormAttachment( middle, 0 );
    fdAddResult.top = new FormAttachment( wResult, 2 * margin );
    wAddResult.setLayoutData( fdAddResult );

    // THE BUTTONS
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel }, margin, null );

    // Add listeners
    lsOk = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    wOk.addListener( SWT.Selection, lsOk );
    wCancel.addListener( SWT.Selection, lsCancel );

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
    setSize();

    getData();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "FileLockedDialog.Log.GettingKeyInfo" ) );
    }

    if ( input.getDynamicFilenameField() != null ) {
      wFileName.setText( input.getDynamicFilenameField() );
    }
    if ( input.getResultFieldName() != null ) {
      wResult.setText( input.getResultFieldName() );
    }
    wAddResult.setSelection( input.addResultFilenames() );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }
    input.setDynamicFilenameField( wFileName.getText() );
    input.setResultFieldName( wResult.getText() );
    input.setaddResultFilenames( wAddResult.getSelection() );
    transformName = wTransformName.getText(); // return value

    dispose();
  }

  private void get() {
    if ( !gotPreviousFields ) {
      try {
        String filefield = wFileName.getText();
        wFileName.removeAll();
        IRowMeta r = pipelineMeta.getPrevTransformFields( transformName );
        if ( r != null ) {
          wFileName.setItems( r.getFieldNames() );
        }
        if ( filefield != null ) {
          wFileName.setText( filefield );
        }
      } catch ( HopException ke ) {
        new ErrorDialog(
          shell, BaseMessages.getString( PKG, "FileLockedDialog.FailedToGetFields.DialogTitle" ), BaseMessages
          .getString( PKG, "FileLockedDialog.FailedToGetFields.DialogMessage" ), ke );
      }
      gotPreviousFields = true;
    }
  }
}
