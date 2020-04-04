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

package org.apache.hop.ui.pipeline.transforms.transformmeta;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transforms.transformmeta.TransformMetastructureMeta;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
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
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class TransformMetastructureDialog extends BaseTransformDialog implements ITransformDialog {
  private static Class<?> PKG = TransformMetastructureMeta.class; // for i18n purposes, needed by Translator!!

  private TransformMetastructureMeta input;

  private Label wlOutputRowcount;
  private Button wOutputRowcount;
  private FormData fdlOutputRowcount, fdOutputRowcount;

  private Label wlRowcountField;
  private TextVar wRowcountField;
  private FormData fdlRowcountField, fdRowcountField;

  public TransformMetastructureDialog( Shell parent, Object in, PipelineMeta tr, String sname ) {
    super( parent, (BaseTransformMeta) in, tr, sname );
    input = (TransformMetastructureMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
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
    shell.setText( BaseMessages.getString( PKG, "TransformMetastructureDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "TransformMetastructureDialog.TransformName.Label" ) );
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

    // Rowcout Output
    wlOutputRowcount = new Label( shell, SWT.RIGHT );
    wlOutputRowcount.setText( BaseMessages.getString( PKG, "TransformMetastructureDialog.outputRowcount.Label" ) );
    props.setLook( wlOutputRowcount );
    fdlOutputRowcount = new FormData();
    fdlOutputRowcount.left = new FormAttachment( 0, 0 );
    fdlOutputRowcount.top = new FormAttachment( wTransformName, margin );
    fdlOutputRowcount.right = new FormAttachment( middle, -margin );
    wlOutputRowcount.setLayoutData( fdlOutputRowcount );
    wOutputRowcount = new Button( shell, SWT.CHECK );
    props.setLook( wOutputRowcount );
    fdOutputRowcount = new FormData();
    fdOutputRowcount.left = new FormAttachment( middle, 0 );
    fdOutputRowcount.top = new FormAttachment( wTransformName, margin );
    fdOutputRowcount.right = new FormAttachment( 100, 0 );
    wOutputRowcount.setLayoutData( fdOutputRowcount );
    wOutputRowcount.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();

        if ( wOutputRowcount.getSelection() ) {
          wRowcountField.setEnabled( true );
        } else {
          wRowcountField.setEnabled( false );
        }
      }
    } );

    // Rowcout Field
    wlRowcountField = new Label( shell, SWT.RIGHT );
    wlRowcountField.setText( BaseMessages.getString( PKG, "TransformMetastructureDialog.RowcountField.Label" ) );
    props.setLook( wlRowcountField );
    fdlRowcountField = new FormData();
    fdlRowcountField.left = new FormAttachment( 0, 0 );
    fdlRowcountField.right = new FormAttachment( middle, -margin );
    fdlRowcountField.top = new FormAttachment( wOutputRowcount, margin );
    wlRowcountField.setLayoutData( fdlRowcountField );

    wRowcountField = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wRowcountField );
    wRowcountField.addModifyListener( lsMod );
    fdRowcountField = new FormData();
    fdRowcountField.left = new FormAttachment( middle, 0 );
    fdRowcountField.top = new FormAttachment( wOutputRowcount, margin );
    fdRowcountField.right = new FormAttachment( 100, -margin );
    wRowcountField.setLayoutData( fdRowcountField );
    wRowcountField.setEnabled( false );

    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel }, margin, wRowcountField );

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
    if ( input.getRowcountField() != null ) {
      wRowcountField.setText( input.getRowcountField() );
    }

    if ( input.isOutputRowcount() ) {
      wRowcountField.setEnabled( true );
    }

    wOutputRowcount.setSelection( input.isOutputRowcount() );

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

    transformName = wTransformName.getText(); // return value

    getInfo( input );

    dispose();
  }

  private void getInfo( TransformMetastructureMeta tfoi ) {
    tfoi.setOutputRowcount( wOutputRowcount.getSelection() );
    tfoi.setRowcountField( wRowcountField.getText() );
  }
}
