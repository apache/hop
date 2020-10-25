/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.pipeline.transforms.socketwriter;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transforms.socketwriter.SocketWriterMeta;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ComponentSelectionListener;
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

public class SocketWriterDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = SocketWriterMeta.class; // for i18n purposes, needed by Translator!!

  private SocketWriterMeta input;
  private TextVar wPort;
  private TextVar wBufferSize;
  private TextVar wFlushInterval;
  private Button wCompressed;

  public SocketWriterDialog( Shell parent, Object in, PipelineMeta tr, String sname ) {
    super( parent, (BaseTransformMeta) in, tr, sname );
    input = (SocketWriterMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "SocketWriterDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "SocketWriterDialog.TransformName.Label" ) );
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

    // Port line
    Label wlPort = new Label( shell, SWT.RIGHT );
    wlPort.setText( BaseMessages.getString( PKG, "SocketWriterDialog.Port.Label" ) );
    props.setLook( wlPort );
    FormData fdlPort = new FormData();
    fdlPort.left = new FormAttachment( 0, 0 );
    fdlPort.right = new FormAttachment( middle, -margin );
    fdlPort.top = new FormAttachment( wTransformName, margin );
    wlPort.setLayoutData( fdlPort );
    wPort = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wPort.setText( transformName );
    props.setLook( wPort );
    wPort.addModifyListener( lsMod );
    FormData fdPort = new FormData();
    fdPort.left = new FormAttachment( middle, 0 );
    fdPort.top = new FormAttachment( wTransformName, margin );
    fdPort.right = new FormAttachment( 100, 0 );
    wPort.setLayoutData( fdPort );

    // BufferSize line
    Label wlBufferSize = new Label( shell, SWT.RIGHT );
    wlBufferSize.setText( BaseMessages.getString( PKG, "SocketWriterDialog.BufferSize.Label" ) );
    props.setLook( wlBufferSize );
    FormData fdlBufferSize = new FormData();
    fdlBufferSize.left = new FormAttachment( 0, 0 );
    fdlBufferSize.right = new FormAttachment( middle, -margin );
    fdlBufferSize.top = new FormAttachment( wPort, margin );
    wlBufferSize.setLayoutData( fdlBufferSize );
    wBufferSize = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wBufferSize.setText( transformName );
    props.setLook( wBufferSize );
    wBufferSize.addModifyListener( lsMod );
    FormData fdBufferSize = new FormData();
    fdBufferSize.left = new FormAttachment( middle, 0 );
    fdBufferSize.top = new FormAttachment( wPort, margin );
    fdBufferSize.right = new FormAttachment( 100, 0 );
    wBufferSize.setLayoutData( fdBufferSize );

    // FlushInterval line
    Label wlFlushInterval = new Label( shell, SWT.RIGHT );
    wlFlushInterval.setText( BaseMessages.getString( PKG, "SocketWriterDialog.FlushInterval.Label" ) );
    props.setLook( wlFlushInterval );
    FormData fdlFlushInterval = new FormData();
    fdlFlushInterval.left = new FormAttachment( 0, 0 );
    fdlFlushInterval.right = new FormAttachment( middle, -margin );
    fdlFlushInterval.top = new FormAttachment( wBufferSize, margin );
    wlFlushInterval.setLayoutData( fdlFlushInterval );
    wFlushInterval = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wFlushInterval.setText( transformName );
    props.setLook( wFlushInterval );
    wFlushInterval.addModifyListener( lsMod );
    FormData fdFlushInterval = new FormData();
    fdFlushInterval.left = new FormAttachment( middle, 0 );
    fdFlushInterval.top = new FormAttachment( wBufferSize, margin );
    fdFlushInterval.right = new FormAttachment( 100, 0 );
    wFlushInterval.setLayoutData( fdFlushInterval );

    // Compress socket data?
    Label wlCompressed = new Label( shell, SWT.RIGHT );
    props.setLook( wlCompressed );
    wlCompressed.setText( BaseMessages.getString( PKG, "SocketWriterDialog.Compressed.Label" ) );
    FormData fdlCompressed = new FormData();
    fdlCompressed.top = new FormAttachment( wFlushInterval, margin );
    fdlCompressed.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlCompressed.right = new FormAttachment( middle, 0 );
    wlCompressed.setLayoutData( fdlCompressed );
    wCompressed = new Button( shell, SWT.CHECK );
    props.setLook( wCompressed );
    FormData fdCompressed = new FormData();
    fdCompressed.top = new FormAttachment( wFlushInterval, margin );
    fdCompressed.left = new FormAttachment( middle, margin ); // To the right of the label
    fdCompressed.right = new FormAttachment( 95, 0 );
    wCompressed.setLayoutData( fdCompressed );
    wCompressed.addSelectionListener( new ComponentSelectionListener( input ) );

    // Some buttons
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel }, margin, wCompressed );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOk = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOk.addListener( SWT.Selection, lsOk );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );
    wPort.addSelectionListener( lsDef );
    wBufferSize.addSelectionListener( lsDef );
    wFlushInterval.addSelectionListener( lsDef );

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
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    wPort.setText( Const.NVL( input.getPort(), "" ) );
    wBufferSize.setText( Const.NVL( input.getBufferSize(), "" ) );
    wFlushInterval.setText( Const.NVL( input.getFlushInterval(), "" ) );
    wCompressed.setSelection( input.isCompressed() );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    input.setPort( wPort.getText() );
    input.setBufferSize( wBufferSize.getText() );
    input.setFlushInterval( wFlushInterval.getText() );
    input.setCompressed( wCompressed.getSelection() );

    transformName = wTransformName.getText(); // return value

    dispose();
  }
}
