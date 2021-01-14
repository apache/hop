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

package org.apache.hop.ui.pipeline.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

public class PipelineHopDialog extends Dialog {
  private static final Class<?> PKG = PipelineDialog.class; // For Translator

  private Label wlFrom;
  private CCombo wFrom;
  private FormData fdlFrom, fdFrom;

  private Label wlTo;
  private Button wFlip;
  private CCombo wTo;
  private FormData fdlTo, fdFlip, fdTo;

  private Label wlEnabled;
  private Button wEnabled;
  private FormData fdlEnabled, fdEnabled;

  private Button wOk, wCancel;
  private FormData fdOk, fdCancel;
  private Listener lsOk, lsCancel, lsFlip;

  private PipelineHopMeta input;
  private Shell shell;
  private PipelineMeta pipelineMeta;
  private PropsUi props;

  private ModifyListener lsMod;

  private boolean changed;

  public PipelineHopDialog( Shell parent, int style, PipelineHopMeta pipelineHopMeta, PipelineMeta tr ) {
    super( parent, style );
    this.props = PropsUi.getInstance();
    input = pipelineHopMeta;
    pipelineMeta = tr;
  }

  public Object open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE );
    props.setLook( shell );
    shell.setImage( GuiResource.getInstance().getImageHop() );

    lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "PipelineHopDialog.Shell.Label" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();
    int width = 0;

    // From transform line
    wlFrom = new Label( shell, SWT.RIGHT );
    wlFrom.setText( BaseMessages.getString( PKG, "PipelineHopDialog.FromTransform.Label" ) );
    props.setLook( wlFrom );
    fdlFrom = new FormData();
    fdlFrom.left = new FormAttachment( 0, 0 );
    fdlFrom.right = new FormAttachment( middle, -margin );
    fdlFrom.top = new FormAttachment( 0, margin );
    wlFrom.setLayoutData( fdlFrom );
    wFrom = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wFrom.setText( BaseMessages.getString( PKG, "PipelineHopDialog.FromTransformDropdownList.Label" ) );
    props.setLook( wFrom );

    for ( int i = 0; i < pipelineMeta.nrTransforms(); i++ ) {
      TransformMeta transformMeta = pipelineMeta.getTransform( i );
      wFrom.add( transformMeta.getName() );
    }
    wFrom.addModifyListener( lsMod );

    fdFrom = new FormData();
    fdFrom.left = new FormAttachment( middle, 0 );
    fdFrom.top = new FormAttachment( 0, margin );
    fdFrom.right = new FormAttachment( 100, 0 );
    wFrom.setLayoutData( fdFrom );

    // To line
    wlTo = new Label( shell, SWT.RIGHT );
    wlTo.setText( BaseMessages.getString( PKG, "PipelineHopDialog.TargetTransform.Label" ) );
    props.setLook( wlTo );
    fdlTo = new FormData();
    fdlTo.left = new FormAttachment( 0, 0 );
    fdlTo.right = new FormAttachment( middle, -margin );
    fdlTo.top = new FormAttachment( wFrom, margin );
    wlTo.setLayoutData( fdlTo );
    wTo = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    wTo.setText( BaseMessages.getString( PKG, "PipelineHopDialog.TargetTransformDropdownList.Label" ) );
    props.setLook( wTo );

    for ( int i = 0; i < pipelineMeta.nrTransforms(); i++ ) {
      TransformMeta transformMeta = pipelineMeta.getTransform( i );
      wTo.add( transformMeta.getName() );
    }
    wTo.addModifyListener( lsMod );

    fdTo = new FormData();
    fdTo.left = new FormAttachment( middle, 0 );
    fdTo.top = new FormAttachment( wFrom, margin );
    fdTo.right = new FormAttachment( 100, 0 );
    wTo.setLayoutData( fdTo );

    // Enabled?
    wlEnabled = new Label( shell, SWT.RIGHT );
    wlEnabled.setText( BaseMessages.getString( PKG, "PipelineHopDialog.EnableHop.Label" ) );
    props.setLook( wlEnabled );
    fdlEnabled = new FormData();
    fdlEnabled.left = new FormAttachment( 0, 0 );
    fdlEnabled.right = new FormAttachment( middle, -margin );
    fdlEnabled.top = new FormAttachment( wlTo, margin * 5 );
    wlEnabled.setLayoutData( fdlEnabled );
    wEnabled = new Button( shell, SWT.CHECK );
    props.setLook( wEnabled );
    fdEnabled = new FormData();
    fdEnabled.left = new FormAttachment( middle, 0 );
    fdEnabled.top = new FormAttachment( wlTo, margin * 5 );
    wEnabled.setLayoutData( fdEnabled );
    wEnabled.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setEnabled( !input.isEnabled() );
        input.setChanged();
      }
    } );

    wFlip = new Button( shell, SWT.PUSH );
    wFlip.setText( BaseMessages.getString( PKG, "PipelineHopDialog.FromTo.Button" ) );
    fdFlip = new FormData();
    fdFlip.right = new FormAttachment( 100, 0 );
    fdFlip.top = new FormAttachment( wlTo, 20 );
    wFlip.setLayoutData( fdFlip );

    // Some buttons
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.pack( true );
    Rectangle rOK = wOk.getBounds();

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.pack( true );
    Rectangle rCancel = wCancel.getBounds();

    width = ( rOK.width > rCancel.width ? rOK.width : rCancel.width );
    width += margin;

    fdOk = new FormData();
    fdOk.top = new FormAttachment( wFlip, margin * 5 );
    fdOk.left = new FormAttachment( 50, -width );
    fdOk.right = new FormAttachment( 50, -( margin / 2 ) );
    // fdOk.bottom = new FormAttachment(100, 0);
    wOk.setLayoutData( fdOk );

    fdCancel = new FormData();
    fdCancel.top = new FormAttachment( wFlip, margin * 5 );
    fdCancel.left = new FormAttachment( 50, margin / 2 );
    fdCancel.right = new FormAttachment( 50, width );
    // fdCancel.bottom = new FormAttachment(100, 0);
    wCancel.setLayoutData( fdCancel );

    // Add listeners
    lsCancel = e -> cancel();
    lsOk = e -> ok();
    lsFlip = e -> flip();

    wOk.addListener( SWT.Selection, lsOk );
    wCancel.addListener( SWT.Selection, lsCancel );
    wFlip.addListener( SWT.Selection, lsFlip );

    // Detect [X] or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseTransformDialog.setSize( shell );

    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return input;
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( input.getFromTransform() != null ) {
      wFrom.setText( input.getFromTransform().getName() );
    }
    if ( input.getToTransform() != null ) {
      wTo.setText( input.getToTransform().getName() );
    }
    wEnabled.setSelection( input.isEnabled() );
  }

  private void cancel() {
    input.setChanged( changed );
    input = null;
    dispose();
  }

  private void ok() {
    TransformMeta fromBackup = input.getFromTransform();
    TransformMeta toBackup = input.getToTransform();
    input.setFromTransform( pipelineMeta.findTransform( wFrom.getText() ) );
    input.setToTransform( pipelineMeta.findTransform( wTo.getText() ) );

    pipelineMeta.clearCaches();

    if ( input.getFromTransform() == null || input.getToTransform() == null ) {
      MessageBox mb = new MessageBox( shell, SWT.YES | SWT.ICON_WARNING );
      mb.setMessage( BaseMessages.getString( PKG, "PipelineHopDialog.TransformDoesNotExist.DialogMessage", input.getFromTransform() == null ? wFrom
        .getText() : wTo.getText() ) );
      mb.setText( BaseMessages.getString( PKG, "PipelineHopDialog.TransformDoesNotExist.DialogTitle" ) );
      mb.open();
    } else if ( input.getFromTransform().equals( input.getToTransform() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.YES | SWT.ICON_WARNING );
      mb.setMessage( BaseMessages.getString( PKG, "PipelineHopDialog.CannotGoToSameTransform.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "PipelineHopDialog.CannotGoToSameTransform.DialogTitle" ) );
      mb.open();
    } else if ( pipelineMeta.hasLoop( input.getToTransform() ) ) {
      input.setFromTransform( fromBackup );
      input.setToTransform( toBackup );
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "PipelineHopDialog.LoopsNotAllowed.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "PipelineHopDialog.LoopsNotAllowed.DialogTitle" ) );
      mb.open();
    } else {
      dispose();
    }
  }

  private void flip() {
    String dummy;
    dummy = wFrom.getText();
    wFrom.setText( wTo.getText() );
    wTo.setText( dummy );
    input.setChanged();
  }
}
