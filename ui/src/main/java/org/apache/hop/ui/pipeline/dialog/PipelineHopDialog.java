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

package org.apache.hop.ui.pipeline.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.step.BaseStepDialog;
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
  private static Class<?> PKG = PipelineDialog.class; // for i18n purposes, needed by Translator!!

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

  private Button wOK, wCancel;
  private FormData fdOK, fdCancel;
  private Listener lsOK, lsCancel, lsFlip;

  private PipelineHopMeta input;
  private Shell shell;
  private PipelineMeta pipelineMeta;
  private PropsUI props;

  private ModifyListener lsMod;

  private boolean changed;

  public PipelineHopDialog( Shell parent, int style, PipelineHopMeta pipelineHopMeta, PipelineMeta tr ) {
    super( parent, style );
    this.props = PropsUI.getInstance();
    input = pipelineHopMeta;
    pipelineMeta = tr;
  }

  public Object open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE );
    props.setLook( shell );
    shell.setImage( GUIResource.getInstance().getImageHop() );

    lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        input.setChanged();
      }
    };
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "PipelineHopDialog.Shell.Label" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();
    int width = 0;

    // From step line
    wlFrom = new Label( shell, SWT.RIGHT );
    wlFrom.setText( BaseMessages.getString( PKG, "PipelineHopDialog.FromStep.Label" ) );
    props.setLook( wlFrom );
    fdlFrom = new FormData();
    fdlFrom.left = new FormAttachment( 0, 0 );
    fdlFrom.right = new FormAttachment( middle, -margin );
    fdlFrom.top = new FormAttachment( 0, margin );
    wlFrom.setLayoutData( fdlFrom );
    wFrom = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wFrom.setText( BaseMessages.getString( PKG, "PipelineHopDialog.FromStepDropdownList.Label" ) );
    props.setLook( wFrom );

    for ( int i = 0; i < pipelineMeta.nrSteps(); i++ ) {
      StepMeta stepMeta = pipelineMeta.getStep( i );
      wFrom.add( stepMeta.getName() );
    }
    wFrom.addModifyListener( lsMod );

    fdFrom = new FormData();
    fdFrom.left = new FormAttachment( middle, 0 );
    fdFrom.top = new FormAttachment( 0, margin );
    fdFrom.right = new FormAttachment( 100, 0 );
    wFrom.setLayoutData( fdFrom );

    // To line
    wlTo = new Label( shell, SWT.RIGHT );
    wlTo.setText( BaseMessages.getString( PKG, "PipelineHopDialog.TargetStep.Label" ) );
    props.setLook( wlTo );
    fdlTo = new FormData();
    fdlTo.left = new FormAttachment( 0, 0 );
    fdlTo.right = new FormAttachment( middle, -margin );
    fdlTo.top = new FormAttachment( wFrom, margin );
    wlTo.setLayoutData( fdlTo );
    wTo = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    wTo.setText( BaseMessages.getString( PKG, "PipelineHopDialog.TargetStepDropdownList.Label" ) );
    props.setLook( wTo );

    for ( int i = 0; i < pipelineMeta.nrSteps(); i++ ) {
      StepMeta stepMeta = pipelineMeta.getStep( i );
      wTo.add( stepMeta.getName() );
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
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOK.pack( true );
    Rectangle rOK = wOK.getBounds();

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.pack( true );
    Rectangle rCancel = wCancel.getBounds();

    width = ( rOK.width > rCancel.width ? rOK.width : rCancel.width );
    width += margin;

    fdOK = new FormData();
    fdOK.top = new FormAttachment( wFlip, margin * 5 );
    fdOK.left = new FormAttachment( 50, -width );
    fdOK.right = new FormAttachment( 50, -( margin / 2 ) );
    // fdOK.bottom = new FormAttachment(100, 0);
    wOK.setLayoutData( fdOK );

    fdCancel = new FormData();
    fdCancel.top = new FormAttachment( wFlip, margin * 5 );
    fdCancel.left = new FormAttachment( 50, margin / 2 );
    fdCancel.right = new FormAttachment( 50, width );
    // fdCancel.bottom = new FormAttachment(100, 0);
    wCancel.setLayoutData( fdCancel );

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
    lsFlip = new Listener() {
      public void handleEvent( Event e ) {
        flip();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wCancel.addListener( SWT.Selection, lsCancel );
    wFlip.addListener( SWT.Selection, lsFlip );

    // Detect [X] or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseStepDialog.setSize( shell );

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
    if ( input.getFromStep() != null ) {
      wFrom.setText( input.getFromStep().getName() );
    }
    if ( input.getToStep() != null ) {
      wTo.setText( input.getToStep().getName() );
    }
    wEnabled.setSelection( input.isEnabled() );
  }

  private void cancel() {
    input.setChanged( changed );
    input = null;
    dispose();
  }

  private void ok() {
    StepMeta fromBackup = input.getFromStep();
    StepMeta toBackup = input.getToStep();
    input.setFromStep( pipelineMeta.findStep( wFrom.getText() ) );
    input.setToStep( pipelineMeta.findStep( wTo.getText() ) );

    pipelineMeta.clearCaches();

    if ( input.getFromStep() == null || input.getToStep() == null ) {
      MessageBox mb = new MessageBox( shell, SWT.YES | SWT.ICON_WARNING );
      mb.setMessage( BaseMessages.getString( PKG, "PipelineHopDialog.StepDoesNotExist.DialogMessage", input.getFromStep() == null ? wFrom
        .getText() : wTo.getText() ) );
      mb.setText( BaseMessages.getString( PKG, "PipelineHopDialog.StepDoesNotExist.DialogTitle" ) );
      mb.open();
    } else if ( input.getFromStep().equals( input.getToStep() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.YES | SWT.ICON_WARNING );
      mb.setMessage( BaseMessages.getString( PKG, "PipelineHopDialog.CannotGoToSameStep.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "PipelineHopDialog.CannotGoToSameStep.DialogTitle" ) );
      mb.open();
    } else if ( pipelineMeta.hasLoop( input.getToStep() ) ) {
      input.setFromStep( fromBackup );
      input.setToStep( toBackup );
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
