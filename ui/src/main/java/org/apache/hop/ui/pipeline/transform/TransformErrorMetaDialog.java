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

package org.apache.hop.ui.pipeline.transform;

import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.TextVar;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
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
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import java.util.List;

/**
 * Dialog that allows you to edit the transform error handling meta-data
 *
 * @author Matt
 * @since 31-10-2006
 */
public class TransformErrorMetaDialog extends Dialog {
  private static final Class<?> PKG = ITransform.class; // For Translator

  private final IVariables variables;
  private TransformErrorMeta transformErrorMeta;
  private List<TransformMeta> targetTransforms;

  private Composite composite;
  private Shell shell;

  // Service
  private Text wSourceTransform;
  private CCombo wTargetTransform;
  private Button wEnabled;
  private TextVar wNrErrors, wErrDesc, wErrFields, wErrCodes;
  private TextVar wMaxErrors, wMaxPct, wMinPctRows;

  private Button wOk, wCancel;

  private ModifyListener lsMod;

  private PropsUi props;

  private int middle;
  private int margin;

  private TransformErrorMeta originalTransformErrorMeta;
  private boolean ok;

  private PipelineMeta pipelineMeta;

  public TransformErrorMetaDialog( Shell par, IVariables variables, TransformErrorMeta transformErrorMeta, PipelineMeta pipelineMeta,
                                   List<TransformMeta> targetTransforms ) {
    super( par, SWT.NONE );
    this.variables = variables;
    this.transformErrorMeta = transformErrorMeta.clone();
    this.originalTransformErrorMeta = transformErrorMeta;
    this.targetTransforms = targetTransforms;
    this.pipelineMeta = pipelineMeta;
    props = PropsUi.getInstance();
    ok = false;
  }

  public boolean open() {
    Shell parent = getParent();
    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );

    lsMod = e -> transformErrorMeta.setChanged();

    middle = props.getMiddlePct();
    margin = props.getMargin();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setText( BaseMessages.getString( PKG, "BaseTransformDialog.ErrorHandling.Title.Label" ) );
    shell.setImage( GuiResource.getInstance().getImagePipeline() );
    shell.setLayout( formLayout );

    // First, add the buttons...

    // Buttons
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( " &OK " );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( " &Cancel " );

    Button[] buttons = new Button[] { wOk, wCancel };
    BaseTransformDialog.positionBottomButtons( shell, buttons, margin, null );

    // The rest stays above the buttons...

    composite = new Composite( shell, SWT.NONE );
    props.setLook( composite );
    composite.setLayout( new FormLayout() );

    // What's the source transform
    Label wlSourceTransform = new Label( composite, SWT.RIGHT );
    props.setLook( wlSourceTransform );
    wlSourceTransform.setText( BaseMessages.getString( PKG, "BaseTransformDialog.ErrorHandling.TransformName.Label" ) );
    FormData fdlSourceTransform = new FormData();
    fdlSourceTransform.top = new FormAttachment( 0, 0 );
    fdlSourceTransform.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlSourceTransform.right = new FormAttachment( middle, -margin );
    wlSourceTransform.setLayoutData( fdlSourceTransform );

    wSourceTransform = new Text( composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSourceTransform );
    wSourceTransform.addModifyListener( lsMod );
    FormData fdSourceTransform = new FormData();
    fdSourceTransform.top = new FormAttachment( 0, 0 );
    fdSourceTransform.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdSourceTransform.right = new FormAttachment( 95, 0 );
    wSourceTransform.setLayoutData( fdSourceTransform );
    wSourceTransform.setEnabled( false );

    // What's the target transform
    Label wlTargetTransform = new Label( composite, SWT.RIGHT );
    props.setLook( wlTargetTransform );
    wlTargetTransform.setText( BaseMessages.getString( PKG, "BaseTransformDialog.ErrorHandling.TargetTransform.Label" ) );
    FormData fdlTargetTransform = new FormData();
    fdlTargetTransform.top = new FormAttachment( wSourceTransform, margin );
    fdlTargetTransform.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlTargetTransform.right = new FormAttachment( middle, -margin );
    wlTargetTransform.setLayoutData( fdlTargetTransform );

    wTargetTransform = new CCombo( composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTargetTransform );
    wTargetTransform.addModifyListener( lsMod );
    FormData fdTargetTransform = new FormData();
    fdTargetTransform.top = new FormAttachment( wSourceTransform, margin );
    fdTargetTransform.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdTargetTransform.right = new FormAttachment( 95, 0 );
    wTargetTransform.setLayoutData( fdTargetTransform );
    for ( int i = 0; i < targetTransforms.size(); i++ ) {
      wTargetTransform.add( targetTransforms.get( i ).getName() );
    }

    // is the error handling enabled?
    Label wlEnabled = new Label( composite, SWT.RIGHT );
    props.setLook( wlEnabled );
    wlEnabled.setText( BaseMessages.getString( PKG, "BaseTransformDialog.ErrorHandling.Enable.Label" ) );
    FormData fdlEnabled = new FormData();
    fdlEnabled.top = new FormAttachment( wTargetTransform, margin );
    fdlEnabled.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlEnabled.right = new FormAttachment( middle, -margin );
    wlEnabled.setLayoutData( fdlEnabled );

    wEnabled = new Button( composite, SWT.CHECK );
    props.setLook( wEnabled );
    FormData fdEnabled = new FormData();
    fdEnabled.top = new FormAttachment( wTargetTransform, margin );
    fdEnabled.left = new FormAttachment( middle, 0 ); // To the right of the label
    wEnabled.setLayoutData( fdEnabled );

    // What's the field for the nr of errors
    Label wlNrErrors = new Label( composite, SWT.RIGHT );
    props.setLook( wlNrErrors );
    wlNrErrors.setText( BaseMessages.getString( PKG, "BaseTransformDialog.ErrorHandling.NrErrField.Label" ) );
    FormData fdlNrErrors = new FormData();
    fdlNrErrors.top = new FormAttachment( wEnabled, margin * 2 );
    fdlNrErrors.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlNrErrors.right = new FormAttachment( middle, -margin );
    wlNrErrors.setLayoutData( fdlNrErrors );

    wNrErrors = new TextVar( variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wNrErrors );
    wNrErrors.addModifyListener( lsMod );
    FormData fdNrErrors = new FormData();
    fdNrErrors.top = new FormAttachment( wEnabled, margin * 2 );
    fdNrErrors.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdNrErrors.right = new FormAttachment( 95, 0 );
    wNrErrors.setLayoutData( fdNrErrors );

    // What's the field for the error descriptions
    Label wlErrDesc = new Label( composite, SWT.RIGHT );
    props.setLook( wlErrDesc );
    wlErrDesc.setText( BaseMessages.getString( PKG, "BaseTransformDialog.ErrorHandling.ErrDescField.Label" ) );
    FormData fdlErrDesc = new FormData();
    fdlErrDesc.top = new FormAttachment( wNrErrors, margin );
    fdlErrDesc.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlErrDesc.right = new FormAttachment( middle, -margin );
    wlErrDesc.setLayoutData( fdlErrDesc );

    wErrDesc = new TextVar( variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wErrDesc );
    wErrDesc.addModifyListener( lsMod );
    FormData fdErrDesc = new FormData();
    fdErrDesc.top = new FormAttachment( wNrErrors, margin );
    fdErrDesc.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdErrDesc.right = new FormAttachment( 95, 0 );
    wErrDesc.setLayoutData( fdErrDesc );

    // What's the field for the error fields
    Label wlErrFields = new Label( composite, SWT.RIGHT );
    wlErrFields.setText( BaseMessages.getString( PKG, "BaseTransformDialog.ErrorHandling.ErrFieldName.Label" ) );
    props.setLook( wlErrFields );
    FormData fdlErrFields = new FormData();
    fdlErrFields.top = new FormAttachment( wErrDesc, margin );
    fdlErrFields.left = new FormAttachment( 0, 0 );
    fdlErrFields.right = new FormAttachment( middle, -margin );
    wlErrFields.setLayoutData( fdlErrFields );

    wErrFields = new TextVar( variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wErrFields );
    wErrFields.addModifyListener( lsMod );
    FormData fdErrFields = new FormData();
    fdErrFields.top = new FormAttachment( wErrDesc, margin );
    fdErrFields.left = new FormAttachment( middle, 0 );
    fdErrFields.right = new FormAttachment( 95, 0 );
    wErrFields.setLayoutData( fdErrFields );

    // What's the fieldname for the error codes field
    Label wlErrCodes = new Label( composite, SWT.RIGHT );
    wlErrCodes.setText( BaseMessages.getString( PKG, "BaseTransformDialog.ErrorHandling.ErrCodeFieldName.Label" ) );
    props.setLook( wlErrCodes );
    FormData fdlErrCodes = new FormData();
    fdlErrCodes.top = new FormAttachment( wErrFields, margin );
    fdlErrCodes.left = new FormAttachment( 0, 0 );
    fdlErrCodes.right = new FormAttachment( middle, -margin );
    wlErrCodes.setLayoutData( fdlErrCodes );

    wErrCodes = new TextVar( variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wErrCodes );
    wErrCodes.addModifyListener( lsMod );
    FormData fdErrCodes = new FormData();
    fdErrCodes.top = new FormAttachment( wErrFields, margin );
    fdErrCodes.left = new FormAttachment( middle, 0 );
    fdErrCodes.right = new FormAttachment( 95, 0 );
    wErrCodes.setLayoutData( fdErrCodes );

    // What's the maximum number of errors allowed before we stop?
    Label wlMaxErrors = new Label( composite, SWT.RIGHT );
    wlMaxErrors.setText( BaseMessages.getString( PKG, "BaseTransformDialog.ErrorHandling.MaxErr.Label" ) );
    props.setLook( wlMaxErrors );
    FormData fdlMaxErrors = new FormData();
    fdlMaxErrors.top = new FormAttachment( wErrCodes, margin );
    fdlMaxErrors.left = new FormAttachment( 0, 0 );
    fdlMaxErrors.right = new FormAttachment( middle, -margin );
    wlMaxErrors.setLayoutData( fdlMaxErrors );

    wMaxErrors = new TextVar( variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMaxErrors );
    wMaxErrors.addModifyListener( lsMod );
    FormData fdMaxErrors = new FormData();
    fdMaxErrors.top = new FormAttachment( wErrCodes, margin );
    fdMaxErrors.left = new FormAttachment( middle, 0 );
    fdMaxErrors.right = new FormAttachment( 95, 0 );
    wMaxErrors.setLayoutData( fdMaxErrors );

    // What's the maximum % of errors allowed?
    Label wlMaxPct = new Label( composite, SWT.RIGHT );
    wlMaxPct.setText( BaseMessages.getString( PKG, "BaseTransformDialog.ErrorHandling.MaxPctErr.Label" ) );
    props.setLook( wlMaxPct );
    FormData fdlMaxPct = new FormData();
    fdlMaxPct.top = new FormAttachment( wMaxErrors, margin );
    fdlMaxPct.left = new FormAttachment( 0, 0 );
    fdlMaxPct.right = new FormAttachment( middle, -margin );
    wlMaxPct.setLayoutData( fdlMaxPct );

    wMaxPct = new TextVar( variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMaxPct );
    wMaxPct.addModifyListener( lsMod );
    FormData fdMaxPct = new FormData();
    fdMaxPct.top = new FormAttachment( wMaxErrors, margin );
    fdMaxPct.left = new FormAttachment( middle, 0 );
    fdMaxPct.right = new FormAttachment( 95, 0 );
    wMaxPct.setLayoutData( fdMaxPct );

    // What's the min nr of rows to read before doing % evaluation
    Label wlMinPctRows = new Label( composite, SWT.RIGHT );
    wlMinPctRows.setText( BaseMessages.getString( PKG, "BaseTransformDialog.ErrorHandling.MinErr.Label" ) );
    props.setLook( wlMinPctRows );
    FormData fdlMinPctRows = new FormData();
    fdlMinPctRows.top = new FormAttachment( wMaxPct, margin );
    fdlMinPctRows.left = new FormAttachment( 0, 0 );
    fdlMinPctRows.right = new FormAttachment( middle, -margin );
    wlMinPctRows.setLayoutData( fdlMinPctRows );

    wMinPctRows = new TextVar( variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMinPctRows );
    wMinPctRows.addModifyListener( lsMod );
    FormData fdMinPctRows = new FormData();
    fdMinPctRows.top = new FormAttachment( wMaxPct, margin );
    fdMinPctRows.left = new FormAttachment( middle, 0 );
    fdMinPctRows.right = new FormAttachment( 95, 0 );
    wMinPctRows.setLayoutData( fdMinPctRows );

    FormData fdComposite = new FormData();
    fdComposite.left = new FormAttachment( 0, 0 );
    fdComposite.top = new FormAttachment( 0, 0 );
    fdComposite.right = new FormAttachment( 100, 0 );
    fdComposite.bottom = new FormAttachment( wOk, -margin );
    composite.setLayoutData( fdComposite );

    // Add listeners
    wOk.addListener( SWT.Selection, e -> ok() );
    wCancel.addListener( SWT.Selection, e -> cancel() );

    SelectionAdapter selAdapter = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    wErrFields.addSelectionListener( selAdapter );
    wErrCodes.addSelectionListener( selAdapter );
    wNrErrors.addSelectionListener( selAdapter );
    wErrDesc.addSelectionListener( selAdapter );
    wMaxErrors.addSelectionListener( selAdapter );
    wMaxPct.addSelectionListener( selAdapter );
    wMinPctRows.addSelectionListener( selAdapter );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseTransformDialog.setSize( shell );

    shell.open();
    Display display = parent.getDisplay();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return ok;
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  public void getData() {
    wSourceTransform.setText( transformErrorMeta.getSourceTransform() != null ? transformErrorMeta.getSourceTransform().getName() : "" );
    wTargetTransform.setText( transformErrorMeta.getTargetTransform() != null ? transformErrorMeta.getTargetTransform().getName() : "" );
    wEnabled.setSelection( transformErrorMeta.isEnabled() );
    wNrErrors.setText( Const.NVL( transformErrorMeta.getNrErrorsValuename(), "" ) );
    wErrDesc.setText( Const.NVL( transformErrorMeta.getErrorDescriptionsValuename(), "" ) );
    wErrFields.setText( Const.NVL( transformErrorMeta.getErrorFieldsValuename(), "" ) );
    wErrCodes.setText( Const.NVL( transformErrorMeta.getErrorCodesValuename(), "" ) );
    wMaxErrors.setText( transformErrorMeta.getMaxErrors() != null ? transformErrorMeta.getMaxErrors() : "" );
    wMaxPct.setText( transformErrorMeta.getMaxPercentErrors() != null ? transformErrorMeta.getMaxPercentErrors() : "" );
    wMinPctRows.setText( transformErrorMeta.getMinPercentRows() != null ? transformErrorMeta.getMinPercentRows() : "" );

    wSourceTransform.setFocus();
  }

  private void cancel() {
    originalTransformErrorMeta = null;
    dispose();
  }

  public void ok() {
    getInfo();
    originalTransformErrorMeta.setTargetTransform( transformErrorMeta.getTargetTransform() );
    originalTransformErrorMeta.setEnabled( transformErrorMeta.isEnabled() );
    originalTransformErrorMeta.setNrErrorsValuename( transformErrorMeta.getNrErrorsValuename() );
    originalTransformErrorMeta.setErrorDescriptionsValuename( transformErrorMeta.getErrorDescriptionsValuename() );
    originalTransformErrorMeta.setErrorFieldsValuename( transformErrorMeta.getErrorFieldsValuename() );
    originalTransformErrorMeta.setErrorCodesValuename( transformErrorMeta.getErrorCodesValuename() );
    originalTransformErrorMeta.setMaxErrors( transformErrorMeta.getMaxErrors() );
    originalTransformErrorMeta.setMaxPercentErrors( transformErrorMeta.getMaxPercentErrors() );
    originalTransformErrorMeta.setMinPercentRows( transformErrorMeta.getMinPercentRows() );

    originalTransformErrorMeta.setChanged();

    ok = true;

    dispose();
  }

  // Get dialog info in securityService
  private void getInfo() {
    transformErrorMeta.setTargetTransform( TransformMeta.findTransform( targetTransforms, wTargetTransform.getText() ) );
    transformErrorMeta.setEnabled( wEnabled.getSelection() );
    transformErrorMeta.setNrErrorsValuename( wNrErrors.getText() );
    transformErrorMeta.setErrorDescriptionsValuename( wErrDesc.getText() );
    transformErrorMeta.setErrorFieldsValuename( wErrFields.getText() );
    transformErrorMeta.setErrorCodesValuename( wErrCodes.getText() );
    transformErrorMeta.setMaxErrors( wMaxErrors.getText() );
    transformErrorMeta.setMaxPercentErrors( Const.replace( wMaxPct.getText(), "%", "" ) );
    transformErrorMeta.setMinPercentRows( wMinPctRows.getText() );
  }
}
