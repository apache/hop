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

package org.apache.hop.pipeline.transforms.xml.xmljoin;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class XmlJoinDialog extends BaseTransformDialog implements ITransformDialog {
  private static Class<?> PKG = XmlJoinMeta.class; // for i18n purposes, needed by Translator2!!

  private Label wlComplexJoin;
  private Button wComplexJoin;
  private FormData fdlComplexJoin, fdComplexJoin;

  private Label wlTargetXMLstep;
  private CCombo wTargetXMLstep;
  private FormData fdlTargetXMLstep, fdTargetXMLstep;

  private Label wlTargetXMLfield;
  private TextVar wTargetXMLfield;
  private FormData fdlTargetXMLfield, fdTargetXMLfield;

  private Label wlSourceXMLstep;
  private CCombo wSourceXMLstep;
  private FormData fdlSourceXMLstep, fdSourceXMLstep;

  private Label wlSourceXMLfield;
  private TextVar wSourceXMLfield;
  private FormData fdlSourceXMLfield, fdSourceXMLfield;

  private Label wlValueXMLfield;
  private TextVar wValueXMLfield;
  private FormData fdlValueXMLfield, fdValueXMLfield;

  private Label wlJoinCompareField;
  private TextVar wJoinCompareField;
  private FormData fdlJoinCompareField, fdJoinCompareField;

  private Label wlTargetXPath;
  private TextVar wTargetXPath;
  private FormData fdlTargetXPath, fdTargetXPath;

  private Label wlEncoding;
  private CCombo wEncoding;
  private FormData fdlEncoding, fdEncoding;

  private Label wlOmitXMLHeader;
  private Button wOmitXMLHeader;
  private FormData fdlOmitXMLHeader, fdOmitXMLHeader;

  private Label wlOmitNullValues;
  private Button wOmitNullValues;
  private FormData fdlOmitNullValues, fdOmitNullValues;

  private XmlJoinMeta input;

  private Group gJoin, gTarget, gSource, gResult;
  private FormData fdJoin, fdTarget, fdSource, fdResult;

  // private Button wMinWidth;
  // private Listener lsMinWidth;

  private boolean gotEncodings = false;

  public XmlJoinDialog(Shell parent, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (XmlJoinMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "XMLJoin.DialogTitle" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Step name line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "System.Label.StepName" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.top = new FormAttachment( 0, margin );
    fdlTransformName.right = new FormAttachment( middle, -margin );
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

    // Target Group
    gTarget = new Group( shell, SWT.NONE );
    gTarget.setText( BaseMessages.getString( PKG, "XMLJoin.TargetGroup.Label" ) );
    FormLayout targetLayout = new FormLayout();
    targetLayout.marginHeight = margin;
    targetLayout.marginWidth = margin;
    gTarget.setLayout( targetLayout );
    props.setLook( gTarget );
    fdTarget = new FormData();
    fdTarget.left = new FormAttachment( 0, 0 );
    fdTarget.right = new FormAttachment( 100, 0 );
    fdTarget.top = new FormAttachment( wTransformName, 2 * margin );
    gTarget.setLayoutData( fdTarget );
    // Target XML step line
    wlTargetXMLstep = new Label( gTarget, SWT.RIGHT );
    wlTargetXMLstep.setText( BaseMessages.getString( PKG, "XMLJoin.TargetXMLStep.Label" ) );
    props.setLook( wlTargetXMLstep );
    fdlTargetXMLstep = new FormData();
    fdlTargetXMLstep.left = new FormAttachment( 0, 0 );
    fdlTargetXMLstep.top = new FormAttachment( wTransformName, margin );
    fdlTargetXMLstep.right = new FormAttachment( middle, -margin );
    wlTargetXMLstep.setLayoutData( fdlTargetXMLstep );
    wTargetXMLstep = new CCombo( gTarget, SWT.BORDER | SWT.READ_ONLY );
    wTargetXMLstep.setEditable( true );
    props.setLook( wTargetXMLstep );
    wTargetXMLstep.addModifyListener( lsMod );
    fdTargetXMLstep = new FormData();
    fdTargetXMLstep.left = new FormAttachment( middle, 0 );
    fdTargetXMLstep.top = new FormAttachment( wTransformName, margin );
    fdTargetXMLstep.right = new FormAttachment( 100, 0 );
    wTargetXMLstep.setLayoutData( fdTargetXMLstep );

    // Target XML Field line
    wlTargetXMLfield = new Label( gTarget, SWT.RIGHT );
    wlTargetXMLfield.setText( BaseMessages.getString( PKG, "XMLJoin.TargetXMLField.Label" ) );
    props.setLook( wlTargetXMLfield );
    fdlTargetXMLfield = new FormData();
    fdlTargetXMLfield.left = new FormAttachment( 0, 0 );
    fdlTargetXMLfield.right = new FormAttachment( middle, -margin );
    fdlTargetXMLfield.top = new FormAttachment( wTargetXMLstep, margin );
    wlTargetXMLfield.setLayoutData( fdlTargetXMLfield );

    wTargetXMLfield = new TextVar( pipelineMeta, gTarget, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTargetXMLfield );
    wTargetXMLfield.addModifyListener( lsMod );
    fdTargetXMLfield = new FormData();
    fdTargetXMLfield.left = new FormAttachment( middle, 0 );
    fdTargetXMLfield.top = new FormAttachment( wTargetXMLstep, margin );
    fdTargetXMLfield.right = new FormAttachment( 100, -margin );
    wTargetXMLfield.setLayoutData( fdTargetXMLfield );

    // Source Group
    gSource = new Group( shell, SWT.NONE );
    gSource.setText( BaseMessages.getString( PKG, "XMLJoin.SourceGroup.Label" ) );
    FormLayout SourceLayout = new FormLayout();
    SourceLayout.marginHeight = margin;
    SourceLayout.marginWidth = margin;
    gSource.setLayout( SourceLayout );
    props.setLook( gSource );
    fdSource = new FormData();
    fdSource.left = new FormAttachment( 0, 0 );
    fdSource.right = new FormAttachment( 100, 0 );
    fdSource.top = new FormAttachment( gTarget, 2 * margin );
    gSource.setLayoutData( fdSource );
    // Source XML step line
    wlSourceXMLstep = new Label( gSource, SWT.RIGHT );
    wlSourceXMLstep.setText( BaseMessages.getString( PKG, "XMLJoin.SourceXMLStep.Label" ) );
    props.setLook( wlSourceXMLstep );
    fdlSourceXMLstep = new FormData();
    fdlSourceXMLstep.left = new FormAttachment( 0, 0 );
    fdlSourceXMLstep.top = new FormAttachment( wTargetXMLfield, margin );
    fdlSourceXMLstep.right = new FormAttachment( middle, -margin );
    wlSourceXMLstep.setLayoutData( fdlSourceXMLstep );
    wSourceXMLstep = new CCombo( gSource, SWT.BORDER | SWT.READ_ONLY );
    wSourceXMLstep.setEditable( true );
    props.setLook( wSourceXMLstep );
    wSourceXMLstep.addModifyListener( lsMod );
    fdSourceXMLstep = new FormData();
    fdSourceXMLstep.left = new FormAttachment( middle, 0 );
    fdSourceXMLstep.top = new FormAttachment( wTargetXMLfield, margin );
    fdSourceXMLstep.right = new FormAttachment( 100, 0 );
    wSourceXMLstep.setLayoutData( fdSourceXMLstep );

    // Source XML Field line
    wlSourceXMLfield = new Label( gSource, SWT.RIGHT );
    wlSourceXMLfield.setText( BaseMessages.getString( PKG, "XMLJoin.SourceXMLField.Label" ) );
    props.setLook( wlSourceXMLfield );
    fdlSourceXMLfield = new FormData();
    fdlSourceXMLfield.left = new FormAttachment( 0, 0 );
    fdlSourceXMLfield.right = new FormAttachment( middle, -margin );
    fdlSourceXMLfield.top = new FormAttachment( wSourceXMLstep, margin );
    wlSourceXMLfield.setLayoutData( fdlSourceXMLfield );

    wSourceXMLfield = new TextVar( pipelineMeta, gSource, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSourceXMLfield );
    wSourceXMLfield.addModifyListener( lsMod );
    fdSourceXMLfield = new FormData();
    fdSourceXMLfield.left = new FormAttachment( middle, 0 );
    fdSourceXMLfield.top = new FormAttachment( wSourceXMLstep, margin );
    fdSourceXMLfield.right = new FormAttachment( 100, -margin );
    wSourceXMLfield.setLayoutData( fdSourceXMLfield );

    // Join Group
    gJoin = new Group( shell, SWT.NONE );
    gJoin.setText( BaseMessages.getString( PKG, "XMLJoin.JoinGroup.Label" ) );
    FormLayout JoinLayout = new FormLayout();
    JoinLayout.marginHeight = margin;
    JoinLayout.marginWidth = margin;
    gJoin.setLayout( JoinLayout );
    props.setLook( gJoin );
    fdJoin = new FormData();
    fdJoin.left = new FormAttachment( 0, 0 );
    fdJoin.right = new FormAttachment( 100, 0 );
    fdJoin.top = new FormAttachment( gSource, 2 * margin );
    gJoin.setLayoutData( fdJoin );

    // Target XPath line
    wlTargetXPath = new Label( gJoin, SWT.RIGHT );
    wlTargetXPath.setText( BaseMessages.getString( PKG, "XMLJoin.TargetXPath.Label" ) );
    props.setLook( wlTargetXPath );
    fdlTargetXPath = new FormData();
    fdlTargetXPath.left = new FormAttachment( 0, 0 );
    fdlTargetXPath.right = new FormAttachment( middle, -margin );
    fdlTargetXPath.top = new FormAttachment( wSourceXMLfield, margin );
    wlTargetXPath.setLayoutData( fdlTargetXPath );

    wTargetXPath = new TextVar( pipelineMeta, gJoin, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTargetXPath );
    wTargetXPath.addModifyListener( lsMod );
    fdTargetXPath = new FormData();
    fdTargetXPath.left = new FormAttachment( middle, 0 );
    fdTargetXPath.top = new FormAttachment( wSourceXMLfield, margin );
    fdTargetXPath.right = new FormAttachment( 100, -margin );
    wTargetXPath.setLayoutData( fdTargetXPath );

    // Complex Join Line
    wlComplexJoin = new Label( gJoin, SWT.RIGHT );
    wlComplexJoin.setText( BaseMessages.getString( PKG, "XMLJoin.ComplexJoin.Label" ) );
    props.setLook( wlComplexJoin );
    fdlComplexJoin = new FormData();
    fdlComplexJoin.left = new FormAttachment( 0, 0 );
    fdlComplexJoin.top = new FormAttachment( wTargetXPath, margin );
    fdlComplexJoin.right = new FormAttachment( middle, -margin );
    wlComplexJoin.setLayoutData( fdlComplexJoin );
    wComplexJoin = new Button( gJoin, SWT.CHECK );
    props.setLook( wComplexJoin );
    fdComplexJoin = new FormData();
    fdComplexJoin.left = new FormAttachment( middle, 0 );
    fdComplexJoin.top = new FormAttachment( wTargetXPath, margin );
    fdComplexJoin.right = new FormAttachment( 100, 0 );
    wComplexJoin.setLayoutData( fdComplexJoin );
    wComplexJoin.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();

        if ( wComplexJoin.getSelection() ) {
          wJoinCompareField.setEnabled( true );
        } else {
          wJoinCompareField.setEnabled( false );
        }
      }
    } );

    // Join Compare field line
    wlJoinCompareField = new Label( gJoin, SWT.RIGHT );
    wlJoinCompareField.setText( BaseMessages.getString( PKG, "XMLJoin.JoinCompareFiled.Label" ) );
    props.setLook( wlJoinCompareField );
    fdlJoinCompareField = new FormData();
    fdlJoinCompareField.left = new FormAttachment( 0, 0 );
    fdlJoinCompareField.right = new FormAttachment( middle, -margin );
    fdlJoinCompareField.top = new FormAttachment( wComplexJoin, margin );
    wlJoinCompareField.setLayoutData( fdlJoinCompareField );

    wJoinCompareField = new TextVar( pipelineMeta, gJoin, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wJoinCompareField );
    wJoinCompareField.addModifyListener( lsMod );
    fdJoinCompareField = new FormData();
    fdJoinCompareField.left = new FormAttachment( middle, 0 );
    fdJoinCompareField.top = new FormAttachment( wComplexJoin, margin );
    fdJoinCompareField.right = new FormAttachment( 100, -margin );
    wJoinCompareField.setLayoutData( fdJoinCompareField );
    wJoinCompareField.setEnabled( false );

    // Result Group
    gResult = new Group( shell, SWT.NONE );
    gResult.setText( BaseMessages.getString( PKG, "XMLJoin.ResultGroup.Label" ) );
    FormLayout ResultLayout = new FormLayout();
    ResultLayout.marginHeight = margin;
    ResultLayout.marginWidth = margin;
    gResult.setLayout( ResultLayout );
    props.setLook( gResult );
    fdResult = new FormData();
    fdResult.left = new FormAttachment( 0, 0 );
    fdResult.right = new FormAttachment( 100, 0 );
    fdResult.top = new FormAttachment( gJoin, 2 * margin );
    gResult.setLayoutData( fdResult );
    // Value XML Field line
    wlValueXMLfield = new Label( gResult, SWT.RIGHT );
    wlValueXMLfield.setText( BaseMessages.getString( PKG, "XMLJoin.ValueXMLField.Label" ) );
    props.setLook( wlValueXMLfield );
    fdlValueXMLfield = new FormData();
    fdlValueXMLfield.left = new FormAttachment( 0, 0 );
    fdlValueXMLfield.right = new FormAttachment( middle, -margin );
    fdlValueXMLfield.top = new FormAttachment( wJoinCompareField, margin );
    wlValueXMLfield.setLayoutData( fdlValueXMLfield );

    wValueXMLfield = new TextVar( pipelineMeta, gResult, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wValueXMLfield );
    wValueXMLfield.addModifyListener( lsMod );
    fdValueXMLfield = new FormData();
    fdValueXMLfield.left = new FormAttachment( middle, 0 );
    fdValueXMLfield.top = new FormAttachment( wJoinCompareField, margin );
    fdValueXMLfield.right = new FormAttachment( 100, -margin );
    wValueXMLfield.setLayoutData( fdValueXMLfield );

    // Encoding Line
    wlEncoding = new Label( gResult, SWT.RIGHT );
    wlEncoding.setText( BaseMessages.getString( PKG, "XMLJoin.Encoding.Label" ) );
    props.setLook( wlEncoding );
    fdlEncoding = new FormData();
    fdlEncoding.left = new FormAttachment( 0, 0 );
    fdlEncoding.top = new FormAttachment( wValueXMLfield, margin );
    fdlEncoding.right = new FormAttachment( middle, -margin );
    wlEncoding.setLayoutData( fdlEncoding );
    wEncoding = new CCombo( gResult, SWT.BORDER | SWT.READ_ONLY );
    wEncoding.setEditable( true );
    props.setLook( wEncoding );
    wEncoding.addModifyListener( lsMod );
    fdEncoding = new FormData();
    fdEncoding.left = new FormAttachment( middle, 0 );
    fdEncoding.top = new FormAttachment( wValueXMLfield, margin );
    fdEncoding.right = new FormAttachment( 100, 0 );
    wEncoding.setLayoutData( fdEncoding );
    wEncoding.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        setEncodings();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    // Complex Join Line
    wlOmitXMLHeader = new Label( gResult, SWT.RIGHT );
    wlOmitXMLHeader.setText( BaseMessages.getString( PKG, "XMLJoin.OmitXMLHeader.Label" ) );
    props.setLook( wlOmitXMLHeader );
    fdlOmitXMLHeader = new FormData();
    fdlOmitXMLHeader.left = new FormAttachment( 0, 0 );
    fdlOmitXMLHeader.top = new FormAttachment( wEncoding, margin );
    fdlOmitXMLHeader.right = new FormAttachment( middle, -margin );
    wlOmitXMLHeader.setLayoutData( fdlOmitXMLHeader );
    wOmitXMLHeader = new Button( gResult, SWT.CHECK );
    props.setLook( wOmitXMLHeader );
    fdOmitXMLHeader = new FormData();
    fdOmitXMLHeader.left = new FormAttachment( middle, 0 );
    fdOmitXMLHeader.top = new FormAttachment( wEncoding, margin );
    fdOmitXMLHeader.right = new FormAttachment( 100, 0 );
    wOmitXMLHeader.setLayoutData( fdOmitXMLHeader );
    wOmitXMLHeader.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    wlOmitNullValues = new Label( gResult, SWT.RIGHT );
    wlOmitNullValues.setText( BaseMessages.getString( PKG, "XMLJoin.OmitNullValues.Label" ) );
    props.setLook( wlOmitNullValues );
    fdlOmitNullValues = new FormData();
    fdlOmitNullValues.left = new FormAttachment( 0, 0 );
    fdlOmitNullValues.top = new FormAttachment( wOmitXMLHeader, margin );
    fdlOmitNullValues.right = new FormAttachment( middle, -margin );
    wlOmitNullValues.setLayoutData( fdlOmitNullValues );
    wOmitNullValues = new Button( gResult, SWT.CHECK );
    props.setLook( wOmitNullValues );
    fdOmitNullValues = new FormData();
    fdOmitNullValues.left = new FormAttachment( middle, 0 );
    fdOmitNullValues.top = new FormAttachment( wOmitXMLHeader, margin );
    fdOmitNullValues.right = new FormAttachment( 100, 0 );
    wOmitNullValues.setLayoutData( fdOmitNullValues );
    wOmitNullValues.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    shell.layout();

    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel }, margin, gResult );

    // Add listeners
    lsOk = e -> ok();
    // lsMinWidth = new Listener() { public void handleEvent(Event e) { setMinimalWidth(); } };
    lsCancel = e -> cancel();

    wOk.addListener( SWT.Selection, lsOk );
    // wGet.addListener (SWT.Selection, lsGet );
    // wMinWidth.addListener (SWT.Selection, lsMinWidth );
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

    lsResize = event -> {
      // TODO - implement if necessary
    };
    shell.addListener( SWT.Resize, lsResize );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    input.setChanged( changed );

    List<TransformMeta> steps = pipelineMeta.findPreviousTransforms( pipelineMeta.findTransform( transformName ), true );
    for ( TransformMeta stepMeta : steps ) {
      wTargetXMLstep.add( stepMeta.getName() );
      wSourceXMLstep.add( stepMeta.getName() );
    }

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  public void setMinimalWidth() {
    // TODO - implement when necessary
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    logDebug( BaseMessages.getString( PKG, "AddXMLDialog.Log.GettingFieldsInfo" ) );

    if ( input.getTargetXmlStep() != null ) {
      wTargetXMLstep.setText( input.getTargetXmlStep() );
    }
    if ( input.getTargetXmlField() != null ) {
      wTargetXMLfield.setText( input.getTargetXmlField() );
    }
    if ( input.getSourceXmlStep() != null ) {
      wSourceXMLstep.setText( input.getSourceXmlStep() );
    }
    if ( input.getSourceXmlField() != null ) {
      wSourceXMLfield.setText( input.getSourceXmlField() );
    }
    if ( input.getValueXmlField() != null ) {
      wValueXMLfield.setText( input.getValueXmlField() );
    }
    if ( input.getTargetXPath() != null ) {
      wTargetXPath.setText( input.getTargetXPath() );
    }
    if ( input.getEncoding() != null ) {
      wEncoding.setText( input.getEncoding() );
    }
    if ( input.getJoinCompareField() != null ) {
      wJoinCompareField.setText( input.getJoinCompareField() );
    }

    wComplexJoin.setSelection( input.isComplexJoin() );
    wOmitXMLHeader.setSelection( input.isOmitXmlHeader() );
    wOmitNullValues.setSelection( input.isOmitNullValues() );

    if ( input.isComplexJoin() ) {
      wJoinCompareField.setEnabled( true );
    }

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;

    input.setChanged( backupChanged );

    dispose();
  }

  private void getInfo( XmlJoinMeta tfoi ) {
    tfoi.setTargetXmlStep( wTargetXMLstep.getText() );
    tfoi.setTargetXmlField( wTargetXMLfield.getText() );
    tfoi.setSourceXmlStep( wSourceXMLstep.getText() );
    tfoi.setSourceXmlField( wSourceXMLfield.getText() );
    tfoi.setValueXmlField( wValueXMLfield.getText() );
    tfoi.setTargetXPath( wTargetXPath.getText() );
    tfoi.setJoinCompareField( wJoinCompareField.getText() );
    tfoi.setComplexJoin( wComplexJoin.getSelection() );
    tfoi.setEncoding( wEncoding.getText() );
    tfoi.setOmitXmlHeader( wOmitXMLHeader.getSelection() );
    tfoi.setOmitNullValues( wOmitNullValues.getSelection() );
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    getInfo( input );

    dispose();
  }

  /*
   * private void get() { try { RowMetaInterface r = pipelineMeta.getPrevStepFields(stepname);
   * 
   * } catch(KettleException ke) { new ErrorDialog(shell, BaseMessages.getString(PKG,
   * "System.Dialog.GetFieldsFailed.Title"), BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"), ke);
   * }
   * 
   * }
   */

  private void setEncodings() {
    // Encoding of the text file:
    if ( !gotEncodings ) {
      gotEncodings = true;

      wEncoding.removeAll();
      List<Charset> values = new ArrayList<Charset>( Charset.availableCharsets().values() );
      for ( int i = 0; i < values.size(); i++ ) {
        Charset charSet = values.get( i );
        wEncoding.add( charSet.displayName() );
      }

      // Now select the default!
      String defEncoding = Const.getEnvironmentVariable( "file.encoding", "UTF-8" );
      int idx = Const.indexOfString( defEncoding, wEncoding.getItems() );
      if ( idx >= 0 ) {
        wEncoding.select( idx );
      } else {
        wEncoding.select( Const.indexOfString( "UTF-8", wEncoding.getItems() ) );
      }
    }
  }
}
