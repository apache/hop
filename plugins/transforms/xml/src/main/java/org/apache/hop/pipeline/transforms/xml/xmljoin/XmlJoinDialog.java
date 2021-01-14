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

package org.apache.hop.pipeline.transforms.xml.xmljoin;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
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
  private static final Class<?> PKG = XmlJoinMeta.class; // For Translator

  private Button wComplexJoin;

  private CCombo wTargetXmlTransform;

  private TextVar wTargetXmlField;

  private CCombo wSourceXmlTransform;

  private TextVar wSourceXmlField;

  private TextVar wValueXmlField;

  private TextVar wJoinCompareField;

  private TextVar wTargetXPath;

  private CCombo wEncoding;

  private Button wOmitXmlHeader;

  private Button wOmitNullValues;

  private final XmlJoinMeta input;

  // private Button wMinWidth;
  // private Listener lsMinWidth;

  private boolean gotEncodings = false;

  public XmlJoinDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
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
    shell.setText( BaseMessages.getString( PKG, "XmlJoin.DialogTitle" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Buttons at the bottom
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection,  e -> ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    setButtonPositions( new Button[] { wOk, wCancel }, margin, null);


    // Transform name line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "System.Label.TransformName" ) );
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
    Group gTarget = new Group(shell, SWT.NONE);
    gTarget.setText( BaseMessages.getString( PKG, "XmlJoin.TargetGroup.Label" ) );
    FormLayout targetLayout = new FormLayout();
    targetLayout.marginHeight = margin;
    targetLayout.marginWidth = margin;
    gTarget.setLayout( targetLayout );
    props.setLook(gTarget);
    FormData fdTarget = new FormData();
    fdTarget.left = new FormAttachment( 0, 0 );
    fdTarget.right = new FormAttachment( 100, 0 );
    fdTarget.top = new FormAttachment( wTransformName, 2 * margin );
    gTarget.setLayoutData(fdTarget);
    // Target XML transform line
    Label wlTargetXMLtransform = new Label(gTarget, SWT.RIGHT);
    wlTargetXMLtransform.setText( BaseMessages.getString( PKG, "XmlJoin.TargetXMLTransform.Label" ) );
    props.setLook(wlTargetXMLtransform);
    FormData fdlTargetXMLtransform = new FormData();
    fdlTargetXMLtransform.left = new FormAttachment( 0, 0 );
    fdlTargetXMLtransform.top = new FormAttachment( wTransformName, margin );
    fdlTargetXMLtransform.right = new FormAttachment( middle, -margin );
    wlTargetXMLtransform.setLayoutData(fdlTargetXMLtransform);
    wTargetXmlTransform = new CCombo(gTarget, SWT.BORDER | SWT.READ_ONLY );
    wTargetXmlTransform.setEditable( true );
    props.setLook( wTargetXmlTransform );
    wTargetXmlTransform.addModifyListener( lsMod );
    FormData fdTargetXMLtransform = new FormData();
    fdTargetXMLtransform.left = new FormAttachment( middle, 0 );
    fdTargetXMLtransform.top = new FormAttachment( wTransformName, margin );
    fdTargetXMLtransform.right = new FormAttachment( 100, 0 );
    wTargetXmlTransform.setLayoutData(fdTargetXMLtransform);

    // Target XML Field line
    Label wlTargetXMLfield = new Label(gTarget, SWT.RIGHT);
    wlTargetXMLfield.setText( BaseMessages.getString( PKG, "XmlJoin.TargetXMLField.Label" ) );
    props.setLook(wlTargetXMLfield);
    FormData fdlTargetXMLfield = new FormData();
    fdlTargetXMLfield.left = new FormAttachment( 0, 0 );
    fdlTargetXMLfield.right = new FormAttachment( middle, -margin );
    fdlTargetXMLfield.top = new FormAttachment( wTargetXmlTransform, margin );
    wlTargetXMLfield.setLayoutData(fdlTargetXMLfield);

    wTargetXmlField = new TextVar( variables, gTarget, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTargetXmlField );
    wTargetXmlField.addModifyListener( lsMod );
    FormData fdTargetXMLfield = new FormData();
    fdTargetXMLfield.left = new FormAttachment( middle, 0 );
    fdTargetXMLfield.top = new FormAttachment( wTargetXmlTransform, margin );
    fdTargetXMLfield.right = new FormAttachment( 100, -margin );
    wTargetXmlField.setLayoutData(fdTargetXMLfield);

    // Source Group
    Group gSource = new Group(shell, SWT.NONE);
    gSource.setText( BaseMessages.getString( PKG, "XmlJoin.SourceGroup.Label" ) );
    FormLayout SourceLayout = new FormLayout();
    SourceLayout.marginHeight = margin;
    SourceLayout.marginWidth = margin;
    gSource.setLayout( SourceLayout );
    props.setLook(gSource);
    FormData fdSource = new FormData();
    fdSource.left = new FormAttachment( 0, 0 );
    fdSource.right = new FormAttachment( 100, 0 );
    fdSource.top = new FormAttachment(gTarget, 2 * margin );
    gSource.setLayoutData(fdSource);
    // Source XML transform line
    Label wlSourceXMLtransform = new Label(gSource, SWT.RIGHT);
    wlSourceXMLtransform.setText( BaseMessages.getString( PKG, "XmlJoin.SourceXMLTransform.Label" ) );
    props.setLook(wlSourceXMLtransform);
    FormData fdlSourceXMLtransform = new FormData();
    fdlSourceXMLtransform.left = new FormAttachment( 0, 0 );
    fdlSourceXMLtransform.top = new FormAttachment( wTargetXmlField, margin );
    fdlSourceXMLtransform.right = new FormAttachment( middle, -margin );
    wlSourceXMLtransform.setLayoutData(fdlSourceXMLtransform);
    wSourceXmlTransform = new CCombo(gSource, SWT.BORDER | SWT.READ_ONLY );
    wSourceXmlTransform.setEditable( true );
    props.setLook( wSourceXmlTransform );
    wSourceXmlTransform.addModifyListener( lsMod );
    FormData fdSourceXMLtransform = new FormData();
    fdSourceXMLtransform.left = new FormAttachment( middle, 0 );
    fdSourceXMLtransform.top = new FormAttachment( wTargetXmlField, margin );
    fdSourceXMLtransform.right = new FormAttachment( 100, 0 );
    wSourceXmlTransform.setLayoutData(fdSourceXMLtransform);

    // Source XML Field line
    Label wlSourceXMLfield = new Label(gSource, SWT.RIGHT);
    wlSourceXMLfield.setText( BaseMessages.getString( PKG, "XmlJoin.SourceXMLField.Label" ) );
    props.setLook(wlSourceXMLfield);
    FormData fdlSourceXMLfield = new FormData();
    fdlSourceXMLfield.left = new FormAttachment( 0, 0 );
    fdlSourceXMLfield.right = new FormAttachment( middle, -margin );
    fdlSourceXMLfield.top = new FormAttachment( wSourceXmlTransform, margin );
    wlSourceXMLfield.setLayoutData(fdlSourceXMLfield);

    wSourceXmlField = new TextVar( variables, gSource, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSourceXmlField );
    wSourceXmlField.addModifyListener( lsMod );
    FormData fdSourceXMLfield = new FormData();
    fdSourceXMLfield.left = new FormAttachment( middle, 0 );
    fdSourceXMLfield.top = new FormAttachment( wSourceXmlTransform, margin );
    fdSourceXMLfield.right = new FormAttachment( 100, -margin );
    wSourceXmlField.setLayoutData(fdSourceXMLfield);

    // Join Group
    Group gJoin = new Group(shell, SWT.NONE);
    gJoin.setText( BaseMessages.getString( PKG, "XmlJoin.JoinGroup.Label" ) );
    FormLayout JoinLayout = new FormLayout();
    JoinLayout.marginHeight = margin;
    JoinLayout.marginWidth = margin;
    gJoin.setLayout( JoinLayout );
    props.setLook(gJoin);
    FormData fdJoin = new FormData();
    fdJoin.left = new FormAttachment( 0, 0 );
    fdJoin.right = new FormAttachment( 100, 0 );
    fdJoin.top = new FormAttachment(gSource, 2 * margin );
    gJoin.setLayoutData(fdJoin);

    // Target XPath line
    Label wlTargetXPath = new Label(gJoin, SWT.RIGHT);
    wlTargetXPath.setText( BaseMessages.getString( PKG, "XmlJoin.TargetXPath.Label" ) );
    props.setLook(wlTargetXPath);
    FormData fdlTargetXPath = new FormData();
    fdlTargetXPath.left = new FormAttachment( 0, 0 );
    fdlTargetXPath.right = new FormAttachment( middle, -margin );
    fdlTargetXPath.top = new FormAttachment( wSourceXmlField, margin );
    wlTargetXPath.setLayoutData(fdlTargetXPath);

    wTargetXPath = new TextVar( variables, gJoin, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTargetXPath );
    wTargetXPath.addModifyListener( lsMod );
    FormData fdTargetXPath = new FormData();
    fdTargetXPath.left = new FormAttachment( middle, 0 );
    fdTargetXPath.top = new FormAttachment( wSourceXmlField, margin );
    fdTargetXPath.right = new FormAttachment( 100, -margin );
    wTargetXPath.setLayoutData(fdTargetXPath);

    // Complex Join Line
    Label wlComplexJoin = new Label(gJoin, SWT.RIGHT);
    wlComplexJoin.setText( BaseMessages.getString( PKG, "XmlJoin.ComplexJoin.Label" ) );
    props.setLook(wlComplexJoin);
    FormData fdlComplexJoin = new FormData();
    fdlComplexJoin.left = new FormAttachment( 0, 0 );
    fdlComplexJoin.top = new FormAttachment( wTargetXPath, margin );
    fdlComplexJoin.right = new FormAttachment( middle, -margin );
    wlComplexJoin.setLayoutData(fdlComplexJoin);
    wComplexJoin = new Button(gJoin, SWT.CHECK );
    props.setLook( wComplexJoin );
    FormData fdComplexJoin = new FormData();
    fdComplexJoin.left = new FormAttachment( middle, 0 );
    fdComplexJoin.top = new FormAttachment( wlComplexJoin, 0, SWT.CENTER );
    fdComplexJoin.right = new FormAttachment( 100, 0 );
    wComplexJoin.setLayoutData(fdComplexJoin);
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
    Label wlJoinCompareField = new Label(gJoin, SWT.RIGHT);
    wlJoinCompareField.setText( BaseMessages.getString( PKG, "XmlJoin.JoinCompareFiled.Label" ) );
    props.setLook(wlJoinCompareField);
    FormData fdlJoinCompareField = new FormData();
    fdlJoinCompareField.left = new FormAttachment( 0, 0 );
    fdlJoinCompareField.right = new FormAttachment( middle, -margin );
    fdlJoinCompareField.top = new FormAttachment( wComplexJoin, margin );
    wlJoinCompareField.setLayoutData(fdlJoinCompareField);

    wJoinCompareField = new TextVar( variables, gJoin, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wJoinCompareField );
    wJoinCompareField.addModifyListener( lsMod );
    FormData fdJoinCompareField = new FormData();
    fdJoinCompareField.left = new FormAttachment( middle, 0 );
    fdJoinCompareField.top = new FormAttachment( wComplexJoin, margin );
    fdJoinCompareField.right = new FormAttachment( 100, -margin );
    wJoinCompareField.setLayoutData(fdJoinCompareField);
    wJoinCompareField.setEnabled( false );

    // Result Group
    Group gResult = new Group(shell, SWT.NONE);
    gResult.setText( BaseMessages.getString( PKG, "XmlJoin.ResultGroup.Label" ) );
    FormLayout ResultLayout = new FormLayout();
    ResultLayout.marginHeight = margin;
    ResultLayout.marginWidth = margin;
    gResult.setLayout( ResultLayout );
    props.setLook(gResult);
    FormData fdResult = new FormData();
    fdResult.left = new FormAttachment( 0, 0 );
    fdResult.right = new FormAttachment( 100, 0 );
    fdResult.top = new FormAttachment(gJoin, 2 * margin );
    fdResult.bottom = new FormAttachment( wOk, -2*margin);
    gResult.setLayoutData(fdResult);

    // Value XML Field line
    Label wlValueXmlField = new Label(gResult, SWT.RIGHT);
    wlValueXmlField.setText( BaseMessages.getString( PKG, "XmlJoin.ValueXMLField.Label" ) );
    props.setLook(wlValueXmlField);
    FormData fdlValueXmlField = new FormData();
    fdlValueXmlField.left = new FormAttachment( 0, 0 );
    fdlValueXmlField.right = new FormAttachment( middle, -margin );
    fdlValueXmlField.top = new FormAttachment( wJoinCompareField, margin );
    wlValueXmlField.setLayoutData(fdlValueXmlField);
    wValueXmlField = new TextVar( variables, gResult, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wValueXmlField );
    wValueXmlField.addModifyListener( lsMod );
    FormData fdValueXMLfield = new FormData();
    fdValueXMLfield.left = new FormAttachment( middle, 0 );
    fdValueXMLfield.top = new FormAttachment( wJoinCompareField, margin );
    fdValueXMLfield.right = new FormAttachment( 100, -margin );
    wValueXmlField.setLayoutData(fdValueXMLfield);

    // Encoding Line
    Label wlEncoding = new Label(gResult, SWT.RIGHT);
    wlEncoding.setText( BaseMessages.getString( PKG, "XmlJoin.Encoding.Label" ) );
    props.setLook(wlEncoding);
    FormData fdlEncoding = new FormData();
    fdlEncoding.left = new FormAttachment( 0, 0 );
    fdlEncoding.top = new FormAttachment( wValueXmlField, margin );
    fdlEncoding.right = new FormAttachment( middle, -margin );
    wlEncoding.setLayoutData(fdlEncoding);
    wEncoding = new CCombo(gResult, SWT.BORDER | SWT.READ_ONLY );
    wEncoding.setEditable( true );
    props.setLook( wEncoding );
    wEncoding.addModifyListener( lsMod );
    FormData fdEncoding = new FormData();
    fdEncoding.left = new FormAttachment( middle, 0 );
    fdEncoding.top = new FormAttachment( wValueXmlField, margin );
    fdEncoding.right = new FormAttachment( 100, 0 );
    wEncoding.setLayoutData(fdEncoding);
    wEncoding.addFocusListener( new FocusListener() {
      public void focusLost( FocusEvent e ) {
      }

      public void focusGained( FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        setEncodings();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    // Complex Join Line
    Label wlOmitXMLHeader = new Label(gResult, SWT.RIGHT);
    wlOmitXMLHeader.setText( BaseMessages.getString( PKG, "XmlJoin.OmitXMLHeader.Label" ) );
    props.setLook(wlOmitXMLHeader);
    FormData fdlOmitXMLHeader = new FormData();
    fdlOmitXMLHeader.left = new FormAttachment( 0, 0 );
    fdlOmitXMLHeader.top = new FormAttachment( wEncoding, margin );
    fdlOmitXMLHeader.right = new FormAttachment( middle, -margin );
    wlOmitXMLHeader.setLayoutData(fdlOmitXMLHeader);
    wOmitXmlHeader = new Button(gResult, SWT.CHECK );
    props.setLook( wOmitXmlHeader );
    FormData fdOmitXMLHeader = new FormData();
    fdOmitXMLHeader.left = new FormAttachment( middle, 0 );
    fdOmitXMLHeader.top = new FormAttachment( wlOmitXMLHeader, 0, SWT.CENTER );
    fdOmitXMLHeader.right = new FormAttachment( 100, 0 );
    wOmitXmlHeader.setLayoutData(fdOmitXMLHeader);
    wOmitXmlHeader.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    Label wlOmitNullValues = new Label(gResult, SWT.RIGHT);
    wlOmitNullValues.setText( BaseMessages.getString( PKG, "XmlJoin.OmitNullValues.Label" ) );
    props.setLook(wlOmitNullValues);
    FormData fdlOmitNullValues = new FormData();
    fdlOmitNullValues.left = new FormAttachment( 0, 0 );
    fdlOmitNullValues.top = new FormAttachment( wOmitXmlHeader, margin );
    fdlOmitNullValues.right = new FormAttachment( middle, -margin );
    wlOmitNullValues.setLayoutData(fdlOmitNullValues);
    wOmitNullValues = new Button(gResult, SWT.CHECK );
    props.setLook( wOmitNullValues );
    FormData fdOmitNullValues = new FormData();
    fdOmitNullValues.left = new FormAttachment( middle, 0 );
    fdOmitNullValues.top = new FormAttachment( wlOmitNullValues, 0, SWT.CENTER );
    fdOmitNullValues.right = new FormAttachment( 100, 0 );
    wOmitNullValues.setLayoutData(fdOmitNullValues);
    wOmitNullValues.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    shell.layout();

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

    List<TransformMeta> transforms = pipelineMeta.findPreviousTransforms( pipelineMeta.findTransform( transformName ), true );
    for ( TransformMeta transformMeta : transforms ) {
      wTargetXmlTransform.add( transformMeta.getName() );
      wSourceXmlTransform.add( transformMeta.getName() );
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

    wTargetXmlTransform.setText( Const.NVL(input.getTargetXmlTransform(), "") );
    wTargetXmlField.setText( Const.NVL(input.getTargetXmlField(), "") );
    wSourceXmlTransform.setText( Const.NVL(input.getSourceXmlTransform(), "") );
    wSourceXmlField.setText( Const.NVL(input.getSourceXmlField(), "") );
    wValueXmlField.setText( Const.NVL(input.getValueXmlField(), "") );
    wTargetXPath.setText( Const.NVL(input.getTargetXPath(), "") );
    wEncoding.setText( Const.NVL(input.getEncoding(), "") );
    wJoinCompareField.setText( Const.NVL(input.getJoinCompareField(), "") );

    wComplexJoin.setSelection( input.isComplexJoin() );
    wOmitXmlHeader.setSelection( input.isOmitXmlHeader() );
    wOmitNullValues.setSelection( input.isOmitNullValues() );

    wJoinCompareField.setEnabled( input.isComplexJoin() );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;

    input.setChanged( backupChanged );

    dispose();
  }

  private void getInfo( XmlJoinMeta tfoi ) {
    tfoi.setTargetXmlTransform( wTargetXmlTransform.getText() );
    tfoi.setTargetXmlField( wTargetXmlField.getText() );
    tfoi.setSourceXmlTransform( wSourceXmlTransform.getText() );
    tfoi.setSourceXmlField( wSourceXmlField.getText() );
    tfoi.setValueXmlField( wValueXmlField.getText() );
    tfoi.setTargetXPath( wTargetXPath.getText() );
    tfoi.setJoinCompareField( wJoinCompareField.getText() );
    tfoi.setComplexJoin( wComplexJoin.getSelection() );
    tfoi.setEncoding( wEncoding.getText() );
    tfoi.setOmitXmlHeader( wOmitXmlHeader.getSelection() );
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

  private void setEncodings() {
    // Encoding of the text file:
    if ( !gotEncodings ) {
      gotEncodings = true;

      wEncoding.removeAll();
      List<Charset> values = new ArrayList<>(Charset.availableCharsets().values());
      for (Charset charSet : values) {
        wEncoding.add(charSet.displayName());
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
