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

package org.apache.hop.pipeline.transforms.webservices;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.webservices.wsdl.ComplexType;
import org.apache.hop.pipeline.transforms.webservices.wsdl.Wsdl;
import org.apache.hop.pipeline.transforms.webservices.wsdl.WsdlOpParameter;
import org.apache.hop.pipeline.transforms.webservices.wsdl.WsdlOpParameter.ParameterMode;
import org.apache.hop.pipeline.transforms.webservices.wsdl.WsdlOpParameterContainer;
import org.apache.hop.pipeline.transforms.webservices.wsdl.WsdlOpParameterList;
import org.apache.hop.pipeline.transforms.webservices.wsdl.WsdlOperation;
import org.apache.hop.pipeline.transforms.webservices.wsdl.WsdlOperationContainer;
import org.apache.hop.pipeline.transforms.webservices.wsdl.WsdlParamContainer;
import org.apache.hop.pipeline.transforms.webservices.wsdl.XsdType;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import javax.xml.namespace.QName;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class WebServiceDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = WebServiceMeta.class; // For Translator

  private WebServiceMeta meta;

  private CTabFolder wTabFolder;

  private TextVar wURL;

  private CCombo wOperation;

  private Text wOperationRequest;

  private Label wlTransform;
  private Text wTransform;

  private Button wPassInputData;

  private Button wCompatible;

  private TextVar wRepeatingElement;

  private Button wReplyAsString;

  private TextVar wHttpLogin;

  private TextVar wHttpPassword;

  private TextVar wProxyHost;

  private TextVar wProxyPort;

  /**
   * The input fields
   */
  private TableView fieldInTableView;

  /**
   * The output fields
   */
  private TableView fieldOutTableView;

  /**
   * input fields tab item
   */
  private CTabItem tabItemFieldIn;

  /**
   * output fields tab item
   */
  private CTabItem tabItemFieldOut;

  /**
   * WSDL
   */
  private Wsdl wsdl;

  private WsdlOperation wsdlOperation;
  private WsdlParamContainer inWsdlParamContainer;
  private WsdlParamContainer outWsdlParamContainer;

  private final ModifyListener lsMod = e -> meta.setChanged();

  private void selectWSDLOperation( String anOperationName ) throws HopException {
    // Tab management
    //
    loadOperation( anOperationName );

    // We close all tabs and reconstruct it all to make sure we always show the correct data
    //
    if ( inWsdlParamContainer != null ) {
      wTransform.setVisible( true );
      wlTransform.setVisible( true );
      if ( !inWsdlParamContainer.isArray() ) {
        wTransform.setText( "1" );
      }
      addTabFieldIn();
      setComboValues();
    } else {
      wTransform.setText( "1" );

      removeTabField( tabItemFieldIn );
      tabItemFieldIn = null;
    }
    if ( outWsdlParamContainer != null ) {
      addTabFieldOut();
    } else {
      removeTabField( tabItemFieldOut );
      tabItemFieldOut = null;
    }
  }

  private void loadWebService( String anURI ) throws HopException {
    anURI = variables.resolve( anURI );

    try {
      if ( wProxyHost.getText() != null && !"".equals( wProxyHost.getText() ) ) {
        Properties systemProperties = System.getProperties();
        systemProperties.setProperty( "http.proxyHost", variables.resolve( wProxyHost.getText() ) );
        systemProperties.setProperty( "http.proxyPort", variables.resolve( wProxyPort.getText() ) );
      }
      wsdl = new Wsdl( new URI( anURI ), null, null, wHttpLogin.getText(), wHttpPassword.getText() );
    } catch ( Exception e ) {
      wsdl = null;
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "WebServiceDialog.ERROR0009.UnreachableURI" ), BaseMessages
        .getString( PKG, "WebServiceDialog.ErrorDialog.Title" )
        + anURI, e );

      log.logError( BaseMessages.getString( PKG, "WebServiceDialog.ErrorDialog.Title" ) + anURI, e.getMessage() );
      return;
    }
    String text = wOperation.getText();
    wOperation.removeAll();
    if ( wsdl != null ) {
      List<WsdlOperation> listeOperations = wsdl.getOperations();
      Collections.sort( listeOperations, ( op1, op2 ) -> op1.getOperationQName().getLocalPart().compareTo( op2.getOperationQName().getLocalPart() ) );
      for ( WsdlOperation op : listeOperations ) {
        wOperation.add( op.getOperationQName().getLocalPart() );
        if ( op.getOperationQName().getLocalPart().equals( text ) ) {
          wOperation.setText( text );
        }
      }
    }

  }

  private void loadOperation( String anOperationName ) throws HopException {
    wsdlOperation = null;
    inWsdlParamContainer = null;
    outWsdlParamContainer = null;
    if ( wsdl != null ) {
      for ( Iterator<WsdlOperation> vItOperation = wsdl.getOperations().iterator(); vItOperation.hasNext()
        && wsdlOperation == null; ) {
        WsdlOperation vCurrentOperation = vItOperation.next();
        if ( vCurrentOperation.getOperationQName().getLocalPart().equals( anOperationName ) ) {
          wsdlOperation = vCurrentOperation;
        }
      }
    }

    if ( wsdlOperation != null ) {
      // figure out the request name
      //
      String request = "";
      WsdlOpParameterList parameters = wsdlOperation.getParameters();
      if ( parameters != null
        && parameters.getOperation() != null && parameters.getOperation().getInput() != null
        && parameters.getOperation().getInput().getName() != null ) {
        request = wsdlOperation.getParameters().getOperation().getInput().getName().toString();
      }
      wOperationRequest.setText( request );

      for ( int cpt = 0; cpt < wsdlOperation.getParameters().size(); cpt++ ) {
        WsdlOpParameter param = wsdlOperation.getParameters().get( cpt );
        if ( param.isArray() ) {
          // setInFieldArgumentName(param.getName().getLocalPart());
          if ( param.getItemXmlType() != null ) {
            ComplexType type = param.getItemComplexType();
            if ( type != null ) {
              for ( String attributeName : type.listObjectNames() ) {
                QName attributeType = type.getElementType( attributeName );
                if ( !WebServiceMeta.XSD_NS_URI.equals( attributeType.getNamespaceURI() ) ) {
                  throw new HopTransformException( BaseMessages.getString(
                    PKG, "WebServiceDialog.ERROR0007.UnsupporteOperation.ComplexType" ) );
                }
              }
            }
            if ( ParameterMode.IN.equals( param.getMode() )
              || ParameterMode.INOUT.equals( param.getMode() )
              || ParameterMode.UNDEFINED.equals( param.getMode() ) ) {
              if ( inWsdlParamContainer != null ) {
                throw new HopTransformException( BaseMessages.getString(
                  PKG, "WebServiceDialog.ERROR0006.UnsupportedOperation.MultipleArrays" ) );
              } else {
                inWsdlParamContainer = new WsdlOpParameterContainer( param );
              }
            } else if ( ParameterMode.OUT.equals( param.getMode() )
              || ParameterMode.INOUT.equals( param.getMode() )
              || ParameterMode.UNDEFINED.equals( param.getMode() ) ) {
              if ( outWsdlParamContainer != null ) {
                throw new HopTransformException( BaseMessages.getString(
                  PKG, "WebServiceDialog.ERROR0006.UnsupportedOperation.MultipleArrays" ) );
              } else {
                outWsdlParamContainer = new WsdlOpParameterContainer( param );
              }
            }
          }
        } else {
          if ( ParameterMode.IN.equals( param.getMode() )
            || ParameterMode.INOUT.equals( param.getMode() ) || ParameterMode.UNDEFINED.equals( param.getMode() ) ) {
            if ( inWsdlParamContainer != null && !( inWsdlParamContainer instanceof WsdlOperationContainer ) ) {
              throw new HopTransformException( BaseMessages.getString(
                PKG, "WebServiceDialog.ERROR0008.UnsupportedOperation.IncorrectParams" ) );
            } else {
              inWsdlParamContainer = new WsdlOperationContainer( wsdlOperation, param.getMode() );
            }
          } else if ( ParameterMode.OUT.equals( param.getMode() )
            || ParameterMode.INOUT.equals( param.getMode() ) || ParameterMode.UNDEFINED.equals( param.getMode() ) ) {
            if ( outWsdlParamContainer != null && !( outWsdlParamContainer instanceof WsdlOperationContainer ) ) {
              throw new HopTransformException( BaseMessages.getString(
                PKG, "WebServiceDialog.ERROR0008.UnsupportedOperation.IncorrectParams" ) );
            } else {
              outWsdlParamContainer = new WsdlOperationContainer( wsdlOperation, param.getMode() );
            }
          } else {
            System.out.println( "Parameter : "
              + param.getName().getLocalPart() + ", mode=" + param.getMode().toString() + ", is not considered" );
          }
        }
      }
      if ( wsdlOperation.getReturnType() != null ) {
        outWsdlParamContainer = new WsdlOpParameterContainer( (WsdlOpParameter) wsdlOperation.getReturnType() );
        if ( wsdlOperation.getReturnType().isArray() ) {
          if ( wsdlOperation.getReturnType().getItemXmlType() != null ) {
            ComplexType type = wsdlOperation.getReturnType().getItemComplexType();
            if ( type != null ) {
              for ( String attributeName : type.listObjectNames() ) {
                QName attributeType = type.getElementType( attributeName );
                if ( !WebServiceMeta.XSD_NS_URI.equals( attributeType.getNamespaceURI() ) ) {
                  throw new HopTransformException( BaseMessages.getString(
                    PKG, "WebServiceDialog.ERROR0007.UnsupportedOperation.ComplexType" ) );
                }
              }
            }
          }
        }
      }
    }
  }

  /**
   * Initialization of the tree: - construction using the URL of the WS - add selection listeners to the tree
   *
   * @throws HopException
   */
  private void initTreeTabWebService( String anURI ) throws HopException {
    String text = wOperation.getText();

    loadWebService( anURI );

    selectWSDLOperation( text );

    if ( wsdlOperation != null ) {
      wOperation.setText( text );
    }
  }

  private void addTabFieldIn() {
    TableView oldTableView = fieldInTableView;
    int margin = props.getMargin();

    Composite vCompositeTabField = new Composite( wTabFolder, SWT.NONE );
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    vCompositeTabField.setLayout( formLayout );
    props.setLook( vCompositeTabField );

    if ( tabItemFieldIn == null ) {
      tabItemFieldIn = new CTabItem( wTabFolder, SWT.NONE );
    }
    final ColumnInfo fieldColumn =
      new ColumnInfo(
        BaseMessages.getString( PKG, "WebServiceDialog.NameColumn.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        new String[] {}, false );
    fieldColumns.add( fieldColumn );
    ColumnInfo[] colinf =
      new ColumnInfo[] {
        fieldColumn,
        new ColumnInfo(
          BaseMessages.getString( PKG, "WebServiceDialog.WsNameColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "WebServiceDialog.TypeColumn.Column" ), ColumnInfo.COLUMN_TYPE_TEXT,
          false ), };
    fieldInTableView =
      new TableView( variables, vCompositeTabField, SWT.FULL_SELECTION | SWT.MULTI, colinf, 1, lsMod, props );
    fieldInTableView.setReadonly( false );
    fieldInTableView.clearAll();
    String containerName =
      inWsdlParamContainer == null ? meta.getInFieldContainerName() : inWsdlParamContainer.getContainerName();
    tabItemFieldIn.setText( containerName == null ? "in" : containerName );

    Button vButton = new Button( vCompositeTabField, SWT.NONE );
    vButton.setText( BaseMessages.getString( PKG, "System.Button.GetFields" ) );
    vButton.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent event ) {
        if ( inWsdlParamContainer == null ) {
          try {
            loadWebService( wURL.getText() );
            loadOperation( wOperation.getText() );
          } catch ( HopException e ) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
        IRowMeta r = getInWebServiceFields();
        if ( r != null ) {
          BaseTransformDialog.getFieldsFromPrevious(
            r, fieldInTableView, 2, new int[] { 2 }, new int[] {}, -1, -1, null );
        }
        // Define type for new entries
        if ( inWsdlParamContainer != null ) {
          TableItem[] items = fieldInTableView.table.getItems();
          for ( TableItem item : items ) {
            String type = inWsdlParamContainer.getParamType( item.getText( 2 ) );
            if ( type != null ) {
              item.setText( 3, type );
            } else {
              item.dispose();
            }
          }
        }
      }
    } );

    Button[] buttons = new Button[] { vButton };
    BaseTransformDialog.positionBottomButtons( vCompositeTabField, buttons, props.getMargin(), null );

    FormData fdTable = new FormData();
    fdTable.left = new FormAttachment( 0, 0 );
    fdTable.top = new FormAttachment( 0, margin );
    fdTable.right = new FormAttachment( 100, 0 );
    fdTable.bottom = new FormAttachment( vButton, 0 );
    fieldInTableView.setLayoutData( fdTable );

    FormData fdInComp = new FormData();
    fdInComp.left = new FormAttachment( 0, 0 );
    fdInComp.top = new FormAttachment( 0, 0 );
    fdInComp.right = new FormAttachment( 100, 0 );
    fdInComp.bottom = new FormAttachment( 100, 0 );
    vCompositeTabField.setLayoutData( fdInComp );

    vCompositeTabField.layout();

    tabItemFieldIn.setControl( vCompositeTabField );

    if ( inWsdlParamContainer != null ) {
      IRowMeta r = getInWebServiceFields();
      for ( int i = 0; i < r.size(); ++i ) {
        String wsName = r.getValueMeta( i ).getName();
        TableItem vTableItem = new TableItem( fieldInTableView.table, SWT.NONE );
        vTableItem.setText( 2, Const.NVL( wsName, "" ) );
        vTableItem.setText( 3, Const.NVL( inWsdlParamContainer.getParamType( wsName ), "" ) );

        if ( oldTableView != null ) {
          TableItem[] oldItems = oldTableView.table.getItems();
          String previousField = getField( oldItems, wsName );
          if ( previousField != null ) {
            vTableItem.setText( 1, previousField );
          }
        }
      }
    }
    if ( oldTableView != null ) {
      oldTableView.dispose();
    }
    fieldInTableView.removeEmptyRows();
    fieldInTableView.setRowNums();
    fieldInTableView.optWidth( true );
  }

  private String getField( TableItem[] items, String wsName ) {
    if ( wsName == null ) {
      return null;
    }

    String ret = null;
    for ( int i = 0; i < items.length && ret == null; i++ ) {
      if ( items[ i ].getText( 2 ).equals( wsName ) ) {
        ret = items[ i ].getText( 1 );
      }
    }
    return ret;
  }

  private void addTabFieldOut() {
    TableView oldTableView = fieldOutTableView;
    int margin = props.getMargin();

    // Initialization of the output tab
    //
    Composite vCompositeTabFieldOut = new Composite( wTabFolder, SWT.NONE );
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    vCompositeTabFieldOut.setLayout( formLayout );
    props.setLook( vCompositeTabFieldOut );

    if ( tabItemFieldOut == null ) {
      tabItemFieldOut = new CTabItem( wTabFolder, SWT.NONE );
    }
    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "WebServiceDialog.NameColumn.Column" ), ColumnInfo.COLUMN_TYPE_TEXT,
          false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "WebServiceDialog.WsNameColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "WebServiceDialog.TypeColumn.Column" ), ColumnInfo.COLUMN_TYPE_TEXT,
          false ) };
    fieldOutTableView =
      new TableView( variables, vCompositeTabFieldOut, SWT.FULL_SELECTION | SWT.MULTI, colinf, 1, lsMod, props );
    String outContainerName =
      outWsdlParamContainer == null ? meta.getOutFieldContainerName() : outWsdlParamContainer.getContainerName();
    tabItemFieldOut.setText( outContainerName == null ? "out" : outContainerName );
    fieldOutTableView.setReadonly( false );

    Button vButton = new Button( vCompositeTabFieldOut, SWT.NONE );
    vButton.setText( BaseMessages.getString( PKG, "System.Button.GetFields" ) );
    vButton.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent event ) {
        if ( outWsdlParamContainer == null ) {
          try {
            loadWebService( wURL.getText() );
            loadOperation( wOperation.getText() );
          } catch ( HopException e ) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
        IRowMeta r = getOutWebServiceFields();
        if ( r != null ) {
          BaseTransformDialog.getFieldsFromPrevious(
            r, fieldOutTableView, 2, new int[] { 1, 2 }, new int[] {}, -1, -1, null );
        }
        // Define type for new entries
        if ( outWsdlParamContainer != null ) {
          TableItem[] items = fieldOutTableView.table.getItems();
          for ( TableItem item : items ) {
            item.setText( 3, outWsdlParamContainer.getParamType( item.getText( 2 ) ) );
          }
        }
      }
    } );
    Button[] buttons = new Button[] { vButton };
    BaseTransformDialog.positionBottomButtons( vCompositeTabFieldOut, buttons, props.getMargin(), null );

    FormData fdTable = new FormData();
    fdTable.left = new FormAttachment( 0, 0 );
    fdTable.top = new FormAttachment( 0, margin );
    fdTable.right = new FormAttachment( 100, 0 );
    fdTable.bottom = new FormAttachment( vButton, 0 );
    fieldOutTableView.setLayoutData( fdTable );

    FormData fdInComp = new FormData();
    fdInComp.left = new FormAttachment( 0, 0 );
    fdInComp.top = new FormAttachment( 0, 0 );
    fdInComp.right = new FormAttachment( 100, 0 );
    fdInComp.bottom = new FormAttachment( 100, 0 );
    vCompositeTabFieldOut.setLayoutData( fdInComp );

    vCompositeTabFieldOut.layout();

    tabItemFieldOut.setControl( vCompositeTabFieldOut );

    if ( fieldOutTableView.table.getItemCount() == 0 && outWsdlParamContainer != null ) {
      IRowMeta r = getOutWebServiceFields();
      for ( int i = 0; i < r.size(); ++i ) {
        String wsName = r.getValueMeta( i ).getName();
        String wsType = r.getValueMeta( i ).getTypeDesc();

        TableItem vTableItem = new TableItem( fieldOutTableView.table, SWT.NONE );
        vTableItem.setText( 2, wsName );
        vTableItem.setText( 3, wsType );
        if ( oldTableView != null ) {
          String previousField = getField( oldTableView.table.getItems(), wsName );
          if ( previousField != null && !"".equals( previousField ) ) {
            vTableItem.setText( 1, previousField );
          } else {
            vTableItem.setText( 1, wsName );
          }
        } else {
          vTableItem.setText( 1, wsName );
        }
      }
    }
    fieldOutTableView.removeEmptyRows();
    fieldOutTableView.setRowNums();
    fieldOutTableView.optWidth( true );
  }

  private IRowMeta getInWebServiceFields() {
    IRowMeta r = null;
    if ( inWsdlParamContainer != null ) {
      r = new RowMeta();
      String[] params = inWsdlParamContainer.getParamNames();
      // If we have already saved fields mapping, we only show these mappings
      for ( String param : params ) {
        IValueMeta value =
          new ValueMetaBase( param, XsdType.xsdTypeToHopType( inWsdlParamContainer
            .getParamType( param ) ) );
        r.addValueMeta( value );
      }
    }
    return r;
  }

  private IRowMeta getOutWebServiceFields() {
    IRowMeta r = null;
    if ( outWsdlParamContainer != null ) {
      r = new RowMeta();
      String[] outParams = outWsdlParamContainer.getParamNames();
      // If we have already saved fields mapping, we only show these mappings
      for ( String outParam : outParams ) {
        IValueMeta value =
          new ValueMetaBase( outParam, XsdType.xsdTypeToHopType( outWsdlParamContainer
            .getParamType( outParam ) ) );
        r.addValueMeta( value );
      }
    }
    return r;
  }

  private void removeTabField( CTabItem tab ) {
    if ( tab != null ) {
      tab.dispose();
      tab = null;
    }
  }

  /**
   * Here we populate the dialog using the incoming web services meta data
   */
  private void getData() {
    wTransformName.setText( transformName );

    wURL.setText( meta.getUrl() == null ? "" : meta.getUrl() );
    wProxyHost.setText( meta.getProxyHost() == null ? "" : meta.getProxyHost() );
    wProxyPort.setText( meta.getProxyPort() == null ? "" : meta.getProxyPort() );
    wHttpLogin.setText( meta.getHttpLogin() == null ? "" : meta.getHttpLogin() );
    wHttpPassword.setText( meta.getHttpPassword() == null ? "" : meta.getHttpPassword() );
    wTransform.setText( Integer.toString( meta.getCallTransform() ) );
    wPassInputData.setSelection( meta.isPassingInputData() );
    wCompatible.setSelection( meta.isCompatible() );
    wRepeatingElement.setText( Const.NVL( meta.getRepeatingElementName(), "" ) );
    wReplyAsString.setSelection( meta.isReturningReplyAsString() );

    if ( wURL.getText() != null && !"".equals( wURL.getText() ) ) {
      wOperation.setText( meta.getOperationName() == null ? "" : meta.getOperationName() );
    }
    wOperationRequest.setText( Const.NVL( meta.getOperationRequestName(), "" ) );
    if ( meta.getInFieldContainerName() != null
      || meta.getInFieldArgumentName() != null || !meta.getFieldsIn().isEmpty() ) {
      addTabFieldIn();

      for ( WebServiceField field : meta.getFieldsIn() ) {
        TableItem vTableItem = new TableItem( fieldInTableView.table, SWT.NONE );
        if ( field.getName() != null ) {
          vTableItem.setText( 1, field.getName() );
        }
        vTableItem.setText( 2, field.getWsName() );
        vTableItem.setText( 3, field.getXsdType() );
      }

      fieldInTableView.removeEmptyRows();
      fieldInTableView.setRowNums();
      fieldInTableView.optWidth( true );
    }
    if ( !meta.getFieldsOut().isEmpty() ) {
      addTabFieldOut();

      for ( WebServiceField field : meta.getFieldsOut() ) {
        TableItem vTableItem = new TableItem( fieldOutTableView.table, SWT.NONE );
        if ( field.getName() != null ) {
          vTableItem.setText( 1, field.getName() );
        }
        vTableItem.setText( 2, field.getWsName() );
        vTableItem.setText( 3, field.getXsdType() );
      }
      fieldOutTableView.removeEmptyRows();
      fieldOutTableView.setRowNums();
      fieldOutTableView.optWidth( true );
    }
  }

  /**
   * Save the data and close the dialog
   */
  private void getInfo( WebServiceMeta webServiceMeta ) {
    webServiceMeta.setUrl( wURL.getText() );
    webServiceMeta.setProxyHost( wProxyHost.getText() );
    webServiceMeta.setProxyPort( wProxyPort.getText() );
    webServiceMeta.setHttpLogin( wHttpLogin.getText() );
    webServiceMeta.setHttpPassword( wHttpPassword.getText() );
    webServiceMeta.setCallTransform( Const.toInt( wTransform.getText(), WebServiceMeta.DEFAULT_TRANSFORM ) );
    webServiceMeta.setPassingInputData( wPassInputData.getSelection() );
    webServiceMeta.setCompatible( wCompatible.getSelection() );
    webServiceMeta.setRepeatingElementName( wRepeatingElement.getText() );
    webServiceMeta.setReturningReplyAsString( wReplyAsString.getSelection() );
    webServiceMeta.setOperationRequestName( wOperationRequest.getText() );
    webServiceMeta.setOperationName( wOperation.getText() );

    if ( wsdlOperation != null ) {
      webServiceMeta.setOperationName( wsdlOperation.getOperationQName().getLocalPart() );
      webServiceMeta.setOperationNamespace( wsdlOperation.getOperationQName().getNamespaceURI() );
    } else if ( wsdl != null ) {
      webServiceMeta.setOperationName( null );
      webServiceMeta.setOperationNamespace( null );
    }
    if ( inWsdlParamContainer != null ) {
      webServiceMeta.setInFieldContainerName( inWsdlParamContainer.getContainerName() );
      webServiceMeta.setInFieldArgumentName( inWsdlParamContainer.getItemName() );
    } else if ( wsdl != null ) {
      webServiceMeta.setInFieldContainerName( null );
      webServiceMeta.setInFieldArgumentName( null );
    }
    if ( outWsdlParamContainer != null ) {
      webServiceMeta.setOutFieldContainerName( outWsdlParamContainer.getContainerName() );
      webServiceMeta.setOutFieldArgumentName( outWsdlParamContainer.getItemName() );
    } else if ( wsdl != null ) {
      webServiceMeta.setOutFieldContainerName( null );
      webServiceMeta.setOutFieldArgumentName( null );
    }

    // Input fields...
    //
    webServiceMeta.getFieldsIn().clear();
    if ( tabItemFieldIn != null ) {
      int nbRow = fieldInTableView.nrNonEmpty();

      for ( int i = 0; i < nbRow; ++i ) {
        TableItem vTableItem = fieldInTableView.getNonEmpty( i );
        WebServiceField field = new WebServiceField();
        field.setName( vTableItem.getText( 1 ) );
        field.setWsName( vTableItem.getText( 2 ) );
        field.setXsdType( Const.NVL( vTableItem.getText( 3 ), "String" ) );
        webServiceMeta.addFieldIn( field );
      }
    }

    // output fields...
    //
    webServiceMeta.getFieldsOut().clear();
    if ( tabItemFieldOut != null ) {
      int nbRow = fieldOutTableView.nrNonEmpty();

      for ( int i = 0; i < nbRow; ++i ) {
        TableItem vTableItem = fieldOutTableView.getNonEmpty( i );
        // If output name is null we do not add the field
        if ( !"".equals( vTableItem.getText( 1 ) ) ) {
          WebServiceField field = new WebServiceField();
          field.setName( vTableItem.getText( 1 ) );
          field.setWsName( vTableItem.getText( 2 ) );
          field.setXsdType( vTableItem.getText( 3 ) );
          webServiceMeta.addFieldOut( field );
        }
      }
    }
  }

  public WebServiceDialog( Shell aShell, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( aShell, variables, (ITransformMeta) in, pipelineMeta, sname );
    meta = (WebServiceMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, meta );

    changed = meta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "WebServiceDialog.DialogTitle" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Buttons OK / Cancel / ... at the bottom
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    Button wAddInput = new Button( shell, SWT.PUSH );
    wAddInput.setText( BaseMessages.getString( PKG, "WebServiceDialog.Label.AddInputButton" ) );
    wAddInput.addListener( SWT.Selection, e -> {
      addTabFieldIn();
      wTabFolder.setSelection( tabItemFieldIn );
    } );Button wAddOutput = new Button( shell, SWT.PUSH );
    wAddOutput.setText( BaseMessages.getString( PKG, "WebServiceDialog.Label.AddOutputButton" ) );
    wAddOutput.addListener( SWT.Selection, e-> {
        addTabFieldOut();
        wTabFolder.setSelection( tabItemFieldOut );
      }
    );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e->cancel());
    setButtonPositions( new Button[] { wOk, wAddInput, wAddOutput, wCancel }, margin, null );

    // TransformName line
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

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );

    // Add a tab which contains information on the web service(s)
    //
    /**
     * Web service tab item
     */
    CTabItem tabItemWebService = new CTabItem( wTabFolder, SWT.NONE );
    tabItemWebService.setText( BaseMessages.getString( PKG, "WebServiceDialog.MainTab.TabTitle" ) );
    Composite compositeTabWebService = new Composite( wTabFolder, SWT.NONE );
    props.setLook( compositeTabWebService );

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    compositeTabWebService.setLayout( fileLayout );

    // URL
    Label wlURL = new Label( compositeTabWebService, SWT.RIGHT );
    wlURL.setText( BaseMessages.getString( PKG, "WebServiceDialog.URL.Label" ) );
    props.setLook( wlURL );
    FormData fdlURL = new FormData();
    fdlURL.left = new FormAttachment( 0, 0 );
    fdlURL.top = new FormAttachment( 0, margin );
    fdlURL.right = new FormAttachment( middle, -margin );
    wlURL.setLayoutData( fdlURL );

    Button wbURL = new Button( compositeTabWebService, SWT.PUSH | SWT.CENTER );
    props.setLook( wbURL );
    wbURL.setText( BaseMessages.getString( PKG, "WebServiceDialog.URL.Load" ) );
    FormData fdbURL = new FormData();
    fdbURL.right = new FormAttachment( 100, 0 );
    fdbURL.top = new FormAttachment( 0, 0 );
    wbURL.setLayoutData( fdbURL );

    wbURL.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        // If the URL is specified, we always try to load
        //
        if ( !Utils.isEmpty( wURL.getText() ) ) {
          try {
            initTreeTabWebService( wURL.getText() );
          } catch ( Throwable throwable ) {
            new ErrorDialog( shell, BaseMessages.getString(
              PKG, "WebServiceDialog.Exception.UnableToLoadWebService.Title" ),
              BaseMessages.getString( PKG, "WebServiceDialog.Exception.UnableToLoadWebService.Message" ),
              throwable );
          }
        }
      }
    } );

    Button wbFile = new Button( compositeTabWebService, SWT.PUSH | SWT.CENTER );
    props.setLook( wbFile );
    wbFile.setText( BaseMessages.getString( PKG, "WebServiceDialog.File.Load" ) );
    FormData fdbFile = new FormData();
    fdbFile.right = new FormAttachment( wbURL, 0 );
    fdbFile.top = new FormAttachment( 0, 0 );
    wbFile.setLayoutData( fdbFile );

    wbFile.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        // We will load the WSDL from a file so we can at least try to debug the metadata extraction phase from the
        // support side.
        //
        FileDialog dialog = new FileDialog( shell, SWT.OPEN );
        dialog.setFilterExtensions( new String[] { "*.wsdl;*.WSDL", "*.*" } );
        dialog.setFilterNames( new String[] {
          BaseMessages.getString( PKG, "WebServiceDialog.FileType.WsdlFiles" ),
          BaseMessages.getString( PKG, "System.FileType.CSVFiles" ),
          BaseMessages.getString( PKG, "System.FileType.TextFiles" ),
          BaseMessages.getString( PKG, "System.FileType.AllFiles" ) } );

        if ( dialog.open() != null ) {
          String filename = dialog.getFilterPath() + System.getProperty( "file.separator" ) + dialog.getFileName();
          try {
            initTreeTabWebService( new File( filename ).toURI().toASCIIString() );
          } catch ( Throwable throwable ) {
            new ErrorDialog( shell, BaseMessages.getString(
              PKG, "WebServiceDialog.Exception.UnableToLoadWebService.Title" ),
              BaseMessages.getString( PKG, "WebServiceDialog.Exception.UnableToLoadWebService.Message" ),
              throwable );
          }
        }
      }
    } );

    wURL = new TextVar( variables, compositeTabWebService, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wURL.addModifyListener( lsMod );
    props.setLook( wURL );
    FormData fdURL = new FormData();
    fdURL.left = new FormAttachment( middle, 0 );
    fdURL.top = new FormAttachment( 0, margin );
    fdURL.right = new FormAttachment( wbFile, -margin );
    wURL.setLayoutData( fdURL );

    // Operation
    Label wlOperation = new Label( compositeTabWebService, SWT.RIGHT );
    wlOperation.setText( BaseMessages.getString( PKG, "WebServiceDialog.Operation.Label" ) );
    props.setLook( wlOperation );
    FormData fdlOperation = new FormData();
    fdlOperation.left = new FormAttachment( 0, 0 );
    fdlOperation.top = new FormAttachment( wURL, margin );
    fdlOperation.right = new FormAttachment( middle, -margin );
    wlOperation.setLayoutData( fdlOperation );
    wOperation = new CCombo( compositeTabWebService, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wOperation.addModifyListener( lsMod );
    wOperation.setToolTipText( BaseMessages.getString( PKG, "WebServiceDialog.Operation.Tooltip" ) );
    props.setLook( wOperation );
    FormData fdOperation = new FormData();
    fdOperation.top = new FormAttachment( wURL, margin );
    fdOperation.left = new FormAttachment( middle, 0 );
    fdOperation.right = new FormAttachment( 100, 0 );
    wOperation.setLayoutData( fdOperation );
    wOperation.addSelectionListener( new SelectionListener() {

      public void widgetSelected( SelectionEvent arg0 ) {
        try {
          selectWSDLOperation( wOperation.getText() );
        } catch ( HopException e ) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }

      public void widgetDefaultSelected( SelectionEvent arg0 ) {
        // TODO Auto-generated method stub

      }

    } );

    // Operation request name (optional)
    Label wlOperationRequest = new Label( compositeTabWebService, SWT.RIGHT );
    wlOperationRequest.setText( BaseMessages.getString( PKG, "WebServiceDialog.OperationRequest.Label" ) );
    props.setLook( wlOperationRequest );
    FormData fdlOperationRequest = new FormData();
    fdlOperationRequest.left = new FormAttachment( 0, 0 );
    fdlOperationRequest.top = new FormAttachment( wOperation, margin );
    fdlOperationRequest.right = new FormAttachment( middle, -margin );
    wlOperationRequest.setLayoutData( fdlOperationRequest );
    wOperationRequest = new Text( compositeTabWebService, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wOperationRequest.addModifyListener( lsMod );
    wOperationRequest.setToolTipText( BaseMessages.getString( PKG, "WebServiceDialog.OperationRequest.Tooltip" ) );
    props.setLook( wOperationRequest );
    FormData fdOperationRequest = new FormData();
    fdOperationRequest.top = new FormAttachment( wOperation, margin );
    fdOperationRequest.left = new FormAttachment( middle, 0 );
    fdOperationRequest.right = new FormAttachment( 100, 0 );
    wOperationRequest.setLayoutData( fdOperationRequest );

    // Pas d'appel
    wlTransform = new Label( compositeTabWebService, SWT.RIGHT );
    wlTransform.setText( BaseMessages.getString( PKG, "WebServiceDialog.Transform.Label" ) );
    props.setLook( wlTransform );
    FormData fdlTransform = new FormData();
    fdlTransform.left = new FormAttachment( 0, 0 );
    fdlTransform.top = new FormAttachment( wOperationRequest, margin );
    fdlTransform.right = new FormAttachment( middle, -margin );
    wlTransform.setLayoutData( fdlTransform );
    wTransform = new Text( compositeTabWebService, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransform.addModifyListener( lsMod );
    wTransform.setToolTipText( BaseMessages.getString( PKG, "WebServiceDialog.Transform.Tooltip" ) );
    props.setLook( wTransform );
    FormData fdTransform = new FormData();
    fdTransform.top = new FormAttachment( wOperationRequest, margin );
    fdTransform.left = new FormAttachment( middle, 0 );
    fdTransform.right = new FormAttachment( 100, 0 );
    wTransform.setLayoutData( fdTransform );

    // Option to pass all input data to output
    //
    Label wlPassInputData = new Label( compositeTabWebService, SWT.RIGHT );
    wlPassInputData.setText( BaseMessages.getString( PKG, "WebServiceDialog.PassInputData.Label" ) );
    props.setLook( wlPassInputData );
    FormData fdlPassInputData = new FormData();
    fdlPassInputData.left = new FormAttachment( 0, 0 );
    fdlPassInputData.top = new FormAttachment( wTransform, margin );
    fdlPassInputData.right = new FormAttachment( middle, -margin );
    wlPassInputData.setLayoutData( fdlPassInputData );
    wPassInputData = new Button( compositeTabWebService, SWT.CHECK );
    wPassInputData.setToolTipText( BaseMessages.getString( PKG, "WebServiceDialog.PassInputData.Tooltip" ) );
    props.setLook( wPassInputData );
    FormData fdPassInputData = new FormData();
    fdPassInputData.top = new FormAttachment( wlPassInputData, 0, SWT.CENTER );
    fdPassInputData.left = new FormAttachment( middle, 0 );
    fdPassInputData.right = new FormAttachment( 100, 0 );
    wPassInputData.setLayoutData( fdPassInputData );

    // Option to use 2.5/3.0 compatible parsing logic
    //
    Label wlCompatible = new Label( compositeTabWebService, SWT.RIGHT );
    wlCompatible.setText( BaseMessages.getString( PKG, "WebServiceDialog.Compatible.Label" ) );
    props.setLook( wlCompatible );
    FormData fdlCompatible = new FormData();
    fdlCompatible.left = new FormAttachment( 0, 0 );
    fdlCompatible.top = new FormAttachment( wlPassInputData, 0, SWT.CENTER );
    fdlCompatible.right = new FormAttachment( middle, -margin );
    wlCompatible.setLayoutData( fdlCompatible );
    wCompatible = new Button( compositeTabWebService, SWT.CHECK );
    wCompatible.setToolTipText( BaseMessages.getString( PKG, "WebServiceDialog.Compatible.Tooltip" ) );
    props.setLook( wCompatible );
    FormData fdCompatible = new FormData();
    fdCompatible.top = new FormAttachment( wlCompatible, 0, SWT.CENTER );
    fdCompatible.left = new FormAttachment( middle, 0 );
    fdCompatible.right = new FormAttachment( 100, 0 );
    wCompatible.setLayoutData( fdCompatible );

    // HTTP Login
    Label wlRepeatingElement = new Label( compositeTabWebService, SWT.RIGHT );
    wlRepeatingElement.setText( BaseMessages.getString( PKG, "WebServiceDialog.RepeatingElement.Label" ) );
    props.setLook( wlRepeatingElement );
    FormData fdlRepeatingElement = new FormData();
    fdlRepeatingElement.top = new FormAttachment( wCompatible, margin );
    fdlRepeatingElement.left = new FormAttachment( 0, 0 );
    fdlRepeatingElement.right = new FormAttachment( middle, -margin );
    wlRepeatingElement.setLayoutData( fdlRepeatingElement );
    wRepeatingElement = new TextVar( variables, compositeTabWebService, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wRepeatingElement.addModifyListener( lsMod );
    wRepeatingElement.setToolTipText( BaseMessages.getString( PKG, "WebServiceDialog.RepeatingElement.Tooltip" ) );
    props.setLook( wRepeatingElement );
    FormData fdRepeatingElement = new FormData();
    fdRepeatingElement.top = new FormAttachment( wCompatible, margin );
    fdRepeatingElement.left = new FormAttachment( middle, 0 );
    fdRepeatingElement.right = new FormAttachment( 100, 0 );
    wRepeatingElement.setLayoutData( fdRepeatingElement );

    // Return the SOAP body as a String or not?
    //
    Label wlReplyAsString = new Label( compositeTabWebService, SWT.RIGHT );
    wlReplyAsString.setText( BaseMessages.getString( PKG, "WebServiceDialog.ReplyAsString.Label" ) );
    props.setLook( wlReplyAsString );
    FormData fdlBodyAsString = new FormData();
    fdlBodyAsString.left = new FormAttachment( 0, 0 );
    fdlBodyAsString.top = new FormAttachment( wRepeatingElement, margin );
    fdlBodyAsString.right = new FormAttachment( middle, -margin );
    wlReplyAsString.setLayoutData( fdlBodyAsString );
    wReplyAsString = new Button( compositeTabWebService, SWT.CHECK );
    wReplyAsString.setToolTipText( BaseMessages.getString( PKG, "WebServiceDialog.ReplyAsString.Tooltip" ) );
    props.setLook( wReplyAsString );
    FormData fdBodyAsString = new FormData();
    fdBodyAsString.top = new FormAttachment( wlReplyAsString, 0, SWT.CENTER );
    fdBodyAsString.left = new FormAttachment( middle, 0 );
    fdBodyAsString.right = new FormAttachment( 100, 0 );
    wReplyAsString.setLayoutData( fdBodyAsString );

    // ////////////////////////
    // START HTTP AUTH GROUP

    Group gHttpAuth = new Group( compositeTabWebService, SWT.SHADOW_ETCHED_IN );
    gHttpAuth.setText( BaseMessages.getString( PKG, "WebServicesDialog.HttpAuthGroup.Label" ) );
    FormLayout httpAuthLayout = new FormLayout();
    httpAuthLayout.marginWidth = 3;
    httpAuthLayout.marginHeight = 3;
    gHttpAuth.setLayout( httpAuthLayout );
    props.setLook( gHttpAuth );

    // HTTP Login
    Label wlHttpLogin = new Label( gHttpAuth, SWT.RIGHT );
    wlHttpLogin.setText( BaseMessages.getString( PKG, "WebServiceDialog.HttpLogin.Label" ) );
    props.setLook( wlHttpLogin );
    FormData fdlHttpLogin = new FormData();
    fdlHttpLogin.top = new FormAttachment( 0, margin );
    fdlHttpLogin.left = new FormAttachment( 0, 0 );
    fdlHttpLogin.right = new FormAttachment( middle, -margin );
    wlHttpLogin.setLayoutData( fdlHttpLogin );
    wHttpLogin = new TextVar( variables, gHttpAuth, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wHttpLogin.addModifyListener( lsMod );
    wHttpLogin.setToolTipText( BaseMessages.getString( PKG, "WebServiceDialog.HttpLogin.Tooltip" ) );
    props.setLook( wHttpLogin );
    FormData fdHttpLogin = new FormData();
    fdHttpLogin.top = new FormAttachment( 0, margin );
    fdHttpLogin.left = new FormAttachment( middle, 0 );
    fdHttpLogin.right = new FormAttachment( 100, 0 );
    wHttpLogin.setLayoutData( fdHttpLogin );

    // HTTP Password
    Label wlHttpPassword = new Label( gHttpAuth, SWT.RIGHT );
    wlHttpPassword.setText( BaseMessages.getString( PKG, "WebServiceDialog.HttpPassword.Label" ) );
    props.setLook( wlHttpPassword );
    FormData fdlHttpPassword = new FormData();
    fdlHttpPassword.top = new FormAttachment( wHttpLogin, margin );
    fdlHttpPassword.left = new FormAttachment( 0, 0 );
    fdlHttpPassword.right = new FormAttachment( middle, -margin );
    wlHttpPassword.setLayoutData( fdlHttpPassword );
    wHttpPassword = new PasswordTextVar( variables, gHttpAuth, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wHttpPassword.addModifyListener( lsMod );
    wHttpPassword.setToolTipText( BaseMessages.getString( PKG, "WebServiceDialog.HttpPassword.Tooltip" ) );
    props.setLook( wHttpPassword );
    FormData fdHttpPassword = new FormData();
    fdHttpPassword.top = new FormAttachment( wHttpLogin, margin );
    fdHttpPassword.left = new FormAttachment( middle, 0 );
    fdHttpPassword.right = new FormAttachment( 100, 0 );
    wHttpPassword.setLayoutData( fdHttpPassword );

    FormData fdHttpAuth = new FormData();
    fdHttpAuth.left = new FormAttachment( 0, 0 );
    fdHttpAuth.right = new FormAttachment( 100, 0 );
    fdHttpAuth.top = new FormAttachment( wReplyAsString, margin );
    gHttpAuth.setLayoutData( fdHttpAuth );

    // END HTTP AUTH GROUP
    // ////////////////////////

    // ////////////////////////
    // START PROXY GROUP

    Group gProxy = new Group( compositeTabWebService, SWT.SHADOW_ETCHED_IN );
    gProxy.setText( BaseMessages.getString( PKG, "WebServicesDialog.ProxyGroup.Label" ) );
    FormLayout proxyLayout = new FormLayout();
    proxyLayout.marginWidth = 3;
    proxyLayout.marginHeight = 3;
    gProxy.setLayout( proxyLayout );
    props.setLook( gProxy );

    // HTTP Login
    Label wlProxyHost = new Label( gProxy, SWT.RIGHT );
    wlProxyHost.setText( BaseMessages.getString( PKG, "WebServiceDialog.ProxyHost.Label" ) );
    props.setLook( wlProxyHost );
    FormData fdlProxyHost = new FormData();
    fdlProxyHost.top = new FormAttachment( 0, margin );
    fdlProxyHost.left = new FormAttachment( 0, 0 );
    fdlProxyHost.right = new FormAttachment( middle, -margin );
    wlProxyHost.setLayoutData( fdlProxyHost );
    wProxyHost = new TextVar( variables, gProxy, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wProxyHost.addModifyListener( lsMod );
    wProxyHost.setToolTipText( BaseMessages.getString( PKG, "WebServiceDialog.ProxyHost.Tooltip" ) );
    props.setLook( wProxyHost );
    FormData fdProxyHost = new FormData();
    fdProxyHost.top = new FormAttachment( 0, margin );
    fdProxyHost.left = new FormAttachment( middle, 0 );
    fdProxyHost.right = new FormAttachment( 100, 0 );
    wProxyHost.setLayoutData( fdProxyHost );

    // HTTP Password
    Label wlProxyPort = new Label( gProxy, SWT.RIGHT );
    wlProxyPort.setText( BaseMessages.getString( PKG, "WebServiceDialog.ProxyPort.Label" ) );
    props.setLook( wlProxyPort );
    FormData fdlProxyPort = new FormData();
    fdlProxyPort.top = new FormAttachment( wProxyHost, margin );
    fdlProxyPort.left = new FormAttachment( 0, 0 );
    fdlProxyPort.right = new FormAttachment( middle, -margin );
    wlProxyPort.setLayoutData( fdlProxyPort );
    wProxyPort = new TextVar( variables, gProxy, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wProxyPort.addModifyListener( lsMod );
    wProxyPort.setToolTipText( BaseMessages.getString( PKG, "WebServiceDialog.ProxyPort.Tooltip" ) );
    props.setLook( wProxyPort );
    FormData fdProxyPort = new FormData();
    fdProxyPort.top = new FormAttachment( wProxyHost, margin );
    fdProxyPort.left = new FormAttachment( middle, 0 );
    fdProxyPort.right = new FormAttachment( 100, 0 );
    wProxyPort.setLayoutData( fdProxyPort );

    FormData fdProxy = new FormData();
    fdProxy.left = new FormAttachment( 0, 0 );
    fdProxy.right = new FormAttachment( 100, 0 );
    fdProxy.top = new FormAttachment( gHttpAuth, margin );
    gProxy.setLayoutData( fdProxy );

    // END HTTP AUTH GROUP
    // ////////////////////////

    // Layout du tab
    FormData fdFileComp = new FormData();
    fdFileComp.left = new FormAttachment( 0, 0 );
    fdFileComp.top = new FormAttachment( 0, 0 );
    fdFileComp.right = new FormAttachment( 100, 0 );
    fdFileComp.bottom = new FormAttachment( 100, 0 );
    compositeTabWebService.setLayoutData( fdFileComp );

    compositeTabWebService.layout();
    tabItemWebService.setControl( compositeTabWebService );

    wURL.addListener( SWT.Selection, e -> getData() );

    SelectionAdapter selAdapter = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    wHttpPassword.addSelectionListener( selAdapter );
    wHttpLogin.addSelectionListener( selAdapter );
    wTransform.addSelectionListener( selAdapter );
    wProxyHost.addSelectionListener( selAdapter );
    wProxyPort.addSelectionListener( selAdapter );
    wTransformName.addSelectionListener( selAdapter );

    wTabFolder.setSelection( tabItemWebService );
    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wTransformName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOk, -2 * margin );
    wTabFolder.setLayoutData( fdTabFolder );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    getData();

    setComboValues();
    // Set the shell size, based upon previous time...
    setSize();

    shell.open();

    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }

    return transformName;
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    getInfo( meta );

    dispose();
  }

  private void cancel() {
    transformName = null;

    meta.setChanged( changed );

    dispose();
  }

  private final List<ColumnInfo> fieldColumns = new ArrayList<>();

  /**
   * Fields from previous transform
   */
  private IRowMeta prevFields;

  /*
   * Previous fields are read asynchonous because this might take some time and the user is able to do other things,
   * where he will not need the previous fields
   *
   * private boolean bPreviousFieldsLoaded = false;
   */

  private void setComboValues() {
    Runnable fieldLoader = () -> {
      try {
        prevFields = pipelineMeta.getPrevTransformFields( variables, transformName );
      } catch ( HopException e ) {
        prevFields = new RowMeta();
        String msg = BaseMessages.getString( PKG, "SelectValuesDialog.DoMapping.UnableToFindInput" );
        logError( msg );
      }
      String[] prevTransformFieldNames = prevFields.getFieldNames();
      Arrays.sort( prevTransformFieldNames );
      // bPreviousFieldsLoaded = true;
      for ( ColumnInfo colInfo : fieldColumns ) {
        colInfo.setComboValues( prevTransformFieldNames );
      }
    };
    shell.getDisplay().asyncExec( fieldLoader );
  }
}
