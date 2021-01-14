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

package org.apache.hop.pipeline.transforms.xml.getxmldata;

import java.io.InputStream;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.hop.core.Const;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.dialogs.ProgressMonitorDialog;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.swt.widgets.Shell;


/**
 * Takes care of displaying a dialog that will handle the wait while we're finding out loop nodes for an XML file
 * 
 * @author Samatar
 * @since 07-apr-2010
 */
public class XmlInputFieldsImportProgressDialog {
  private static final Class<?> PKG = GetXmlDataMeta.class; // For Translator

  private static String VALUE_NAME = "Name";
  private static String VALUE_PATH = "Path";
  private static String VALUE_ELEMENT = "Element";
  private static String VALUE_RESULT = "result";
  private static String VALUE_TYPE = "Type";
  private static String VALUE_FORMAT = "Format";

  private Shell shell;

  private GetXmlDataMeta meta;

  private String filename;
  private String encoding;

  private int nr;

  private String loopXPath;
  private HashSet<String> list;

  private List<RowMetaAndData> fieldsList;
  private RowMetaAndData[] fields;

  private String xml;
  private String url;

  /**
   * Creates a new dialog that will handle the wait while we're finding out loop nodes for an XML file
   */
  public XmlInputFieldsImportProgressDialog(Shell shell, GetXmlDataMeta meta, String filename, String encoding,
                                            String loopXPath ) {
    this.shell = shell;
    this.meta = meta;
    this.fields = null;
    this.filename = filename;
    this.encoding = encoding;
    this.nr = 0;
    this.loopXPath = loopXPath;
    this.list = new HashSet<>();
    this.fieldsList = new ArrayList<>();
  }

  public XmlInputFieldsImportProgressDialog(Shell shell, GetXmlDataMeta meta, String xmlSource, boolean useUrl,
                                            String loopXPath ) {
    this.shell = shell;
    this.meta = meta;
    this.fields = null;
    this.filename = null;
    this.encoding = null;
    this.nr = 0;
    this.loopXPath = loopXPath;
    this.list = new HashSet<>();
    this.fieldsList = new ArrayList<>();
    if ( useUrl ) {
      this.xml = null;
      this.url = xmlSource;
    } else {
      this.xml = xmlSource;
      this.url = null;
    }
  }

  public RowMetaAndData[] open() {
    IRunnableWithProgress op = monitor -> {
      try {
        fields = doScan( monitor );
      } catch ( Exception e ) {
        e.printStackTrace();
        throw new InvocationTargetException( e, BaseMessages.getString( PKG,
            "GetXMLDateLoopNodesImportProgressDialog.Exception.ErrorScanningFile", filename, e.toString() ) );
      }
    };

    try {
      ProgressMonitorDialog pmd = new ProgressMonitorDialog( shell );
      pmd.run( true, true, op );
    } catch ( InvocationTargetException e ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG,
          "GetXMLDateLoopNodesImportProgressDialog.ErrorScanningFile.Title" ), BaseMessages.getString( PKG,
          "GetXMLDateLoopNodesImportProgressDialog.ErrorScanningFile.Message" ), e );
    } catch ( InterruptedException e ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG,
          "GetXMLDateLoopNodesImportProgressDialog.ErrorScanningFile.Title" ), BaseMessages.getString( PKG,
          "GetXMLDateLoopNodesImportProgressDialog.ErrorScanningFile.Message" ), e );
    }

    return fields;
  }

  @SuppressWarnings( "unchecked" )
  private RowMetaAndData[] doScan( IProgressMonitor monitor ) throws Exception {
    monitor.beginTask( BaseMessages.getString( PKG, "GetXMLDateLoopNodesImportProgressDialog.Task.ScanningFile",
        filename ), 1 );

    SAXReader reader = XmlParserFactoryProducer.getSAXReader( null );
    monitor.worked( 1 );
    if ( monitor.isCanceled() ) {
      return null;
    }
    // Validate XML against specified schema?
    if ( meta.isValidating() ) {
      reader.setValidation( true );
      reader.setFeature( "http://apache.org/xml/features/validation/schema", true );
    } else {
      // Ignore DTD
      reader.setEntityResolver( new IgnoreDtdEntityResolver() );
    }
    monitor.worked( 1 );
    monitor
        .beginTask( BaseMessages.getString( PKG, "GetXMLDateLoopNodesImportProgressDialog.Task.ReadingDocument" ), 1 );
    if ( monitor.isCanceled() ) {
      return null;
    }
    InputStream is = null;
    try {

      Document document = null;
      if ( !Utils.isEmpty( filename ) ) {
        is = HopVfs.getInputStream( filename );
        document = reader.read( is, encoding );
      } else {
        if ( !Utils.isEmpty( xml ) ) {
          document = reader.read( new StringReader( xml ) );
        } else {
          document = reader.read( new URL( url ) );
        }
      }

      monitor.worked( 1 );
      monitor.beginTask( BaseMessages.getString( PKG, "GetXMLDateLoopNodesImportProgressDialog.Task.DocumentOpened" ),
          1 );
      monitor.worked( 1 );
      monitor.beginTask( BaseMessages.getString( PKG, "GetXMLDateLoopNodesImportProgressDialog.Task.ReadingNode" ), 1 );

      if ( monitor.isCanceled() ) {
        return null;
      }
      List<Node> nodes = document.selectNodes( this.loopXPath );
      monitor.worked( 1 );
      monitor.subTask( BaseMessages.getString( PKG, "GetXMLDateLoopNodesImportProgressDialog.Task.FetchNodes" ) );

      if ( monitor.isCanceled() ) {
        return null;
      }
      for ( Node node : nodes ) {
        if ( monitor.isCanceled() ) {
          return null;
        }

        nr++;
        monitor.subTask( BaseMessages.getString( PKG, "GetXMLDateLoopNodesImportProgressDialog.Task.FetchNodes", String
            .valueOf( nr ) ) );
        monitor.subTask( BaseMessages.getString( PKG, "GetXMLDateLoopNodesImportProgressDialog.Task.FetchNodes", node
            .getPath() ) );
        setNodeField( node, monitor );
        childNode( node, monitor );

      }
      monitor.worked( 1 );
    } finally {
      try {
        if ( is != null ) {
          is.close();
        }
      } catch ( Exception e ) { /* Ignore */
      }
    }

    RowMetaAndData[] listFields = fieldsList.toArray( new RowMetaAndData[fieldsList.size()] );

    monitor.setTaskName( BaseMessages.getString( PKG, "GetXMLDateLoopNodesImportProgressDialog.Task.NodesReturned" ) );

    monitor.done();

    return listFields;

  }

  @SuppressWarnings( "unchecked" )
  private void setNodeField( Node node, IProgressMonitor monitor ) {
    Element e = (Element) node;
    // get all attributes
    List<Attribute> lista = e.attributes();
    for ( int i = 0; i < lista.size(); i++ ) {
      setAttributeField( lista.get( i ), monitor );
    }

    // Get Node Name
    String nodename = node.getName();
    String nodenametxt = cleanString( node.getPath() );

    if ( !Utils.isEmpty( nodenametxt ) && !list.contains( nodenametxt ) ) {
      nr++;
      monitor.subTask( BaseMessages.getString( PKG, "GetXMLDataXMLInputFieldsImportProgressDialog.Task.FetchFields",
          String.valueOf( nr ) ) );
      monitor.subTask( BaseMessages.getString( PKG, "GetXMLDataXMLInputFieldsImportProgressDialog.Task.AddingField",
          nodename ) );

      RowMetaAndData row = new RowMetaAndData();
      row.addValue( VALUE_NAME, IValueMeta.TYPE_STRING, nodename );
      row.addValue( VALUE_PATH, IValueMeta.TYPE_STRING, nodenametxt );
      row.addValue( VALUE_ELEMENT, IValueMeta.TYPE_STRING, GetXmlDataField.ElementTypeDesc[0] );
      row.addValue( VALUE_RESULT, IValueMeta.TYPE_STRING, GetXmlDataField.ResultTypeDesc[0] );

      // Get Node value
      String valueNode = node.getText();

      // Try to get the Type

      if ( IsDate( valueNode ) ) {
        row.addValue( VALUE_TYPE, IValueMeta.TYPE_STRING, "Date" );
        row.addValue( VALUE_FORMAT, IValueMeta.TYPE_STRING, "yyyy/MM/dd" );
      } else if ( IsInteger( valueNode ) ) {
        row.addValue( VALUE_TYPE, IValueMeta.TYPE_STRING, "Integer" );
        row.addValue( VALUE_FORMAT, IValueMeta.TYPE_STRING, null );
      } else if ( IsNumber( valueNode ) ) {
        row.addValue( VALUE_TYPE, IValueMeta.TYPE_STRING, "Number" );
        row.addValue( VALUE_FORMAT, IValueMeta.TYPE_STRING, null );
      } else {
        row.addValue( VALUE_TYPE, IValueMeta.TYPE_STRING, "String" );
        row.addValue( VALUE_FORMAT, IValueMeta.TYPE_STRING, null );
      }
      fieldsList.add( row );
      list.add( nodenametxt );

    } // end if
  }

  private void setAttributeField( Attribute attribute, IProgressMonitor monitor ) {
    // Get Attribute Name
    String attributname = attribute.getName();
    String attributnametxt = cleanString( attribute.getPath() );
    if ( !Utils.isEmpty( attributnametxt ) && !list.contains( attribute.getPath() ) ) {
      nr++;
      monitor.subTask( BaseMessages.getString( PKG, "GetXMLDataXMLInputFieldsImportProgressDialog.Task.FetchFields",
          String.valueOf( nr ) ) );
      monitor.subTask( BaseMessages.getString( PKG, "GetXMLDataXMLInputFieldsImportProgressDialog.Task.AddingField",
          attributname ) );

      RowMetaAndData row = new RowMetaAndData();
      row.addValue( VALUE_NAME, IValueMeta.TYPE_STRING, attributname );
      row.addValue( VALUE_PATH, IValueMeta.TYPE_STRING, attributnametxt );
      row.addValue( VALUE_ELEMENT, IValueMeta.TYPE_STRING, GetXmlDataField.ElementTypeDesc[1] );
      row.addValue( VALUE_RESULT, IValueMeta.TYPE_STRING, GetXmlDataField.ResultTypeDesc[0] );

      // Get attribute value
      String valueAttr = attribute.getText();

      // Try to get the Type

      if ( IsDate( valueAttr ) ) {
        row.addValue( VALUE_TYPE, IValueMeta.TYPE_STRING, "Date" );
        row.addValue( VALUE_FORMAT, IValueMeta.TYPE_STRING, "yyyy/MM/dd" );
      } else if ( IsInteger( valueAttr ) ) {
        row.addValue( VALUE_TYPE, IValueMeta.TYPE_STRING, "Integer" );
        row.addValue( VALUE_FORMAT, IValueMeta.TYPE_STRING, null );
      } else if ( IsNumber( valueAttr ) ) {
        row.addValue( VALUE_TYPE, IValueMeta.TYPE_STRING, "Number" );
        row.addValue( VALUE_FORMAT, IValueMeta.TYPE_STRING, null );
      } else {
        row.addValue( VALUE_TYPE, IValueMeta.TYPE_STRING, "String" );
        row.addValue( VALUE_FORMAT, IValueMeta.TYPE_STRING, null );
      }
      list.add( attribute.getPath() );
    } // end if

  }

  private String cleanString( String inputstring ) {
    String retval = inputstring;
    retval = retval.replace( this.loopXPath, "" );
    while ( retval.startsWith( GetXmlDataMeta.N0DE_SEPARATOR ) ) {
      retval = retval.substring( 1, retval.length() );
    }

    return retval;
  }

  private boolean IsDate( String str ) {
    // TODO: What about other dates? Maybe something for a CRQ
    try {
      SimpleDateFormat fdate = new SimpleDateFormat( "yyyy/MM/dd" );
      fdate.setLenient( false );
      fdate.parse( str );
    } catch ( Exception e ) {
      return false;
    }
    return true;
  }

  private boolean IsInteger( String str ) {
    try {
      Integer.parseInt( str );
    } catch ( NumberFormatException e ) {
      return false;
    }
    return true;
  }

  private boolean IsNumber( String str ) {
    try {
      Float.parseFloat( str );
    } catch ( Exception e ) {
      return false;
    }
    return true;
  }

  private boolean childNode( Node node, IProgressMonitor monitor ) {
    boolean rc = false; // true: we found child nodes
    Element ce = (Element) node;
    // List child
    for ( int j = 0; j < ce.nodeCount(); j++ ) {
      Node cnode = ce.node( j );
      if ( !Utils.isEmpty( cnode.getName() ) ) {
        Element cce = (Element) cnode;
        if ( cce.nodeCount() > 1 ) {
          if ( childNode( cnode, monitor ) == false ) {
            // We do not have child nodes ...
            setNodeField( cnode, monitor );
            rc = true;
          }
        } else {
          setNodeField( cnode, monitor );
          rc = true;
        }
      }
    }
    return rc;
  }

}
