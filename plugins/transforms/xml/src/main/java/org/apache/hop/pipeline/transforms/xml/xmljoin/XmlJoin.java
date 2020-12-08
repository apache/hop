/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.StringReader;
import java.io.StringWriter;

/**
 * Converts input rows to one or more XML files.
 *
 * @author Matt
 * @since 14-jan-2006
 */
public class XmlJoin extends BaseTransform<XmlJoinMeta, XmlJoinData> implements ITransform<XmlJoinMeta, XmlJoinData> {
  private static final Class<?> PKG = XmlJoinMeta.class; // for i18n purposes, needed by Translator2!!

  private Transformer transformer;

  public XmlJoin(TransformMeta transformMeta, XmlJoinMeta meta, XmlJoinData data, int copyNr, PipelineMeta pipelineMeta, Pipeline trans ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, trans );
  }

  @Override
  public boolean processRow( ) throws HopException {

    XPath xpath = XPathFactory.newInstance().newXPath();

    // if first row we do some initializing and process the first row of the target XML Transform
    if ( first ) {
      first = false;
      int targetField_id = -1;

      // Get the two input row sets
      data.TargetRowSet = findInputRowSet( meta.getTargetXmlTransform() );
      data.SourceRowSet = findInputRowSet( meta.getSourceXmlTransform() );

      // get the first line from the target row set
      Object[] rTarget = getRowFrom( data.TargetRowSet );

      if ( rTarget == null ) { // nothing to do
        logBasic( BaseMessages.getString( PKG, "XMLJoin.NoRowsFoundInTarget" ) );
        setOutputDone();
        return false;
      }

      // get target xml
      String[] targetFieldNames = data.TargetRowSet.getRowMeta().getFieldNames();
      for ( int i = 0; i < targetFieldNames.length; i++ ) {
        if ( meta.getTargetXmlField().equals( targetFieldNames[i] ) ) {
          targetField_id = i;
        }
      }
      // Throw exception if target field has not been found
      if ( targetField_id == -1 ) {
        throw new HopException( BaseMessages.getString( PKG, "XMLJoin.Exception.FieldNotFound", meta
            .getTargetXmlField() ) );
      }

      data.outputRowMeta = data.TargetRowSet.getRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), new IRowMeta[] { data.TargetRowSet.getRowMeta() },
          null, variables, metadataProvider );
      data.outputRowData = rTarget.clone();

      // get the target xml structure and create a DOM
      String strTarget = (String) rTarget[targetField_id];
      // parse the XML as a W3C Document

      InputSource inputSource = new InputSource( new StringReader( strTarget ) );

      data.XPathStatement = meta.getTargetXPath();
      try {
        DocumentBuilder builder = XmlParserFactoryProducer.createSecureDocBuilderFactory().newDocumentBuilder();
        data.targetDOM = builder.parse( inputSource );
        if ( !meta.isComplexJoin() ) {
          data.targetNode = (Node) xpath.evaluate( data.XPathStatement, data.targetDOM, XPathConstants.NODE );
          if ( data.targetNode == null ) {
            throw new HopXmlException( "XPath statement returned no result [" + data.XPathStatement + "]" );
          }
        }
      } catch ( Exception e ) {
        throw new HopXmlException( e );
      }

    }

    Object[] rJoinSource = getRowFrom( data.SourceRowSet ); // This also waits for a row to be finished.
    if ( rJoinSource == null ) {
      // no more input to be expected... create the output row
      try {
        if ( meta.isOmitNullValues() ) {
          removeEmptyNodes( data.targetDOM.getChildNodes() );
        }
        // create string from xml tree
        StringWriter sw = new StringWriter();
        StreamResult resultXML = new StreamResult( sw );
        DOMSource source = new DOMSource( data.targetDOM );
        getTransformer().transform( source, resultXML );

        int outputIndex = data.outputRowMeta.size() - 1;

        // send the row to the next steps...
        putRow( data.outputRowMeta, RowDataUtil.addValueData( data.outputRowData, outputIndex, sw.toString() ) );
        // finishing up
        setOutputDone();

        return false;
      } catch ( Exception e ) {
        throw new HopException( e );
      }
    } else {
      if ( data.iSourceXMLField == -1 ) {
        // assume failure
        // get the column of the join xml set
        // get target xml
        String[] sourceFieldNames = data.SourceRowSet.getRowMeta().getFieldNames();
        for ( int i = 0; i < sourceFieldNames.length; i++ ) {
          if ( meta.getSourceXmlField().equals( sourceFieldNames[i] ) ) {
            data.iSourceXMLField = i;
          }
        }
        // Throw exception if source xml field has not been found
        if ( data.iSourceXMLField == -1 ) {
          throw new HopException( BaseMessages.getString( PKG, "XMLJoin.Exception.FieldNotFound", meta
              .getSourceXmlField() ) );
        }
      }

      if ( meta.isComplexJoin() && data.iCompareFieldID == -1 ) {
        // get the column of the compare value
        String[] sourceFieldNames = data.SourceRowSet.getRowMeta().getFieldNames();
        for ( int i = 0; i < sourceFieldNames.length; i++ ) {
          if ( meta.getJoinCompareField().equals( sourceFieldNames[i] ) ) {
            data.iCompareFieldID = i;
          }
        }
        // Throw exception if source xml field has not been found
        if ( data.iCompareFieldID == -1 ) {
          throw new HopException( BaseMessages.getString( PKG, "XMLJoin.Exception.FieldNotFound", meta
              .getJoinCompareField() ) );
        }
      }

      // get XML tags to join
      String strJoinXML = (String) rJoinSource[data.iSourceXMLField];

      try {
        DocumentBuilder builder = XmlParserFactoryProducer.createSecureDocBuilderFactory().newDocumentBuilder();
        Document joinDocument = builder.parse( new InputSource( new StringReader( strJoinXML ) ) );

        Node node = data.targetDOM.importNode( joinDocument.getDocumentElement(), true );

        if ( meta.isComplexJoin() ) {
          String strCompareValue = rJoinSource[data.iCompareFieldID].toString();
          String strXPathStatement = data.XPathStatement.replace( "?", strCompareValue );

          data.targetNode = (Node) xpath.evaluate( strXPathStatement, data.targetDOM, XPathConstants.NODE );
          if ( data.targetNode == null ) {
            throw new HopXmlException( "XPath statement returned no result [" + strXPathStatement + "]" );
          }
        }
        data.targetNode.appendChild( node );
      } catch ( Exception e ) {
        throw new HopException( e );
      }
    }

    return true;
  }

  @Override
  public boolean init( ) {
    if ( !super.init() ) {
      return false;
    }

    try {
      Transformer transformer = TransformerFactory.newInstance().newTransformer();

      if ( meta.getEncoding() != null ) {
        transformer.setOutputProperty( OutputKeys.ENCODING, meta.getEncoding() );
      }

      if ( meta.isOmitXmlHeader() ) {
        transformer.setOutputProperty( OutputKeys.OMIT_XML_DECLARATION, "yes" );
      }

      transformer.setOutputProperty( OutputKeys.INDENT, "no" );

      setTransformer( transformer );

      // See if a main step is supplied: in that case move the corresponding rowset to position 0
      //
      swapFirstInputRowSetIfExists( meta.getTargetXmlTransform() );
    } catch ( Exception e ) {
      log.logError( BaseMessages.getString( PKG, "XMLJoin.Error.Init" ), e );
      return false;
    }

    return true;
  }

  @Override
  public void dispose(  ) {

    super.dispose(  );
  }

  private void setTransformer( Transformer transformer ) {
    this.transformer = transformer;
  }

  private Transformer getTransformer() {
    return transformer;
  }

  private void removeEmptyNodes( NodeList nodes ) {
    for ( int i = 0; i < nodes.getLength(); i++ ) {
      Node node = nodes.item( i );

      // Process the tree bottom-up
      if ( node.hasChildNodes() ) {
        removeEmptyNodes( node.getChildNodes() );
      }

      boolean nodeIsEmpty =
          node.getNodeType() == Node.ELEMENT_NODE && !node.hasAttributes() && !node.hasChildNodes()
              && node.getTextContent().length() == 0;

      if ( nodeIsEmpty ) {
        // We shifted elements left, do not increment counter
        node.getParentNode().removeChild( node );
        i--;
      }
    }
  }
}
