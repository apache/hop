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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
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
import java.util.List;

/**
 * Converts input rows to one or more XML files.
 *
 * @author Matt
 * @since 14-jan-2006
 */
public class XmlJoin extends BaseTransform<XmlJoinMeta, XmlJoinData> implements ITransform<XmlJoinMeta, XmlJoinData> {
  private static final Class<?> PKG = XmlJoinMeta.class; // For Translator

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

      // Find the row sets to read from
      //
      List<IStream> infoStreams = meta.getTransformIOMeta().getInfoStreams();
      String targetStreamTransformName = infoStreams.get( 0 ).getTransformName();
      if ( StringUtils.isEmpty( targetStreamTransformName )) {
        throw new HopException("Please specify which transform to read the XML target stream rows from");
      }
      String sourceStreamTransformName = infoStreams.get( 1 ).getTransformName();
      if ( StringUtils.isEmpty( sourceStreamTransformName )) {
        throw new HopException("Please specify which transform to read the XML source stream rows from");
      }

      // Get the two input row sets
      data.targetRowSet = findInputRowSet( targetStreamTransformName );
      if (data.targetRowSet==null) {
        throw new HopException("Unable to find the XML target stream transform '"+ targetStreamTransformName +"' to read from");
      }
      data.sourceRowSet = findInputRowSet( sourceStreamTransformName );
      if (data.sourceRowSet==null) {
        throw new HopException("Unable to find the XML join source stream '"+ sourceStreamTransformName +"' to read from");
      }

      // get the first line from the target row set
      Object[] rTarget = getRowFrom( data.targetRowSet );
      if ( rTarget == null ) { // nothing to do
        logBasic( BaseMessages.getString( PKG, "XmlJoin.NoRowsFoundInTarget" ) );
        setOutputDone();
        return false;
      }

      // get target xml
      IRowMeta targetStreamRowMeta = data.targetRowSet.getRowMeta();
      String[] targetStreamFieldNames = targetStreamRowMeta.getFieldNames();
      for ( int i = 0; i < targetStreamFieldNames.length; i++ ) {
        if ( meta.getTargetXmlField().equals( targetStreamFieldNames[i] ) ) {
          targetField_id = i;
        }
      }
      // Throw exception if target field has not been found
      if ( targetField_id == -1 ) {
        throw new HopException( BaseMessages.getString( PKG, "XmlJoin.Exception.FieldNotFound", meta
            .getTargetXmlField() ) );
      }

      IRowMeta sourceStreamRowMeta = getPipelineMeta().getTransformFields( this, sourceStreamTransformName );

      data.outputRowMeta = data.targetRowSet.getRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), new IRowMeta[] { targetStreamRowMeta, sourceStreamRowMeta },
          null, variables, metadataProvider );
      data.outputRowData = rTarget.clone();

      // get the target xml structure and create a DOM
      String strTarget = (String) rTarget[targetField_id];
      // parse the XML as a W3C Document

      InputSource inputSource = new InputSource( new StringReader( strTarget ) );

      data.xPathStatement = meta.getTargetXPath();
      try {
        DocumentBuilder builder = XmlParserFactoryProducer.createSecureDocBuilderFactory().newDocumentBuilder();
        data.targetDom = builder.parse( inputSource );
        if ( !meta.isComplexJoin() ) {
          data.targetNode = (Node) xpath.evaluate( data.xPathStatement, data.targetDom, XPathConstants.NODE );
          if ( data.targetNode == null ) {
            throw new HopXmlException( "XPath statement returned no result [" + data.xPathStatement + "]" );
          }
        }
      } catch ( Exception e ) {
        throw new HopXmlException( e );
      }

    }

    Object[] rJoinSource = getRowFrom( data.sourceRowSet ); // This also waits for a row to be finished.
    if ( rJoinSource == null ) {
      // no more input to be expected... create the output row
      try {
        if ( meta.isOmitNullValues() ) {
          removeEmptyNodes( data.targetDom.getChildNodes() );
        }
        // create string from xml tree
        StringWriter sw = new StringWriter();
        StreamResult resultXML = new StreamResult( sw );
        DOMSource source = new DOMSource( data.targetDom );
        getTransformer().transform( source, resultXML );

        int outputIndex = data.outputRowMeta.size() - 1;

        // send the row to the next transforms...
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
        String[] sourceFieldNames = data.sourceRowSet.getRowMeta().getFieldNames();
        for ( int i = 0; i < sourceFieldNames.length; i++ ) {
          if ( meta.getSourceXmlField().equals( sourceFieldNames[i] ) ) {
            data.iSourceXMLField = i;
          }
        }
        // Throw exception if source xml field has not been found
        if ( data.iSourceXMLField == -1 ) {
          throw new HopException( BaseMessages.getString( PKG, "XmlJoin.Exception.FieldNotFound", meta
              .getSourceXmlField() ) );
        }
      }

      if ( meta.isComplexJoin() && data.iCompareFieldID == -1 ) {
        // get the column of the compare value
        String[] sourceFieldNames = data.sourceRowSet.getRowMeta().getFieldNames();
        for ( int i = 0; i < sourceFieldNames.length; i++ ) {
          if ( meta.getJoinCompareField().equals( sourceFieldNames[i] ) ) {
            data.iCompareFieldID = i;
          }
        }
        // Throw exception if source xml field has not been found
        if ( data.iCompareFieldID == -1 ) {
          throw new HopException( BaseMessages.getString( PKG, "XmlJoin.Exception.FieldNotFound", meta
              .getJoinCompareField() ) );
        }
      }

      // get XML tags to join
      String strJoinXML = (String) rJoinSource[data.iSourceXMLField];

      try {
        DocumentBuilder builder = XmlParserFactoryProducer.createSecureDocBuilderFactory().newDocumentBuilder();
        Document joinDocument = builder.parse( new InputSource( new StringReader( strJoinXML ) ) );

        Node node = data.targetDom.importNode( joinDocument.getDocumentElement(), true );

        if ( meta.isComplexJoin() ) {
          String strCompareValue = rJoinSource[data.iCompareFieldID].toString();
          String strXPathStatement = data.xPathStatement.replace( "?", strCompareValue );

          data.targetNode = (Node) xpath.evaluate( strXPathStatement, data.targetDom, XPathConstants.NODE );
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

      // See if a main transform is supplied: in that case move the corresponding rowset to position 0
      //
      swapFirstInputRowSetIfExists( meta.getTargetXmlTransform() );
    } catch ( Exception e ) {
      log.logError( BaseMessages.getString( PKG, "XmlJoin.Error.Init" ), e );
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
