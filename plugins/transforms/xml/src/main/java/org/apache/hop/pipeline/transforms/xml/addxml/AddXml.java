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

package org.apache.hop.pipeline.transforms.xml.addxml;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;

/**
 * Converts input rows to one or more XML files.
 * 
 * @author Matt
 * @since 14-jan-2006
 */
public class AddXml extends BaseTransform<AddXmlMeta, AddXmlData> implements ITransform<AddXmlMeta, AddXmlData> {
  private static final Class<?> PKG = AddXml.class; // For Translator

  private DOMImplementation domImplentation;
  private Transformer serializer;

  public AddXml(TransformMeta transformMeta,  AddXmlMeta meta, AddXmlData sdi, int copyNr, PipelineMeta tm, Pipeline trans ) {
    super( transformMeta, meta, sdi, copyNr, tm, trans );
  }

  public boolean processRow() throws HopException {

    Object[] r = getRow(); // This also waits for a row to be finished.
    if ( r == null ) {
      // no more input to be expected...
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      // Cache the field name indexes
      //
      data.fieldIndexes = new int[meta.getOutputFields().length];
      for ( int i = 0; i < data.fieldIndexes.length; i++ ) {
        String fieldsName = meta.getOutputFields()[i].getFieldName();
        data.fieldIndexes[i] = getInputRowMeta().indexOfValue( fieldsName );
        if ( data.fieldIndexes[i] < 0 ) {
          throw new HopException( BaseMessages.getString( PKG, "AddXML.Exception.FieldNotFound", fieldsName ) );
        }
      }
    }

    Document xmldoc = getDomImplentation().createDocument( null, meta.getRootNode(), null );
    Element root = xmldoc.getDocumentElement();
    for ( int i = 0; i < meta.getOutputFields().length; i++ ) {
      XmlField outputField = meta.getOutputFields()[i];
      String fieldname = outputField.getFieldName();

      IValueMeta v = getInputRowMeta().getValueMeta( data.fieldIndexes[i] );
      Object valueData = r[data.fieldIndexes[i]];

      if ( !meta.isOmitNullValues() || !v.isNull( valueData ) ) {
        String value = formatField( v, valueData, outputField );

        String element = outputField.getElementName();
        if ( element == null || element.length() == 0 ) {
          element = fieldname;
        }

        if ( element == null || element.length() == 0 ) {
          throw new HopException( "XML does not allow empty strings for element names." );
        }
        if ( outputField.isAttribute() ) {
          String attributeParentName = outputField.getAttributeParentName();

          Element node;

          if ( attributeParentName == null || attributeParentName.length() == 0 ) {
            node = root;
          } else {
            NodeList nodelist = root.getElementsByTagName( attributeParentName );
            if ( nodelist.getLength() > 0 ) {
              node = (Element) nodelist.item( 0 );
            } else {
              node = root;
            }
          }

          node.setAttribute( element, value );

        } else { /* encode as subnode */
          if ( !element.equals( meta.getRootNode() ) ) {
            Element e = xmldoc.createElement( element );
            Node n = xmldoc.createTextNode( value );
            e.appendChild( n );
            root.appendChild( e );
          } else {
            Node n = xmldoc.createTextNode( value );
            root.appendChild( n );
          }
        }
      }
    }

    StringWriter sw = new StringWriter();
    DOMSource domSource = new DOMSource( xmldoc );
    try {
      this.getSerializer().transform( domSource, new StreamResult( sw ) );

    } catch ( TransformerException e ) {
      throw new HopException( e );
    } catch ( Exception e ) {
      throw new HopException( e );
    }

    Object[] outputRowData = RowDataUtil.addValueData( r, getInputRowMeta().size(), sw.toString() );

    putRow( data.outputRowMeta, outputRowData );

    return true;
  }

  private String formatField(IValueMeta valueMeta, Object valueData, XmlField field )
    throws HopValueException {
    String retval = "";
    if ( field == null ) {
      return "";
    }

    if ( valueMeta == null || valueMeta.isNull( valueData ) ) {
      String defaultNullValue = field.getNullString();
      return Utils.isEmpty( defaultNullValue ) ? "" : defaultNullValue;
    }

    if ( valueMeta.isNumeric() ) {
      // Formatting
      if ( !Utils.isEmpty( field.getFormat() ) ) {
        data.df.applyPattern( field.getFormat() );
      } else {
        data.df.applyPattern( data.defaultDecimalFormat.toPattern() );
      }
      // Decimal
      if ( !Utils.isEmpty( field.getDecimalSymbol() ) ) {
        data.dfs.setDecimalSeparator( field.getDecimalSymbol().charAt( 0 ) );
      } else {
        data.dfs.setDecimalSeparator( data.defaultDecimalFormatSymbols.getDecimalSeparator() );
      }
      // Grouping
      if ( !Utils.isEmpty( field.getGroupingSymbol() ) ) {
        data.dfs.setGroupingSeparator( field.getGroupingSymbol().charAt( 0 ) );
      } else {
        data.dfs.setGroupingSeparator( data.defaultDecimalFormatSymbols.getGroupingSeparator() );
      }
      // Currency symbol
      if ( !Utils.isEmpty( field.getCurrencySymbol() ) ) {
        data.dfs.setCurrencySymbol( field.getCurrencySymbol() );
      } else {
        data.dfs.setCurrencySymbol( data.defaultDecimalFormatSymbols.getCurrencySymbol() );
      }

      data.df.setDecimalFormatSymbols( data.dfs );

      if ( valueMeta.isBigNumber() ) {
        retval = data.df.format( valueMeta.getBigNumber( valueData ) );
      } else if ( valueMeta.isNumber() ) {
        retval = data.df.format( valueMeta.getNumber( valueData ) );
      } else {
        // Integer
        retval = data.df.format( valueMeta.getInteger( valueData ) );
      }
    } else if ( valueMeta.isDate() ) {
      if ( field != null && !Utils.isEmpty( field.getFormat() ) && valueMeta.getDate( valueData ) != null ) {
        if ( !Utils.isEmpty( field.getFormat() ) ) {
          data.daf.applyPattern( field.getFormat() );
        } else {
          data.daf.applyPattern( data.defaultDateFormat.toLocalizedPattern() );
        }
        data.daf.setDateFormatSymbols( data.dafs );
        retval = data.daf.format( valueMeta.getDate( valueData ) );
      } else {
        if ( valueMeta.isNull( valueData ) ) {
          if ( field != null && !Utils.isEmpty( field.getNullString() ) ) {
            retval = field.getNullString();
          }
        } else {
          retval = valueMeta.getString( valueData );
        }
      }
    } else if ( valueMeta.isString() ) {
      retval = valueMeta.getString( valueData );
    } else if ( valueMeta.isBinary() ) {
      if ( valueMeta.isNull( valueData ) ) {
        if ( !Utils.isEmpty( field.getNullString() ) ) {
          retval = field.getNullString();
        } else {
          retval = Const.NULL_BINARY;
        }
      } else {
        try {
          retval = new String( valueMeta.getBinary( valueData ), "UTF-8" );
        } catch ( UnsupportedEncodingException e ) {
          // chances are small we'll get here. UTF-8 is
          // mandatory.
          retval = Const.NULL_BINARY;
        }
      }
    } else {
      // Boolean
      retval = valueMeta.getString( valueData );
    }

    return retval;
  }

  public boolean init() {
    if ( !super.init() ) {
      return false;
    }

    try {
      setSerializer( TransformerFactory.newInstance().newTransformer() );

      setDomImplentation( XmlParserFactoryProducer.createSecureDocBuilderFactory().newDocumentBuilder().getDOMImplementation() );

      if ( meta.getEncoding() != null ) {
        getSerializer().setOutputProperty( OutputKeys.ENCODING, meta.getEncoding() );
      }

      if ( meta.isOmitXMLheader() ) {
        getSerializer().setOutputProperty( OutputKeys.OMIT_XML_DECLARATION, "yes" );
      }
    } catch ( TransformerConfigurationException e ) {
      return false;
    } catch ( ParserConfigurationException e ) {
      return false;
    }

    return true;
  }

  public void dispose(ITransformMeta smi, ITransformData sdi ) {
    meta = (AddXmlMeta) smi;
    data = (AddXmlData) sdi;

    super.dispose();

  }

  private void setDomImplentation( DOMImplementation domImplentation ) {
    this.domImplentation = domImplentation;
  }

  private DOMImplementation getDomImplentation() {
    return domImplentation;
  }

  private void setSerializer( Transformer serializer ) {
    this.serializer = serializer;
  }

  private Transformer getSerializer() {
    return serializer;
  }

}
