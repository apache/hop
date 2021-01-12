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

package org.apache.hop.pipeline.transforms.xml.xslt;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.Properties;

import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;


/**
 * Executes a XSL Transform on the values in the input stream.
 * 
 * @author Samatar
 * @since 15-Oct-2007
 * 
 */
public class Xslt extends BaseTransform<XsltMeta, XsltData> implements ITransform<XsltMeta, XsltData> {
  private static final Class<?> PKG = XsltMeta.class; // For Translator

  static final String JAXP_SCHEMA_LANGUAGE = "http://java.sun.com/xml/jaxp/properties/schemaLanguage";
  static final String JAXP_SCHEMA_SOURCE = "http://java.sun.com/xml/jaxp/properties/schemaSource";

  public Xslt(TransformMeta transformMeta, XsltMeta meta, XsltData data, int copyNr, PipelineMeta pipelineMeta, Pipeline trans ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, trans );
  }

  public boolean processRow() throws HopException {

    Object[] row = getRow();

    if ( row == null ) { // no more input to be expected...
      setOutputDone();
      return false;
    }
    if ( first ) {
      first = false;
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      // Check if The result field is given
      if ( Utils.isEmpty( meta.getResultfieldname() ) ) {
        // Result Field is missing !
        logError( BaseMessages.getString( PKG, "Xslt.Log.ErrorResultFieldMissing" ) );
        throw new HopTransformException( BaseMessages.getString( PKG, "Xslt.Exception.ErrorResultFieldMissing" ) );
      }

      // Check if The XML field is given
      if ( Utils.isEmpty( meta.getFieldname() ) ) {
        // Result Field is missing !
        logError( BaseMessages.getString( PKG, "Xslt.Exception.ErrorXMLFieldMissing" ) );
        throw new HopTransformException( BaseMessages.getString( PKG, "Xslt.Exception.ErrorXMLFieldMissing" ) );
      }

      // Try to get XML Field index
      data.fieldposition = getInputRowMeta().indexOfValue( meta.getFieldname() );
      // Let's check the Field
      if ( data.fieldposition < 0 ) {
        // The field is unreachable !
        logError( BaseMessages.getString( PKG, "Xslt.Log.ErrorFindingField" ) + "[" + meta.getFieldname() + "]" );
        throw new HopTransformException( BaseMessages.getString( PKG, "Xslt.Exception.CouldnotFindField", meta
            .getFieldname() ) );
      }

      // Check if the XSL Filename is contained in a column
      if ( meta.useXSLField() ) {
        if ( Utils.isEmpty( meta.getXSLFileField() ) ) {
          // The field is missing
          // Result field is missing !
          logError( BaseMessages.getString( PKG, "Xslt.Log.ErrorXSLFileFieldMissing" ) );
          throw new HopTransformException( BaseMessages.getString( PKG, "Xslt.Exception.ErrorXSLFileFieldMissing" ) );
        }

        // Try to get Field index
        data.fielxslfiledposition = getInputRowMeta().indexOfValue( meta.getXSLFileField() );

        // Let's check the Field
        if ( data.fielxslfiledposition < 0 ) {
          // The field is unreachable !
          logError( BaseMessages.getString( PKG, "Xslt.Log.ErrorXSLFileFieldFinding" ) + "[" + meta.getXSLFileField()
              + "]" );
          throw new HopTransformException( BaseMessages.getString( PKG, "Xslt.Exception.ErrorXSLFileFieldFinding", meta
              .getXSLFileField() ) );
        }

      } else {
        if ( Utils.isEmpty( meta.getXslFilename() ) ) {
          logError( BaseMessages.getString( PKG, "Xslt.Log.ErrorXSLFile" ) );
          throw new HopTransformException( BaseMessages.getString( PKG, "Xslt.Exception.ErrorXSLFile" ) );
        }

        // Check if XSL File exists!
        data.xslfilename = resolve( meta.getXslFilename() );
        FileObject file = null;
        try {
          file = HopVfs.getFileObject( data.xslfilename );
          if ( !file.exists() ) {
            logError( BaseMessages.getString( PKG, "Xslt.Log.ErrorXSLFileNotExists", data.xslfilename ) );
            throw new HopTransformException( BaseMessages.getString( PKG, "Xslt.Exception.ErrorXSLFileNotExists",
                data.xslfilename ) );
          }
          if ( file.getType() != FileType.FILE ) {
            logError( BaseMessages.getString( PKG, "Xslt.Log.ErrorXSLNotAFile", data.xslfilename ) );
            throw new HopTransformException( BaseMessages.getString( PKG, "Xslt.Exception.ErrorXSLNotAFile",
                data.xslfilename ) );
          }
        } catch ( Exception e ) {
          throw new HopTransformException( e );
        } finally {
          try {
            if ( file != null ) {
              file.close();
            }
          } catch ( Exception e ) { /* Ignore */
          }
        }
      }

      // Check output parameters
      int nrOutputProps = meta.getOutputPropertyName() == null ? 0 : meta.getOutputPropertyName().length;
      if ( nrOutputProps > 0 ) {
        data.outputProperties = new Properties();
        for ( int i = 0; i < nrOutputProps; i++ ) {
          data.outputProperties.put( meta.getOutputPropertyName()[i], resolve( meta
              .getOutputPropertyValue()[i] ) );
        }
        data.setOutputProperties = true;
      }

      // Check parameters
      data.nrParams = meta.getParameterField() == null ? 0 : meta.getParameterField().length;
      if ( data.nrParams > 0 ) {
        data.indexOfParams = new int[data.nrParams];
        data.nameOfParams = new String[data.nrParams];
        for ( int i = 0; i < data.nrParams; i++ ) {
          String name = resolve( meta.getParameterName()[i] );
          String field = resolve( meta.getParameterField()[i] );
          if ( Utils.isEmpty( field ) ) {
            throw new HopTransformException( BaseMessages
                .getString( PKG, "Xslt.Exception.ParameterFieldMissing", name, i ) );
          }
          data.indexOfParams[i] = getInputRowMeta().indexOfValue( field );
          if ( data.indexOfParams[i] < 0 ) {
            throw new HopTransformException( BaseMessages.getString( PKG, "Xslt.Exception.ParameterFieldNotFound", name ) );
          }
          data.nameOfParams[i] = name;
        }
        data.useParameters = true;
      }

      data.factory = TransformerFactory.newInstance();

      if ( meta.getXSLFactory().equals( "SAXON" ) ) {
        // Set the TransformerFactory to the SAXON implementation.
        data.factory = new net.sf.saxon.TransformerFactoryImpl();
      }
    } // end if first

    // Get the field value
    String xmlValue = getInputRowMeta().getString( row, data.fieldposition );

    if ( meta.useXSLField() ) {
      // Get the value
      data.xslfilename = getInputRowMeta().getString( row, data.fielxslfiledposition );
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "Xslt.Log.XslfileNameFromFied", data.xslfilename, meta
            .getXSLFileField() ) );
      }
    }

    try {

      if ( log.isDetailed() ) {
        if ( meta.isXSLFieldIsAFile() ) {
          logDetailed( BaseMessages.getString( PKG, "Xslt.Log.Filexsl" ) + data.xslfilename );
        } else {
          logDetailed( BaseMessages.getString( PKG, "Xslt.Log.XslStream", data.xslfilename ) );
        }
      }

      // Get the template from the cache
      Transformer transformer = data.getTemplate( data.xslfilename, data.xslIsAfile );

      // Do we need to set output properties?
      if ( data.setOutputProperties ) {
        transformer.setOutputProperties( data.outputProperties );
      }

      // Do we need to pass parameters?
      if ( data.useParameters ) {
        for ( int i = 0; i < data.nrParams; i++ ) {
          transformer.setParameter( data.nameOfParams[i], row[data.indexOfParams[i]] );
        }
      }

      Source source = new StreamSource( new StringReader( xmlValue ) );
      // Prepare output stream
      StreamResult result = new StreamResult( new StringWriter() );
      // transform xml source
      transformer.transform( source, result );

      String xmlString = result.getWriter().toString();
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "Xslt.Log.FileResult" ) );
        logDetailed( xmlString );
      }
      Object[] outputRowData = RowDataUtil.addValueData( row, getInputRowMeta().size(), xmlString );

      if ( log.isRowLevel() ) {
        logRowlevel( BaseMessages.getString( PKG, "Xslt.Log.ReadRow" ) + " " + getInputRowMeta().getString( row ) );
      }

      // add new values to the row.
      putRow( data.outputRowMeta, outputRowData ); // copy row to output rowset(s);

    } catch ( Exception e ) {
      String errorMessage = e.getClass().toString() + ": " + e.getMessage();

      if ( getTransformMeta().isDoingErrorHandling() ) {
        // Simply add this row to the error row
        putError( getInputRowMeta(), row, 1, errorMessage, meta.getResultfieldname(), "XSLT01" );
      } else {
        logError( BaseMessages.getString( PKG, "Xslt.ErrorProcesing" + " : " + errorMessage ), e );
        throw new HopTransformException( BaseMessages.getString( PKG, "Xslt.ErrorProcesing" ), e );
      }
    }

    return true;
  }

  public boolean init(  ) {
    if ( super.init() ) {
      // Specify weither or not we have to deal with XSL filename
      data.xslIsAfile = ( meta.useXSLField() && meta.isXSLFieldIsAFile() ) || ( !meta.useXSLField() );
      // Add init code here.
      return true;
    }
    return false;
  }

  public void dispose( ) {
    data.dispose();
    super.dispose( );
  }
}
