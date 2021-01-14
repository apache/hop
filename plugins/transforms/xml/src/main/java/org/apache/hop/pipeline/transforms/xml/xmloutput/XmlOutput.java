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

package org.apache.hop.pipeline.transforms.xml.xmloutput;

import java.io.File;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.*;
import org.apache.hop.pipeline.transforms.xml.xmloutput.XmlField.ContentType;

/**
 * Converts input rows to one or more XML files.
 * 
 * @author Matt
 * @since 14-jan-2006
 */
public class XmlOutput extends BaseTransform<XmlOutputMeta, XmlOutputData> implements ITransform<XmlOutputMeta, XmlOutputData> {
  private static final String EOL = "\n"; // force EOL char because woodstox library encodes CRLF incorrectly

  private static final XMLOutputFactory XML_OUT_FACTORY = XMLOutputFactory.newInstance();

  private OutputStream outputStream;

  public XmlOutput(TransformMeta transformMeta, XmlOutputMeta meta, XmlOutputData transformDataInterface, int copyNr, PipelineMeta pipelineMeta, Pipeline trans ) {
    super( transformMeta, meta, transformDataInterface, copyNr, pipelineMeta, trans );
  }

  public boolean processRow() throws HopException {

    Object[] r;
    boolean result = true;

    r = getRow(); // This also waits for a row to be finished.

    if ( first && meta.isDoNotOpenNewFileInit() ) {
      // no more input to be expected...
      // In this case, no file was opened.
      if ( r == null ) {
        setOutputDone();
        return false;
      }

      if ( openNewFile() ) {
        data.OpenedNewFile = true;
      } else {
        logError( "Couldn't open file " + meta.getFileName() );
        setErrors( 1L );
        return false;
      }
    }

    if ( ( r != null && getLinesOutput() > 0 && meta.getSplitEvery() > 0 && ( getLinesOutput() % meta.getSplitEvery() ) == 0 ) ) {
      // Done with this part or with everything.
      closeFile();

      // Not finished: open another file...
      if ( r != null ) {
        if ( !openNewFile() ) {
          logError( "Unable to open new file (split #" + data.splitnr + "..." );
          setErrors( 1 );
          return false;
        }
      }
    }

    if ( r == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    writeRowToFile( getInputRowMeta(), r );

    data.outputRowMeta = getInputRowMeta().clone();
    meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );
    putRow( data.outputRowMeta, r ); // in case we want it to go further...

    if ( checkFeedback( getLinesOutput() ) ) {
      logBasic( "linenr " + getLinesOutput() );
    }

    return result;
  }

  private void writeRowToFile(IRowMeta rowMeta, Object[] r ) throws HopException {
    try {
      if ( first ) {
        data.formatRowMeta = rowMeta.clone();

        first = false;

        data.fieldnrs = new int[meta.getOutputFields().length];
        for ( int i = 0; i < meta.getOutputFields().length; i++ ) {
          data.fieldnrs[i] = data.formatRowMeta.indexOfValue( meta.getOutputFields()[i].getFieldName() );
          if ( data.fieldnrs[i] < 0 ) {
            throw new HopException( "Field [" + meta.getOutputFields()[i].getFieldName()
                + "] couldn't be found in the input stream!" );
          }

          // Apply the formatting settings to the valueMeta object...
          //
          IValueMeta valueMeta = data.formatRowMeta.getValueMeta( data.fieldnrs[i] );
          XmlField field = meta.getOutputFields()[i];
          valueMeta.setConversionMask( field.getFormat() );
          valueMeta.setLength( field.getLength(), field.getPrecision() );
          valueMeta.setDecimalSymbol( field.getDecimalSymbol() );
          valueMeta.setGroupingSymbol( field.getGroupingSymbol() );
          valueMeta.setCurrencySymbol( field.getCurrencySymbol() );
        }
      }

      if ( meta.getOutputFields() == null || meta.getOutputFields().length == 0 ) {
        /*
         * Write all values in stream to text file.
         */

        // OK, write a new row to the XML file:
        data.writer.writeStartElement( meta.getRepeatElement() );

        for ( int i = 0; i < data.formatRowMeta.size(); i++ ) {
          // Put a variables between the XML elements of the row
          //
          if ( i > 0 ) {
            data.writer.writeCharacters( " " );
          }

          IValueMeta valueMeta = data.formatRowMeta.getValueMeta( i );
          Object valueData = r[i];

          writeField( valueMeta, valueData, valueMeta.getName() );
        }
      } else {
        /*
         * Only write the fields specified!
         */
        // Write a new row to the XML file:
        data.writer.writeStartElement( meta.getRepeatElement() );

        // First do the attributes and write them...
        writeRowAttributes( r );

        // Now write the elements
        //
        for ( int i = 0; i < meta.getOutputFields().length; i++ ) {
          XmlField outputField = meta.getOutputFields()[i];
          if ( outputField.getContentType() == ContentType.Element ) {
            if ( i > 0 ) {
              data.writer.writeCharacters( " " ); // a variables between
              // elements
            }

            IValueMeta valueMeta = data.formatRowMeta.getValueMeta( data.fieldnrs[i] );
            Object valueData = r[data.fieldnrs[i]];

            String elementName = outputField.getElementName();
            if ( Utils.isEmpty( elementName ) ) {
              elementName = outputField.getFieldName();
            }

            if ( !( valueMeta.isNull( valueData ) && meta.isOmitNullValues() ) ) {
              writeField( valueMeta, valueData, elementName );
            }
          }
        }
      }

      data.writer.writeEndElement();
      data.writer.writeCharacters( EOL );
    } catch ( Exception e ) {
      throw new HopException( "Error writing XML row :" + e.toString() + Const.CR + "Row: "
          + getInputRowMeta().getString( r ), e );
    }

    incrementLinesOutput();
  }

  void writeRowAttributes( Object[] r ) throws HopValueException, XMLStreamException {
    for ( int i = 0; i < meta.getOutputFields().length; i++ ) {
      XmlField xmlField = meta.getOutputFields()[i];
      if ( xmlField.getContentType() == ContentType.Attribute ) {
        IValueMeta valueMeta = data.formatRowMeta.getValueMeta( data.fieldnrs[i] );
        Object valueData = r[data.fieldnrs[i]];

        String elementName = xmlField.getElementName();
        if ( Utils.isEmpty( elementName ) ) {
          elementName = xmlField.getFieldName();
        }

        if ( valueData != null ) {

          data.writer.writeAttribute( elementName, valueMeta.getString( valueData ) );
        } else if ( isNullValueAllowed( valueMeta.getType() ) ) {

          data.writer.writeAttribute( elementName, "null" );
        }
      }
    }
  }

  private boolean isNullValueAllowed( int valueMetaType ) {

    //Check if retro compatibility is set or not, to guaranty compatibility with older versions.
    //In 6.1 null values were written with string "null". Since then the attribute is not written.

    String val = getVariable( Const.HOP_COMPATIBILITY_XML_OUTPUT_NULL_VALUES, "N" );

    return ValueMetaBase.convertStringToBoolean( Const.NVL( val, "N" ) ) && valueMetaType == IValueMeta.TYPE_STRING;
  }

  private void writeField(IValueMeta valueMeta, Object valueData, String element ) throws HopTransformException {
    try {
      String value = valueMeta.getString( valueData );
      if ( value != null ) {
        data.writer.writeStartElement( element );
        data.writer.writeCharacters( value );
        data.writer.writeEndElement();
      } else {
        data.writer.writeEmptyElement( element );
      }
    } catch ( Exception e ) {
      throw new HopTransformException( "Error writing line :", e );
    }
  }

  public String buildFilename( boolean ziparchive ) {
    return meta.buildFilename( this, getCopy(), data.splitnr, ziparchive );
  }

  public boolean openNewFile() {
    boolean retval = false;
    data.writer = null;

    try {
      if ( meta.isServletOutput() ) {
        data.writer = XML_OUT_FACTORY.createXMLStreamWriter(((Pipeline) getPipeline()).getServletPrintWriter() );
        if ( meta.getEncoding() != null && meta.getEncoding().length() > 0 ) {
          data.writer.writeStartDocument( meta.getEncoding(), "1.0" );
        } else {
          data.writer.writeStartDocument( Const.XML_ENCODING, "1.0" );
        }
        data.writer.writeCharacters( EOL );
      } else {

        FileObject file = HopVfs.getFileObject( buildFilename( true ) );

        if ( meta.isAddToResultFiles() ) {
          // Add this to the result file names...
          ResultFile resultFile =
              new ResultFile( ResultFile.FILE_TYPE_GENERAL, file, getPipelineMeta().getName(), getTransformName() );
          resultFile.setComment( "This file was created with a xml output transform" );
          addResultFile( resultFile );
        }

        if ( meta.isZipped() ) {
          OutputStream fos = HopVfs.getOutputStream( file, false );
          data.zip = new ZipOutputStream( fos );
          File entry = new File( buildFilename( false ) );
          ZipEntry zipentry = new ZipEntry( entry.getName() );
          zipentry.setComment( "Compressed by Kettle" );
          data.zip.putNextEntry( zipentry );
          outputStream = data.zip;
        } else {
          outputStream = HopVfs.getOutputStream( file, false );
        }
        if ( meta.getEncoding() != null && meta.getEncoding().length() > 0 ) {
          logBasic( "Opening output stream in encoding: " + meta.getEncoding() );
          data.writer = XML_OUT_FACTORY.createXMLStreamWriter( outputStream, meta.getEncoding() );
          data.writer.writeStartDocument( meta.getEncoding(), "1.0" );
        } else {
          logBasic( "Opening output stream in default encoding : " + Const.XML_ENCODING );
          data.writer = XML_OUT_FACTORY.createXMLStreamWriter( outputStream );
          data.writer.writeStartDocument( Const.XML_ENCODING, "1.0" );
        }
        data.writer.writeCharacters( EOL );
      }

      // OK, write the header & the parent element:
      data.writer.writeStartElement( meta.getMainElement() );
      // Add the name variables if defined
      if ( ( meta.getNameSpace() != null ) && ( !"".equals( meta.getNameSpace() ) ) ) {
        data.writer.writeDefaultNamespace( meta.getNameSpace() );
      }
      data.writer.writeCharacters( EOL );

      retval = true;
    } catch ( Exception e ) {
      logError( "Error opening new file : " + e.toString() );
    }
    // System.out.println("end of newFile(), splitnr="+splitnr);

    data.splitnr++;

    return retval;
  }

  void closeOutputStream( OutputStream stream ) {
    try {
      if ( stream != null ) {
        stream.close();
      }
    } catch ( Exception e ) {
      logError( "Error closing output stream : " + e.toString() );
    }
  }

  private boolean closeFile() {
    boolean retval = false;
    if ( data.OpenedNewFile ) {
      try {
        // Close the parent element
        data.writer.writeEndElement();
        data.writer.writeCharacters( EOL );

        // System.out.println("Closed xml file...");

        data.writer.writeEndDocument();
        data.writer.close();

        if ( meta.isZipped() ) {
          // System.out.println("close zip entry ");
          data.zip.closeEntry();
          // System.out.println("finish file...");
          data.zip.finish();
          data.zip.close();
        }

        closeOutputStream( outputStream );

        retval = true;
      } catch ( Exception e ) {
        // Ignore errors
      }
    }
    return retval;
  }

  public boolean init(  ) {

    if ( super.init( ) ) {
      data.splitnr = 0;
      if ( !meta.isDoNotOpenNewFileInit() ) {
        if ( openNewFile() ) {
          data.OpenedNewFile = true;
          return true;
        } else {
          logError( "Couldn't open file " + meta.getFileName() );
          setErrors( 1L );
          stopAll();
        }
      } else {
        return true;
      }
    }
    return false;
  }

  public void dispose( ) {
    closeFile();

    super.dispose( );
  }

}
