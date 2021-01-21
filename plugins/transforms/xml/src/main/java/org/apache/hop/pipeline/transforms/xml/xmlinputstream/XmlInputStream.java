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

package org.apache.hop.pipeline.transforms.xml.xmlinputstream;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.Namespace;
import javax.xml.stream.events.XMLEvent;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Use a StAX parser to read XML in a flexible and fast way.
 *
 * @author Jens Bleuel
 * @since 2011-01-13
 */
// TODO black box testing
public class XmlInputStream extends BaseTransform<XmlInputStreamMeta, XmlInputStreamData> implements ITransform<XmlInputStreamMeta, XmlInputStreamData> {
  private static final Class<?> PKG = XmlInputStream.class; // For Translator

  private static int PARENT_ID_ALLOCATE_SIZE = 1000; // max. number of nested elements, we may let the user configure
                                                     // this

  private int inputFieldIndex;

  static final String[] eventDescription = { "UNKNOWN", "START_ELEMENT", "END_ELEMENT", "PROCESSING_INSTRUCTION",
    "CHARACTERS", "COMMENT", "SPACE", "START_DOCUMENT", "END_DOCUMENT", "ENTITY_REFERENCE", "ATTRIBUTE", "DTD",
    "CDATA", "NAMESPACE", "NOTATION_DECLARATION", "ENTITY_DECLARATION" };

  public XmlInputStream(TransformMeta transformMeta, XmlInputStreamMeta meta, XmlInputStreamData transformDataInterface, int copyNr, PipelineMeta pipelineMeta,
                        Pipeline trans ) {
    super( transformMeta, meta, transformDataInterface, copyNr, pipelineMeta, trans );
  }

  @Override
  public boolean processRow(  ) throws HopException {
    if ( first && !meta.sourceFromInput ) {
      first = false;

      if ( data.filenames == null ) {
        getFilenames();
      }
      openNextFile();
      resetElementCounters();
      prepareProcessPreviousFields();
    }

    Object[] outputRowData;
    if ( meta.sourceFromInput ) {
      Object[] row = null;
      if ( first ) {
        first = false;
        row = getRow();
        // get input field index
        if ( getInputRowMeta() == null ) {
          throw new HopException( BaseMessages.getString( PKG, "XMLInputStream.NoIncomingRowsFound" ) );
        }
        inputFieldIndex = getInputRowMeta().indexOfValue( meta.sourceFieldName );
        if ( inputFieldIndex < 0 ) {
          throw new HopException( BaseMessages.getString( PKG, "XMLInputStream.FilenameFieldNotFound",
              meta.sourceFieldName ) );
        }
        prepareProcessPreviousFields();
      }
      if ( data.xmlEventReader == null ) {
        if ( row == null ) {
          row = getRow();
        }
        if ( row == null ) {
          setOutputDone(); // signal end to receiver(s)
          return false; // This is the end of this transform.
        }
        String xml = getInputRowMeta().getString( row, inputFieldIndex );
        try {
          data.xmlEventReader = data.staxInstance.createXMLEventReader( new StringReader( xml ) );
        } catch ( XMLStreamException e ) {
          throw new HopException( e );
        }
        resetElementCounters();
      }
      if ( row != null ) {
        data.currentInputRow = (Object[]) row.clone();
      }
      outputRowData = getRowFromXML();
      if ( outputRowData == null ) {
        data.xmlEventReader = null;
        return true;
      }
    } else {
      if ( data.inputDataRows != null ) {
        data.currentInputRow = (Object[]) ( data.inputDataRows.get( data.filenames[ data.filenr - 1 ] ) ).clone();
      }
      outputRowData = getRowFromXML();
      if ( outputRowData == null ) {
        if ( openNextFile() ) {
          resetElementCounters();
          return true;
        } else {
          setOutputDone(); // signal end to receiver(s)
          return false; // This is the end of this transform.
        }
      }
    }

    putRowOut( outputRowData );

    // limit has been reached: stop now. (not exact science since some attributes could be mixed within the last row)
    if ( data.rowLimit > 0 && data.rowNumber >= data.rowLimit ) {
      setOutputDone();
      return false;
    }
    return true;
  }

  private void prepareProcessPreviousFields() throws HopTransformException {
    if ( getInputRowMeta() == null ) {
      data.previousFieldsNumber = 0;
      data.finalOutputRowMeta = data.outputRowMeta;
    } else {
      data.previousFieldsNumber = getInputRowMeta().size();
      data.finalOutputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.finalOutputRowMeta, getTransformName(), null, null, this, metadataProvider );
    }
  }

  private boolean openNextFile() throws HopException {
    try {
      closeFile();
      if ( data.filenr >= data.filenames.length ) {
        return false;
      }
      data.fileObject = HopVfs.getFileObject( data.filenames[data.filenr] );
      data.inputStream = HopVfs.getInputStream( data.fileObject );
      data.xmlEventReader = data.staxInstance.createXMLEventReader( data.inputStream, data.encoding );
    } catch ( IOException e ) {
      throw new HopException( e );
    } catch ( XMLStreamException e ) {
      throw new HopException( e );
    }
    data.filenr++;
    if ( meta.isAddResultFile() ) {
      // Add this to the result file names...
      ResultFile resultFile =
        new ResultFile( ResultFile.FILE_TYPE_GENERAL, data.fileObject, getPipelineMeta().getName(), getTransformName() );
      resultFile.setComment( BaseMessages.getString( PKG, "XMLInputStream.Log.ResultFileWasRead" ) );
      addResultFile( resultFile );
    }
    return true;
  }

  private void closeFile() {
    if ( data.xmlEventReader != null ) {
      try {
        data.xmlEventReader.close();
      } catch ( XMLStreamException e ) {
        if ( log.isBasic() ) {
          log.logBasic( BaseMessages.getString( PKG, "XMLInputStream.Log.UnableToCloseFile",
            data.filenames[( data.filenr - 1 )] ), e );
        }
      }
    }
    if ( data.inputStream != null ) {
      try {
        data.inputStream.close();
      } catch ( IOException e ) {
        if ( log.isBasic() ) {
          log.logBasic( BaseMessages.getString( PKG, "XMLInputStream.Log.UnableToCloseFile",
            data.filenames[( data.filenr - 1 )] ), e );
        }
      }
    }
    if ( data.fileObject != null ) {
      try {
        data.fileObject.close();
      } catch ( FileSystemException e ) {
        if ( log.isBasic() ) {
          log.logBasic( BaseMessages.getString( PKG, "XMLInputStream.Log.UnableToCloseFile",
             data.filenames[( data.filenr - 1 )] ), e );
        }
      }
    }
  }

  /**
   * If there are incoming hops:
   *  Retrieves filenames from all rows that pass through this transform
   *  using field name selected from incoming hops. If no input field is
   *  chosen and there is some manually typed text it will try to open
   *  a file using this text (for compatibility reasons)
   * If there is no incoming hops it will simply try to open file with
   * path provided in transform dialog
   *
   * @throws HopException
   */
  private void getFilenames() throws HopException {
    List<String> filenames = new ArrayList<>();
    int index = -1;

    Object[] row = getRow();

    // Get the filename field index...
    //
    String filenameValue = resolve( meta.getFilename() );
    index = getInputRowMeta().indexOfValue( filenameValue );
    if ( index < 0 ) {
      // try using this value as a path if not found in incoming hops
      data.filenames = new String[] { filenameValue };
    } else {
      data.inputDataRows = new HashMap<>();
      while ( row != null ) {

        String filename = getInputRowMeta().getString( row, index );
        filenames.add( filename ); // add it to the list...
        data.inputDataRows.put( filename, row );

        row = getRow(); // Grab another row...
      }

      data.filenames = filenames.toArray( new String[filenames.size()] );

      logDetailed( BaseMessages.getString( PKG, "XMLInputStream.Log.ReadingFromNrFiles", Integer
              .toString( data.filenames.length ) ) );
    }
  }

  // sends the normal row and attributes
  private void putRowOut( Object[] r ) throws HopTransformException, HopValueException {

    data.rowNumber++;
    if ( data.pos_xmlFilename != -1 ) {
      r[data.pos_xmlFilename] = new String( data.filenames[( data.filenr - 1 )] );
    }
    if ( data.pos_xmlRow_number != -1 ) {
      r[data.pos_xmlRow_number] = new Long( data.rowNumber );
    }
    if ( data.pos_xml_element_id != -1 ) {
      r[data.pos_xml_element_id] = data.elementLevelID[data.elementLevel];
    }
    if ( data.pos_xml_element_level != -1 ) {
      r[data.pos_xml_element_level] = new Long( data.elementLevel );
    }
    if ( data.pos_xml_parent_element_id != -1 ) {
      r[data.pos_xml_parent_element_id] = data.elementParentID[data.elementLevel];
    }
    if ( data.pos_xml_path != -1 ) {
      r[data.pos_xml_path] = data.elementPath[data.elementLevel];
    }
    if ( data.pos_xml_parent_path != -1 && data.elementLevel > 0 ) {
      r[data.pos_xml_parent_path] = data.elementPath[data.elementLevel - 1];
    }

    // We could think of adding an option to filter Start_end Document / Elements, RegEx?
    // We could think of adding columns identifying Element-Blocks

    // Skip rows? (not exact science since some attributes could be mixed within the last row)
    if ( data.nrRowsToSkip == 0 || data.rowNumber > data.nrRowsToSkip ) {
      if ( log.isRowLevel() ) {
        logRowlevel( "Read row: " + data.outputRowMeta.getString( r ) );
      }
      if ( data.currentInputRow != null ) {
        r = RowDataUtil.addRowData( (Object[]) data.currentInputRow.clone(), data.previousFieldsNumber, r );
      }
      putRow( data.finalOutputRowMeta, r );
    }
  }

  private Object[] getRowFromXML() throws HopException {

    Object[] outputRowData = null;
    // loop until significant data is there and more data is there
    while ( data.xmlEventReader.hasNext() && outputRowData == null && !isStopped() ) {
      outputRowData = processEvent();
      // log all events (but no attributes sent by the EventReader)
      incrementLinesInput();
      if ( checkFeedback( getLinesInput() ) && isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "XMLInputStream.Log.LineNumber", Long.toString( getLinesInput() ) ) );
      }
    }

    return outputRowData;
  }

  private Object[] processEvent() throws HopException {

    Object[] outputRowData = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
    XMLEvent e = null;
    try {
      e = data.xmlEventReader.nextEvent();
    } catch ( XMLStreamException ex ) {
      throw new HopException( ex );
    }

    int eventType = e.getEventType();
    if ( data.pos_xml_dataType_numeric != -1 ) {
      outputRowData[data.pos_xml_dataType_numeric] = new Long( eventType );
    }
    if ( data.pos_xml_dataTypeDescription != -1 ) {
      if ( eventType == 0 || eventType > eventDescription.length ) {
        // unknown eventType
        outputRowData[data.pos_xml_dataTypeDescription] = eventDescription[0] + "(" + eventType + ")";
      } else {
        outputRowData[data.pos_xml_dataTypeDescription] = eventDescription[eventType];
      }
    }
    if ( data.pos_xml_location_line != -1 ) {
      outputRowData[data.pos_xml_location_line] = new Long( e.getLocation().getLineNumber() );
    }
    if ( data.pos_xml_locationColumn != -1 ) {
      outputRowData[data.pos_xml_locationColumn] = new Long( e.getLocation().getColumnNumber() );
    }

    switch ( eventType ) {

      case XMLStreamConstants.START_ELEMENT:
        data.elementLevel++;
        if ( data.elementLevel > PARENT_ID_ALLOCATE_SIZE - 1 ) {
          throw new HopException(
            BaseMessages.getString( PKG, "XMLInputStream.Log.TooManyNestedElements", PARENT_ID_ALLOCATE_SIZE ) );
        }
        if ( data.elementParentID[data.elementLevel] == null ) {
          data.elementParentID[data.elementLevel] = data.elementID;
        }
        data.elementID++;
        data.elementLevelID[data.elementLevel] = data.elementID;

        String xml_dataName;
        if ( meta.isEnableNamespaces() ) {
          String prefix = e.asStartElement().getName().getPrefix();
          if ( Utils.isEmpty( prefix ) ) {
            xml_dataName = e.asStartElement().getName().getLocalPart();
          } else { // add namespace prefix:
            xml_dataName = prefix + ":" + e.asStartElement().getName().getLocalPart();
          }
        } else {
          xml_dataName = e.asStartElement().getName().getLocalPart();
        }
        if ( data.pos_xml_dataName >= 0 ) {
          outputRowData[data.pos_xml_dataName] = xml_dataName;
        }
        // store the name
        data.elementName[data.elementLevel] = xml_dataName;
        // store simple path
        data.elementPath[data.elementLevel] = data.elementPath[data.elementLevel - 1] + "/" + xml_dataName;

        // write Namespaces out
        if ( meta.isEnableNamespaces() ) {
          outputRowData = parseNamespaces( outputRowData, e );
        }

        // write Attributes out
        outputRowData = parseAttributes( outputRowData, e );

        break;

      case XMLStreamConstants.END_ELEMENT:
        parseEndElement( outputRowData, e.asEndElement() );
        putRowOut( outputRowData );
        data.elementParentID[data.elementLevel + 1] = null;
        data.elementLevel--;
        outputRowData = null; // continue
        break;

      case XMLStreamConstants.SPACE:
        outputRowData = null; // ignore & continue
        break;

      case XMLStreamConstants.CHARACTERS:
      case XMLStreamConstants.CDATA:
        if ( data.pos_xml_dataName >= 0 ) {
          outputRowData[data.pos_xml_dataName] = data.elementName[data.elementLevel];
        }
        String xml_dataValue = e.asCharacters().getData();
        if ( data.pos_xml_dataValue >= 0 ) {
          if ( meta.isEnableTrim() ) {
            // optional trim is also eliminating white spaces, tab, cr, lf
            xml_dataValue = Const.trim( xml_dataValue );
          }
          outputRowData[data.pos_xml_dataValue] = xml_dataValue;
        }

        if ( data.pos_xml_dataValue < 0 || Utils.isEmpty( (String) outputRowData[data.pos_xml_dataValue] ) ) {
          outputRowData = null; // ignore & continue
        }
        break;

      case XMLStreamConstants.PROCESSING_INSTRUCTION:
        outputRowData = null; // ignore & continue
        // TODO test if possible
        break;

      case XMLStreamConstants.COMMENT:
        outputRowData = null; // ignore & continue
        // TODO test if possible
        break;

      case XMLStreamConstants.ENTITY_REFERENCE:
        // should be resolved by default
        outputRowData = null; // ignore & continue
        break;

      case XMLStreamConstants.START_DOCUMENT:
        // just get this information out
        break;

      case XMLStreamConstants.END_DOCUMENT:
        // just get this information out
        break;

      default:
        logBasic( "Event:" + eventType );
        outputRowData = null; // ignore & continue
    }

    return outputRowData;
  }

  private void parseEndElement( Object[] outputRowData, EndElement el ) {
    if ( data.pos_xml_dataName >= 0 ) {
      outputRowData[data.pos_xml_dataName] = getEndElementName( el, meta.isEnableNamespaces() );
    }
  }

  /**
   * Returns the qualified name of the end element
   *
   * @param el
   *          an EndElement event
   * @param enabledNamespaces
   *          indicates if namespaces should be added or not
   * @return the qualified name of the end element
   */
  private String getEndElementName( EndElement el, boolean enabledNamespaces ) {
    if ( !enabledNamespaces ) {
      return el.getName().getLocalPart();
    } else {
      return getName( el.getName().getPrefix(), el.getName().getLocalPart() );
    }
  }

  // Namespaces: put an extra row out for each namespace
  @SuppressWarnings( "unchecked" )
  private Object[] parseNamespaces( Object[] outputRowData, XMLEvent e ) throws HopValueException,
    HopTransformException {
    Iterator<Namespace> iter = e.asStartElement().getNamespaces();
    if ( iter.hasNext() ) {
      Object[] outputRowDataNamespace = data.outputRowMeta.cloneRow( outputRowData );
      putRowOut( outputRowDataNamespace ); // first put the element name info out
      // change data_type to ATTRIBUTE
      if ( data.pos_xml_dataType_numeric != -1 ) {
        outputRowData[data.pos_xml_dataType_numeric] = new Long( XMLStreamConstants.NAMESPACE );
      }
      if ( data.pos_xml_dataTypeDescription != -1 ) {
        outputRowData[data.pos_xml_dataTypeDescription] = eventDescription[XMLStreamConstants.NAMESPACE];
      }
    }
    while ( iter.hasNext() ) {
      Object[] outputRowDataNamespace = data.outputRowMeta.cloneRow( outputRowData );
      Namespace n = iter.next();
      outputRowDataNamespace[data.pos_xml_dataName] = n.getPrefix();
      outputRowDataNamespace[data.pos_xml_dataValue] = n.getNamespaceURI();
      if ( iter.hasNext() ) {
        // send out the Namespace row
        putRowOut( outputRowDataNamespace );
      } else {
        // last row: this will be sent out by the outer loop
        outputRowData = outputRowDataNamespace;
      }
    }

    return outputRowData;
  }

  // Attributes: put an extra row out for each attribute
  @SuppressWarnings( "unchecked" )
  private Object[] parseAttributes( Object[] outputRowData, XMLEvent e ) throws HopValueException,
    HopTransformException {
    Iterator<Attribute> iter = e.asStartElement().getAttributes();
    if ( iter.hasNext() ) {
      Object[] outputRowDataAttribute = data.outputRowMeta.cloneRow( outputRowData );
      putRowOut( outputRowDataAttribute ); // first put the element name (or namespace) info out
      // change data_type to ATTRIBUTE
      if ( data.pos_xml_dataType_numeric != -1 ) {
        outputRowData[data.pos_xml_dataType_numeric] = new Long( XMLStreamConstants.ATTRIBUTE );
      }
      if ( data.pos_xml_dataTypeDescription != -1 ) {
        outputRowData[data.pos_xml_dataTypeDescription] = eventDescription[XMLStreamConstants.ATTRIBUTE];
      }
    }
    while ( iter.hasNext() ) {
      Object[] outputRowDataAttribute = data.outputRowMeta.cloneRow( outputRowData );
      Attribute a = iter.next();
      parseAttribute( outputRowDataAttribute, a, meta.isEnableNamespaces() );
      if ( iter.hasNext() ) {
        // send out the Attribute row
        putRowOut( outputRowDataAttribute );
      } else {
        // last row: this will be sent out by the outer loop
        outputRowData = outputRowDataAttribute;
      }
    }

    return outputRowData;
  }

  private void parseAttribute( Object[] outputRowDataAttribute, Attribute a, boolean enabledNamespaces ) {
    if ( data.pos_xml_dataName != -1 ) {
      outputRowDataAttribute[data.pos_xml_dataName] = getAttributeName( a, enabledNamespaces );
    }
    if ( data.pos_xml_dataValue != -1 ) {
      outputRowDataAttribute[data.pos_xml_dataValue] = a.getValue();
    }
  }

  /**
   * Returns the qualified name of the attribute
   *
   * @param a
   *          an attribute event
   * @param enabledNamespaces
   *          indicates if namespaces should be added or not
   * @return the qualified name of the attribute
   */
  private String getAttributeName( Attribute a, boolean enabledNamespaces ) {
    if ( !enabledNamespaces ) {
      return a.getName().getLocalPart();
    } else {
      return getName( a.getName().getPrefix(), a.getName().getLocalPart() );
    }
  }

  /**
   * Returns the qualified name in the format: <code>prefix:localPart</code> if the prefix is present otherwise just
   * <code>localPart</code>
   *
   * @param prefix
   *          the namespace prefix part of the qualified name
   * @param localPart
   *          the local part of the qualified name
   * @return the qualified name
   */
  private String getName( String prefix, String localPart ) {
    return ( !Utils.isEmpty( prefix ) ) ? prefix + ":" + localPart : localPart;
  }

  private void resetElementCounters() {
    data.rowNumber = new Long( 0 );
    data.elementLevel = 0;
    data.elementID = new Long( 0 ); // init value, could be parameterized later on
    data.elementLevelID = new Long[PARENT_ID_ALLOCATE_SIZE];
    data.elementLevelID[0] = data.elementID; // inital id for level 0
    data.elementParentID = new Long[PARENT_ID_ALLOCATE_SIZE];
    data.elementName = new String[PARENT_ID_ALLOCATE_SIZE];
    data.elementPath = new String[PARENT_ID_ALLOCATE_SIZE];
    data.elementPath[0] = ""; // initial empty
  }

  @Override
  public boolean init() {
    if ( super.init( ) ) {
      data.staxInstance = XMLInputFactory.newInstance(); // could select the parser later on
      data.staxInstance.setProperty( "javax.xml.stream.isCoalescing", false );
      data.filenr = 0;
      if ( getPipelineMeta().findNrPrevTransforms( getTransformMeta() ) == 0 && !meta.sourceFromInput ) {
        String filename = resolve( meta.getFilename() );
        if ( Utils.isEmpty( filename ) ) {
          logError( BaseMessages.getString( PKG, "XMLInputStream.MissingFilename.Message" ) );
          return false;
        }

        data.filenames = new String[] { filename, };
      } else {
        data.filenames = null;
      }

      data.nrRowsToSkip = Const.toLong( this.resolve( meta.getNrRowsToSkip() ), 0 );
      data.rowLimit = Const.toLong( this.resolve( meta.getRowLimit() ), 0 );
      data.encoding = this.resolve( meta.getEncoding() );

      data.outputRowMeta = new RowMeta();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      // get and save field positions
      data.pos_xmlFilename = data.outputRowMeta.indexOfValue( meta.getFilenameField() );
      data.pos_xmlRow_number = data.outputRowMeta.indexOfValue( meta.getRowNumberField() );
      data.pos_xml_dataType_numeric = data.outputRowMeta.indexOfValue( meta.getXmlDataTypeNumericField() );
      data.pos_xml_dataTypeDescription = data.outputRowMeta.indexOfValue( meta.getXmlDataTypeDescriptionField() );
      data.pos_xml_location_line = data.outputRowMeta.indexOfValue( meta.getXmlLocationLineField() );
      data.pos_xml_locationColumn = data.outputRowMeta.indexOfValue( meta.getXmlLocationColumnField() );
      data.pos_xml_element_id = data.outputRowMeta.indexOfValue( meta.getXmlElementIDField() );
      data.pos_xml_parent_element_id = data.outputRowMeta.indexOfValue( meta.getXmlParentElementIDField() );
      data.pos_xml_element_level = data.outputRowMeta.indexOfValue( meta.getXmlElementLevelField() );
      data.pos_xml_path = data.outputRowMeta.indexOfValue( meta.getXmlPathField() );
      data.pos_xml_parent_path = data.outputRowMeta.indexOfValue( meta.getXmlParentPathField() );
      data.pos_xml_dataName = data.outputRowMeta.indexOfValue( meta.getXmlDataNameField() );
      data.pos_xml_dataValue = data.outputRowMeta.indexOfValue( meta.getXmlDataValueField() );
      return true;
    }
    return false;
  }

  @Override
  public void dispose() {
    // free resources
    closeFile();

    data.staxInstance = null;

    super.dispose( );
  }

}
