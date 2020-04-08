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

package org.apache.hop.pipeline.transforms.csvinput;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopAttributeInterface;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformInjectionMetaEntry;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInjectionInterface;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transforms.common.CsvInputAwareMeta;
import org.apache.hop.pipeline.transforms.fileinput.InputFileMetaInterface;
import org.apache.hop.pipeline.transforms.fileinput.TextFileInputField;
import org.apache.hop.pipeline.transforms.fileinput.TextFileInputMeta;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author matt
 * @version 3.0
 * @since 2007-07-05
 */

public class CsvInputMeta extends BaseTransformMeta implements ITransform, InputFileMetaInterface,
  TransformMetaInjectionInterface, CsvInputAwareMeta {
  private static Class<?> PKG = CsvInput.class; // for i18n purposes, needed by Translator!!

  private String filename;

  private String filenameField;

  private boolean includingFilename;

  private String rowNumField;

  private boolean headerPresent;

  private String delimiter;
  private String enclosure;

  private String bufferSize;

  private boolean lazyConversionActive;

  private TextFileInputField[] inputFields;

  private boolean isaddresult;

  private boolean runningInParallel;

  private String encoding;

  private boolean newlinePossibleInFields;

  public CsvInputMeta() {
    super(); // allocate BaseTransformMeta
    allocate( 0 );
  }

  @Override
  public void loadXml( Node transformNode, IMetaStore metaStore ) throws HopXmlException {
    readData( transformNode );
  }

  @Override
  public Object clone() {
    final CsvInputMeta retval = (CsvInputMeta) super.clone();
    retval.inputFields = new TextFileInputField[ inputFields.length ];
    for ( int i = 0; i < inputFields.length; i++ ) {
      retval.inputFields[ i ] = (TextFileInputField) inputFields[ i ].clone();
    }
    return retval;
  }

  @Override
  public void setDefault() {
    delimiter = ",";
    enclosure = "\"";
    headerPresent = true;
    lazyConversionActive = true;
    isaddresult = false;
    bufferSize = "50000";
  }

  private void readData( Node transformNode ) throws HopXmlException {
    try {
      filename = XmlHandler.getTagValue( transformNode, getXmlCode( "FILENAME" ) );
      filenameField = XmlHandler.getTagValue( transformNode, getXmlCode( "FILENAME_FIELD" ) );
      rowNumField = XmlHandler.getTagValue( transformNode, getXmlCode( "ROW_NUM_FIELD" ) );
      includingFilename =
        "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, getXmlCode( "INCLUDE_FILENAME" ) ) );
      delimiter = XmlHandler.getTagValue( transformNode, getXmlCode( "DELIMITER" ) );
      enclosure = XmlHandler.getTagValue( transformNode, getXmlCode( "ENCLOSURE" ) );
      bufferSize = XmlHandler.getTagValue( transformNode, getXmlCode( "BUFFERSIZE" ) );
      headerPresent = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, getXmlCode( "HEADER_PRESENT" ) ) );
      lazyConversionActive =
        "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, getXmlCode( "LAZY_CONVERSION" ) ) );
      isaddresult = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, getXmlCode( "ADD_FILENAME_RESULT" ) ) );
      runningInParallel = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, getXmlCode( "PARALLEL" ) ) );
      String nlp = XmlHandler.getTagValue( transformNode, getXmlCode( "NEWLINE_POSSIBLE" ) );
      if ( Utils.isEmpty( nlp ) ) {
        if ( runningInParallel ) {
          newlinePossibleInFields = false;
        } else {
          newlinePossibleInFields = true;
        }
      } else {
        newlinePossibleInFields = "Y".equalsIgnoreCase( nlp );
      }
      encoding = XmlHandler.getTagValue( transformNode, getXmlCode( "ENCODING" ) );

      Node fields = XmlHandler.getSubNode( transformNode, getXmlCode( "FIELDS" ) );
      int nrFields = XmlHandler.countNodes( fields, getXmlCode( "FIELD" ) );

      allocate( nrFields );

      for ( int i = 0; i < nrFields; i++ ) {
        inputFields[ i ] = new TextFileInputField();

        Node fnode = XmlHandler.getSubNodeByNr( fields, getXmlCode( "FIELD" ), i );

        inputFields[ i ].setName( XmlHandler.getTagValue( fnode, getXmlCode( "FIELD_NAME" ) ) );
        inputFields[ i ].setType(
          ValueMetaFactory.getIdForValueMeta( XmlHandler.getTagValue( fnode, getXmlCode( "FIELD_TYPE" ) ) ) );
        inputFields[ i ].setFormat( XmlHandler.getTagValue( fnode, getXmlCode( "FIELD_FORMAT" ) ) );
        inputFields[ i ].setCurrencySymbol( XmlHandler.getTagValue( fnode, getXmlCode( "FIELD_CURRENCY" ) ) );
        inputFields[ i ].setDecimalSymbol( XmlHandler.getTagValue( fnode, getXmlCode( "FIELD_DECIMAL" ) ) );
        inputFields[ i ].setGroupSymbol( XmlHandler.getTagValue( fnode, getXmlCode( "FIELD_GROUP" ) ) );
        inputFields[ i ]
          .setLength( Const.toInt( XmlHandler.getTagValue( fnode, getXmlCode( "FIELD_LENGTH" ) ), -1 ) );
        inputFields[ i ].setPrecision( Const.toInt(
          XmlHandler.getTagValue( fnode, getXmlCode( "FIELD_PRECISION" ) ), -1 ) );
        inputFields[ i ].setTrimType( ValueMetaString.getTrimTypeByCode( XmlHandler.getTagValue(
          fnode, getXmlCode( "FIELD_TRIM_TYPE" ) ) ) );
      }
    } catch ( Exception e ) {
      throw new HopXmlException( "Unable to load transform info from XML", e );
    }
  }

  public void allocate( int nrFields ) {
    inputFields = new TextFileInputField[ nrFields ];
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder( 500 );

    retval.append( "    " ).append( XmlHandler.addTagValue( getXmlCode( "FILENAME" ), filename ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( getXmlCode( "FILENAME_FIELD" ), filenameField ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( getXmlCode( "ROW_NUM_FIELD" ), rowNumField ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( getXmlCode( "INCLUDE_FILENAME" ), includingFilename ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( getXmlCode( "DELIMITER" ), delimiter ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( getXmlCode( "ENCLOSURE" ), enclosure ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( getXmlCode( "HEADER_PRESENT" ), headerPresent ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( getXmlCode( "BUFFERSIZE" ), bufferSize ) );
    retval
      .append( "    " ).append( XmlHandler.addTagValue( getXmlCode( "LAZY_CONVERSION" ), lazyConversionActive ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( getXmlCode( "ADD_FILENAME_RESULT" ), isaddresult ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( getXmlCode( "PARALLEL" ), runningInParallel ) );
    retval.append( "    " ).append(
      XmlHandler.addTagValue( getXmlCode( "NEWLINE_POSSIBLE" ), newlinePossibleInFields ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( getXmlCode( "ENCODING" ), encoding ) );

    retval.append( "    " ).append( XmlHandler.openTag( getXmlCode( "FIELDS" ) ) ).append( Const.CR );
    for ( int i = 0; i < inputFields.length; i++ ) {
      TextFileInputField field = inputFields[ i ];

      retval.append( "      " ).append( XmlHandler.openTag( getXmlCode( "FIELD" ) ) ).append( Const.CR );
      retval.append( "        " ).append( XmlHandler.addTagValue( getXmlCode( "FIELD_NAME" ), field.getName() ) );
      retval.append( "        " ).append(
        XmlHandler.addTagValue( getXmlCode( "FIELD_TYPE" ), ValueMetaFactory.getValueMetaName( field.getType() ) ) );
      retval
        .append( "        " ).append( XmlHandler.addTagValue( getXmlCode( "FIELD_FORMAT" ), field.getFormat() ) );
      retval.append( "        " ).append(
        XmlHandler.addTagValue( getXmlCode( "FIELD_CURRENCY" ), field.getCurrencySymbol() ) );
      retval.append( "        " ).append(
        XmlHandler.addTagValue( getXmlCode( "FIELD_DECIMAL" ), field.getDecimalSymbol() ) );
      retval.append( "        " ).append(
        XmlHandler.addTagValue( getXmlCode( "FIELD_GROUP" ), field.getGroupSymbol() ) );
      retval
        .append( "        " ).append( XmlHandler.addTagValue( getXmlCode( "FIELD_LENGTH" ), field.getLength() ) );
      retval.append( "        " ).append(
        XmlHandler.addTagValue( getXmlCode( "FIELD_PRECISION" ), field.getPrecision() ) );
      retval.append( "        " ).append(
        XmlHandler
          .addTagValue( getXmlCode( "FIELD_TRIM_TYPE" ), ValueMetaString.getTrimTypeCode( field.getTrimType() ) ) );
      retval.append( "      " ).append( XmlHandler.closeTag( getXmlCode( "FIELD" ) ) ).append( Const.CR );
    }
    retval.append( "    " ).append( XmlHandler.closeTag( getXmlCode( "FIELDS" ) ) ).append( Const.CR );

    return retval.toString();
  }

  @Override
  public void getFields( IRowMeta rowMeta, String origin, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {
    try {
      rowMeta.clear(); // Start with a clean slate, eats the input

      for ( int i = 0; i < inputFields.length; i++ ) {
        TextFileInputField field = inputFields[ i ];

        IValueMeta valueMeta = ValueMetaFactory.createValueMeta( field.getName(), field.getType() );
        valueMeta.setConversionMask( field.getFormat() );
        valueMeta.setLength( field.getLength() );
        valueMeta.setPrecision( field.getPrecision() );
        valueMeta.setConversionMask( field.getFormat() );
        valueMeta.setDecimalSymbol( field.getDecimalSymbol() );
        valueMeta.setGroupingSymbol( field.getGroupSymbol() );
        valueMeta.setCurrencySymbol( field.getCurrencySymbol() );
        valueMeta.setTrimType( field.getTrimType() );
        if ( lazyConversionActive ) {
          valueMeta.setStorageType( IValueMeta.STORAGE_TYPE_BINARY_STRING );
        }
        valueMeta.setStringEncoding( variables.environmentSubstitute( encoding ) );

        // In case we want to convert Strings...
        // Using a copy of the valueMeta object means that the inner and outer representation format is the same.
        // Preview will show the data the same way as we read it.
        // This layout is then taken further down the road by the metadata through the pipeline.
        //
        IValueMeta storageMetadata =
          ValueMetaFactory.cloneValueMeta( valueMeta, IValueMeta.TYPE_STRING );
        storageMetadata.setStorageType( IValueMeta.STORAGE_TYPE_NORMAL );
        storageMetadata.setLength( -1, -1 ); // we don't really know the lengths of the strings read in advance.
        valueMeta.setStorageMetadata( storageMetadata );

        valueMeta.setOrigin( origin );

        rowMeta.addValueMeta( valueMeta );
      }

      if ( !Utils.isEmpty( filenameField ) && includingFilename ) {
        IValueMeta filenameMeta = new ValueMetaString( filenameField );
        filenameMeta.setOrigin( origin );
        if ( lazyConversionActive ) {
          filenameMeta.setStorageType( IValueMeta.STORAGE_TYPE_BINARY_STRING );
          filenameMeta.setStorageMetadata( new ValueMetaString( filenameField ) );
        }
        rowMeta.addValueMeta( filenameMeta );
      }

      if ( !Utils.isEmpty( rowNumField ) ) {
        IValueMeta rowNumMeta = new ValueMetaInteger( rowNumField );
        rowNumMeta.setLength( 10 );
        rowNumMeta.setOrigin( origin );
        rowMeta.addValueMeta( rowNumMeta );
      }
    } catch ( Exception e ) {
      throw new HopTransformException( e );
    }

  }

  @Override
  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "CsvInputMeta.CheckResult.NotReceivingFields" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "CsvInputMeta.CheckResult.TransformRecevingData", prev.size() + "" ), transformMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "CsvInputMeta.CheckResult.TransformRecevingData2" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "CsvInputMeta.CheckResult.NoInputReceivedFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    }
  }

  @Override
  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new CsvInput( transformMeta, this, data, cnr, tr, pipeline );
  }

  @Override
  public ITransformData getTransformData() {
    return new CsvInputData();
  }

  /**
   * @return the delimiter
   */
  public String getDelimiter() {
    return delimiter;
  }

  /**
   * @param delimiter the delimiter to set
   */
  public void setDelimiter( String delimiter ) {
    this.delimiter = delimiter;
  }

  /**
   * @return the filename
   */
  public String getFilename() {
    return filename;
  }

  /**
   * @param filename the filename to set
   */
  public void setFilename( String filename ) {
    this.filename = filename;
  }

  /**
   * @return the bufferSize
   */
  public String getBufferSize() {
    return bufferSize;
  }

  /**
   * @param bufferSize the bufferSize to set
   */
  public void setBufferSize( String bufferSize ) {
    this.bufferSize = bufferSize;
  }

  /**
   * @return true if lazy conversion is turned on: conversions are delayed as long as possible, perhaps to never occur
   * at all.
   */
  public boolean isLazyConversionActive() {
    return lazyConversionActive;
  }

  /**
   * @param lazyConversionActive true if lazy conversion is to be turned on: conversions are delayed as long as possible, perhaps to never
   *                             occur at all.
   */
  public void setLazyConversionActive( boolean lazyConversionActive ) {
    this.lazyConversionActive = lazyConversionActive;
  }

  /**
   * @return the headerPresent
   */
  public boolean isHeaderPresent() {
    return headerPresent;
  }

  /**
   * @param headerPresent the headerPresent to set
   */
  public void setHeaderPresent( boolean headerPresent ) {
    this.headerPresent = headerPresent;
  }

  /**
   * @return the enclosure
   */
  @Override
  public String getEnclosure() {
    return enclosure;
  }

  /**
   * @param enclosure the enclosure to set
   */
  public void setEnclosure( String enclosure ) {
    this.enclosure = enclosure;
  }

  @Override
  public List<ResourceReference> getResourceDependencies( PipelineMeta pipelineMeta, TransformMeta transformInfo ) {
    List<ResourceReference> references = new ArrayList<ResourceReference>( 5 );

    ResourceReference reference = new ResourceReference( transformInfo );
    references.add( reference );
    if ( !Utils.isEmpty( filename ) ) {
      // Add the filename to the references, including a reference to this
      // transform meta data.
      //
      reference.getEntries().add(
        new ResourceEntry( pipelineMeta.environmentSubstitute( filename ), ResourceType.FILE ) );
    }
    return references;
  }

  /**
   * @return the inputFields
   */
  @Override
  public TextFileInputField[] getInputFields() {
    return inputFields;
  }

  /**
   * @param inputFields the inputFields to set
   */
  public void setInputFields( TextFileInputField[] inputFields ) {
    this.inputFields = inputFields;
  }

  @Override
  public int getFileFormatTypeNr() {
    return TextFileInputMeta.FILE_FORMAT_MIXED; // TODO: check this
  }

  @Override
  public String[] getFilePaths( iVariables variables ) {
    return new String[] { variables.environmentSubstitute( filename ), };
  }

  @Override
  public int getNrHeaderLines() {
    return 1;
  }

  @Override
  public boolean hasHeader() {
    return isHeaderPresent();
  }

  @Override
  public String getErrorCountField() {
    return null;
  }

  @Override
  public String getErrorFieldsField() {
    return null;
  }

  @Override
  public String getErrorTextField() {
    return null;
  }

  @Override
  public String getEscapeCharacter() {
    return null;
  }

  @Override
  public String getFileType() {
    return "CSV";
  }

  @Override
  public String getSeparator() {
    return delimiter;
  }

  @Override
  public boolean includeFilename() {
    return false;
  }

  @Override
  public boolean includeRowNumber() {
    return false;
  }

  @Override
  public boolean isErrorIgnored() {
    return false;
  }

  @Override
  public boolean isErrorLineSkipped() {
    return false;
  }

  /**
   * @return the filenameField
   */
  public String getFilenameField() {
    return filenameField;
  }

  /**
   * @param filenameField the filenameField to set
   */
  public void setFilenameField( String filenameField ) {
    this.filenameField = filenameField;
  }

  /**
   * @return the includingFilename
   */
  public boolean isIncludingFilename() {
    return includingFilename;
  }

  /**
   * @param includingFilename the includingFilename to set
   */
  public void setIncludingFilename( boolean includingFilename ) {
    this.includingFilename = includingFilename;
  }

  /**
   * @return the rowNumField
   */
  public String getRowNumField() {
    return rowNumField;
  }

  /**
   * @param rowNumField the rowNumField to set
   */
  public void setRowNumField( String rowNumField ) {
    this.rowNumField = rowNumField;
  }

  /**
   * @param isaddresult The isaddresult to set.
   */
  public void setAddResultFile( boolean isaddresult ) {
    this.isaddresult = isaddresult;
  }

  /**
   * @return Returns isaddresult.
   */
  public boolean isAddResultFile() {
    return isaddresult;
  }

  /**
   * @return the runningInParallel
   */
  public boolean isRunningInParallel() {
    return runningInParallel;
  }

  /**
   * @param runningInParallel the runningInParallel to set
   */
  public void setRunningInParallel( boolean runningInParallel ) {
    this.runningInParallel = runningInParallel;
  }

  /**
   * @return the encoding
   */
  public String getEncoding() {
    return encoding;
  }

  /**
   * @param encoding the encoding to set
   */
  public void setEncoding( String encoding ) {
    this.encoding = encoding;
  }

  /**
   * @param variables                   the variable space to use
   * @param definitions
   * @param iResourceNaming
   * @param metaStore               the metaStore in which non-kettle metadata could reside.
   * @return the filename of the exported resource
   */
  @Override
  public String exportResources( iVariables variables, Map<String, ResourceDefinition> definitions,
                                 IResourceNaming iResourceNaming, IMetaStore metaStore ) throws HopException {
    try {
      // The object that we're modifying here is a copy of the original!
      // So let's change the filename from relative to absolute by grabbing the file object...
      // In case the name of the file comes from previous transforms, forget about this!
      //
      if ( Utils.isEmpty( filenameField ) && !Utils.isEmpty( filename ) ) {
        // From : ${Internal.Pipeline.Filename.Directory}/../foo/bar.csv
        // To : /home/matt/test/files/foo/bar.csv
        //
        FileObject fileObject = HopVFS.getFileObject( variables.environmentSubstitute( filename ), variables );

        // If the file doesn't exist, forget about this effort too!
        //
        if ( fileObject.exists() ) {
          // Convert to an absolute path...
          //
          filename = iResourceNaming.nameResource( fileObject, variables, true );

          return filename;
        }
      }
      return null;
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  @Override
  public TransformMetaInjectionInterface getTransformMetaInjectionInterface() {

    return this;
  }

  @Override
  public void injectTransformMetadataEntries( List<TransformInjectionMetaEntry> metadata ) {
    for ( TransformInjectionMetaEntry entry : metadata ) {
      HopAttributeInterface attr = findAttribute( entry.getKey() );

      // Set top level attributes...
      //
      if ( entry.getValueType() != IValueMeta.TYPE_NONE ) {
        if ( attr.getKey().equals( "FILENAME" ) ) {
          filename = (String) entry.getValue();
        } else if ( attr.getKey().equals( "FILENAME_FIELD" ) ) {
          filenameField = (String) entry.getValue();
        } else if ( attr.getKey().equals( "ROW_NUM_FIELD" ) ) {
          rowNumField = (String) entry.getValue();
        } else if ( attr.getKey().equals( "HEADER_PRESENT" ) ) {
          headerPresent = (Boolean) entry.getValue();
        } else if ( attr.getKey().equals( "DELIMITER" ) ) {
          delimiter = (String) entry.getValue();
        } else if ( attr.getKey().equals( "ENCLOSURE" ) ) {
          enclosure = (String) entry.getValue();
        } else if ( attr.getKey().equals( "BUFFERSIZE" ) ) {
          bufferSize = (String) entry.getValue();
        } else if ( attr.getKey().equals( "LAZY_CONVERSION" ) ) {
          lazyConversionActive = (Boolean) entry.getValue();
        } else if ( attr.getKey().equals( "PARALLEL" ) ) {
          runningInParallel = (Boolean) entry.getValue();
        } else if ( attr.getKey().equals( "NEWLINE_POSSIBLE" ) ) {
          newlinePossibleInFields = (Boolean) entry.getValue();
        } else if ( attr.getKey().equals( "ADD_FILENAME_RESULT" ) ) {
          isaddresult = (Boolean) entry.getValue();
        } else if ( attr.getKey().equals( "ENCODING" ) ) {
          encoding = (String) entry.getValue();
        } else {
          throw new RuntimeException( "Unhandled metadata injection of attribute: "
            + attr.toString() + " - " + attr.getDescription() );
        }
      } else {
        if ( attr.getKey().equals( "FIELDS" ) ) {
          // This entry contains a list of lists...
          // Each list contains a single CSV input field definition (one line in the dialog)
          //
          List<TransformInjectionMetaEntry> inputFieldEntries = entry.getDetails();
          inputFields = new TextFileInputField[ inputFieldEntries.size() ];
          for ( int row = 0; row < inputFieldEntries.size(); row++ ) {
            TransformInjectionMetaEntry inputFieldEntry = inputFieldEntries.get( row );
            TextFileInputField inputField = new TextFileInputField();

            List<TransformInjectionMetaEntry> fieldAttributes = inputFieldEntry.getDetails();
            for ( int i = 0; i < fieldAttributes.size(); i++ ) {
              TransformInjectionMetaEntry fieldAttribute = fieldAttributes.get( i );
              HopAttributeInterface fieldAttr = findAttribute( fieldAttribute.getKey() );

              String attributeValue = (String) fieldAttribute.getValue();
              if ( fieldAttr.getKey().equals( "FIELD_NAME" ) ) {
                inputField.setName( attributeValue );
              } else if ( fieldAttr.getKey().equals( "FIELD_TYPE" ) ) {
                inputField.setType( ValueMetaFactory.getIdForValueMeta( attributeValue ) );
              } else if ( fieldAttr.getKey().equals( "FIELD_FORMAT" ) ) {
                inputField.setFormat( attributeValue );
              } else if ( fieldAttr.getKey().equals( "FIELD_LENGTH" ) ) {
                inputField.setLength( attributeValue == null ? -1 : Integer.parseInt( attributeValue ) );
              } else if ( fieldAttr.getKey().equals( "FIELD_PRECISION" ) ) {
                inputField.setPrecision( attributeValue == null ? -1 : Integer.parseInt( attributeValue ) );
              } else if ( fieldAttr.getKey().equals( "FIELD_CURRENCY" ) ) {
                inputField.setCurrencySymbol( attributeValue );
              } else if ( fieldAttr.getKey().equals( "FIELD_DECIMAL" ) ) {
                inputField.setDecimalSymbol( attributeValue );
              } else if ( fieldAttr.getKey().equals( "FIELD_GROUP" ) ) {
                inputField.setGroupSymbol( attributeValue );
              } else if ( fieldAttr.getKey().equals( "FIELD_TRIM_TYPE" ) ) {
                inputField.setTrimType( ValueMetaString.getTrimTypeByCode( attributeValue ) );
              } else {
                throw new RuntimeException( "Unhandled metadata injection of attribute: "
                  + fieldAttr.toString() + " - " + fieldAttr.getDescription() );
              }
            }

            inputFields[ row ] = inputField;
          }
        }
      }
    }
  }

  @Override
  public List<TransformInjectionMetaEntry> extractTransformMetadataEntries() throws HopException {
    return null;
  }

  /**
   * Describe the metadata attributes that can be injected into this transform metadata object.
   *
   * @throws HopException
   */
  @Override
  public List<TransformInjectionMetaEntry> getTransformInjectionMetadataEntries() throws HopException {
    return getTransformInjectionMetadataEntries( PKG );
  }

  /**
   * @return the newlinePossibleInFields
   */
  public boolean isNewlinePossibleInFields() {
    return newlinePossibleInFields;
  }

  /**
   * @param newlinePossibleInFields the newlinePossibleInFields to set
   */
  public void setNewlinePossibleInFields( boolean newlinePossibleInFields ) {
    this.newlinePossibleInFields = newlinePossibleInFields;
  }

  @Override
  public FileObject getHeaderFileObject( final PipelineMeta pipelineMeta ) {
    final String filename = pipelineMeta.environmentSubstitute( getFilename() );
    try {
      return HopVFS.getFileObject( filename );
    } catch ( final HopFileException e ) {
      return null;
    }
  }

}
