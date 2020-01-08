/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.trans.steps.fixedinput;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceNamingInterface;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInjectionInterface;
import org.apache.hop.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author matt
 * @version 3.0
 * @since 2007-07-05
 */

public class FixedInputMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = FixedInputMeta.class; // for i18n purposes, needed by Translator2!!

  public static final int FILE_TYPE_NONE = 0;
  public static final int FILE_TYPE_UNIX = 1;
  public static final int FILE_TYPE_DOS = 2;

  public static final String[] fileTypeCode = new String[] { "NONE", "UNIX", "DOS", };
  public static final String[] fileTypeDesc = new String[] {
    BaseMessages.getString( PKG, "FixedFileInputMeta.FileType.None.Desc" ),
    BaseMessages.getString( PKG, "FixedFileInputMeta.FileType.Unix.Desc" ),
    BaseMessages.getString( PKG, "FixedFileInputMeta.FileType.Dos.Desc" ), };

  private String filename;

  private boolean headerPresent;

  private String lineWidth;

  private String bufferSize;

  private boolean lazyConversionActive;

  private boolean lineFeedPresent;

  private boolean runningInParallel;

  private int fileType;

  private boolean isaddresult;

  /**
   * The encoding to use for reading: null or empty string means system default encoding
   */
  private String encoding;

  private FixedFileInputField[] fieldDefinition;

  public FixedInputMeta() {
    super(); // allocate BaseStepMeta
    allocate( 0 );
  }

  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode );
  }

  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  public void setDefault() {
    isaddresult = false;
    lineWidth = "80";
    headerPresent = true;
    lazyConversionActive = true;
    bufferSize = "50000";
    lineFeedPresent = true;
  }

  private void readData( Node stepnode ) throws HopXMLException {
    try {
      filename = XMLHandler.getTagValue( stepnode, "filename" );
      lineWidth = XMLHandler.getTagValue( stepnode, "line_width" );
      bufferSize = XMLHandler.getTagValue( stepnode, "buffer_size" );
      headerPresent = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "header" ) );
      lineFeedPresent = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "line_feed" ) );
      lazyConversionActive = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "lazy_conversion" ) );
      runningInParallel = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "parallel" ) );
      fileType = getFileType( XMLHandler.getTagValue( stepnode, "file_type" ) );
      encoding = XMLHandler.getTagValue( stepnode, "encoding" );
      isaddresult = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "add_to_result_filenames" ) );

      Node fields = XMLHandler.getSubNode( stepnode, "fields" );
      int nrfields = XMLHandler.countNodes( fields, "field" );

      allocate( nrfields );

      for ( int i = 0; i < nrfields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );
        fieldDefinition[ i ] = new FixedFileInputField( fnode );
      }
    } catch ( Exception e ) {
      throw new HopXMLException( "Unable to load step info from XML", e );
    }
  }

  public void allocate( int nrFields ) {
    fieldDefinition = new FixedFileInputField[ nrFields ];
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 500 );

    retval.append( "    " ).append( XMLHandler.addTagValue( "filename", filename ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "line_width", lineWidth ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "header", headerPresent ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "buffer_size", bufferSize ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "lazy_conversion", lazyConversionActive ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "line_feed", lineFeedPresent ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "parallel", runningInParallel ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "file_type", getFileTypeCode() ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "encoding", encoding ) );

    retval.append( "    " ).append( XMLHandler.addTagValue( "add_to_result_filenames", isaddresult ) );

    retval.append( "    <fields>" ).append( Const.CR );
    for ( int i = 0; i < fieldDefinition.length; i++ ) {
      retval.append( fieldDefinition[ i ].getXML() );
    }
    retval.append( "    </fields>" ).append( Const.CR );

    return retval.toString();
  }

  public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, IMetaStore metaStore ) throws HopStepException {
    try {
      for ( int i = 0; i < fieldDefinition.length; i++ ) {
        FixedFileInputField field = fieldDefinition[ i ];

        ValueMetaInterface valueMeta = ValueMetaFactory.createValueMeta( field.getName(), field.getType() );
        valueMeta.setConversionMask( field.getFormat() );
        valueMeta.setTrimType( field.getTrimType() );
        valueMeta.setLength( field.getLength() );
        valueMeta.setPrecision( field.getPrecision() );
        valueMeta.setConversionMask( field.getFormat() );
        valueMeta.setDecimalSymbol( field.getDecimal() );
        valueMeta.setGroupingSymbol( field.getGrouping() );
        valueMeta.setCurrencySymbol( field.getCurrency() );
        valueMeta.setStringEncoding( space.environmentSubstitute( encoding ) );
        if ( lazyConversionActive ) {
          valueMeta.setStorageType( ValueMetaInterface.STORAGE_TYPE_BINARY_STRING );
        }

        // In case we want to convert Strings...
        //
        ValueMetaInterface storageMetadata =
          ValueMetaFactory.cloneValueMeta( valueMeta, ValueMetaInterface.TYPE_STRING );
        storageMetadata.setStorageType( ValueMetaInterface.STORAGE_TYPE_NORMAL );

        valueMeta.setStorageMetadata( storageMetadata );

        valueMeta.setOrigin( origin );

        rowMeta.addValueMeta( valueMeta );
      }
    } catch ( Exception e ) {
      throw new HopStepException( e );
    }
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "FixedInputMeta.CheckResult.NotReceivingFields" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "FixedInputMeta.CheckResult.StepRecevingData", prev.size() + "" ), stepMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this step!
    if ( Utils.isEmpty( filename ) ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "FixedInputMeta.CheckResult.NoFilenameSpecified" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "FixedInputMeta.CheckResult.FilenameSpecified" ), stepMeta );
      remarks.add( cr );
    }
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
                                Trans trans ) {
    return new FixedInput( stepMeta, stepDataInterface, cnr, tr, trans );
  }

  public StepDataInterface getStepData() {
    return new FixedInputData();
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
   * @return the lineWidth
   */
  public String getLineWidth() {
    return lineWidth;
  }

  /**
   * @return the lineFeedPresent
   */
  public boolean isLineFeedPresent() {
    return lineFeedPresent;
  }

  /**
   * @param lineWidth the lineWidth to set
   */
  public void setLineWidth( String lineWidth ) {
    this.lineWidth = lineWidth;
  }

  /**
   * @param lineFeedPresent the lineFeedPresent to set
   */
  public void setLineFeedPresent( boolean lineFeedPresent ) {
    this.lineFeedPresent = lineFeedPresent;
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
   * @return the fieldDefinition
   */
  public FixedFileInputField[] getFieldDefinition() {
    return fieldDefinition;
  }

  /**
   * @param fieldDefinition the fieldDefinition to set
   */
  public void setFieldDefinition( FixedFileInputField[] fieldDefinition ) {
    this.fieldDefinition = fieldDefinition;
  }

  @Override
  public List<ResourceReference> getResourceDependencies( TransMeta transMeta, StepMeta stepInfo ) {
    List<ResourceReference> references = new ArrayList<ResourceReference>( 5 );

    ResourceReference reference = new ResourceReference( stepInfo );
    references.add( reference );
    if ( !Utils.isEmpty( filename ) ) {
      // Add the filename to the references, including a reference to this step meta data.
      //
      reference.getEntries().add(
        new ResourceEntry( transMeta.environmentSubstitute( filename ), ResourceType.FILE ) );
    }
    return references;
  }

  /**
   * @return the fileType
   */
  public int getFileType() {
    return fileType;
  }

  /**
   * @param fileType the fileType to set
   */
  public void setFileType( int fileType ) {
    this.fileType = fileType;
  }

  public static final String getFileTypeCode( int fileType ) {
    return fileTypeCode[ fileType ];
  }

  public static final String getFileTypeDesc( int fileType ) {
    return fileTypeDesc[ fileType ];
  }

  public String getFileTypeCode() {
    return getFileTypeCode( fileType );
  }

  public String getFileTypeDesc() {
    return getFileTypeDesc( fileType );
  }

  public static final int getFileType( String fileTypeCode ) {
    int t = Const.indexOfString( fileTypeCode, FixedInputMeta.fileTypeCode );
    if ( t >= 0 ) {
      return t;
    }
    t = Const.indexOfString( fileTypeCode, FixedInputMeta.fileTypeDesc );
    if ( t >= 0 ) {
      return t;
    }
    return FILE_TYPE_NONE;
  }

  public int getLineSeparatorLength() {
    if ( isLineFeedPresent() ) {
      switch ( fileType ) {
        case FILE_TYPE_NONE:
          return 0;
        case FILE_TYPE_UNIX:
          return 1;
        case FILE_TYPE_DOS:
          return 2;
        default:
          return 0;
      }
    } else {
      return 0;
    }
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
   * @param space                   the variable space to use
   * @param definitions
   * @param resourceNamingInterface
   * @param metaStore               the metaStore in which non-kettle metadata could reside.
   * @return the filename of the exported resource
   */
  public String exportResources( VariableSpace space, Map<String, ResourceDefinition> definitions,
                                 ResourceNamingInterface resourceNamingInterface, IMetaStore metaStore ) throws HopException {
    try {
      // The object that we're modifying here is a copy of the original!
      // So let's change the filename from relative to absolute by grabbing the file object...
      //
      // From : ${Internal.Transformation.Filename.Directory}/../foo/bar.txt
      // To : /home/matt/test/files/foo/bar.txt
      //
      FileObject fileObject = HopVFS.getFileObject( space.environmentSubstitute( filename ), space );

      // If the file doesn't exist, forget about this effort too!
      //
      if ( fileObject.exists() ) {
        // Convert to an absolute path...
        //
        filename = resourceNamingInterface.nameResource( fileObject, space, true );

        return filename;
      }
      return null;
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

  public StepMetaInjectionInterface getStepMetaInjectionInterface() {
    return new FixedInputMetaInjection( this );
  }
}
