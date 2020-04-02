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

package org.apache.hop.pipeline.transforms.propertyinput;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.w3c.dom.Node;

import java.util.List;
import java.util.Map;

public class PropertyInputMeta extends BaseTransformMeta implements TransformMetaInterface {
  private static Class<?> PKG = PropertyInputMeta.class; // for i18n purposes, needed by Translator!!

  public static final String[] RequiredFilesDesc = new String[] {
    BaseMessages.getString( PKG, "System.Combo.No" ), BaseMessages.getString( PKG, "System.Combo.Yes" ) };

  public static final String[] RequiredFilesCode = new String[] { "N", "Y" };

  public static final String DEFAULT_ENCODING = "UTF-8";

  private static final String YES = "Y";

  public static final String[] type_trim_code = { "none", "left", "right", "both" };

  public static final String[] column_code = { "key", "value" };

  public static final String[] fileTypeDesc = new String[] {
    BaseMessages.getString( PKG, "PropertyInputMeta.FileType.Property" ),
    BaseMessages.getString( PKG, "PropertyInputMeta.FileType.Ini" ) };
  public static final String[] fileTypeCode = new String[] { "property", "ini" };
  public static final int FILE_TYPE_PROPERTY = 0;
  public static final int FILE_TYPE_INI = 1;

  private String encoding;

  private String fileType;

  /**
   * Array of filenames
   */
  private String[] fileName;

  /**
   * Wildcard or filemask (regular expression)
   */
  private String[] fileMask;

  /**
   * Wildcard or filemask to exclude (regular expression)
   */
  private String[] excludeFileMask;

  /**
   * Flag indicating that we should include the filename in the output
   */
  private boolean includeFilename;

  /**
   * Array of boolean values as string, indicating if a file is required.
   */
  private String[] fileRequired;

  /**
   * Array of boolean values as string, indicating if we need to fetch sub folders.
   */
  private String[] includeSubFolders;

  /**
   * Flag indicating that we should reset RowNum for each file
   */
  private boolean resetRowNumber;

  /**
   * Flag do variable substitution for value
   */
  private boolean resolvevaluevariable;

  /**
   * The name of the field in the output containing the filename
   */
  private String filenameField;

  /**
   * Flag indicating that a row number field should be included in the output
   */
  private boolean includeRowNumber;

  /**
   * The name of the field in the output containing the row number
   */
  private String rowNumberField;

  /**
   * The maximum number or lines to read
   */
  private long rowLimit;

  /**
   * The fields to import...
   */
  private PropertyInputField[] inputFields;

  /**
   * file name from previous fields
   **/
  private boolean filefield;

  private boolean isaddresult;

  private String dynamicFilenameField;

  /**
   * Flag indicating that a INI file section field should be included in the output
   */
  private boolean includeIniSection;

  /**
   * The name of the field in the output containing the INI file section
   */
  private String iniSectionField;

  private String section;

  /**
   * Additional fields
   **/
  private String shortFileFieldName;
  private String pathFieldName;
  private String hiddenFieldName;
  private String lastModificationTimeFieldName;
  private String uriNameFieldName;
  private String rootUriNameFieldName;
  private String extensionFieldName;
  private String sizeFieldName;

  public PropertyInputMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * @return Returns the extensionFieldName.
   */
  public String getExtensionField() {
    return extensionFieldName;
  }

  /**
   * @param field The extensionFieldName to set.
   */
  public void setExtensionField( String field ) {
    extensionFieldName = field;
  }

  /**
   * @return Returns the sizeFieldName.
   */
  public String getSizeField() {
    return sizeFieldName;
  }

  /**
   * @param field The sizeFieldName to set.
   */
  public void setSizeField( String field ) {
    sizeFieldName = field;
  }

  /**
   * @return Returns the shortFileFieldName.
   */
  public String getShortFileNameField() {
    return shortFileFieldName;
  }

  /**
   * @param field The shortFileFieldName to set.
   */
  public void setShortFileNameField( String field ) {
    shortFileFieldName = field;
  }

  /**
   * @return Returns the pathFieldName.
   */
  public String getPathField() {
    return pathFieldName;
  }

  /**
   * @param field The pathFieldName to set.
   */
  public void setPathField( String field ) {
    this.pathFieldName = field;
  }

  /**
   * @return Returns the hiddenFieldName.
   */
  public String isHiddenField() {
    return hiddenFieldName;
  }

  /**
   * @param field The hiddenFieldName to set.
   */
  public void setIsHiddenField( String field ) {
    hiddenFieldName = field;
  }

  /**
   * @return Returns the lastModificationTimeFieldName.
   */
  public String getLastModificationDateField() {
    return lastModificationTimeFieldName;
  }

  /**
   * @param field The lastModificationTimeFieldName to set.
   */
  public void setLastModificationDateField( String field ) {
    lastModificationTimeFieldName = field;
  }

  /**
   * @return Returns the uriNameFieldName.
   */
  public String getUriField() {
    return uriNameFieldName;
  }

  /**
   * @param field The uriNameFieldName to set.
   */
  public void setUriField( String field ) {
    uriNameFieldName = field;
  }

  /**
   * @return Returns the rootUriNameFieldName.
   */
  public String getRootUriField() {
    return rootUriNameFieldName;
  }

  /**
   * @param field The rootUriNameFieldName to set.
   */
  public void setRootUriField( String field ) {
    rootUriNameFieldName = field;
  }

  /**
   * @return Returns the input fields.
   */
  public PropertyInputField[] getInputFields() {
    return inputFields;
  }

  /**
   * @param inputFields The input fields to set.
   */
  public void setInputFields( PropertyInputField[] inputFields ) {
    this.inputFields = inputFields;
  }

  /**
   * @return Returns the fileMask.
   */
  public String[] getFileMask() {
    return fileMask;
  }

  /**
   * @param fileMask The fileMask to set.
   */
  public void setFileMask( String[] fileMask ) {
    this.fileMask = fileMask;
  }

  /**
   * @return Returns the fileName.
   */
  public String[] getFileName() {
    return fileName;
  }

  /**
   * @return Returns the excludeFileMask.
   * @deprecated due to typo
   */
  @Deprecated
  public String[] getExludeFileMask() {
    return excludeFileMask;
  }

  /**
   * @return Returns the excludeFileMask.
   */
  public String[] getExcludeFileMask() {
    return excludeFileMask;
  }

  /**
   * @param excludeFileMask The excludeFileMask to set.
   */
  public void setExcludeFileMask( String[] excludeFileMask ) {
    this.excludeFileMask = excludeFileMask;
  }

  public String[] getFileRequired() {
    return this.fileRequired;
  }

  public void setFileRequired( String[] fileRequiredin ) {
    for ( int i = 0; i < fileRequiredin.length; i++ ) {
      this.fileRequired[ i ] = getRequiredFilesCode( fileRequiredin[ i ] );
    }
  }

  public String[] getIncludeSubFolders() {
    return includeSubFolders;
  }

  public void setIncludeSubFolders( String[] includeSubFoldersin ) {
    for ( int i = 0; i < includeSubFoldersin.length; i++ ) {
      this.includeSubFolders[ i ] = getRequiredFilesCode( includeSubFoldersin[ i ] );
    }
  }

  public String getRequiredFilesCode( String tt ) {
    if ( tt == null ) {
      return RequiredFilesCode[ 0 ];
    }
    if ( tt.equals( RequiredFilesDesc[ 1 ] ) ) {
      return RequiredFilesCode[ 1 ];
    } else {
      return RequiredFilesCode[ 0 ];
    }
  }

  public String getRequiredFilesDesc( String tt ) {
    if ( tt == null ) {
      return RequiredFilesDesc[ 0 ];
    }
    if ( tt.equals( RequiredFilesCode[ 1 ] ) ) {
      return RequiredFilesDesc[ 1 ];
    } else {
      return RequiredFilesDesc[ 0 ];
    }
  }

  /**
   * @param fileName The fileName to set.
   */
  public void setFileName( String[] fileName ) {
    this.fileName = fileName;
  }

  /**
   * @return Returns the filenameField.
   */
  public String getFilenameField() {
    return filenameField;
  }

  /**
   * @return Returns the dynamically defined filename field (to read from previous transforms).
   */
  public String getDynamicFilenameField() {
    return dynamicFilenameField;
  }

  /**
   * @param dynamicFilenameField the dynamically defined filename field (to read from previous transforms)
   */
  public void setDynamicFilenameField( String dynamicFilenameField ) {
    this.dynamicFilenameField = dynamicFilenameField;
  }

  /**
   * @param filenameField The filenameField to set.
   */
  public void setFilenameField( String filenameField ) {
    this.filenameField = filenameField;
  }

  /**
   * @return Returns the includeFilename.
   */
  public boolean includeFilename() {
    return includeFilename;
  }

  /**
   * @param includeFilename The includeFilename to set.
   */
  public void setIncludeFilename( boolean includeFilename ) {
    this.includeFilename = includeFilename;
  }

  public static String getFileTypeCode( int i ) {
    if ( i < 0 || i >= fileTypeCode.length ) {
      return fileTypeCode[ 0 ];
    }
    return fileTypeCode[ i ];
  }

  public static int getFileTypeByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < fileTypeDesc.length; i++ ) {
      if ( fileTypeDesc[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    // If this fails, try to match using the code.
    return getFileTypeByCode( tt );
  }

  public static int getFileTypeByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < fileTypeCode.length; i++ ) {
      if ( fileTypeCode[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  public static String getFileTypeDesc( int i ) {
    if ( i < 0 || i >= fileTypeDesc.length ) {
      return fileTypeDesc[ 0 ];
    }
    return fileTypeDesc[ i ];
  }

  public void setFileType( String filetype ) {
    this.fileType = filetype;
  }

  public String getFileType() {
    return fileType;
  }

  /**
   * @param includeIniSection The includeIniSection to set.
   */
  public void setIncludeIniSection( boolean includeIniSection ) {
    this.includeIniSection = includeIniSection;
  }

  /**
   * @return Returns the includeIniSection.
   */
  public boolean includeIniSection() {
    return includeIniSection;
  }

  /**
   * @param encoding The encoding to set.
   */
  public void setEncoding( String encoding ) {
    this.encoding = encoding;
  }

  /**
   * @return Returns encoding.
   */
  public String getEncoding() {
    return encoding;
  }

  /**
   * @param iniSectionField The iniSectionField to set.
   */
  public void setINISectionField( String iniSectionField ) {
    this.iniSectionField = iniSectionField;
  }

  /**
   * @return Returns the iniSectionField.
   */
  public String getINISectionField() {
    return iniSectionField;
  }

  /**
   * @param section The section to set.
   */
  public void setSection( String section ) {
    this.section = section;
  }

  /**
   * @return Returns the section.
   */
  public String getSection() {
    return section;
  }

  /**
   * @return Returns the includeRowNumber.
   */
  public boolean includeRowNumber() {
    return includeRowNumber;
  }

  /**
   * @return Returns the File field.
   */
  public boolean isFileField() {
    return filefield;
  }

  /**
   * @param filefield The filefield to set.
   */
  public void setFileField( boolean filefield ) {
    this.filefield = filefield;
  }

  /**
   * @return Returns the resetRowNumber.
   */
  public boolean resetRowNumber() {
    return resetRowNumber;
  }

  /**
   * @param includeRowNumber The includeRowNumber to set.
   */
  public void setIncludeRowNumber( boolean includeRowNumber ) {
    this.includeRowNumber = includeRowNumber;
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
   * @param resetRowNumber The resetRowNumber to set.
   */
  public void setResetRowNumber( boolean resetRowNumber ) {
    this.resetRowNumber = resetRowNumber;
  }

  /**
   * @param resolvevaluevariable The resolvevaluevariable to set.
   */
  public void setResolveValueVariable( boolean resolvevaluevariable ) {
    this.resolvevaluevariable = resolvevaluevariable;
  }

  /**
   * @return Returns resolvevaluevariable.
   */
  public boolean isResolveValueVariable() {
    return resolvevaluevariable;
  }

  /**
   * @return Returns the rowLimit.
   */
  public long getRowLimit() {
    return rowLimit;
  }

  /**
   * @param rowLimit The rowLimit to set.
   */
  public void setRowLimit( long rowLimit ) {
    this.rowLimit = rowLimit;
  }

  /**
   * @return Returns the rowNumberField.
   */
  public String getRowNumberField() {
    return rowNumberField;
  }

  /**
   * @param rowNumberField The rowNumberField to set.
   */
  public void setRowNumberField( String rowNumberField ) {
    this.rowNumberField = rowNumberField;
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  public Object clone() {
    PropertyInputMeta retval = (PropertyInputMeta) super.clone();
    int nrFiles = fileName.length;
    int nrFields = inputFields.length;
    retval.allocate( nrFiles, nrFields );
    System.arraycopy( fileName, 0, retval.fileName, 0, nrFiles );
    System.arraycopy( fileMask, 0, retval.fileMask, 0, nrFiles );
    System.arraycopy( excludeFileMask, 0, retval.excludeFileMask, 0, nrFiles );
    System.arraycopy( fileRequired, 0, retval.fileRequired, 0, nrFiles );
    System.arraycopy( includeSubFolders, 0, retval.includeSubFolders, 0, nrFiles );
    for ( int i = 0; i < nrFields; i++ ) {
      if ( inputFields[ i ] != null ) {
        retval.inputFields[ i ] = (PropertyInputField) inputFields[ i ].clone();
      }
    }

    return retval;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 500 );
    retval.append( "    " ).append( XMLHandler.addTagValue( "file_type", fileType ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "encoding", encoding ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "include", includeFilename ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "include_field", filenameField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "filename_Field", dynamicFilenameField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "rownum", includeRowNumber ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "isaddresult", isaddresult ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "filefield", filefield ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "rownum_field", rowNumberField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "resetrownumber", resetRowNumber ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "resolvevaluevariable", resolvevaluevariable ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "ini_section", includeIniSection ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "ini_section_field", iniSectionField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "section", section ) );
    retval.append( "    <file>" ).append( Const.CR );
    for ( int i = 0; i < fileName.length; i++ ) {
      retval.append( "      " ).append( XMLHandler.addTagValue( "name", fileName[ i ] ) );
      retval.append( "      " ).append( XMLHandler.addTagValue( "exclude_filemask", excludeFileMask[ i ] ) );
      retval.append( "      " ).append( XMLHandler.addTagValue( "filemask", fileMask[ i ] ) );
      retval.append( "      " ).append( XMLHandler.addTagValue( "file_required", fileRequired[ i ] ) );
      retval.append( "      " ).append( XMLHandler.addTagValue( "include_subfolders", includeSubFolders[ i ] ) );
    }
    retval.append( "    </file>" ).append( Const.CR );

    /*
     * Describe the fields to read
     */
    retval.append( "    <fields>" ).append( Const.CR );
    for ( int i = 0; i < inputFields.length; i++ ) {
      retval.append( "      <field>" ).append( Const.CR );
      retval.append( "        " ).append( XMLHandler.addTagValue( "name", inputFields[ i ].getName() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "column", inputFields[ i ].getColumnCode() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "type", inputFields[ i ].getTypeDesc() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "format", inputFields[ i ].getFormat() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "length", inputFields[ i ].getLength() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "precision", inputFields[ i ].getPrecision() ) );
      retval
        .append( "        " ).append( XMLHandler.addTagValue( "currency", inputFields[ i ].getCurrencySymbol() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "decimal", inputFields[ i ].getDecimalSymbol() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "group", inputFields[ i ].getGroupSymbol() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "trim_type", inputFields[ i ].getTrimTypeCode() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "repeat", inputFields[ i ].isRepeated() ) );
      retval.append( "      </field>" ).append( Const.CR );
    }
    retval.append( "    </fields>" ).append( Const.CR );
    retval.append( "    " ).append( XMLHandler.addTagValue( "limit", rowLimit ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "shortFileFieldName", shortFileFieldName ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "pathFieldName", pathFieldName ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "hiddenFieldName", hiddenFieldName ) );
    retval.append( "    " ).append(
      XMLHandler.addTagValue( "lastModificationTimeFieldName", lastModificationTimeFieldName ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "uriNameFieldName", uriNameFieldName ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "rootUriNameFieldName", rootUriNameFieldName ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "extensionFieldName", extensionFieldName ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "sizeFieldName", sizeFieldName ) );
    return retval.toString();
  }

  private void readData( Node transformNode ) throws HopXMLException {
    try {
      fileType = XMLHandler.getTagValue( transformNode, "file_type" );
      encoding = XMLHandler.getTagValue( transformNode, "encoding" );
      includeFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "include" ) );
      filenameField = XMLHandler.getTagValue( transformNode, "include_field" );
      includeRowNumber = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "rownum" ) );

      String addresult = XMLHandler.getTagValue( transformNode, "isaddresult" );
      if ( Utils.isEmpty( addresult ) ) {
        isaddresult = true;
      } else {
        isaddresult = "Y".equalsIgnoreCase( addresult );
      }
      section = XMLHandler.getTagValue( transformNode, "section" );
      iniSectionField = XMLHandler.getTagValue( transformNode, "ini_section_field" );
      includeIniSection = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "ini_section" ) );
      filefield = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "filefield" ) );
      rowNumberField = XMLHandler.getTagValue( transformNode, "rownum_field" );
      dynamicFilenameField = XMLHandler.getTagValue( transformNode, "filename_Field" );
      resetRowNumber = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "resetrownumber" ) );
      resolvevaluevariable = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "resolvevaluevariable" ) );
      Node filenode = XMLHandler.getSubNode( transformNode, "file" );
      Node fields = XMLHandler.getSubNode( transformNode, "fields" );
      int nrFiles = XMLHandler.countNodes( filenode, "name" );
      int nrFields = XMLHandler.countNodes( fields, "field" );

      allocate( nrFiles, nrFields );

      for ( int i = 0; i < nrFiles; i++ ) {
        Node filenamenode = XMLHandler.getSubNodeByNr( filenode, "name", i );
        Node filemasknode = XMLHandler.getSubNodeByNr( filenode, "filemask", i );
        Node excludefilemasknode = XMLHandler.getSubNodeByNr( filenode, "exclude_filemask", i );
        Node fileRequirednode = XMLHandler.getSubNodeByNr( filenode, "file_required", i );
        Node includeSubFoldersnode = XMLHandler.getSubNodeByNr( filenode, "include_subfolders", i );
        fileName[ i ] = XMLHandler.getNodeValue( filenamenode );
        fileMask[ i ] = XMLHandler.getNodeValue( filemasknode );
        excludeFileMask[ i ] = XMLHandler.getNodeValue( excludefilemasknode );
        fileRequired[ i ] = XMLHandler.getNodeValue( fileRequirednode );
        includeSubFolders[ i ] = XMLHandler.getNodeValue( includeSubFoldersnode );
      }

      for ( int i = 0; i < nrFields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );
        inputFields[ i ] = new PropertyInputField();

        inputFields[ i ].setName( XMLHandler.getTagValue( fnode, "name" ) );
        inputFields[ i ].setColumn( getColumnByCode( XMLHandler.getTagValue( fnode, "column" ) ) );
        inputFields[ i ].setType( ValueMetaFactory.getIdForValueMeta( XMLHandler.getTagValue( fnode, "type" ) ) );
        inputFields[ i ].setLength( Const.toInt( XMLHandler.getTagValue( fnode, "length" ), -1 ) );
        inputFields[ i ].setPrecision( Const.toInt( XMLHandler.getTagValue( fnode, "precision" ), -1 ) );
        String srepeat = XMLHandler.getTagValue( fnode, "repeat" );
        inputFields[ i ].setTrimType( getTrimTypeByCode( XMLHandler.getTagValue( fnode, "trim_type" ) ) );

        if ( srepeat != null ) {
          inputFields[ i ].setRepeated( YES.equalsIgnoreCase( srepeat ) );
        } else {
          inputFields[ i ].setRepeated( false );
        }

        inputFields[ i ].setFormat( XMLHandler.getTagValue( fnode, "format" ) );
        inputFields[ i ].setCurrencySymbol( XMLHandler.getTagValue( fnode, "currency" ) );
        inputFields[ i ].setDecimalSymbol( XMLHandler.getTagValue( fnode, "decimal" ) );
        inputFields[ i ].setGroupSymbol( XMLHandler.getTagValue( fnode, "group" ) );

      }

      // Is there a limit on the number of rows we process?
      rowLimit = Const.toLong( XMLHandler.getTagValue( transformNode, "limit" ), 0L );
      shortFileFieldName = XMLHandler.getTagValue( transformNode, "shortFileFieldName" );
      pathFieldName = XMLHandler.getTagValue( transformNode, "pathFieldName" );
      hiddenFieldName = XMLHandler.getTagValue( transformNode, "hiddenFieldName" );
      lastModificationTimeFieldName = XMLHandler.getTagValue( transformNode, "lastModificationTimeFieldName" );
      uriNameFieldName = XMLHandler.getTagValue( transformNode, "uriNameFieldName" );
      rootUriNameFieldName = XMLHandler.getTagValue( transformNode, "rootUriNameFieldName" );
      extensionFieldName = XMLHandler.getTagValue( transformNode, "extensionFieldName" );
      sizeFieldName = XMLHandler.getTagValue( transformNode, "sizeFieldName" );

    } catch ( Exception e ) {
      throw new HopXMLException( "Unable to load transform info from XML", e );
    }
  }

  public void allocate( int nrfiles, int nrFields ) {
    fileName = new String[ nrfiles ];
    fileMask = new String[ nrfiles ];
    excludeFileMask = new String[ nrfiles ];
    fileRequired = new String[ nrfiles ];
    includeSubFolders = new String[ nrfiles ];
    inputFields = new PropertyInputField[ nrFields ];
  }

  public void setDefault() {
    shortFileFieldName = null;
    pathFieldName = null;
    hiddenFieldName = null;
    lastModificationTimeFieldName = null;
    uriNameFieldName = null;
    rootUriNameFieldName = null;
    extensionFieldName = null;
    sizeFieldName = null;

    fileType = fileTypeCode[ 0 ];
    section = "";
    encoding = DEFAULT_ENCODING;
    includeIniSection = false;
    iniSectionField = "";
    resolvevaluevariable = false;
    isaddresult = true;
    filefield = false;
    includeFilename = false;
    filenameField = "";
    includeRowNumber = false;
    rowNumberField = "";
    dynamicFilenameField = "";

    int nrFiles = 0;
    int nrFields = 0;

    allocate( nrFiles, nrFields );

    for ( int i = 0; i < nrFiles; i++ ) {
      fileName[ i ] = "filename" + ( i + 1 );
      fileMask[ i ] = "";
      excludeFileMask[ i ] = "";
      fileRequired[ i ] = RequiredFilesCode[ 0 ];
      includeSubFolders[ i ] = RequiredFilesCode[ 0 ];
    }

    for ( int i = 0; i < nrFields; i++ ) {
      inputFields[ i ] = new PropertyInputField( "field" + ( i + 1 ) );
    }

    rowLimit = 0;
  }

  public void getFields( IRowMeta r, String name, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {

    int i;
    for ( i = 0; i < inputFields.length; i++ ) {
      PropertyInputField field = inputFields[ i ];

      int type = field.getType();
      if ( type == IValueMeta.TYPE_NONE ) {
        type = IValueMeta.TYPE_STRING;
      }
      try {
        IValueMeta v =
          ValueMetaFactory.createValueMeta( variables.environmentSubstitute( field.getName() ), type );
        v.setLength( field.getLength() );
        v.setPrecision( field.getPrecision() );
        v.setOrigin( name );
        v.setConversionMask( field.getFormat() );
        v.setDecimalSymbol( field.getDecimalSymbol() );
        v.setGroupingSymbol( field.getGroupSymbol() );
        v.setCurrencySymbol( field.getCurrencySymbol() );
        r.addValueMeta( v );
      } catch ( Exception e ) {
        throw new HopTransformException( e );
      }
    }
    String realFilenameField = variables.environmentSubstitute( filenameField );
    if ( includeFilename && !Utils.isEmpty( realFilenameField ) ) {
      IValueMeta v = new ValueMetaString( realFilenameField );
      v.setLength( 500 );
      v.setPrecision( -1 );
      v.setOrigin( name );
      r.addValueMeta( v );
    }

    String realRowNumberField = variables.environmentSubstitute( rowNumberField );
    if ( includeRowNumber && !Utils.isEmpty( realRowNumberField ) ) {
      IValueMeta v = new ValueMetaInteger( realRowNumberField );
      v.setLength( IValueMeta.DEFAULT_INTEGER_LENGTH, 0 );
      v.setOrigin( name );
      r.addValueMeta( v );
    }
    String realSectionField = variables.environmentSubstitute( iniSectionField );
    if ( includeIniSection && !Utils.isEmpty( realSectionField ) ) {
      IValueMeta v = new ValueMetaString( realSectionField );
      v.setLength( 500 );
      v.setPrecision( -1 );
      v.setOrigin( name );
      r.addValueMeta( v );
    }
    // Add additional fields

    if ( getShortFileNameField() != null && getShortFileNameField().length() > 0 ) {
      IValueMeta v =
        new ValueMetaString( variables.environmentSubstitute( getShortFileNameField() ) );
      v.setLength( 100, -1 );
      v.setOrigin( name );
      r.addValueMeta( v );
    }
    if ( getExtensionField() != null && getExtensionField().length() > 0 ) {
      IValueMeta v =
        new ValueMetaString( variables.environmentSubstitute( getExtensionField() ) );
      v.setLength( 100, -1 );
      v.setOrigin( name );
      r.addValueMeta( v );
    }
    if ( getPathField() != null && getPathField().length() > 0 ) {
      IValueMeta v = new ValueMetaString( variables.environmentSubstitute( getPathField() ) );
      v.setLength( 100, -1 );
      v.setOrigin( name );
      r.addValueMeta( v );
    }
    if ( getSizeField() != null && getSizeField().length() > 0 ) {
      IValueMeta v = new ValueMetaInteger( variables.environmentSubstitute( getSizeField() ) );
      v.setOrigin( name );
      v.setLength( 9 );
      r.addValueMeta( v );
    }
    if ( isHiddenField() != null && isHiddenField().length() > 0 ) {
      IValueMeta v =
        new ValueMetaBoolean( variables.environmentSubstitute( isHiddenField() ) );
      v.setOrigin( name );
      r.addValueMeta( v );
    }

    if ( getLastModificationDateField() != null && getLastModificationDateField().length() > 0 ) {
      IValueMeta v =
        new ValueMetaDate( variables.environmentSubstitute( getLastModificationDateField() ) );
      v.setOrigin( name );
      r.addValueMeta( v );
    }
    if ( getUriField() != null && getUriField().length() > 0 ) {
      IValueMeta v = new ValueMetaString( variables.environmentSubstitute( getUriField() ) );
      v.setLength( 100, -1 );
      v.setOrigin( name );
      r.addValueMeta( v );
    }

    if ( getRootUriField() != null && getRootUriField().length() > 0 ) {
      IValueMeta v =
        new ValueMetaString( variables.environmentSubstitute( getRootUriField() ) );
      v.setLength( 100, -1 );
      v.setOrigin( name );
      r.addValueMeta( v );
    }
  }

  public static final int getTrimTypeByCode( String tt ) {
    if ( tt != null ) {
      for ( int i = 0; i < type_trim_code.length; i++ ) {
        if ( type_trim_code[ i ].equalsIgnoreCase( tt ) ) {
          return i;
        }
      }
    }
    return 0;
  }

  public static final int getColumnByCode( String tt ) {
    if ( tt != null ) {
      for ( int i = 0; i < column_code.length; i++ ) {
        if ( column_code[ i ].equalsIgnoreCase( tt ) ) {
          return i;
        }
      }
    }
    return 0;
  }

  public FileInputList getFiles( iVariables variables ) {
    String[] required = new String[ fileName.length ];
    boolean[] subdirs = new boolean[ fileName.length ]; // boolean arrays are defaulted to false.
    for ( int i = 0; i < required.length; i++ ) {
      required[ i ] = "Y";
    }
    return FileInputList.createFileList( variables, fileName, fileMask, excludeFileMask, required, subdirs );

  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {

    CheckResult cr;

    // See if we get input...
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "PropertyInputMeta.CheckResult.NoInputExpected" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "PropertyInputMeta.CheckResult.NoInput" ), transformMeta );
      remarks.add( cr );
    }

    FileInputList fileInputList = getFiles( pipelineMeta );

    if ( fileInputList == null || fileInputList.getFiles().size() == 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "PropertyInputMeta.CheckResult.NoFiles" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "PropertyInputMeta.CheckResult.FilesOk", "" + fileInputList.getFiles().size() ), transformMeta );
      remarks.add( cr );
    }

  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData iTransformData, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new PropertyInput( transformMeta, iTransformData, cnr, tr, pipeline );
  }

  public ITransformData getTransformData() {
    return new PropertyInputData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }

  /**
   * Since the exported pipeline that runs this will reside in a ZIP file, we can't reference files relatively. So
   * what this does is turn the name of files into absolute paths OR it simply includes the resource in the ZIP file.
   * For now, we'll simply turn it into an absolute path and pray that the file is on a shared drive or something like
   * that.
   *
   * @param variables                   the variable space to use
   * @param definitions
   * @param iResourceNaming
   * @param metaStore               the metaStore in which non-kettle metadata could reside.
   * @return the filename of the exported resource
   */
  public String exportResources( iVariables variables, Map<String, ResourceDefinition> definitions,
                                 IResourceNaming iResourceNaming, IMetaStore metaStore ) throws HopException {
    try {
      // The object that we're modifying here is a copy of the original!
      // So let's change the filename from relative to absolute by grabbing the file object...
      // In case the name of the file comes from previous transforms, forget about this!
      //
      if ( !filefield ) {
        for ( int i = 0; i < fileName.length; i++ ) {
          FileObject fileObject = HopVFS.getFileObject( variables.environmentSubstitute( fileName[ i ] ), variables );
          fileName[ i ] = iResourceNaming.nameResource( fileObject, variables, Utils.isEmpty( fileMask[ i ] ) );
        }
      }
      return null;
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

}
