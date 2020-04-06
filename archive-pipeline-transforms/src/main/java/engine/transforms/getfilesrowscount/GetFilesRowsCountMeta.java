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

package org.apache.hop.pipeline.transforms.getfilesrowscount;

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
import org.apache.hop.core.row.value.ValueMetaInteger;
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
import org.apache.hop.pipeline.transform.ITransform;
import org.w3c.dom.Node;

import java.util.List;
import java.util.Map;

public class GetFilesRowsCountMeta extends BaseTransformMeta implements ITransform {
  private static Class<?> PKG = GetFilesRowsCountMeta.class; // for i18n purposes, needed by Translator!!

  public static final String[] RequiredFilesDesc = new String[] {
    BaseMessages.getString( PKG, "System.Combo.No" ), BaseMessages.getString( PKG, "System.Combo.Yes" ) };
  public static final String[] RequiredFilesCode = new String[] { "N", "Y" };
  private static final String NO = "N";
  private static final String YES = "Y";

  public static final String DEFAULT_ROWSCOUNT_FIELDNAME = "rowscount";

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
   * Flag indicating that a row number field should be included in the output
   */
  private boolean includeFilesCount;

  /**
   * The name of the field in the output containing the file number
   */
  private String filesCountFieldName;

  /**
   * The name of the field in the output containing the row number
   */
  private String rowsCountFieldName;

  /**
   * The row separator type
   */
  private String RowSeparator_format;

  /**
   * The row separator
   */
  private String RowSeparator;

  /**
   * file name from previous fields
   **/
  private boolean filefield;

  private boolean isaddresult;

  private String outputFilenameField;

  /**
   * Array of boolean values as string, indicating if a file is required.
   */
  private String[] fileRequired;

  /**
   * Array of boolean values as string, indicating if we need to fetch sub folders.
   */
  private String[] includeSubFolders;

  /**
   * Flag : check if a data is there right after separator
   **/
  private boolean smartCount;

  public GetFilesRowsCountMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * @return Returns the row separator.
   */
  public String getRowSeparator() {
    return RowSeparator;
  }

  /**
   * @param RowSeparatorin The RowSeparator to set.
   */
  public void setRowSeparator( String RowSeparatorin ) {
    this.RowSeparator = RowSeparatorin;
  }

  /**
   * @return Returns the row separator format.
   */
  public String getRowSeparatorFormat() {
    return RowSeparator_format;
  }

  /**
   * @param isaddresult The isaddresult to set.
   */
  public void setAddResultFile( boolean isaddresult ) {
    this.isaddresult = isaddresult;
  }

  /**
   * @param smartCount The smartCount to set.
   */
  public void setSmartCount( boolean smartCount ) {
    this.smartCount = smartCount;
  }

  /**
   * @return Returns the excludeFileMask.
   * Deprecated due to typo
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

  /**
   * @return Returns isaddresult.
   */
  public boolean isAddResultFile() {
    return isaddresult;
  }

  /**
   * @return Returns smartCount.
   */
  public boolean isSmartCount() {
    return smartCount;
  }

  /**
   * @return Returns the output filename_Field.
   * Deprecated due to typo
   */
  @Deprecated
  public String setOutputFilenameField() {
    return outputFilenameField;
  }

  /**
   * @return Returns the output filename_Field.
   */
  public String getOutputFilenameField() {
    return outputFilenameField;
  }

  /**
   * @param outputFilenameField The output filename_field to set.
   */
  public void setOutputFilenameField( String outputFilenameField ) {
    this.outputFilenameField = outputFilenameField;
  }

  /**
   * @return Returns the File field.
   */
  public boolean isFileField() {
    return filefield;
  }

  /**
   * @param filefield The file field to set.
   */
  public void setFileField( boolean filefield ) {
    this.filefield = filefield;
  }

  /**
   * @param RowSeparator_formatin The RowSeparator_format to set.
   */
  public void setRowSeparatorFormat( String RowSeparator_formatin ) {
    this.RowSeparator_format = RowSeparator_formatin;
  }

  /**
   * @return Returns the fileMask.
   */
  public String[] getFileMask() {
    return fileMask;
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
   * @param fileName The fileName to set.
   */
  public void setFileName( String[] fileName ) {
    this.fileName = fileName;
  }

  /**
   * @return Returns the includeCountFiles.
   */
  public boolean includeCountFiles() {
    return includeFilesCount;
  }

  /**
   * @param includeFilesCount The "includes files count" flag to set.
   */
  public void setIncludeCountFiles( boolean includeFilesCount ) {
    this.includeFilesCount = includeFilesCount;
  }

  public String[] getFileRequired() {
    return this.fileRequired;
  }

  /**
   * @return Returns the FilesCountFieldName.
   */
  public String getFilesCountFieldName() {
    return filesCountFieldName;
  }

  /**
   * @return Returns the RowsCountFieldName.
   */
  public String getRowsCountFieldName() {
    return rowsCountFieldName;
  }

  /**
   * @param filesCountFieldName The filesCountFieldName to set.
   */
  public void setFilesCountFieldName( String filesCountFieldName ) {
    this.filesCountFieldName = filesCountFieldName;
  }

  /**
   * @param rowsCountFieldName The rowsCountFieldName to set.
   */
  public void setRowsCountFieldName( String rowsCountFieldName ) {
    this.rowsCountFieldName = rowsCountFieldName;
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  public Object clone() {
    GetFilesRowsCountMeta retval = (GetFilesRowsCountMeta) super.clone();

    int nrFiles = fileName.length;

    retval.allocate( nrFiles );
    System.arraycopy( fileName, 0, retval.fileName, 0, nrFiles );
    System.arraycopy( fileMask, 0, retval.fileMask, 0, nrFiles );
    System.arraycopy( excludeFileMask, 0, retval.excludeFileMask, 0, nrFiles );
    System.arraycopy( fileRequired, 0, retval.fileRequired, 0, nrFiles );
    System.arraycopy( includeSubFolders, 0, retval.includeSubFolders, 0, nrFiles );

    return retval;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( "    " ).append( XMLHandler.addTagValue( "files_count", includeFilesCount ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "files_count_fieldname", filesCountFieldName ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "rows_count_fieldname", rowsCountFieldName ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "rowseparator_format", RowSeparator_format ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "row_separator", RowSeparator ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "isaddresult", isaddresult ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "filefield", filefield ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "filename_Field", outputFilenameField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "smartCount", smartCount ) );

    retval.append( "    <file>" ).append( Const.CR );
    for ( int i = 0; i < fileName.length; i++ ) {
      retval.append( "      " ).append( XMLHandler.addTagValue( "name", fileName[ i ] ) );
      retval.append( "      " ).append( XMLHandler.addTagValue( "filemask", fileMask[ i ] ) );
      retval.append( "      " ).append( XMLHandler.addTagValue( "exclude_filemask", excludeFileMask[ i ] ) );
      retval.append( "      " ).append( XMLHandler.addTagValue( "file_required", fileRequired[ i ] ) );
      retval.append( "      " ).append( XMLHandler.addTagValue( "include_subfolders", includeSubFolders[ i ] ) );
    }
    retval.append( "    </file>" ).append( Const.CR );

    return retval.toString();
  }

  /**
   * Adjust old outdated values to new ones
   *
   * @param original The original value
   * @return The new/correct equivelant
   */
  private String scrubOldRowSeparator( String original ) {
    if ( original != null ) {
      // Update old files to the new format
      if ( original.equalsIgnoreCase( "CR" ) ) {
        return "LINEFEED";
      } else if ( original.equalsIgnoreCase( "LF" ) ) {
        return "CARRIAGERETURN";
      }
    }
    return original;
  }

  private void readData( Node transformNode ) throws HopXMLException {
    try {

      includeFilesCount = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "files_count" ) );
      filesCountFieldName = XMLHandler.getTagValue( transformNode, "files_count_fieldname" );
      rowsCountFieldName = XMLHandler.getTagValue( transformNode, "rows_count_fieldname" );

      RowSeparator_format = scrubOldRowSeparator( XMLHandler.getTagValue( transformNode, "rowseparator_format" ) );
      RowSeparator = XMLHandler.getTagValue( transformNode, "row_separator" );

      smartCount = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "smartCount" ) );

      String addresult = XMLHandler.getTagValue( transformNode, "isaddresult" );
      if ( Utils.isEmpty( addresult ) ) {
        isaddresult = true;
      } else {
        isaddresult = "Y".equalsIgnoreCase( addresult );
      }

      filefield = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "filefield" ) );
      outputFilenameField = XMLHandler.getTagValue( transformNode, "filename_Field" );

      Node filenode = XMLHandler.getSubNode( transformNode, "file" );
      int nrFiles = XMLHandler.countNodes( filenode, "name" );
      allocate( nrFiles );

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

    } catch ( Exception e ) {
      throw new HopXMLException( "Unable to load transform info from XML", e );
    }
  }

  public void allocate( int nrfiles ) {
    fileName = new String[ nrfiles ];
    fileMask = new String[ nrfiles ];
    excludeFileMask = new String[ nrfiles ];
    fileRequired = new String[ nrfiles ];
    includeSubFolders = new String[ nrfiles ];
  }

  public void setDefault() {
    smartCount = false;
    outputFilenameField = "";
    filefield = false;
    isaddresult = true;
    includeFilesCount = false;
    filesCountFieldName = "";
    rowsCountFieldName = "rowscount";
    RowSeparator_format = "CR";
    RowSeparator = "";
    int nrFiles = 0;

    allocate( nrFiles );

    for ( int i = 0; i < nrFiles; i++ ) {
      fileName[ i ] = "filename" + ( i + 1 );
      fileMask[ i ] = "";
      excludeFileMask[ i ] = "";
      fileRequired[ i ] = RequiredFilesCode[ 0 ];
      includeSubFolders[ i ] = RequiredFilesCode[ 0 ];
    }

  }

  public void getFields( IRowMeta r, String name, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {
    IValueMeta v =
      new ValueMetaInteger( variables.environmentSubstitute( rowsCountFieldName ) );
    v.setLength( IValueMeta.DEFAULT_INTEGER_LENGTH, 0 );
    v.setOrigin( name );
    r.addValueMeta( v );

    if ( includeFilesCount ) {
      v = new ValueMetaInteger( variables.environmentSubstitute( filesCountFieldName ) );
      v.setLength( IValueMeta.DEFAULT_INTEGER_LENGTH, 0 );
      v.setOrigin( name );
      r.addValueMeta( v );
    }
  }

  public FileInputList getFiles( iVariables variables ) {
    return FileInputList.createFileList(
      variables, fileName, fileMask, excludeFileMask, fileRequired, includeSubFolderBoolean() );
  }

  private boolean[] includeSubFolderBoolean() {
    int len = fileName.length;
    boolean[] includeSubFolderBoolean = new boolean[ len ];
    for ( int i = 0; i < len; i++ ) {
      includeSubFolderBoolean[ i ] = YES.equalsIgnoreCase( includeSubFolders[ i ] );
    }
    return includeSubFolderBoolean;
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {

    CheckResult cr;

    // See if we get input...
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "GetFilesRowsCountMeta.CheckResult.NoInputExpected" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "GetFilesRowsCountMeta.CheckResult.NoInput" ), transformMeta );
      remarks.add( cr );
    }

    FileInputList fileInputList = getFiles( pipelineMeta );

    if ( fileInputList == null || fileInputList.getFiles().size() == 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "GetFilesRowsCountMeta.CheckResult.NoFiles" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "GetFilesRowsCountMeta.CheckResult.FilesOk", "" + fileInputList.getFiles().size() ), transformMeta );
      remarks.add( cr );
    }

    if ( ( RowSeparator_format.equals( "CUSTOM" ) ) && ( RowSeparator == null ) ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "GetFilesRowsCountMeta.CheckResult.NoSeparator" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "GetFilesRowsCountMeta.CheckResult.SeparatorOk" ), transformMeta );
      remarks.add( cr );
    }

  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new GetFilesRowsCount( transformMeta, this, data, cnr, tr, pipeline );
  }

  public ITransformData getTransformData() {
    return new GetFilesRowsCountData();
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
