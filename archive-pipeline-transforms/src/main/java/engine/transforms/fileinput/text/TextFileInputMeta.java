/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.fileinput.text;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.core.vfs.AliasedFileObject;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.apache.hop.pipeline.transforms.common.CsvInputAwareMeta;
import org.apache.hop.pipeline.transforms.file.BaseFileField;
import org.apache.hop.pipeline.transforms.file.BaseFileInputAdditionalField;
import org.apache.hop.pipeline.transforms.file.BaseFileInputFiles;
import org.apache.hop.pipeline.transforms.file.BaseFileInputMeta;
import org.apache.hop.workarounds.ResolvableResource;
import org.w3c.dom.Node;

import java.util.List;
import java.util.Locale;
import java.util.Map;

@SuppressWarnings( "deprecation" )
@InjectionSupported( localizationPrefix = "TextFileInput.Injection.", groups = { "FILENAME_LINES", "FIELDS", "FILTERS" } )
public class TextFileInputMeta extends BaseFileInputMeta<BaseFileInputAdditionalField, BaseFileInputFiles, BaseFileField>
  implements TransformMetaInterface, ResolvableResource, CsvInputAwareMeta {
  private static Class<?> PKG = TextFileInputMeta.class; // for i18n purposes, needed by Translator!! TODO: check i18n
  // for base

  private static final String STRING_BASE64_PREFIX = "Base64: ";

  public static final int FILE_FORMAT_DOS = 0;
  public static final int FILE_FORMAT_UNIX = 1;
  public static final int FILE_FORMAT_MIXED = 2;

  public static final int FILE_TYPE_CSV = 0;
  public static final int FILE_TYPE_FIXED = 1;

  @InjectionDeep
  public Content content = new Content();

  public static class Content implements Cloneable {

    /**
     * Type of file: CSV or fixed
     */
    @Injection( name = "FILE_TYPE" )
    public String fileType;

    /**
     * String used to separated field (;)
     */
    @Injection( name = "SEPARATOR" )
    public String separator;

    /**
     * String used to enclose separated fields (")
     */
    @Injection( name = "ENCLOSURE" )
    public String enclosure;

    /**
     * Switch to allow breaks (CR/LF) in Enclosures
     */
    @Injection( name = "BREAK_IN_ENCLOSURE" )
    public boolean breakInEnclosureAllowed;

    /**
     * Escape character used to escape the enclosure String (\)
     */
    @Injection( name = "ESCAPE_CHAR" )
    public String escapeCharacter;

    /**
     * Flag indicating that the file contains one header line that should be skipped.
     */
    @Injection( name = "HEADER_PRESENT" )
    public boolean header;

    /**
     * The number of header lines, defaults to 1
     */
    @Injection( name = "NR_HEADER_LINES" )
    public int nrHeaderLines = -1;

    /**
     * Flag indicating that the file contains one footer line that should be skipped.
     */
    @Injection( name = "HAS_FOOTER" )
    public boolean footer;

    /**
     * The number of footer lines, defaults to 1
     */
    @Injection( name = "NR_FOOTER_LINES" )
    public int nrFooterLines = -1;

    /**
     * Flag indicating that a single line is wrapped onto one or more lines in the text file.
     */
    @Injection( name = "HAS_WRAPPED_LINES" )
    public boolean lineWrapped;

    /**
     * The number of times the line wrapped
     */
    @Injection( name = "NR_WRAPS" )
    public int nrWraps = -1;

    /**
     * Flag indicating that the text-file has a paged layout.
     */
    @Injection( name = "HAS_PAGED_LAYOUT" )
    public boolean layoutPaged;

    /**
     * The number of lines to read per page
     */
    @Injection( name = "NR_LINES_PER_PAGE" )
    public int nrLinesPerPage = -1;

    /**
     * The number of lines in the document header
     */
    @Injection( name = "NR_DOC_HEADER_LINES" )
    public int nrLinesDocHeader = -1;

    /**
     * Type of compression being used
     */
    @Injection( name = "COMPRESSION_TYPE" )
    public String fileCompression;

    /**
     * Flag indicating that we should skip all empty lines
     */
    @Injection( name = "NO_EMPTY_LINES" )
    public boolean noEmptyLines;

    /**
     * Flag indicating that we should include the filename in the output
     */
    @Injection( name = "INCLUDE_FILENAME" )
    public boolean includeFilename;

    /**
     * The name of the field in the output containing the filename
     */
    @Injection( name = "FILENAME_FIELD" )
    public String filenameField;

    /**
     * Flag indicating that a row number field should be included in the output
     */
    @Injection( name = "INCLUDE_ROW_NUMBER" )
    public boolean includeRowNumber;

    /**
     * The name of the field in the output containing the row number
     */
    @Injection( name = "ROW_NUMBER_FIELD" )
    public String rowNumberField;

    /**
     * Flag indicating row number is per file
     */
    @Injection( name = "ROW_NUMBER_BY_FILE" )
    public boolean rowNumberByFile;

    /**
     * The file format: DOS or UNIX or mixed
     */
    @Injection( name = "FILE_FORMAT" )
    public String fileFormat;

    /**
     * The encoding to use for reading: null or empty string means system default encoding
     */
    @Injection( name = "ENCODING" )
    public String encoding;

    /**
     * The maximum number or lines to read
     */
    @Injection( name = "ROW_LIMIT" )
    public long rowLimit = -1;

    /**
     * Indicate whether or not we want to date fields strictly according to the format or lenient
     */
    @Injection( name = "DATE_FORMAT_LENIENT" )
    public boolean dateFormatLenient;

    /**
     * Specifies the Locale of the Date format, null means the default
     */
    public Locale dateFormatLocale;

    @Injection( name = "DATE_FORMAT_LOCALE" )
    public void setDateFormatLocale( String locale ) {
      this.dateFormatLocale = new Locale( locale );
    }

    /**
     * Length based on bytes or characters
     */
    @Injection( name = "LENGTH" )
    public String length;

  }

  /**
   * The filters to use...
   */
  @InjectionDeep
  private TextFileFilter[] filter = {};

  /**
   * The name of the field that will contain the number of errors in the row
   */
  @Injection( name = "ERROR_COUNT_FIELD" )
  public String errorCountField;

  /**
   * The name of the field that will contain the names of the fields that generated errors, separated by ,
   */
  @Injection( name = "ERROR_FIELDS_FIELD" )
  public String errorFieldsField;

  /**
   * The name of the field that will contain the error texts, separated by CR
   */
  @Injection( name = "ERROR_TEXT_FIELD" )
  public String errorTextField;

  /**
   * If error line are skipped, you can replay without introducing doubles.
   */
  @Injection( name = "ERROR_LINES_SKIPPED" )
  public boolean errorLineSkipped;

  /**
   * The transform to accept filenames from
   */
  private TransformMeta acceptingTransform;

  public TextFileInputMeta() {
    additionalOutputFields = new BaseFileInputAdditionalField();
    inputFiles = new BaseFileInputFiles();
    inputFields = new BaseFileField[ 0 ];
  }

  /**
   * @return Returns the fileName.
   */
  public String[] getFileName() {
    return inputFiles.fileName;
  }

  /**
   * @param fileName The fileName to set.
   */
  public void setFileName( String[] fileName ) {
    inputFiles.fileName = fileName;
  }

  /**
   * @return The array of filters for the metadata of this text file input transform.
   */
  public TextFileFilter[] getFilter() {
    return filter;
  }

  /**
   * @param filter The array of filters to use
   */
  public void setFilter( TextFileFilter[] filter ) {
    this.filter = filter;
  }

  @Override
  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    try {
      inputFiles.acceptingFilenames = YES.equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "accept_filenames" ) );
      inputFiles.passingThruFields =
        YES.equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "passing_through_fields" ) );
      inputFiles.acceptingField = XMLHandler.getTagValue( transformNode, "accept_field" );
      inputFiles.acceptingTransformName = XMLHandler.getTagValue( transformNode, "accept_transform_name" );

      content.separator = XMLHandler.getTagValue( transformNode, "separator" );
      content.enclosure = XMLHandler.getTagValue( transformNode, "enclosure" );
      content.breakInEnclosureAllowed = YES.equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "enclosure_breaks" ) );
      content.escapeCharacter = XMLHandler.getTagValue( transformNode, "escapechar" );
      content.header = YES.equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "header" ) );
      content.nrHeaderLines = Const.toInt( XMLHandler.getTagValue( transformNode, "nr_headerlines" ), 1 );
      content.footer = YES.equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "footer" ) );
      content.nrFooterLines = Const.toInt( XMLHandler.getTagValue( transformNode, "nr_footerlines" ), 1 );
      content.lineWrapped = YES.equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "line_wrapped" ) );
      content.nrWraps = Const.toInt( XMLHandler.getTagValue( transformNode, "nr_wraps" ), 1 );
      content.layoutPaged = YES.equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "layout_paged" ) );
      content.nrLinesPerPage = Const.toInt( XMLHandler.getTagValue( transformNode, "nr_lines_per_page" ), 1 );
      content.nrLinesDocHeader = Const.toInt( XMLHandler.getTagValue( transformNode, "nr_lines_doc_header" ), 1 );
      String addToResult = XMLHandler.getTagValue( transformNode, "add_to_result_filenames" );
      if ( Utils.isEmpty( addToResult ) ) {
        inputFiles.isaddresult = true;
      } else {
        inputFiles.isaddresult = "Y".equalsIgnoreCase( addToResult );
      }

      String nempty = XMLHandler.getTagValue( transformNode, "noempty" );
      content.noEmptyLines = YES.equalsIgnoreCase( nempty ) || nempty == null;
      content.includeFilename = YES.equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "include" ) );
      content.filenameField = XMLHandler.getTagValue( transformNode, "include_field" );
      content.includeRowNumber = YES.equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "rownum" ) );
      content.rowNumberByFile = YES.equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "rownumByFile" ) );
      content.rowNumberField = XMLHandler.getTagValue( transformNode, "rownum_field" );
      content.fileFormat = XMLHandler.getTagValue( transformNode, "format" );
      content.encoding = XMLHandler.getTagValue( transformNode, "encoding" );
      content.length = XMLHandler.getTagValue( transformNode, "length" );

      Node filenode = XMLHandler.getSubNode( transformNode, "file" );
      Node fields = XMLHandler.getSubNode( transformNode, "fields" );
      Node filtersNode = XMLHandler.getSubNode( transformNode, "filters" );
      int nrfiles = XMLHandler.countNodes( filenode, "name" );
      int nrFields = XMLHandler.countNodes( fields, "field" );
      int nrfilters = XMLHandler.countNodes( filtersNode, "filter" );

      allocate( nrfiles, nrFields, nrfilters );

      for ( int i = 0; i < nrfiles; i++ ) {
        Node filenamenode = XMLHandler.getSubNodeByNr( filenode, "name", i );
        Node filemasknode = XMLHandler.getSubNodeByNr( filenode, "filemask", i );
        Node excludefilemasknode = XMLHandler.getSubNodeByNr( filenode, "exclude_filemask", i );
        Node fileRequirednode = XMLHandler.getSubNodeByNr( filenode, "file_required", i );
        Node includeSubFoldersnode = XMLHandler.getSubNodeByNr( filenode, "include_subfolders", i );
        inputFiles.fileName[ i ] = loadSource( filenode, filenamenode, i, metaStore );
        inputFiles.fileMask[ i ] = XMLHandler.getNodeValue( filemasknode );
        inputFiles.excludeFileMask[ i ] = XMLHandler.getNodeValue( excludefilemasknode );
        inputFiles.fileRequired[ i ] = XMLHandler.getNodeValue( fileRequirednode );
        inputFiles.includeSubFolders[ i ] = XMLHandler.getNodeValue( includeSubFoldersnode );
      }

      content.fileType = XMLHandler.getTagValue( transformNode, "file", "type" );
      content.fileCompression = XMLHandler.getTagValue( transformNode, "file", "compression" );
      if ( content.fileCompression == null ) {
        content.fileCompression = "None";
        if ( YES.equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "zipped" ) ) ) {
          content.fileCompression = "Zip";
        }
      }

      // Backward compatibility : just one filter
      if ( XMLHandler.getTagValue( transformNode, "filter" ) != null ) {
        filter = new TextFileFilter[ 1 ];
        filter[ 0 ] = new TextFileFilter();

        filter[ 0 ].setFilterPosition( Const.toInt( XMLHandler.getTagValue( transformNode, "filter_position" ), -1 ) );
        filter[ 0 ].setFilterString( XMLHandler.getTagValue( transformNode, "filter_string" ) );
        filter[ 0 ].setFilterLastLine( YES.equalsIgnoreCase( XMLHandler.getTagValue( transformNode,
          "filter_is_last_line" ) ) );
        filter[ 0 ].setFilterPositive( YES.equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "filter_is_positive" ) ) );
      } else {
        for ( int i = 0; i < nrfilters; i++ ) {
          Node fnode = XMLHandler.getSubNodeByNr( filtersNode, "filter", i );
          filter[ i ] = new TextFileFilter();

          filter[ i ].setFilterPosition( Const.toInt( XMLHandler.getTagValue( fnode, "filter_position" ), -1 ) );

          String filterString = XMLHandler.getTagValue( fnode, "filter_string" );
          if ( filterString != null && filterString.startsWith( STRING_BASE64_PREFIX ) ) {
            filter[ i ].setFilterString( new String( Base64.decodeBase64( filterString.substring( STRING_BASE64_PREFIX
              .length() ).getBytes() ) ) );
          } else {
            filter[ i ].setFilterString( filterString );
          }

          filter[ i ].setFilterLastLine( YES.equalsIgnoreCase( XMLHandler.getTagValue( fnode, "filter_is_last_line" ) ) );
          filter[ i ].setFilterPositive( YES.equalsIgnoreCase( XMLHandler.getTagValue( fnode, "filter_is_positive" ) ) );
        }
      }

      for ( int i = 0; i < nrFields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );
        BaseFileField field = new BaseFileField();

        field.setName( XMLHandler.getTagValue( fnode, "name" ) );
        field.setType( ValueMetaFactory.getIdForValueMeta( XMLHandler.getTagValue( fnode, "type" ) ) );
        field.setFormat( XMLHandler.getTagValue( fnode, "format" ) );
        field.setCurrencySymbol( XMLHandler.getTagValue( fnode, "currency" ) );
        field.setDecimalSymbol( XMLHandler.getTagValue( fnode, "decimal" ) );
        field.setGroupSymbol( XMLHandler.getTagValue( fnode, "group" ) );
        field.setNullString( XMLHandler.getTagValue( fnode, "nullif" ) );
        field.setIfNullValue( XMLHandler.getTagValue( fnode, "ifnull" ) );
        field.setPosition( Const.toInt( XMLHandler.getTagValue( fnode, "position" ), -1 ) );
        field.setLength( Const.toInt( XMLHandler.getTagValue( fnode, "length" ), -1 ) );
        field.setPrecision( Const.toInt( XMLHandler.getTagValue( fnode, "precision" ), -1 ) );
        field.setTrimType( ValueMetaString.getTrimTypeByCode( XMLHandler.getTagValue( fnode, "trim_type" ) ) );
        field.setRepeated( YES.equalsIgnoreCase( XMLHandler.getTagValue( fnode, "repeat" ) ) );

        inputFields[ i ] = field;
      }

      // Is there a limit on the number of rows we process?
      content.rowLimit = Const.toLong( XMLHandler.getTagValue( transformNode, "limit" ), 0L );

      errorHandling.errorIgnored = YES.equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "error_ignored" ) );
      errorHandling.skipBadFiles = YES.equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "skip_bad_files" ) );
      errorHandling.fileErrorField = XMLHandler.getTagValue( transformNode, "file_error_field" );
      errorHandling.fileErrorMessageField = XMLHandler.getTagValue( transformNode, "file_error_message_field" );
      errorLineSkipped = YES.equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "error_line_skipped" ) );
      errorCountField = XMLHandler.getTagValue( transformNode, "error_count_field" );
      errorFieldsField = XMLHandler.getTagValue( transformNode, "error_fields_field" );
      errorTextField = XMLHandler.getTagValue( transformNode, "error_text_field" );
      errorHandling.warningFilesDestinationDirectory =
        XMLHandler.getTagValue( transformNode, "bad_line_files_destination_directory" );
      errorHandling.warningFilesExtension = XMLHandler.getTagValue( transformNode, "bad_line_files_extension" );
      errorHandling.errorFilesDestinationDirectory =
        XMLHandler.getTagValue( transformNode, "error_line_files_destination_directory" );
      errorHandling.errorFilesExtension = XMLHandler.getTagValue( transformNode, "error_line_files_extension" );
      errorHandling.lineNumberFilesDestinationDirectory =
        XMLHandler.getTagValue( transformNode, "line_number_files_destination_directory" );
      errorHandling.lineNumberFilesExtension = XMLHandler.getTagValue( transformNode, "line_number_files_extension" );
      // Backward compatible

      content.dateFormatLenient = !NO.equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "date_format_lenient" ) );
      String dateLocale = XMLHandler.getTagValue( transformNode, "date_format_locale" );
      if ( dateLocale != null ) {
        content.dateFormatLocale = EnvUtil.createLocale( dateLocale );
      } else {
        content.dateFormatLocale = Locale.getDefault();
      }

      additionalOutputFields.shortFilenameField = XMLHandler.getTagValue( transformNode, "shortFileFieldName" );
      additionalOutputFields.pathField = XMLHandler.getTagValue( transformNode, "pathFieldName" );
      additionalOutputFields.hiddenField = XMLHandler.getTagValue( transformNode, "hiddenFieldName" );
      additionalOutputFields.lastModificationField =
        XMLHandler.getTagValue( transformNode, "lastModificationTimeFieldName" );
      additionalOutputFields.uriField = XMLHandler.getTagValue( transformNode, "uriNameFieldName" );
      additionalOutputFields.rootUriField = XMLHandler.getTagValue( transformNode, "rootUriNameFieldName" );
      additionalOutputFields.extensionField = XMLHandler.getTagValue( transformNode, "extensionFieldName" );
      additionalOutputFields.sizeField = XMLHandler.getTagValue( transformNode, "sizeFieldName" );
    } catch ( Exception e ) {
      throw new HopXMLException( "Unable to load transform info from XML", e );
    }
  }

  @Override
  public Object clone() {
    TextFileInputMeta retval = (TextFileInputMeta) super.clone();
    retval.inputFiles = (BaseFileInputFiles) inputFiles.clone();
    retval.inputFields = new BaseFileField[ inputFields.length ];
    for ( int i = 0; i < inputFields.length; i++ ) {
      retval.inputFields[ i ] = (BaseFileField) inputFields[ i ].clone();
    }

    retval.filter = new TextFileFilter[ filter.length ];
    for ( int i = 0; i < filter.length; i++ ) {
      retval.filter[ i ] = (TextFileFilter) filter[ i ].clone();
    }
    return retval;
  }

  public void allocate( int nrfiles, int nrFields, int nrfilters ) {
    allocateFiles( nrfiles );

    inputFields = new BaseFileField[ nrFields ];
    filter = new TextFileFilter[ nrfilters ];
  }

  public void allocateFiles( int nrFiles ) {
    inputFiles.fileName = new String[ nrFiles ];
    inputFiles.fileMask = new String[ nrFiles ];
    inputFiles.excludeFileMask = new String[ nrFiles ];
    inputFiles.fileRequired = new String[ nrFiles ];
    inputFiles.includeSubFolders = new String[ nrFiles ];
  }

  @Override
  public void setDefault() {
    additionalOutputFields.shortFilenameField = null;
    additionalOutputFields.pathField = null;
    additionalOutputFields.hiddenField = null;
    additionalOutputFields.lastModificationField = null;
    additionalOutputFields.uriField = null;
    additionalOutputFields.rootUriField = null;
    additionalOutputFields.extensionField = null;
    additionalOutputFields.sizeField = null;

    inputFiles.isaddresult = true;

    content.separator = ";";
    content.enclosure = "\"";
    content.breakInEnclosureAllowed = false;
    content.header = true;
    content.nrHeaderLines = 1;
    content.footer = false;
    content.nrFooterLines = 1;
    content.lineWrapped = false;
    content.nrWraps = 1;
    content.layoutPaged = false;
    content.nrLinesPerPage = 80;
    content.nrLinesDocHeader = 0;
    content.fileCompression = "None";
    content.noEmptyLines = true;
    content.fileFormat = "DOS";
    content.fileType = "CSV";
    content.includeFilename = false;
    content.filenameField = "";
    content.includeRowNumber = false;
    content.rowNumberField = "";
    content.dateFormatLenient = true;
    content.rowNumberByFile = false;

    errorHandling.errorIgnored = false;
    errorHandling.skipBadFiles = false;
    errorLineSkipped = false;
    errorHandling.warningFilesDestinationDirectory = null;
    errorHandling.warningFilesExtension = "warning";
    errorHandling.errorFilesDestinationDirectory = null;
    errorHandling.errorFilesExtension = "error";
    errorHandling.lineNumberFilesDestinationDirectory = null;
    errorHandling.lineNumberFilesExtension = "line";

    int nrfiles = 0;
    int nrFields = 0;
    int nrfilters = 0;

    allocate( nrfiles, nrFields, nrfilters );

    for ( int i = 0; i < nrfiles; i++ ) {
      inputFiles.fileName[ i ] = "filename" + ( i + 1 );
      inputFiles.fileMask[ i ] = "";
      inputFiles.excludeFileMask[ i ] = "";
      inputFiles.fileRequired[ i ] = NO;
      inputFiles.includeSubFolders[ i ] = NO;
    }

    for ( int i = 0; i < nrFields; i++ ) {
      inputFields[ i ] = new BaseFileField( "field" + ( i + 1 ), 1, -1 );
    }

    content.dateFormatLocale = Locale.getDefault();

    content.rowLimit = 0L;
  }

  @Override
  public void getFields( IRowMeta row, String name, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {
    if ( !inputFiles.passingThruFields ) {
      // all incoming fields are not transmitted !
      row.clear();
    } else {
      if ( info != null ) {
        boolean found = false;
        for ( int i = 0; i < info.length && !found; i++ ) {
          if ( info[ i ] != null ) {
            row.mergeRowMeta( info[ i ], name );
            found = true;
          }
        }
      }
    }

    for ( int i = 0; i < inputFields.length; i++ ) {
      BaseFileField field = inputFields[ i ];

      int type = field.getType();
      if ( type == IValueMeta.TYPE_NONE ) {
        type = IValueMeta.TYPE_STRING;
      }

      try {
        IValueMeta v = ValueMetaFactory.createValueMeta( field.getName(), type );
        v.setLength( field.getLength() );
        v.setPrecision( field.getPrecision() );
        v.setOrigin( name );
        v.setConversionMask( field.getFormat() );
        v.setDecimalSymbol( field.getDecimalSymbol() );
        v.setGroupingSymbol( field.getGroupSymbol() );
        v.setCurrencySymbol( field.getCurrencySymbol() );
        v.setDateFormatLenient( content.dateFormatLenient );
        v.setDateFormatLocale( content.dateFormatLocale );
        v.setTrimType( field.getTrimType() );

        row.addValueMeta( v );
      } catch ( Exception e ) {
        throw new HopTransformException( e );
      }
    }
    if ( errorHandling.errorIgnored ) {
      if ( errorCountField != null && errorCountField.length() > 0 ) {
        IValueMeta v = new ValueMetaInteger( errorCountField );
        v.setLength( IValueMeta.DEFAULT_INTEGER_LENGTH, 0 );
        v.setOrigin( name );
        row.addValueMeta( v );
      }
      if ( errorFieldsField != null && errorFieldsField.length() > 0 ) {
        IValueMeta v = new ValueMetaString( errorFieldsField );
        v.setOrigin( name );
        row.addValueMeta( v );
      }
      if ( errorTextField != null && errorTextField.length() > 0 ) {
        IValueMeta v = new ValueMetaString( errorTextField );
        v.setOrigin( name );
        row.addValueMeta( v );
      }
    }
    if ( content.includeFilename ) {
      IValueMeta v = new ValueMetaString( content.filenameField );
      v.setLength( 100 );
      v.setOrigin( name );
      row.addValueMeta( v );
    }
    if ( content.includeRowNumber ) {
      IValueMeta v = new ValueMetaInteger( content.rowNumberField );
      v.setLength( IValueMeta.DEFAULT_INTEGER_LENGTH, 0 );
      v.setOrigin( name );
      row.addValueMeta( v );
    }

    // Add additional fields

    if ( StringUtils.isNotBlank( additionalOutputFields.shortFilenameField ) ) {
      IValueMeta v =
        new ValueMetaString( variables.environmentSubstitute( additionalOutputFields.shortFilenameField ) );
      v.setLength( 100, -1 );
      v.setOrigin( name );
      row.addValueMeta( v );
    }
    if ( StringUtils.isNotBlank( additionalOutputFields.extensionField ) ) {
      IValueMeta v =
        new ValueMetaString( variables.environmentSubstitute( additionalOutputFields.extensionField ) );
      v.setLength( 100, -1 );
      v.setOrigin( name );
      row.addValueMeta( v );
    }
    if ( StringUtils.isNotBlank( additionalOutputFields.pathField ) ) {
      IValueMeta v =
        new ValueMetaString( variables.environmentSubstitute( additionalOutputFields.pathField ) );
      v.setLength( 100, -1 );
      v.setOrigin( name );
      row.addValueMeta( v );
    }
    if ( StringUtils.isNotBlank( additionalOutputFields.sizeField ) ) {
      IValueMeta v =
        new ValueMetaString( variables.environmentSubstitute( additionalOutputFields.sizeField ) );
      v.setOrigin( name );
      v.setLength( 9 );
      row.addValueMeta( v );
    }
    if ( StringUtils.isNotBlank( additionalOutputFields.hiddenField ) ) {
      IValueMeta v =
        new ValueMetaBoolean( variables.environmentSubstitute( additionalOutputFields.hiddenField ) );
      v.setOrigin( name );
      row.addValueMeta( v );
    }

    if ( StringUtils.isNotBlank( additionalOutputFields.lastModificationField ) ) {
      IValueMeta v =
        new ValueMetaDate( variables.environmentSubstitute( additionalOutputFields.lastModificationField ) );
      v.setOrigin( name );
      row.addValueMeta( v );
    }
    if ( StringUtils.isNotBlank( additionalOutputFields.uriField ) ) {
      IValueMeta v =
        new ValueMetaString( variables.environmentSubstitute( additionalOutputFields.uriField ) );
      v.setLength( 100, -1 );
      v.setOrigin( name );
      row.addValueMeta( v );
    }

    if ( StringUtils.isNotBlank( additionalOutputFields.rootUriField ) ) {
      IValueMeta v = new ValueMetaString( additionalOutputFields.rootUriField );
      v.setLength( 100, -1 );
      v.setOrigin( name );
      row.addValueMeta( v );
    }

  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 1500 );

    retval.append( "    " ).append( XMLHandler.addTagValue( "accept_filenames", inputFiles.acceptingFilenames ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "passing_through_fields", inputFiles.passingThruFields ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "accept_field", inputFiles.acceptingField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "accept_transform_name", ( acceptingTransform != null ? acceptingTransform
      .getName() : "" ) ) );

    retval.append( "    " ).append( XMLHandler.addTagValue( "separator", content.separator ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "enclosure", content.enclosure ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "enclosure_breaks", content.breakInEnclosureAllowed ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "escapechar", content.escapeCharacter ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "header", content.header ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "nr_headerlines", content.nrHeaderLines ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "footer", content.footer ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "nr_footerlines", content.nrFooterLines ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "line_wrapped", content.lineWrapped ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "nr_wraps", content.nrWraps ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "layout_paged", content.layoutPaged ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "nr_lines_per_page", content.nrLinesPerPage ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "nr_lines_doc_header", content.nrLinesDocHeader ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "noempty", content.noEmptyLines ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "include", content.includeFilename ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "include_field", content.filenameField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "rownum", content.includeRowNumber ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "rownumByFile", content.rowNumberByFile ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "rownum_field", content.rowNumberField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "format", content.fileFormat ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "encoding", content.encoding ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "length", content.length ) );
    retval.append( "    " + XMLHandler.addTagValue( "add_to_result_filenames", inputFiles.isaddresult ) );

    retval.append( "    <file>" ).append( Const.CR );
    //we need the equals by size arrays for inputFiles.fileName[i], inputFiles.fileMask[i], inputFiles.fileRequired[i], inputFiles.includeSubFolders[i]
    //to prevent the ArrayIndexOutOfBoundsException
    inputFiles.normalizeAllocation( inputFiles.fileName.length );
    for ( int i = 0; i < inputFiles.fileName.length; i++ ) {
      saveSource( retval, inputFiles.fileName[ i ] );
      retval.append( "      " ).append( XMLHandler.addTagValue( "filemask", inputFiles.fileMask[ i ] ) );
      retval.append( "      " ).append( XMLHandler.addTagValue( "exclude_filemask", inputFiles.excludeFileMask[ i ] ) );
      retval.append( "      " ).append( XMLHandler.addTagValue( "file_required", inputFiles.fileRequired[ i ] ) );
      retval.append( "      " ).append( XMLHandler.addTagValue( "include_subfolders", inputFiles.includeSubFolders[ i ] ) );
    }
    retval.append( "      " ).append( XMLHandler.addTagValue( "type", content.fileType ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "compression", ( content.fileCompression == null )
      ? "None" : content.fileCompression ) );
    retval.append( "    </file>" ).append( Const.CR );

    retval.append( "    <filters>" ).append( Const.CR );
    for ( int i = 0; i < filter.length; i++ ) {
      String filterString = filter[ i ].getFilterString();
      byte[] filterBytes = new byte[] {};
      String filterPrefix = "";
      if ( filterString != null ) {
        filterBytes = filterString.getBytes();
        filterPrefix = STRING_BASE64_PREFIX;
      }
      String filterEncoded = filterPrefix + new String( Base64.encodeBase64( filterBytes ) );

      retval.append( "      <filter>" ).append( Const.CR );
      retval.append( "        " ).append( XMLHandler.addTagValue( "filter_string", filterEncoded, false ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "filter_position", filter[ i ].getFilterPosition(),
        false ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "filter_is_last_line", filter[ i ].isFilterLastLine(),
        false ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "filter_is_positive", filter[ i ].isFilterPositive(),
        false ) );
      retval.append( "      </filter>" ).append( Const.CR );
    }
    retval.append( "    </filters>" ).append( Const.CR );

    retval.append( "    <fields>" ).append( Const.CR );
    for ( int i = 0; i < inputFields.length; i++ ) {
      BaseFileField field = inputFields[ i ];

      retval.append( "      <field>" ).append( Const.CR );
      retval.append( "        " ).append( XMLHandler.addTagValue( "name", field.getName() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "type", field.getTypeDesc() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "format", field.getFormat() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "currency", field.getCurrencySymbol() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "decimal", field.getDecimalSymbol() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "group", field.getGroupSymbol() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "nullif", field.getNullString() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "ifnull", field.getIfNullValue() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "position", field.getPosition() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "length", field.getLength() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "precision", field.getPrecision() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "trim_type", field.getTrimTypeCode() ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "repeat", field.isRepeated() ) );
      retval.append( "      </field>" ).append( Const.CR );
    }
    retval.append( "    </fields>" ).append( Const.CR );
    retval.append( "    " ).append( XMLHandler.addTagValue( "limit", content.rowLimit ) );

    // ERROR HANDLING
    retval.append( "    " ).append( XMLHandler.addTagValue( "error_ignored", errorHandling.errorIgnored ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "skip_bad_files", errorHandling.skipBadFiles ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "file_error_field", errorHandling.fileErrorField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "file_error_message_field",
      errorHandling.fileErrorMessageField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "error_line_skipped", errorLineSkipped ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "error_count_field", errorCountField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "error_fields_field", errorFieldsField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "error_text_field", errorTextField ) );

    retval.append( "    " ).append( XMLHandler.addTagValue( "bad_line_files_destination_directory",
      errorHandling.warningFilesDestinationDirectory ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "bad_line_files_extension",
      errorHandling.warningFilesExtension ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "error_line_files_destination_directory",
      errorHandling.errorFilesDestinationDirectory ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "error_line_files_extension",
      errorHandling.errorFilesExtension ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "line_number_files_destination_directory",
      errorHandling.lineNumberFilesDestinationDirectory ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "line_number_files_extension",
      errorHandling.lineNumberFilesExtension ) );

    retval.append( "    " ).append( XMLHandler.addTagValue( "date_format_lenient", content.dateFormatLenient ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "date_format_locale", content.dateFormatLocale != null
      ? content.dateFormatLocale.toString() : null ) );

    retval.append( "    " ).append( XMLHandler.addTagValue( "shortFileFieldName",
      additionalOutputFields.shortFilenameField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "pathFieldName", additionalOutputFields.pathField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "hiddenFieldName", additionalOutputFields.hiddenField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "lastModificationTimeFieldName",
      additionalOutputFields.lastModificationField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "uriNameFieldName", additionalOutputFields.uriField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "rootUriNameFieldName",
      additionalOutputFields.rootUriField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "extensionFieldName",
      additionalOutputFields.extensionField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "sizeFieldName", additionalOutputFields.sizeField ) );

    return retval.toString();
  }

  public String getLookupTransformName() {
    if ( inputFiles.acceptingFilenames && acceptingTransform != null && !Utils.isEmpty( acceptingTransform.getName() ) ) {
      return acceptingTransform.getName();
    }
    return null;
  }

  /**
   * @param transforms optionally search the info transform in a list of transforms
   */
  @Override
  public void searchInfoAndTargetTransforms( List<TransformMeta> transforms ) {
    acceptingTransform = TransformMeta.findTransform( transforms, inputFiles.acceptingTransformName );
  }

  public String[] getInfoTransforms() {
    if ( inputFiles.acceptingFilenames && acceptingTransform != null ) {
      return new String[] { acceptingTransform.getName() };
    }
    return null;
  }

  @Override
  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta, IRowMeta prev,
                     String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;

    // See if we get input...
    if ( input.length > 0 ) {
      if ( !inputFiles.acceptingFilenames ) {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
            "TextFileInputMeta.CheckResult.NoInputError" ), transformMeta );
        remarks.add( cr );
      } else {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
            "TextFileInputMeta.CheckResult.AcceptFilenamesOk" ), transformMeta );
        remarks.add( cr );
      }
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
          "TextFileInputMeta.CheckResult.NoInputOk" ), transformMeta );
      remarks.add( cr );
    }

    FileInputList textFileList = getFileInputList( pipelineMeta );
    if ( textFileList.nrOfFiles() == 0 ) {
      if ( !inputFiles.acceptingFilenames ) {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
            "TextFileInputMeta.CheckResult.ExpectedFilesError" ), transformMeta );
        remarks.add( cr );
      }
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
          "TextFileInputMeta.CheckResult.ExpectedFilesOk", "" + textFileList.nrOfFiles() ), transformMeta );
      remarks.add( cr );
    }
  }

  @Override
  public ITransform getTransform( TransformMeta transformMeta, ITransformData iTransformData, int cnr, PipelineMeta pipelineMeta,
                                Pipeline pipeline ) {
    return new TextFileInput( transformMeta, iTransformData, cnr, pipelineMeta, pipeline );
  }

  @Override
  public ITransformData getTransformData() {
    return new TextFileInputData();
  }

  public String getErrorCountField() {
    return errorCountField;
  }

  public void setErrorCountField( String errorCountField ) {
    this.errorCountField = errorCountField;
  }

  public String getErrorFieldsField() {
    return errorFieldsField;
  }

  public void setErrorFieldsField( String errorFieldsField ) {
    this.errorFieldsField = errorFieldsField;
  }

  public String getErrorTextField() {
    return errorTextField;
  }

  public void setErrorTextField( String errorTextField ) {
    this.errorTextField = errorTextField;
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

  public boolean isErrorLineSkipped() {
    return errorLineSkipped;
  }

  public void setErrorLineSkipped( boolean errorLineSkipped ) {
    this.errorLineSkipped = errorLineSkipped;
  }

  /**
   * @param acceptingTransform The acceptingTransform to set.
   */
  public void setAcceptingTransform( TransformMeta acceptingTransform ) {
    this.acceptingTransform = acceptingTransform;
  }

  public int getFileFormatTypeNr() {
    // calculate the file format type in advance so we can use a switch
    if ( content.fileFormat.equalsIgnoreCase( "DOS" ) ) {
      return FILE_FORMAT_DOS;
    } else if ( content.fileFormat.equalsIgnoreCase( "unix" ) ) {
      return TextFileInputMeta.FILE_FORMAT_UNIX;
    } else {
      return TextFileInputMeta.FILE_FORMAT_MIXED;
    }
  }

  public int getFileTypeNr() {
    // calculate the file type in advance CSV or Fixed?
    if ( content.fileType.equalsIgnoreCase( "CSV" ) ) {
      return TextFileInputMeta.FILE_TYPE_CSV;
    } else {
      return TextFileInputMeta.FILE_TYPE_FIXED;
    }
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
  @Override
  public String exportResources( iVariables variables, Map<String, ResourceDefinition> definitions,
                                 IResourceNaming iResourceNaming, IMetaStore metaStore )
    throws HopException {
    try {
      // The object that we're modifying here is a copy of the original!
      // So let's change the filename from relative to absolute by grabbing the file object...
      // In case the name of the file comes from previous transforms, forget about this!
      //
      if ( !inputFiles.acceptingFilenames ) {

        // Replace the filename ONLY (folder or filename)
        //
        for ( int i = 0; i < inputFiles.fileName.length; i++ ) {
          final String fileName = inputFiles.fileName[ i ];
          if ( fileName == null || fileName.isEmpty() ) {
            continue;
          }

          FileObject fileObject = getFileObject( variables.environmentSubstitute( fileName ), variables );

          inputFiles.fileName[ i ] =
            iResourceNaming.nameResource( fileObject, variables, Utils.isEmpty( inputFiles.fileMask[ i ] ) );
        }
      }
      return null;
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return errorHandling.errorIgnored && errorHandling.skipBadFiles;
  }

  @VisibleForTesting
  public void setFileNameForTest( String[] fileName ) {
    allocateFiles( fileName.length );
    setFileName( fileName );
  }

  protected String loadSource( Node filenode, Node filenamenode, int i, IMetaStore metaStore ) {
    return XMLHandler.getNodeValue( filenamenode );
  }

  protected void saveSource( StringBuilder retVal, String source ) {
    retVal.append( "      " ).append( XMLHandler.addTagValue( "name", source ) );
  }

  @Override
  public String getEncoding() {
    return content.encoding;
  }

  /**
   * @return the length
   */
  public String getLength() {
    return content.length;
  }

  /**
   * @param length the length to set
   */
  public void setLength( String length ) {
    content.length = length;
  }

  /**
   * Required for the Data Lineage.
   */
  public boolean isAcceptingFilenames() {
    return inputFiles.acceptingFilenames;
  }

  /**
   * Required for the Data Lineage.
   */
  public String getAcceptingTransformName() {
    return inputFiles.acceptingTransformName;
  }

  /**
   * Required for the Data Lineage.
   */
  public TransformMeta getAcceptingTransform() {
    return acceptingTransform;
  }

  /**
   * Required for the Data Lineage.
   */
  public String getAcceptingField() {
    return inputFiles.acceptingField;
  }

  public String[] getFilePaths( iVariables variables ) {
    return FileInputList.createFilePathList(
      variables, inputFiles.fileName, inputFiles.fileMask, inputFiles.excludeFileMask,
      inputFiles.fileRequired, inputFiles.includeSubFolderBoolean() );
  }

  public FileInputList getTextFileList( iVariables variables ) {
    return FileInputList.createFileList(
      variables, inputFiles.fileName, inputFiles.fileMask, inputFiles.excludeFileMask,
      inputFiles.fileRequired, inputFiles.includeSubFolderBoolean() );
  }

  /**
   * For testing
   */
  FileObject getFileObject( String vfsFileName, iVariables variables ) throws HopFileException {
    return HopVFS.getFileObject( variables.environmentSubstitute( vfsFileName ), variables );
  }

  @Override
  public void resolve() {
    for ( int i = 0; i < inputFiles.fileName.length; i++ ) {
      if ( inputFiles.fileName[ i ] != null && !inputFiles.fileName[ i ].isEmpty() ) {
        try {
          FileObject fileObject = HopVFS.getFileObject( getParentTransformMeta().getParentPipelineMeta().environmentSubstitute( inputFiles.fileName[ i ] ) );
          if ( AliasedFileObject.isAliasedFile( fileObject ) ) {
            inputFiles.fileName[ i ] = ( (AliasedFileObject) fileObject ).getOriginalURIString();
          }
        } catch ( HopFileException e ) {
          throw new RuntimeException( e );
        }
      }
    }
  }

  @Override
  public boolean hasHeader() {
    return content == null ? false : content.header;
  }

  @Override
  public String getEscapeCharacter() {
    return content == null ? null : content.escapeCharacter;
  }

  @Override
  public String getDelimiter() {
    return content == null ? null : content.separator;
  }

  @Override
  public String getEnclosure() {
    return content == null ? null : content.enclosure;
  }

  @Override
  public FileObject getHeaderFileObject( final PipelineMeta pipelineMeta ) {
    final FileInputList fileList = getFileInputList( pipelineMeta );
    return fileList.nrOfFiles() == 0 ? null : fileList.getFile( 0 );
  }
}
