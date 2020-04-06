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

package org.apache.hop.pipeline.transforms.textfileoutput;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
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
import org.apache.hop.pipeline.transform.TransformInjectionMetaEntry;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transforms.file.BaseFileOutputMeta;
import org.apache.hop.workarounds.ResolvableResource;
import org.w3c.dom.Node;

import java.util.Date;
import java.util.List;
import java.util.Map;

/*
 * Created on 4-apr-2003
 *
 */
@InjectionSupported( localizationPrefix = "TextFileOutput.Injection.", groups = { "OUTPUT_FIELDS" } )
public class TextFileOutputMeta extends BaseFileOutputMeta implements ITransform, ResolvableResource {
  private static Class<?> PKG = TextFileOutputMeta.class; // for i18n purposes, needed by Translator!!

  protected static final int FILE_COMPRESSION_TYPE_NONE = 0;

  protected static final int FILE_COMPRESSION_TYPE_ZIP = 1;

  protected static final String[] fileCompressionTypeCodes = new String[] { "None", "Zip" };

  public static final String[] formatMapperLineTerminator = new String[] { "DOS", "UNIX", "CR", "None" };

  /**
   * Whether to push the output into the output of a servlet with the executePipeline HopServer/DI-Server servlet
   */
  @Injection( name = "PASS_TO_SERVLET" )
  private boolean servletOutput;

  /**
   * Flag: create parent folder, default to true
   */
  @Injection( name = "CREATE_PARENT_FOLDER" )
  private boolean createparentfolder = true;

  /**
   * The separator to choose for the CSV file
   */
  @Injection( name = "SEPARATOR" )
  private String separator;

  /**
   * The enclosure to use in case the separator is part of a field's value
   */
  @Injection( name = "ENCLOSURE" )
  private String enclosure;

  /**
   * Setting to allow the enclosure to be always surrounding a String value, even when there is no separator inside
   */
  @Injection( name = "FORCE_ENCLOSURE" )
  private boolean enclosureForced;

  /**
   * Setting to allow for backwards compatibility where the enclosure did not show up at all if Force Enclosure was not
   * checked
   */
  @Injection( name = "DISABLE_ENCLOSURE_FIX" )
  private boolean disableEnclosureFix;

  /**
   * Add a header at the top of the file?
   */
  @Injection( name = "HEADER" )
  private boolean headerEnabled;

  /**
   * Add a footer at the bottom of the file?
   */
  @Injection( name = "FOOTER" )
  private boolean footerEnabled;

  /**
   * The file format: DOS or Unix
   * It could be injected using the key "FORMAT"
   * see the setter {@link TextFileOutputMeta#setFileFormat(java.lang.String)}.
   */
  private String fileFormat;

  /**
   * if this value is larger then 0, the text file is split up into parts of this number of lines
   */
  @Injection( name = "SPLIT_EVERY" )
  private String splitEveryRows;

  /**
   * Flag to indicate the we want to append to the end of an existing file (if it exists)
   */
  @Injection( name = "APPEND" )
  private boolean fileAppended;

  /**
   * Flag: pad fields to their specified length
   */
  @Injection( name = "RIGHT_PAD_FIELDS" )
  private boolean padded;

  /**
   * Flag: Fast dump data without field formatting
   */
  @Injection( name = "FAST_DATA_DUMP" )
  private boolean fastDump;

  /* THE FIELD SPECIFICATIONS ... */

  /**
   * The output fields
   */
  @InjectionDeep
  private TextFileField[] outputFields;

  /**
   * The encoding to use for reading: null or empty string means system default encoding
   */
  @Injection( name = "ENCODING" )
  private String encoding;

  /**
   * The string to use for append to end line of the whole file: null or empty string means no line needed
   */
  @Injection( name = "ADD_ENDING_LINE" )
  private String endedLine;

  /* Specification if file name is in field */

  @Injection( name = "FILENAME_IN_FIELD" )
  private boolean fileNameInField;

  @Injection( name = "FILENAME_FIELD" )
  private String fileNameField;

  /**
   * Calculated value ...
   */
  @Injection( name = "NEW_LINE" )
  private String newline;

  /**
   * Flag: add the filenames to result filenames
   */
  @Injection( name = "ADD_TO_RESULT" )
  private boolean addToResultFilenames;

  /**
   * Flag : Do not open new file when pipeline start
   */
  @Injection( name = "DO_NOT_CREATE_FILE_AT_STARTUP" )
  private boolean doNotOpenNewFileInit;

  public TextFileOutputMeta() {
    super(); // allocate BaseTransformMeta
  }

  public boolean isServletOutput() {
    return servletOutput;
  }

  public void setServletOutput( boolean servletOutput ) {
    this.servletOutput = servletOutput;
  }

  /**
   * @param createparentfolder The createparentfolder to set.
   */
  public void setCreateParentFolder( boolean createparentfolder ) {
    this.createparentfolder = createparentfolder;
  }

  /**
   * @return Returns the createparentfolder.
   */
  public boolean isCreateParentFolder() {
    return createparentfolder;
  }

  /**
   * @param dateInFilename The dateInFilename to set.
   */
  public void setDateInFilename( boolean dateInFilename ) {
    this.dateInFilename = dateInFilename;
  }

  /**
   * @return Returns the enclosure.
   */
  public String getEnclosure() {
    return enclosure;
  }

  /**
   * @param enclosure The enclosure to set.
   */
  public void setEnclosure( String enclosure ) {
    this.enclosure = enclosure;
  }

  /**
   * @return Returns the enclosureForced.
   */
  public boolean isEnclosureForced() {
    return enclosureForced;
  }

  /**
   * @param enclosureForced The enclosureForced to set.
   */
  public void setEnclosureForced( boolean enclosureForced ) {
    this.enclosureForced = enclosureForced;
  }

  /**
   * @return Returns the enclosureFixDisabled.
   */
  public boolean isEnclosureFixDisabled() {
    return disableEnclosureFix;
  }

  /**
   * @param disableEnclosureFix The enclosureFixDisabled to set.
   */
  public void setEnclosureFixDisabled( boolean disableEnclosureFix ) {
    this.disableEnclosureFix = disableEnclosureFix;
  }

  /**
   * @return Returns the add to result filesname.
   */
  public boolean isAddToResultFiles() {
    return addToResultFilenames;
  }

  /**
   * @param addtoresultfilenamesin The addtoresultfilenames to set.
   */
  public void setAddToResultFiles( boolean addtoresultfilenamesin ) {
    this.addToResultFilenames = addtoresultfilenamesin;
  }

  /**
   * @return Returns the fileAppended.
   */
  public boolean isFileAppended() {
    return fileAppended;
  }

  /**
   * @param fileAppended The fileAppended to set.
   */
  public void setFileAppended( boolean fileAppended ) {
    this.fileAppended = fileAppended;
  }

  /**
   * @return Returns the fileFormat.
   */
  public String getFileFormat() {
    return fileFormat;
  }

  /**
   * @param fileFormat The fileFormat to set.
   */
  @Injection( name = "FORMAT" )
  public void setFileFormat( String fileFormat ) {
    this.fileFormat = fileFormat;
    this.newline = getNewLine( fileFormat );
  }

  /**
   * @return Returns the footer.
   */
  public boolean isFooterEnabled() {
    return footerEnabled;
  }

  /**
   * @param footer The footer to set.
   */
  public void setFooterEnabled( boolean footer ) {
    this.footerEnabled = footer;
  }

  /**
   * @return Returns the header.
   */
  public boolean isHeaderEnabled() {
    return headerEnabled;
  }

  /**
   * @param header The header to set.
   */
  public void setHeaderEnabled( boolean header ) {
    this.headerEnabled = header;
  }

  /**
   * @return Returns the newline.
   */
  public String getNewline() {
    return newline;
  }

  /**
   * @param newline The newline to set.
   */
  public void setNewline( String newline ) {
    this.newline = newline;
  }

  /**
   * @return Returns the padded.
   */
  public boolean isPadded() {
    return padded;
  }

  /**
   * @param padded The padded to set.
   */
  public void setPadded( boolean padded ) {
    this.padded = padded;
  }

  /**
   * @return Returns the fastDump.
   */
  public boolean isFastDump() {
    return fastDump;
  }

  /**
   * @param fastDump The fastDump to set.
   */
  public void setFastDump( boolean fastDump ) {
    this.fastDump = fastDump;
  }

  /**
   * @return Returns the separator.
   */
  public String getSeparator() {
    return separator;
  }

  /**
   * @param separator The separator to set.
   */
  public void setSeparator( String separator ) {
    this.separator = separator;
  }

  /**
   * @return Returns the "do not open new file at init" flag.
   */
  public boolean isDoNotOpenNewFileInit() {
    return doNotOpenNewFileInit;
  }

  /**
   * @param doNotOpenNewFileInit The "do not open new file at init" flag to set.
   */
  public void setDoNotOpenNewFileInit( boolean doNotOpenNewFileInit ) {
    this.doNotOpenNewFileInit = doNotOpenNewFileInit;
  }

  /**
   * @return Returns the splitEvery.
   * @deprecated use {@link #getSplitEvery(iVariables)} or {@link #getSplitEveryRows()}
   */
  @Override
  public int getSplitEvery() {
    return Const.toInt( splitEveryRows, 0 );
  }

  /**
   * @param varSpace for variable substitution
   * @return At how many rows to split into another file.
   */
  @Override
  public int getSplitEvery( iVariables varSpace ) {
    return Const.toInt( varSpace == null ? splitEveryRows : varSpace.environmentSubstitute( splitEveryRows ), 0 );
  }

  /**
   * @return At how many rows to split into a new file.
   */
  public String getSplitEveryRows() {
    return splitEveryRows;
  }

  /**
   * @param value At how many rows to split into a new file.
   */
  public void setSplitEveryRows( String value ) {
    splitEveryRows = value;
  }

  /**
   * @return <tt>1</tt> if <tt>isFooterEnabled()</tt> and <tt>0</tt> otherwise
   */
  public int getFooterShift() {
    return isFooterEnabled() ? 1 : 0;
  }

  /**
   * @param splitEvery The splitEvery to set.
   * @deprecated use {@link #setSplitEveryRows(String)}
   */
  public void setSplitEvery( int splitEvery ) {
    splitEveryRows = Integer.toString( splitEvery );
  }

  /**
   * @param transformNrInFilename The transformNrInFilename to set.
   */
  public void setTransformNrInFilename( boolean transformNrInFilename ) {
    this.transformNrInFilename = transformNrInFilename;
  }

  /**
   * @param partNrInFilename The partNrInFilename to set.
   */
  public void setPartNrInFilename( boolean partNrInFilename ) {
    this.partNrInFilename = partNrInFilename;
  }

  /**
   * @param timeInFilename The timeInFilename to set.
   */
  public void setTimeInFilename( boolean timeInFilename ) {
    this.timeInFilename = timeInFilename;
  }

  /**
   * @return Returns the outputFields.
   */
  public TextFileField[] getOutputFields() {
    return outputFields;
  }

  /**
   * @param outputFields The outputFields to set.
   */
  public void setOutputFields( TextFileField[] outputFields ) {
    this.outputFields = outputFields;
  }

  /**
   * @return The desired encoding of output file, null or empty if the default system encoding needs to be used.
   */
  public String getEncoding() {
    return encoding;
  }

  /**
   * @param encoding The desired encoding of output file, null or empty if the default system encoding needs to be used.
   */
  public void setEncoding( String encoding ) {
    this.encoding = encoding;
  }

  /**
   * @return The desired last line in the output file, null or empty if nothing has to be added.
   */
  public String getEndedLine() {
    return endedLine;
  }

  /**
   * @param endedLine The desired last line in the output file, null or empty if nothing has to be added.
   */
  public void setEndedLine( String endedLine ) {
    this.endedLine = endedLine;
  }

  /**
   * @return Is the file name coded in a field?
   */
  public boolean isFileNameInField() {
    return fileNameInField;
  }

  /**
   * @param fileNameInField Is the file name coded in a field?
   */
  public void setFileNameInField( boolean fileNameInField ) {
    this.fileNameInField = fileNameInField;
  }

  /**
   * @return The field name that contains the output file name.
   */
  public String getFileNameField() {
    return fileNameField;
  }

  /**
   * @param fileNameField Name of the field that contains the file name
   */
  public void setFileNameField( String fileNameField ) {
    this.fileNameField = fileNameField;
  }

  @Override
  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode, metaStore );
  }

  public void allocate( int nrFields ) {
    outputFields = new TextFileField[ nrFields ];
  }

  @Override
  public Object clone() {
    TextFileOutputMeta retval = (TextFileOutputMeta) super.clone();
    int nrFields = outputFields.length;

    retval.allocate( nrFields );

    for ( int i = 0; i < nrFields; i++ ) {
      retval.outputFields[ i ] = (TextFileField) outputFields[ i ].clone();
    }

    return retval;
  }

  protected void readData( Node transformNode, IMetaStore metastore ) throws HopXMLException {
    try {
      separator = XMLHandler.getTagValue( transformNode, "separator" );
      if ( separator == null ) {
        separator = "";
      }

      enclosure = XMLHandler.getTagValue( transformNode, "enclosure" );
      if ( enclosure == null ) {
        enclosure = "";
      }

      enclosureForced = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "enclosure_forced" ) );

      String sDisableEnclosureFix = XMLHandler.getTagValue( transformNode, "enclosure_fix_disabled" );
      // Default this value to true for backwards compatibility
      if ( sDisableEnclosureFix == null ) {
        disableEnclosureFix = true;
      } else {
        disableEnclosureFix = "Y".equalsIgnoreCase( sDisableEnclosureFix );
      }

      // Default createparentfolder to true if the tag is missing
      String createParentFolderTagValue = XMLHandler.getTagValue( transformNode, "create_parent_folder" );
      createparentfolder =
        ( createParentFolderTagValue == null ) ? true : "Y".equalsIgnoreCase( createParentFolderTagValue );

      headerEnabled = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "header" ) );
      footerEnabled = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "footer" ) );
      fileFormat = XMLHandler.getTagValue( transformNode, "format" );
      setFileCompression( XMLHandler.getTagValue( transformNode, "compression" ) );
      if ( getFileCompression() == null ) {
        if ( "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "zipped" ) ) ) {
          setFileCompression( fileCompressionTypeCodes[ FILE_COMPRESSION_TYPE_ZIP ] );
        } else {
          setFileCompression( fileCompressionTypeCodes[ FILE_COMPRESSION_TYPE_NONE ] );
        }
      }
      encoding = XMLHandler.getTagValue( transformNode, "encoding" );

      endedLine = XMLHandler.getTagValue( transformNode, "endedLine" );
      if ( endedLine == null ) {
        endedLine = "";
      }

      fileName = loadSource( transformNode, metastore );
      servletOutput = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "servlet_output" ) );
      doNotOpenNewFileInit =
        "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "do_not_open_new_file_init" ) );
      extension = XMLHandler.getTagValue( transformNode, "file", "extention" );
      fileAppended = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "append" ) );
      transformNrInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "split" ) );
      partNrInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "haspartno" ) );
      dateInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "add_date" ) );
      timeInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "add_time" ) );
      setSpecifyingFormat( "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "SpecifyFormat" ) ) );
      setDateTimeFormat( XMLHandler.getTagValue( transformNode, "file", "date_time_format" ) );

      String AddToResultFiles = XMLHandler.getTagValue( transformNode, "file", "add_to_result_filenames" );
      if ( Utils.isEmpty( AddToResultFiles ) ) {
        addToResultFilenames = true;
      } else {
        addToResultFilenames = "Y".equalsIgnoreCase( AddToResultFiles );
      }

      padded = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "pad" ) );
      fastDump = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "file", "fast_dump" ) );
      splitEveryRows = XMLHandler.getTagValue( transformNode, "file", "splitevery" );

      newline = getNewLine( fileFormat );

      fileNameInField = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "fileNameInField" ) );
      fileNameField = XMLHandler.getTagValue( transformNode, "fileNameField" );

      Node fields = XMLHandler.getSubNode( transformNode, "fields" );
      int nrFields = XMLHandler.countNodes( fields, "field" );

      allocate( nrFields );

      for ( int i = 0; i < nrFields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );

        outputFields[ i ] = new TextFileField();
        outputFields[ i ].setName( XMLHandler.getTagValue( fnode, "name" ) );
        outputFields[ i ].setType( XMLHandler.getTagValue( fnode, "type" ) );
        outputFields[ i ].setFormat( XMLHandler.getTagValue( fnode, "format" ) );
        outputFields[ i ].setCurrencySymbol( XMLHandler.getTagValue( fnode, "currency" ) );
        outputFields[ i ].setDecimalSymbol( XMLHandler.getTagValue( fnode, "decimal" ) );
        outputFields[ i ].setGroupingSymbol( XMLHandler.getTagValue( fnode, "group" ) );
        outputFields[ i ].setTrimType( ValueMetaString.getTrimTypeByCode( XMLHandler.getTagValue( fnode, "trim_type" ) ) );
        outputFields[ i ].setNullString( XMLHandler.getTagValue( fnode, "nullif" ) );
        outputFields[ i ].setLength( Const.toInt( XMLHandler.getTagValue( fnode, "length" ), -1 ) );
        outputFields[ i ].setPrecision( Const.toInt( XMLHandler.getTagValue( fnode, "precision" ), -1 ) );
      }
    } catch ( Exception e ) {
      throw new HopXMLException( "Unable to load transform info from XML", e );
    }
  }

  public void readData( Node transformNode ) throws HopXMLException {
    readData( transformNode, null );
  }

  public String getNewLine( String fformat ) {
    String nl = System.getProperty( "line.separator" );

    if ( fformat != null ) {
      if ( fformat.equalsIgnoreCase( "DOS" ) ) {
        nl = "\r\n";
      } else if ( fformat.equalsIgnoreCase( "UNIX" ) ) {
        nl = "\n";
      } else if ( fformat.equalsIgnoreCase( "CR" ) ) {
        nl = "\r";
      } else if ( fformat.equalsIgnoreCase( "None" ) ) {
        nl = "";
      }
    }

    return nl;
  }

  @Override
  public void setDefault() {
    createparentfolder = true; // Default createparentfolder to true
    separator = ";";
    enclosure = "\"";
    setSpecifyingFormat( false );
    setDateTimeFormat( null );
    enclosureForced = false;
    disableEnclosureFix = false;
    headerEnabled = true;
    footerEnabled = false;
    fileFormat = "DOS";
    setFileCompression( fileCompressionTypeCodes[ FILE_COMPRESSION_TYPE_NONE ] );
    fileName = "file";
    servletOutput = false;
    doNotOpenNewFileInit = false;
    extension = "txt";
    transformNrInFilename = false;
    partNrInFilename = false;
    dateInFilename = false;
    timeInFilename = false;
    padded = false;
    fastDump = false;
    addToResultFilenames = true;

    newline = getNewLine( fileFormat );

    int i, nrFields = 0;

    allocate( nrFields );

    for ( i = 0; i < nrFields; i++ ) {
      outputFields[ i ] = new TextFileField();

      outputFields[ i ].setName( "field" + i );
      outputFields[ i ].setType( "Number" );
      outputFields[ i ].setFormat( " 0,000,000.00;-0,000,000.00" );
      outputFields[ i ].setCurrencySymbol( "" );
      outputFields[ i ].setDecimalSymbol( "," );
      outputFields[ i ].setGroupingSymbol( "." );
      outputFields[ i ].setNullString( "" );
      outputFields[ i ].setLength( -1 );
      outputFields[ i ].setPrecision( -1 );
    }
    fileAppended = false;
  }

  public String buildFilename( iVariables variables, int transformnr, String partnr, int splitnr, boolean ziparchive ) {
    return super.buildFilename( variables, Integer.toString( transformnr ), partnr, Integer.toString( splitnr ), ziparchive );
  }

  public String buildFilename( String filename, String extension, iVariables variables, int transformnr, String partnr,
                               int splitnr, boolean ziparchive, TextFileOutputMeta meta ) {

    final String realFileName = variables.environmentSubstitute( filename );
    final String realExtension = variables.environmentSubstitute( extension );
    return super.buildFilename( variables, realFileName, realExtension, Integer.toString( transformnr ), partnr, Integer
      .toString( splitnr ), new Date(), ziparchive, true, meta );
  }

  @Override
  public void getFields( IRowMeta row, String name, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {
    // No values are added to the row in this type of transform
    // However, in case of Fixed length records,
    // the field precisions and lengths are altered!

    for ( int i = 0; i < outputFields.length; i++ ) {
      TextFileField field = outputFields[ i ];
      IValueMeta v = row.searchValueMeta( field.getName() );
      if ( v != null ) {
        v.setLength( field.getLength() );
        v.setPrecision( field.getPrecision() );
        if ( field.getFormat() != null ) {
          v.setConversionMask( field.getFormat() );
        }
        v.setDecimalSymbol( field.getDecimalSymbol() );
        v.setGroupingSymbol( field.getGroupingSymbol() );
        v.setCurrencySymbol( field.getCurrencySymbol() );
        v.setOutputPaddingEnabled( isPadded() );
        v.setTrimType( field.getTrimType() );
        if ( !Utils.isEmpty( getEncoding() ) ) {
          v.setStringEncoding( getEncoding() );
        }

        // enable output padding by default to be compatible with v2.5.x
        //
        v.setOutputPaddingEnabled( true );
      }
    }
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 800 );

    retval.append( "    " ).append( XMLHandler.addTagValue( "separator", separator ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "enclosure", enclosure ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "enclosure_forced", enclosureForced ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "enclosure_fix_disabled", disableEnclosureFix ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "header", headerEnabled ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "footer", footerEnabled ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "format", fileFormat ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "compression", getFileCompression() ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "encoding", encoding ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "endedLine", endedLine ) );
    retval.append( "    " + XMLHandler.addTagValue( "fileNameInField", fileNameInField ) );
    retval.append( "    " + XMLHandler.addTagValue( "fileNameField", fileNameField ) );
    retval.append( "    " + XMLHandler.addTagValue( "create_parent_folder", createparentfolder ) );
    retval.append( "    <file>" ).append( Const.CR );
    saveFileOptions( retval );
    retval.append( "    </file>" ).append( Const.CR );

    retval.append( "    <fields>" ).append( Const.CR );
    for ( int i = 0; i < outputFields.length; i++ ) {
      TextFileField field = outputFields[ i ];

      if ( field.getName() != null && field.getName().length() != 0 ) {
        retval.append( "      <field>" ).append( Const.CR );
        retval.append( "        " ).append( XMLHandler.addTagValue( "name", field.getName() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "type", field.getTypeDesc() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "format", field.getFormat() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "currency", field.getCurrencySymbol() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "decimal", field.getDecimalSymbol() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "group", field.getGroupingSymbol() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "nullif", field.getNullString() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "trim_type", field.getTrimTypeCode() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "length", field.getLength() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "precision", field.getPrecision() ) );
        retval.append( "      </field>" ).append( Const.CR );
      }
    }
    retval.append( "    </fields>" ).append( Const.CR );

    return retval.toString();
  }

  protected void saveFileOptions( StringBuilder retval ) {
    saveSource( retval, fileName );
    retval.append( "      " ).append( XMLHandler.addTagValue( "servlet_output", servletOutput ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "do_not_open_new_file_init", doNotOpenNewFileInit ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "extention", extension ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "append", fileAppended ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "split", transformNrInFilename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "haspartno", partNrInFilename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "add_date", dateInFilename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "add_time", timeInFilename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "SpecifyFormat", isSpecifyingFormat() ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "date_time_format", getDateTimeFormat() ) );

    retval.append( "      " ).append( XMLHandler.addTagValue( "add_to_result_filenames", addToResultFilenames ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "pad", padded ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "fast_dump", fastDump ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "splitevery", splitEveryRows ) );
  }

  @Override
  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta, IRowMeta prev,
                     String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;

    // Check output fields
    if ( prev != null && prev.size() > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
          "TextFileOutputMeta.CheckResult.FieldsReceived", "" + prev.size() ), transformMeta );
      remarks.add( cr );

      String error_message = "";
      boolean error_found = false;

      // Starting from selected fields in ...
      for ( int i = 0; i < outputFields.length; i++ ) {
        int idx = prev.indexOfValue( outputFields[ i ].getName() );
        if ( idx < 0 ) {
          error_message += "\t\t" + outputFields[ i ].getName() + Const.CR;
          error_found = true;
        }
      }
      if ( error_found ) {
        error_message = BaseMessages.getString( PKG, "TextFileOutputMeta.CheckResult.FieldsNotFound", error_message );
        cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, transformMeta );
        remarks.add( cr );
      } else {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
            "TextFileOutputMeta.CheckResult.AllFieldsFound" ), transformMeta );
        remarks.add( cr );
      }
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
          "TextFileOutputMeta.CheckResult.ExpectedInputOk" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
          "TextFileOutputMeta.CheckResult.ExpectedInputError" ), transformMeta );
      remarks.add( cr );
    }

    cr =
      new CheckResult( CheckResultInterface.TYPE_RESULT_COMMENT, BaseMessages.getString( PKG,
        "TextFileOutputMeta.CheckResult.FilesNotChecked" ), transformMeta );
    remarks.add( cr );
  }

  @Override
  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr, PipelineMeta pipelineMeta,
                                Pipeline pipeline ) {
    return new TextFileOutput( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }

  @Override
  public ITransformData getTransformData() {
    return new TextFileOutputData();
  }

  /**
   * Since the exported pipeline that runs this will reside in a ZIP file, we can't reference files relatively. So
   * what this does is turn the name of the base path into an absolute path.
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
      if ( !fileNameInField ) {

        if ( !Utils.isEmpty( fileName ) ) {
          FileObject fileObject = HopVFS.getFileObject( variables.environmentSubstitute( fileName ), variables );
          fileName = iResourceNaming.nameResource( fileObject, variables, true );
        }
      }

      return null;
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

  public void setFilename( String fileName ) {
    this.fileName = fileName;
  }

  @Override
  public List<TransformInjectionMetaEntry> extractTransformMetadataEntries() throws HopException {
    return getTransformMetaInjectionInterface().extractTransformMetadataEntries();
  }

  protected String loadSource( Node transformNode, IMetaStore metastore ) {
    return XMLHandler.getTagValue( transformNode, "file", "name" );
  }

  protected void saveSource( StringBuilder retVal, String value ) {
    retVal.append( "      " ).append( XMLHandler.addTagValue( "name", fileName ) );
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean passDataToServletOutput() {
    return servletOutput;
  }

  @Override
  public void resolve() {
    if ( fileName != null && !fileName.isEmpty() ) {
      try {
        FileObject fileObject = HopVFS.getFileObject( getParentTransformMeta().getParentPipelineMeta().environmentSubstitute( fileName ) );
        if ( AliasedFileObject.isAliasedFile( fileObject ) ) {
          fileName = ( (AliasedFileObject) fileObject ).getOriginalURIString();
        }
      } catch ( HopFileException e ) {
        throw new RuntimeException( e );
      }
    }
  }
}
