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

package org.apache.hop.trans.steps.excelwriter;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Step;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceNamingInterface;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.*;
import org.w3c.dom.Node;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Step(
        id = "TypeExitExcelWriterStep",
        image = "ui/images/XWS.svg",
        i18nPackageName = "org.apache.hop.trans.steps.excelwriter",
        name = "BaseStep.TypeLongDesc.TypeExitExcelWriterStep",
        description = "i18n:org.apache.hop.trans.step:BaseStep.TypeLongDesc.TypeExitExcelWriterStep",
        categoryDescription = "i18n:org.apache.hop.trans.step:BaseStep.Category.Output"
)
public class ExcelWriterStepMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = ExcelWriterStepMeta.class; // for i18n purposes, needed by Translator2!!

  public static final String IF_FILE_EXISTS_REUSE = "reuse";
  public static final String IF_FILE_EXISTS_CREATE_NEW = "new";

  public static final String IF_SHEET_EXISTS_REUSE = "reuse";
  public static final String IF_SHEET_EXISTS_CREATE_NEW = "new";

  public static final String ROW_WRITE_OVERWRITE = "overwrite";
  public static final String ROW_WRITE_PUSH_DOWN = "push";

  /**
   * The base name of the output file
   */
  private String fileName;
  /**
   * what to do if file exists
   **/
  private String ifFileExists;
  private String ifSheetExists;

  private boolean makeSheetActive;
  private boolean forceFormulaRecalculation = false;
  private boolean leaveExistingStylesUnchanged = false;

  /**
   * advanced line append options
   **/
  private int appendOffset = 0;
  private int appendEmpty = 0;
  private boolean appendOmitHeader = false;

  /**
   * how to write rows
   **/
  private String rowWritingMethod;

  /**
   * where to start writing
   **/
  private String startingCell;

  /**
   * The file extension in case of a generated filename
   */
  private String extension;

  /**
   * The password to protect the sheet
   */
  private String password;
  private String protectedBy;

  /**
   * Add a header at the top of the file?
   */
  private boolean headerEnabled;

  /**
   * Add a footer at the bottom of the file?
   */
  private boolean footerEnabled;

  /**
   * if this value is larger then 0, the text file is split up into parts of this number of lines
   */
  private int splitEvery;

  /**
   * Flag: add the stepnr in the filename
   */
  private boolean stepNrInFilename;

  /**
   * Flag: add the date in the filename
   */
  private boolean dateInFilename;

  /**
   * Flag: add the filenames to result filenames
   */
  private boolean addToResultFilenames;

  /**
   * Flag: protect the sheet
   */
  private boolean protectsheet;

  /**
   * Flag: add the time in the filename
   */
  private boolean timeInFilename;

  /**
   * Flag: use a template
   */
  private boolean templateEnabled;
  private boolean templateSheetEnabled;
  private boolean templateSheetHidden;

  /**
   * the excel template
   */
  private String templateFileName;
  private String templateSheetName;

  /**
   * the excel sheet name
   */
  private String sheetname;

  /* THE FIELD SPECIFICATIONS ... */

  /**
   * The output fields
   */
  private ExcelWriterStepField[] outputFields;

  /**
   * Flag : appendLines lines?
   */
  private boolean appendLines;

  /**
   * Flag : Do not open new file when transformation start
   */
  private boolean doNotOpenNewFileInit;

  private boolean SpecifyFormat;

  private String date_time_format;

  /**
   * Flag : auto size columns?
   */
  private boolean autosizecolums;

  /**
   * Do we need to stream data to handle very large files?
   */
  private boolean streamingData;

  public ExcelWriterStepMeta() {
    super();
  }

  public int getAppendOffset() {
    return appendOffset;
  }

  public void setAppendOffset( int appendOffset ) {
    this.appendOffset = appendOffset;
  }

  public int getAppendEmpty() {
    return appendEmpty;
  }

  public void setAppendEmpty( int appendEmpty ) {
    this.appendEmpty = appendEmpty >= 0 ? appendEmpty : 0;
  }

  /**
   * @return Returns the dateInFilename.
   */
  public boolean isDateInFilename() {
    return dateInFilename;
  }

  /**
   * @param dateInFilename The dateInFilename to set.
   */
  public void setDateInFilename( boolean dateInFilename ) {
    this.dateInFilename = dateInFilename;
  }

  public boolean isAppendOmitHeader() {
    return appendOmitHeader;
  }

  public void setAppendOmitHeader( boolean appendOmitHeader ) {
    this.appendOmitHeader = appendOmitHeader;
  }

  public String getStartingCell() {
    return startingCell;
  }

  public void setStartingCell( String startingCell ) {
    this.startingCell = startingCell;
  }

  public String getRowWritingMethod() {
    return rowWritingMethod;
  }

  public void setRowWritingMethod( String rowWritingMethod ) {
    this.rowWritingMethod = rowWritingMethod;
  }

  public String getIfFileExists() {
    return ifFileExists;
  }

  public void setIfFileExists( String ifFileExists ) {
    this.ifFileExists = ifFileExists;
  }

  public String getIfSheetExists() {
    return ifSheetExists;
  }

  public void setIfSheetExists( String ifSheetExists ) {
    this.ifSheetExists = ifSheetExists;
  }

  public String getProtectedBy() {
    return protectedBy;
  }

  public void setProtectedBy( String protectedBy ) {
    this.protectedBy = protectedBy;
  }

  /**
   * @return Returns the extension.
   */
  public String getExtension() {
    return extension;
  }

  /**
   * @param extension The extension to set.
   */
  public void setExtension( String extension ) {
    this.extension = extension;
  }

  /**
   * @return Returns the fileName.
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * @return Returns the password.
   */
  public String getPassword() {
    return password;
  }

  /**
   * @return Returns the sheet name.
   */
  public String getSheetname() {
    return sheetname;
  }

  /**
   * @param sheetname The sheet name.
   */
  public void setSheetname( String sheetname ) {
    this.sheetname = sheetname;
  }

  /**
   * @param fileName The fileName to set.
   */
  public void setFileName( String fileName ) {
    this.fileName = fileName;
  }

  /**
   * @param password teh passwoed to set.
   */
  public void setPassword( String password ) {
    this.password = password;
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
   * @return Returns the autosizecolums.
   */
  public boolean isAutoSizeColums() {
    return autosizecolums;
  }

  /**
   * @param autosizecolums The autosizecolums to set.
   */
  public void setAutoSizeColums( boolean autosizecolums ) {
    this.autosizecolums = autosizecolums;
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

  public boolean isSpecifyFormat() {
    return SpecifyFormat;
  }

  public void setSpecifyFormat( boolean SpecifyFormat ) {
    this.SpecifyFormat = SpecifyFormat;
  }

  public String getDateTimeFormat() {
    return date_time_format;
  }

  public void setDateTimeFormat( String date_time_format ) {
    this.date_time_format = date_time_format;
  }

  /**
   * @return Returns the splitEvery.
   */
  public int getSplitEvery() {
    return splitEvery;
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
   * @param splitEvery The splitEvery to set.
   */
  public void setSplitEvery( int splitEvery ) {
    this.splitEvery = splitEvery >= 0 ? splitEvery : 0;
  }

  /**
   * @return Returns the stepNrInFilename.
   */
  public boolean isStepNrInFilename() {
    return stepNrInFilename;
  }

  /**
   * @param stepNrInFilename The stepNrInFilename to set.
   */
  public void setStepNrInFilename( boolean stepNrInFilename ) {
    this.stepNrInFilename = stepNrInFilename;
  }

  /**
   * @return Returns the timeInFilename.
   */
  public boolean isTimeInFilename() {
    return timeInFilename;
  }

  /**
   * @return Returns the protectsheet.
   */
  public boolean isSheetProtected() {
    return protectsheet;
  }

  /**
   * @param timeInFilename The timeInFilename to set.
   */
  public void setTimeInFilename( boolean timeInFilename ) {
    this.timeInFilename = timeInFilename;
  }

  /**
   * @param protectsheet the value to set.
   */
  public void setProtectSheet( boolean protectsheet ) {
    this.protectsheet = protectsheet;
  }

  /**
   * @return Returns the outputFields.
   */
  public ExcelWriterStepField[] getOutputFields() {
    return outputFields;
  }

  /**
   * @param outputFields The outputFields to set.
   */
  public void setOutputFields( ExcelWriterStepField[] outputFields ) {
    this.outputFields = outputFields;
  }

  /**
   * @return Returns the template.
   */
  public boolean isTemplateEnabled() {
    return templateEnabled;
  }

  /**
   * @param template The template to set.
   */
  public void setTemplateEnabled( boolean template ) {
    this.templateEnabled = template;
  }

  public boolean isTemplateSheetEnabled() {
    return templateSheetEnabled;
  }

  public void setTemplateSheetEnabled( boolean templateSheetEnabled ) {
    this.templateSheetEnabled = templateSheetEnabled;
  }

  /**
   * @return Returns the templateFileName.
   */
  public String getTemplateFileName() {
    return templateFileName;
  }

  /**
   * @param templateFileName The templateFileName to set.
   */
  public void setTemplateFileName( String templateFileName ) {
    this.templateFileName = templateFileName;
  }

  public String getTemplateSheetName() {
    return templateSheetName;
  }

  public void setTemplateSheetName( String templateSheetName ) {
    this.templateSheetName = templateSheetName;
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
   * @return Returns the appendLines.
   */
  public boolean isAppendLines() {
    return appendLines;
  }

  /**
   * @param append The appendLines to set.
   */
  public void setAppendLines( boolean append ) {
    this.appendLines = append;
  }

  public void setMakeSheetActive( boolean makeSheetActive ) {
    this.makeSheetActive = makeSheetActive;
  }

  public boolean isMakeSheetActive() {
    return makeSheetActive;
  }

  public boolean isForceFormulaRecalculation() {
    return forceFormulaRecalculation;
  }

  public void setForceFormulaRecalculation( boolean forceFormulaRecalculation ) {
    this.forceFormulaRecalculation = forceFormulaRecalculation;
  }

  public boolean isLeaveExistingStylesUnchanged() {
    return leaveExistingStylesUnchanged;
  }

  public void setLeaveExistingStylesUnchanged( boolean leaveExistingStylesUnchanged ) {
    this.leaveExistingStylesUnchanged = leaveExistingStylesUnchanged;
  }

  @Override
  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode );
  }

  public void allocate( int nrfields ) {
    outputFields = new ExcelWriterStepField[ nrfields ];
  }

  @Override
  public Object clone() {
    ExcelWriterStepMeta retval = (ExcelWriterStepMeta) super.clone();
    int nrfields = outputFields.length;

    retval.allocate( nrfields );

    for ( int i = 0; i < nrfields; i++ ) {
      retval.outputFields[ i ] = (ExcelWriterStepField) outputFields[ i ].clone();
    }

    return retval;
  }

  private void readData( Node stepnode ) throws HopXMLException {
    try {

      headerEnabled = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "header" ) );
      footerEnabled = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "footer" ) );
      appendOmitHeader = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "appendOmitHeader" ) );
      appendLines = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "appendLines" ) );
      makeSheetActive = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "makeSheetActive" ) );
      appendOffset = Const.toInt( XMLHandler.getTagValue( stepnode, "appendOffset" ), 0 );
      appendEmpty = Const.toInt( XMLHandler.getTagValue( stepnode, "appendEmpty" ), 0 );

      startingCell = XMLHandler.getTagValue( stepnode, "startingCell" );
      rowWritingMethod = XMLHandler.getTagValue( stepnode, "rowWritingMethod" );
      forceFormulaRecalculation =
        "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "forceFormulaRecalculation" ) );
      leaveExistingStylesUnchanged =
        "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "leaveExistingStylesUnchanged" ) );

      String addToResult = XMLHandler.getTagValue( stepnode, "add_to_result_filenames" );
      if ( Utils.isEmpty( addToResult ) ) {
        addToResultFilenames = true;
      } else {
        addToResultFilenames = "Y".equalsIgnoreCase( addToResult );
      }

      fileName = XMLHandler.getTagValue( stepnode, "file", "name" );
      extension = XMLHandler.getTagValue( stepnode, "file", "extention" );

      doNotOpenNewFileInit =
        "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "file", "do_not_open_newfile_init" ) );
      stepNrInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "file", "split" ) );
      dateInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "file", "add_date" ) );
      timeInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "file", "add_time" ) );
      SpecifyFormat = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "file", "SpecifyFormat" ) );
      date_time_format = XMLHandler.getTagValue( stepnode, "file", "date_time_format" );

      autosizecolums = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "file", "autosizecolums" ) );
      streamingData = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "file", "stream_data" ) );
      protectsheet = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "file", "protect_sheet" ) );
      password = Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( stepnode, "file", "password" ) );
      protectedBy = XMLHandler.getTagValue( stepnode, "file", "protected_by" );
      splitEvery = Const.toInt( XMLHandler.getTagValue( stepnode, "file", "splitevery" ), 0 );

      templateEnabled = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "template", "enabled" ) );
      templateSheetEnabled =
        "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "template", "sheet_enabled" ) );
      templateSheetHidden =
        "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "template", "hidden" ) );
      templateFileName = XMLHandler.getTagValue( stepnode, "template", "filename" );
      templateSheetName = XMLHandler.getTagValue( stepnode, "template", "sheetname" );
      sheetname = XMLHandler.getTagValue( stepnode, "file", "sheetname" );
      ifFileExists = XMLHandler.getTagValue( stepnode, "file", "if_file_exists" );
      ifSheetExists = XMLHandler.getTagValue( stepnode, "file", "if_sheet_exists" );

      Node fields = XMLHandler.getSubNode( stepnode, "fields" );
      int nrfields = XMLHandler.countNodes( fields, "field" );

      allocate( nrfields );

      for ( int i = 0; i < nrfields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );

        outputFields[ i ] = new ExcelWriterStepField();
        outputFields[ i ].setName( XMLHandler.getTagValue( fnode, "name" ) );
        outputFields[ i ].setType( XMLHandler.getTagValue( fnode, "type" ) );
        outputFields[ i ].setFormat( XMLHandler.getTagValue( fnode, "format" ) );
        outputFields[ i ].setTitle( XMLHandler.getTagValue( fnode, "title" ) );
        outputFields[ i ].setTitleStyleCell( XMLHandler.getTagValue( fnode, "titleStyleCell" ) );
        outputFields[ i ].setStyleCell( XMLHandler.getTagValue( fnode, "styleCell" ) );
        outputFields[ i ].setCommentField( XMLHandler.getTagValue( fnode, "commentField" ) );
        outputFields[ i ].setCommentAuthorField( XMLHandler.getTagValue( fnode, "commentAuthorField" ) );
        outputFields[ i ].setFormula( XMLHandler.getTagValue( fnode, "formula" ) != null
          && XMLHandler.getTagValue( fnode, "formula" ).equalsIgnoreCase( "Y" ) );
        outputFields[ i ].setHyperlinkField( XMLHandler.getTagValue( fnode, "hyperlinkField" ) );
      }

    } catch ( Exception e ) {
      throw new HopXMLException( "Unable to load step info from XML", e );
    }
  }

  public String getNewLine( String fformat ) {
    String nl = System.getProperty( "line.separator" );

    if ( fformat != null ) {
      if ( fformat.equalsIgnoreCase( "DOS" ) ) {
        nl = "\r\n";
      } else if ( fformat.equalsIgnoreCase( "UNIX" ) ) {
        nl = "\n";
      }
    }

    return nl;
  }

  @Override
  public void setDefault() {

    autosizecolums = false;
    streamingData = false;
    headerEnabled = true;
    footerEnabled = false;
    fileName = "file";
    extension = "xls";
    doNotOpenNewFileInit = false;
    stepNrInFilename = false;
    dateInFilename = false;
    timeInFilename = false;
    date_time_format = null;
    SpecifyFormat = false;
    addToResultFilenames = true;
    protectsheet = false;
    splitEvery = 0;
    templateEnabled = false;
    templateFileName = "template.xls";
    templateSheetHidden = false;
    sheetname = "Sheet1";
    appendLines = false;
    ifFileExists = IF_FILE_EXISTS_CREATE_NEW;
    ifSheetExists = IF_SHEET_EXISTS_CREATE_NEW;
    startingCell = "A1";
    rowWritingMethod = ROW_WRITE_OVERWRITE;
    appendEmpty = 0;
    appendOffset = 0;
    appendOmitHeader = false;
    makeSheetActive = true;
    forceFormulaRecalculation = false;

    allocate( 0 );

  }

  public String[] getFiles( VariableSpace space ) {
    int copies = 1;
    int splits = 1;

    if ( stepNrInFilename ) {
      copies = 3;
    }

    if ( splitEvery != 0 ) {
      splits = 4;
    }

    int nr = copies * splits;
    if ( nr > 1 ) {
      nr++;
    }

    String[] retval = new String[ nr ];

    int i = 0;
    for ( int copy = 0; copy < copies; copy++ ) {
      for ( int split = 0; split < splits; split++ ) {
        retval[ i ] = buildFilename( space, copy, split );
        i++;
      }
    }
    if ( i < nr ) {
      retval[ i ] = "...";
    }

    return retval;
  }

  public String buildFilename( VariableSpace space, int stepnr, int splitnr ) {
    SimpleDateFormat daf = new SimpleDateFormat();

    // Replace possible environment variables...
    String retval = space.environmentSubstitute( fileName );
    String realextension = space.environmentSubstitute( extension );

    Date now = new Date();

    if ( SpecifyFormat && !Utils.isEmpty( date_time_format ) ) {
      daf.applyPattern( date_time_format );
      String dt = daf.format( now );
      retval += dt;
    } else {
      if ( dateInFilename ) {
        daf.applyPattern( "yyyMMdd" );
        String d = daf.format( now );
        retval += "_" + d;
      }
      if ( timeInFilename ) {
        daf.applyPattern( "HHmmss" );
        String t = daf.format( now );
        retval += "_" + t;
      }
    }
    if ( stepNrInFilename ) {
      retval += "_" + stepnr;
    }
    if ( splitEvery > 0 ) {
      retval += "_" + splitnr;
    }

    if ( realextension != null && realextension.length() != 0 ) {
      retval += "." + realextension;
    }

    return retval;
  }

  @Override
  public void getFields( RowMetaInterface r, String name, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, IMetaStore metaStore ) {
    if ( r == null ) {
      r = new RowMeta(); // give back values
    }

    // No values are added to the row in this type of step
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 800 );

    retval.append( "    " ).append( XMLHandler.addTagValue( "header", headerEnabled ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "footer", footerEnabled ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "makeSheetActive", makeSheetActive ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "rowWritingMethod", rowWritingMethod ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "startingCell", startingCell ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "appendOmitHeader", appendOmitHeader ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "appendOffset", appendOffset ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "appendEmpty", appendEmpty ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "rowWritingMethod", rowWritingMethod ) );
    retval.append( "    " ).append(
      XMLHandler.addTagValue( "forceFormulaRecalculation", forceFormulaRecalculation ) );
    retval.append( "    " ).append(
      XMLHandler.addTagValue( "leaveExistingStylesUnchanged", leaveExistingStylesUnchanged ) );
    retval.append( "    " + XMLHandler.addTagValue( "appendLines", appendLines ) );
    retval.append( "    " + XMLHandler.addTagValue( "add_to_result_filenames", addToResultFilenames ) );

    retval.append( "    <file>" ).append( Const.CR );
    retval.append( "      " ).append( XMLHandler.addTagValue( "name", fileName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "extention", extension ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "do_not_open_newfile_init", doNotOpenNewFileInit ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "split", stepNrInFilename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "add_date", dateInFilename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "add_time", timeInFilename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "SpecifyFormat", SpecifyFormat ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "date_time_format", date_time_format ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "sheetname", sheetname ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "autosizecolums", autosizecolums ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "stream_data", streamingData ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "protect_sheet", protectsheet ) );
    retval.append( "      " ).append(
      XMLHandler.addTagValue( "password", Encr.encryptPasswordIfNotUsingVariables( password ) ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "protected_by", protectedBy ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "splitevery", splitEvery ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "if_file_exists", ifFileExists ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "if_sheet_exists", ifSheetExists ) );

    retval.append( "      </file>" ).append( Const.CR );

    retval.append( "    <template>" ).append( Const.CR );
    retval.append( "      " ).append( XMLHandler.addTagValue( "enabled", templateEnabled ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "sheet_enabled", templateSheetEnabled ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "filename", templateFileName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "sheetname", templateSheetName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "hidden", templateSheetHidden ) );
    retval.append( "    </template>" ).append( Const.CR );

    retval.append( "    <fields>" ).append( Const.CR );
    for ( int i = 0; i < outputFields.length; i++ ) {
      ExcelWriterStepField field = outputFields[ i ];

      if ( field.getName() != null && field.getName().length() != 0 ) {
        retval.append( "      <field>" ).append( Const.CR );
        retval.append( "        " ).append( XMLHandler.addTagValue( "name", field.getName() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "type", field.getTypeDesc() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "format", field.getFormat() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "title", field.getTitle() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "titleStyleCell", field.getTitleStyleCell() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "styleCell", field.getStyleCell() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "commentField", field.getCommentField() ) );
        retval.append( "        " ).append(
          XMLHandler.addTagValue( "commentAuthorField", field.getCommentAuthorField() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "formula", field.isFormula() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "hyperlinkField", field.getHyperlinkField() ) );
        retval.append( "      </field>" ).append( Const.CR );
      }
    }
    retval.append( "    </fields>" ).append( Const.CR );

    return retval.toString();
  }

  @Override
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;

    // Check output fields
    if ( prev != null && prev.size() > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "ExcelWriterStepMeta.CheckResult.FieldsReceived", "" + prev.size() ), stepMeta );
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
        error_message =
          BaseMessages.getString( PKG, "ExcelWriterStepMeta.CheckResult.FieldsNotFound", error_message );
        cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta );
        remarks.add( cr );
      } else {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "ExcelWriterStepMeta.CheckResult.AllFieldsFound" ), stepMeta );
        remarks.add( cr );
      }
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "ExcelWriterStepMeta.CheckResult.ExpectedInputOk" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "ExcelWriterStepMeta.CheckResult.ExpectedInputError" ), stepMeta );
      remarks.add( cr );
    }

    cr =
      new CheckResult( CheckResultInterface.TYPE_RESULT_COMMENT, BaseMessages.getString(
        PKG, "ExcelWriterStepMeta.CheckResult.FilesNotChecked" ), stepMeta );
    remarks.add( cr );
  }

  /**
   * @param space                   the variable space to use
   * @param definitions
   * @param resourceNamingInterface
   * @param metaStore               the metaStore in which non-kettle metadata could reside.
   * @return the filename of the exported resource
   */
  @Override
  public String exportResources( VariableSpace space, Map<String, ResourceDefinition> definitions,
                                 ResourceNamingInterface resourceNamingInterface, IMetaStore metaStore ) throws HopException {
    try {
      // The object that we're modifying here is a copy of the original!
      // So let's change the filename from relative to absolute by grabbing the file object...
      //
      if ( !Utils.isEmpty( fileName ) ) {
        FileObject fileObject = HopVFS.getFileObject( space.environmentSubstitute( fileName ), space );
        fileName = resourceNamingInterface.nameResource( fileObject, space, true );
      }
      if ( !Utils.isEmpty( templateFileName ) ) {
        FileObject fileObject = HopVFS.getFileObject( space.environmentSubstitute( templateFileName ), space );
        templateFileName = resourceNamingInterface.nameResource( fileObject, space, true );
      }

      return null;
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

  @Override
  public StepMetaInjectionInterface getStepMetaInjectionInterface() {
    return new ExcelWriterMetaInjection( this );
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
                                TransMeta transMeta, Trans trans ) {
    return new ExcelWriterStep( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new ExcelWriterStepData();
  }

  /**
   * @return the streamingData
   */
  public boolean isStreamingData() {
    return streamingData;
  }

  /**
   * @param streamingData the streamingData to set
   */
  public void setStreamingData( boolean streamingData ) {
    this.streamingData = streamingData;
  }

  public boolean isTemplateSheetHidden() {
    return templateSheetHidden;
  }

  public void setTemplateSheetHidden( boolean hide ) {
    this.templateSheetHidden = hide;
  }

}
