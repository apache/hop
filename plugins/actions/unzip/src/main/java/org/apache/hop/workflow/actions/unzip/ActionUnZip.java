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

package org.apache.hop.workflow.actions.unzip;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.vfs2.AllFileSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.AbstractFileValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ValidatorContext;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.w3c.dom.Node;

/**
 * This defines a 'unzip' action. Its main use would be to unzip files in a directory
 *
 * @author Samatar Hassan
 * @since 25-09-2007
 */

@Action(
  id = "UNZIP",
  name = "i18n::ActionUnZip.Name",
  description = "i18n::ActionUnZip.Description",
  image = "UnZip.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
  documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/unzip.html"
)
public class ActionUnZip extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionUnZip.class; // For Translator

  private String zipFilename;
  public int afterunzip;
  private String wildcard;
  private String wildcardExclude;
  private String sourcedirectory; // targetdirectory on screen, renamed because of PDI-7761
  private String movetodirectory;
  private boolean addfiletoresult;
  private boolean isfromprevious;
  private boolean adddate;
  private boolean addtime;
  private boolean specifyFormat;
  private String dateTimeFormat;
  private boolean rootzip;
  private boolean createfolder;
  private String nrLimit;
  private String wildcardSource;
  private int iffileexist;
  private boolean createMoveToDirectory;

  private boolean addOriginalTimestamp;
  private boolean setOriginalModificationDate;

  public String SUCCESS_IF_AT_LEAST_X_FILES_UN_ZIPPED = "success_when_at_least";
  public String SUCCESS_IF_ERRORS_LESS = "success_if_errors_less";
  public String SUCCESS_IF_NO_ERRORS = "success_if_no_errors";
  private String successCondition;

  public static final int IF_FILE_EXISTS_SKIP = 0;
  public static final int IF_FILE_EXISTS_OVERWRITE = 1;
  public static final int IF_FILE_EXISTS_UNIQ = 2;
  public static final int IF_FILE_EXISTS_FAIL = 3;
  public static final int IF_FILE_EXISTS_OVERWRITE_DIFF_SIZE = 4;
  public static final int IF_FILE_EXISTS_OVERWRITE_EQUAL_SIZE = 5;
  public static final int IF_FILE_EXISTS_OVERWRITE_ZIP_BIG = 6;
  public static final int IF_FILE_EXISTS_OVERWRITE_ZIP_BIG_EQUAL = 7;
  public static final int IF_FILE_EXISTS_OVERWRITE_ZIP_SMALL = 8;
  public static final int IF_FILE_EXISTS_OVERWRITE_ZIP_SMALL_EQUAL = 9;

  public static final String[] typeIfFileExistsCode = /* WARNING: DO NOT TRANSLATE THIS. */
    {
      "SKIP", "OVERWRITE", "UNIQ", "FAIL", "OVERWRITE_DIFF_SIZE", "OVERWRITE_EQUAL_SIZE", "OVERWRITE_ZIP_BIG",
      "OVERWRITE_ZIP_BIG_EQUAL", "OVERWRITE_ZIP_BIG_SMALL", "OVERWRITE_ZIP_BIG_SMALL_EQUAL", };

  public static final String[] typeIfFileExistsDesc = {
    BaseMessages.getString( PKG, "JobUnZip.Skip.Label" ),
    BaseMessages.getString( PKG, "JobUnZip.Overwrite.Label" ),
    BaseMessages.getString( PKG, "JobUnZip.Give_Unique_Name.Label" ),
    BaseMessages.getString( PKG, "JobUnZip.Fail.Label" ),
    BaseMessages.getString( PKG, "JobUnZip.OverwriteIfSizeDifferent.Label" ),
    BaseMessages.getString( PKG, "JobUnZip.OverwriteIfSizeEquals.Label" ),
    BaseMessages.getString( PKG, "JobUnZip.OverwriteIfZipBigger.Label" ),
    BaseMessages.getString( PKG, "JobUnZip.OverwriteIfZipBiggerOrEqual.Label" ),
    BaseMessages.getString( PKG, "JobUnZip.OverwriteIfZipSmaller.Label" ),
    BaseMessages.getString( PKG, "JobUnZip.OverwriteIfZipSmallerOrEqual.Label" ), };

  private int nrErrors = 0;
  private int nrSuccess = 0;
  boolean successConditionBroken = false;
  boolean successConditionBrokenExit = false;
  int limitFiles = 0;

  private static SimpleDateFormat daf;
  private boolean dateFormatSet = false;

  public ActionUnZip( String n ) {
    super( n, "" );
    zipFilename = null;
    afterunzip = 0;
    wildcard = null;
    wildcardExclude = null;
    sourcedirectory = null;
    movetodirectory = null;
    addfiletoresult = false;
    isfromprevious = false;
    adddate = false;
    addtime = false;
    specifyFormat = false;
    rootzip = false;
    createfolder = false;
    nrLimit = "10";
    wildcardSource = null;
    iffileexist = IF_FILE_EXISTS_SKIP;
    successCondition = SUCCESS_IF_NO_ERRORS;
    createMoveToDirectory = false;

    addOriginalTimestamp = false;
    setOriginalModificationDate = false;
  }

  public ActionUnZip() {
    this( "" );
  }

  public Object clone() {
    ActionUnZip je = (ActionUnZip) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 550 ); // 450 chars in just spaces and tag names alone

    retval.append( super.getXml() );
    retval.append( "      " ).append( XmlHandler.addTagValue( "zipfilename", zipFilename ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "wildcard", wildcard ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "wildcardexclude", wildcardExclude ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "targetdirectory", sourcedirectory ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "movetodirectory", movetodirectory ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "afterunzip", afterunzip ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "addfiletoresult", addfiletoresult ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "isfromprevious", isfromprevious ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "adddate", adddate ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "addtime", addtime ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "addOriginalTimestamp", addOriginalTimestamp ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "SpecifyFormat", specifyFormat ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "date_time_format", dateTimeFormat ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "rootzip", rootzip ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "createfolder", createfolder ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "nr_limit", nrLimit ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "wildcardSource", wildcardSource ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "success_condition", successCondition ) );
    retval
      .append( "      " ).append( XmlHandler.addTagValue( "iffileexists", getIfFileExistsCode( iffileexist ) ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "create_move_to_directory", createMoveToDirectory ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "setOriginalModificationDate", setOriginalModificationDate ) );

    return retval.toString();
  }

  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider, IVariables variables ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      zipFilename = XmlHandler.getTagValue( entrynode, "zipfilename" );
      afterunzip = Const.toInt( XmlHandler.getTagValue( entrynode, "afterunzip" ), -1 );

      wildcard = XmlHandler.getTagValue( entrynode, "wildcard" );
      wildcardExclude = XmlHandler.getTagValue( entrynode, "wildcardexclude" );
      sourcedirectory = XmlHandler.getTagValue( entrynode, "targetdirectory" );
      movetodirectory = XmlHandler.getTagValue( entrynode, "movetodirectory" );
      addfiletoresult = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "addfiletoresult" ) );
      isfromprevious = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "isfromprevious" ) );
      adddate = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "adddate" ) );
      addtime = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "addtime" ) );
      addOriginalTimestamp = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "addOriginalTimestamp" ) );
      specifyFormat = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "SpecifyFormat" ) );
      dateTimeFormat = XmlHandler.getTagValue( entrynode, "date_time_format" );
      rootzip = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "rootzip" ) );
      createfolder = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "createfolder" ) );
      nrLimit = XmlHandler.getTagValue( entrynode, "nr_limit" );
      wildcardSource = XmlHandler.getTagValue( entrynode, "wildcardSource" );
      successCondition = XmlHandler.getTagValue( entrynode, "success_condition" );
      if ( Utils.isEmpty( successCondition ) ) {
        successCondition = SUCCESS_IF_NO_ERRORS;
      }
      iffileexist = getIfFileExistsInt( XmlHandler.getTagValue( entrynode, "iffileexists" ) );
      createMoveToDirectory =
        "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "create_move_to_directory" ) );
      setOriginalModificationDate =
        "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "setOriginalModificationDate" ) );
    } catch ( HopXmlException xe ) {
      throw new HopXmlException( "Unable to load action of type 'unzip' from XML node", xe );
    }
  }

  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    result.setResult( false );
    result.setNrErrors( 1 );

    List<RowMetaAndData> rows = result.getRows();
    RowMetaAndData resultRow = null;

    String realFilenameSource = resolve( zipFilename );
    String realWildcardSource = resolve( wildcardSource );
    String realWildcard = resolve( wildcard );
    String realWildcardExclude = resolve( wildcardExclude );
    String realTargetdirectory = resolve( sourcedirectory );
    String realMovetodirectory = resolve( movetodirectory );

    limitFiles = Const.toInt( resolve( getLimit() ), 10 );
    nrErrors = 0;
    nrSuccess = 0;
    successConditionBroken = false;
    successConditionBrokenExit = false;

    if ( isfromprevious ) {
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "JobUnZip.Log.ArgFromPrevious.Found", ( rows != null ? rows
          .size() : 0 )
          + "" ) );
      }

      if ( rows.size() == 0 ) {
        return result;
      }
    } else {
      if ( Utils.isEmpty( zipFilename ) ) {
        // Zip file/folder is missing
        logError( BaseMessages.getString( PKG, "JobUnZip.No_ZipFile_Defined.Label" ) );
        return result;
      }
    }

    FileObject fileObject = null;
    FileObject targetdir = null;
    FileObject movetodir = null;

    try {

      // Let's make some checks here, before running action ...

      if ( Utils.isEmpty( realTargetdirectory ) ) {
        logError( BaseMessages.getString( PKG, "JobUnZip.Error.TargetFolderMissing" ) );
        return result;
      }

      boolean exitaction = false;

      // Target folder
      targetdir = HopVfs.getFileObject( realTargetdirectory );

      if ( !targetdir.exists() ) {
        if ( createfolder ) {
          targetdir.createFolder();
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobUnZip.Log.TargetFolderCreated", realTargetdirectory ) );
          }

        } else {
          log.logError( BaseMessages.getString( PKG, "JobUnZip.TargetFolderNotFound.Label" ) );
          exitaction = true;
        }
      } else {
        if ( !( targetdir.getType() == FileType.FOLDER ) ) {
          log.logError( BaseMessages.getString( PKG, "JobUnZip.TargetFolderNotFolder.Label", realTargetdirectory ) );
          exitaction = true;
        } else {
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobUnZip.TargetFolderExists.Label", realTargetdirectory ) );
          }
        }
      }

      // If user want to move zip files after process
      // movetodirectory must be provided
      if ( afterunzip == 2 ) {
        if ( Utils.isEmpty( movetodirectory ) ) {
          log.logError( BaseMessages.getString( PKG, "JobUnZip.MoveToDirectoryEmpty.Label" ) );
          exitaction = true;
        } else {
          movetodir = HopVfs.getFileObject( realMovetodirectory );
          if ( !( movetodir.exists() ) || !( movetodir.getType() == FileType.FOLDER ) ) {
            if ( createMoveToDirectory ) {
              movetodir.createFolder();
              if ( log.isDetailed() ) {
                logDetailed( BaseMessages.getString( PKG, "JobUnZip.Log.MoveToFolderCreated", realMovetodirectory ) );
              }
            } else {
              log.logError( BaseMessages.getString( PKG, "JobUnZip.MoveToDirectoryNotExists.Label" ) );
              exitaction = true;
            }
          }
        }
      }

      // We found errors...now exit
      if ( exitaction ) {
        return result;
      }

      if ( isfromprevious ) {
        if ( rows != null ) { // Copy the input row to the (command line) arguments
          for ( int iteration = 0; iteration < rows.size() && !parentWorkflow.isStopped(); iteration++ ) {
            if ( successConditionBroken ) {
              if ( !successConditionBrokenExit ) {
                logError( BaseMessages.getString( PKG, "JobUnZip.Error.SuccessConditionbroken", "" + nrErrors ) );
                successConditionBrokenExit = true;
              }
              result.setNrErrors( nrErrors );
              return result;
            }

            resultRow = rows.get( iteration );

            // Get sourcefile/folder and wildcard
            realFilenameSource = resultRow.getString( 0, null );
            realWildcardSource = resultRow.getString( 1, null );

            fileObject = HopVfs.getFileObject( realFilenameSource );
            if ( fileObject.exists() ) {
              processOneFile(
                result, parentWorkflow, fileObject, realTargetdirectory, realWildcard, realWildcardExclude,
                movetodir, realMovetodirectory, realWildcardSource );
            } else {
              updateErrors();
              logError( BaseMessages.getString( PKG, "JobUnZip.Error.CanNotFindFile", realFilenameSource ) );
            }
          }
        }
      } else {
        fileObject = HopVfs.getFileObject( realFilenameSource );
        if ( !fileObject.exists() ) {
          log.logError( BaseMessages.getString( PKG, "JobUnZip.ZipFile.NotExists.Label", realFilenameSource ) );
          return result;
        }

        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "JobUnZip.Zip_FileExists.Label", realFilenameSource ) );
        }
        if ( Utils.isEmpty( sourcedirectory ) ) {
          log.logError( BaseMessages.getString( PKG, "JobUnZip.SourceFolderNotFound.Label" ) );
          return result;
        }

        processOneFile(
          result, parentWorkflow, fileObject, realTargetdirectory, realWildcard, realWildcardExclude, movetodir,
          realMovetodirectory, realWildcardSource );
      }
    } catch ( Exception e ) {
      log.logError( BaseMessages.getString( PKG, "JobUnZip.ErrorUnzip.Label", realFilenameSource, e.getMessage() ) );
      updateErrors();
    } finally {
      if ( fileObject != null ) {
        try {
          fileObject.close();
        } catch ( IOException ex ) { /* Ignore */
        }
      }
      if ( targetdir != null ) {
        try {
          targetdir.close();
        } catch ( IOException ex ) { /* Ignore */
        }
      }
      if ( movetodir != null ) {
        try {
          movetodir.close();
        } catch ( IOException ex ) { /* Ignore */
        }
      }
    }

    result.setNrErrors( nrErrors );
    result.setNrLinesWritten( nrSuccess );
    if ( getSuccessStatus() ) {
      result.setResult( true );
    }
    displayResults();

    return result;
  }

  private void displayResults() {
    if ( log.isDetailed() ) {
      logDetailed( "=======================================" );
      logDetailed( BaseMessages.getString( PKG, "JobUnZip.Log.Info.FilesInError", "" + nrErrors ) );
      logDetailed( BaseMessages.getString( PKG, "JobUnZip.Log.Info.FilesInSuccess", "" + nrSuccess ) );
      logDetailed( "=======================================" );
    }
  }

  private boolean processOneFile( Result result, IWorkflowEngine<WorkflowMeta> parentWorkflow, FileObject fileObject, String realTargetdirectory,
                                  String realWildcard, String realWildcardExclude, FileObject movetodir, String realMovetodirectory,
                                  String realWildcardSource ) {
    boolean retval = false;

    try {
      if ( fileObject.getType().equals( FileType.FILE ) ) {
        // We have to unzip one zip file
        if ( !unzipFile(
          fileObject, realTargetdirectory, realWildcard, realWildcardExclude, result, parentWorkflow,
          movetodir, realMovetodirectory ) ) {
          updateErrors();
        } else {
          updateSuccess();
        }
      } else {
        // Folder..let's see wildcard
        FileObject[] children = fileObject.getChildren();

        for ( int i = 0; i < children.length && !parentWorkflow.isStopped(); i++ ) {
          if ( successConditionBroken ) {
            if ( !successConditionBrokenExit ) {
              logError( BaseMessages.getString( PKG, "JobUnZip.Error.SuccessConditionbroken", "" + nrErrors ) );
              successConditionBrokenExit = true;
            }
            return false;
          }
          // Get only file!
          if ( !children[ i ].getType().equals( FileType.FOLDER ) ) {
            boolean unzip = true;

            String filename = children[ i ].getName().getPath();

            Pattern patternSource = null;

            if ( !Utils.isEmpty( realWildcardSource ) ) {
              patternSource = Pattern.compile( realWildcardSource );
            }

            // First see if the file matches the regular expression!
            if ( patternSource != null ) {
              Matcher matcher = patternSource.matcher( filename );
              unzip = matcher.matches();
            }
            if ( unzip ) {
              if ( !unzipFile(
                children[ i ], realTargetdirectory, realWildcard, realWildcardExclude, result, parentWorkflow,
                movetodir, realMovetodirectory ) ) {
                updateErrors();
              } else {
                updateSuccess();
              }
            }
          }
        }
      }
    } catch ( Exception e ) {
      updateErrors();
      logError( BaseMessages.getString( PKG, "JobUnZip.Error.Label", e.getMessage() ) );
    } finally {
      if ( fileObject != null ) {
        try {
          fileObject.close();
        } catch ( IOException ex ) { /* Ignore */
        }
      }
    }
    return retval;
  }

  private boolean unzipFile( FileObject sourceFileObject, String realTargetdirectory, String realWildcard,
                             String realWildcardExclude, Result result, IWorkflowEngine<WorkflowMeta> parentWorkflow, FileObject movetodir, String realMovetodirectory ) {
    boolean retval = false;
    String unzipToFolder = realTargetdirectory;
    try {

      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "JobUnZip.Log.ProcessingFile", sourceFileObject.toString() ) );
      }

      // Do you create a root folder?
      //
      if ( rootzip ) {
        String shortSourceFilename = sourceFileObject.getName().getBaseName();
        int lenstring = shortSourceFilename.length();
        int lastindexOfDot = shortSourceFilename.lastIndexOf( '.' );
        if ( lastindexOfDot == -1 ) {
          lastindexOfDot = lenstring;
        }

        String folderName = realTargetdirectory + "/" + shortSourceFilename.substring( 0, lastindexOfDot );
        FileObject rootfolder = HopVfs.getFileObject( folderName );
        if ( !rootfolder.exists() ) {
          try {
            rootfolder.createFolder();
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "JobUnZip.Log.RootFolderCreated", folderName ) );
            }
          } catch ( Exception e ) {
            throw new Exception(
              BaseMessages.getString( PKG, "JobUnZip.Error.CanNotCreateRootFolder", folderName ), e );
          }
        }
        unzipToFolder = folderName;
      }

      // Try to read the entries from the VFS object...
      //
      String zipFilename = "zip:" + sourceFileObject.getName().getFriendlyURI();
      FileObject zipFile = HopVfs.getFileObject( zipFilename );
      FileObject[] items = zipFile.findFiles( new AllFileSelector() {
        public boolean traverseDescendents( FileSelectInfo info ) {
          return true;
        }

        public boolean includeFile( FileSelectInfo info ) {
          // Never return the parent directory of a file list.
          if ( info.getDepth() == 0 ) {
            return false;
          }

          FileObject fileObject = info.getFile();
          return fileObject != null;
        }
      } );

      Pattern pattern = null;
      if ( !Utils.isEmpty( realWildcard ) ) {
        pattern = Pattern.compile( realWildcard );

      }
      Pattern patternexclude = null;
      if ( !Utils.isEmpty( realWildcardExclude ) ) {
        patternexclude = Pattern.compile( realWildcardExclude );

      }

      for ( FileObject item : items ) {

        if ( successConditionBroken ) {
          if ( !successConditionBrokenExit ) {
            logError( BaseMessages.getString( PKG, "JobUnZip.Error.SuccessConditionbroken", "" + nrErrors ) );
            successConditionBrokenExit = true;
          }
          return false;
        }

        synchronized ( HopVfs.getFileSystemManager() ) {
          FileObject newFileObject = null;
          try {
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString(
                PKG, "JobUnZip.Log.ProcessingZipEntry", item.getName().getURI(), sourceFileObject.toString() ) );
            }

            // get real destination filename
            //
            String newFileName = unzipToFolder + Const.FILE_SEPARATOR + getTargetFilename( item );
            newFileObject = HopVfs.getFileObject( newFileName );

            if ( item.getType().equals( FileType.FOLDER ) ) {
              // Directory
              //
              if ( log.isDetailed() ) {
                logDetailed( BaseMessages.getString( PKG, "JobUnZip.CreatingDirectory.Label", newFileName ) );
              }

              // Create Directory if necessary ...
              //
              if ( !newFileObject.exists() ) {
                newFileObject.createFolder();
              }
            } else {
              // File
              //
              boolean getIt = true;
              boolean getItexclude = false;

              // First see if the file matches the regular expression!
              //
              if ( pattern != null ) {
                Matcher matcher = pattern.matcher( item.getName().getURI() );
                getIt = matcher.matches();
              }

              if ( patternexclude != null ) {
                Matcher matcherexclude = patternexclude.matcher( item.getName().getURI() );
                getItexclude = matcherexclude.matches();
              }

              boolean take = takeThisFile( item, newFileName );

              if ( getIt && !getItexclude && take ) {
                if ( log.isDetailed() ) {
                  logDetailed( BaseMessages.getString( PKG, "JobUnZip.ExtractingEntry.Label", item
                    .getName().getURI(), newFileName ) );
                }

                if ( iffileexist == IF_FILE_EXISTS_UNIQ ) {
                  // Create file with unique name

                  int lenstring = newFileName.length();
                  int lastindexOfDot = newFileName.lastIndexOf( '.' );
                  if ( lastindexOfDot == -1 ) {
                    lastindexOfDot = lenstring;
                  }

                  newFileName =
                    newFileName.substring( 0, lastindexOfDot )
                      + StringUtil.getFormattedDateTimeNow( true )
                      + newFileName.substring( lastindexOfDot, lenstring );

                  if ( log.isDebug() ) {
                    logDebug( BaseMessages.getString( PKG, "JobUnZip.Log.CreatingUniqFile", newFileName ) );
                  }
                }

                // See if the folder to the target file exists...
                //
                if ( !newFileObject.getParent().exists() ) {
                  newFileObject.getParent().createFolder(); // creates the whole path.
                }
                InputStream is = null;
                OutputStream os = null;

                try {
                  is = HopVfs.getInputStream( item );
                  os = HopVfs.getOutputStream( newFileObject, false );

                  if ( is != null ) {
                    byte[] buff = new byte[ 2048 ];
                    int len;

                    while ( ( len = is.read( buff ) ) > 0 ) {
                      os.write( buff, 0, len );
                    }

                    // Add filename to result filenames
                    addFilenameToResultFilenames( result, parentWorkflow, newFileName );
                  }
                } finally {
                  if ( is != null ) {
                    is.close();
                  }
                  if ( os != null ) {
                    os.close();
                  }
                }
              } // end if take
            }
          } catch ( Exception e ) {
            updateErrors();
            logError(
              BaseMessages.getString(
                PKG, "JobUnZip.Error.CanNotProcessZipEntry", item.getName().getURI(), sourceFileObject
                  .toString() ), e );
          } finally {
            if ( newFileObject != null ) {
              try {
                newFileObject.close();
                if ( setOriginalModificationDate ) {
                  // Change last modification date
                  newFileObject.getContent().setLastModifiedTime( item.getContent().getLastModifiedTime() );
                }
              } catch ( Exception e ) { /* Ignore */
              } // ignore this
            }
            // Close file object
            // close() does not release resources!
            HopVfs.getFileSystemManager().closeFileSystem( item.getFileSystem() );
            if ( items != null ) {
              items = null;
            }
          }
        } // Synchronized block on HopVfs.getInstance().getFileSystemManager()
      } // End for

      // Here gc() is explicitly called if e.g. createfile is used in the same
      // workflow for the same file. The problem is that after creating the file the
      // file object is not properly garbaged collected and thus the file cannot
      // be deleted anymore. This is a known problem in the JVM.

      // System.gc();

      // Unzip done...
      if ( afterunzip > 0 ) {
        doUnzipPostProcessing( sourceFileObject, movetodir, realMovetodirectory );
      }
      retval = true;
    } catch ( Exception e ) {
      updateErrors();
      log.logError( BaseMessages.getString(
        PKG, "JobUnZip.ErrorUnzip.Label", sourceFileObject.toString(), e.getMessage() ), e );
    }

    return retval;
  }

  /**
   * Moving or deleting source file.
   */
  private void doUnzipPostProcessing( FileObject sourceFileObject, FileObject movetodir, String realMovetodirectory ) throws FileSystemException {
    if ( afterunzip == 1 ) {
      // delete zip file
      boolean deleted = sourceFileObject.delete();
      if ( !deleted ) {
        updateErrors();
        logError( BaseMessages.getString( PKG, "JobUnZip.Cant_Delete_File.Label", sourceFileObject.toString() ) );
      }
      // File deleted
      if ( log.isDebug() ) {
        logDebug( BaseMessages.getString( PKG, "JobUnZip.File_Deleted.Label", sourceFileObject.toString() ) );
      }
    } else if ( afterunzip == 2 ) {
      FileObject destFile = null;
      // Move File
      try {
        String destinationFilename = movetodir + Const.FILE_SEPARATOR + sourceFileObject.getName().getBaseName();
        destFile = HopVfs.getFileObject( destinationFilename );

        sourceFileObject.moveTo( destFile );

        // File moved
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString(
            PKG, "JobUnZip.Log.FileMovedTo", sourceFileObject.toString(), realMovetodirectory ) );
        }
      } catch ( Exception e ) {
        updateErrors();
        logError( BaseMessages.getString(
          PKG, "JobUnZip.Cant_Move_File.Label", sourceFileObject.toString(), realMovetodirectory, e
            .getMessage() ) );
      } finally {
        if ( destFile != null ) {
          try {
            destFile.close();
          } catch ( IOException ex ) { /* Ignore */
          }
        }
      }
    }
  }

  private void addFilenameToResultFilenames( Result result, IWorkflowEngine<WorkflowMeta> parentWorkflow, String newfile ) throws Exception {
    if ( addfiletoresult ) {
      // Add file to result files name
      ResultFile resultFile =
        new ResultFile( ResultFile.FILE_TYPE_GENERAL, HopVfs.getFileObject( newfile ), parentWorkflow
          .getWorkflowName(), toString() );
      result.getResultFiles().put( resultFile.getFile().toString(), resultFile );
    }
  }

  private void updateErrors() {
    nrErrors++;
    if ( checkIfSuccessConditionBroken() ) {
      // Success condition was broken
      successConditionBroken = true;
    }
  }

  private void updateSuccess() {
    nrSuccess++;
  }

  private boolean checkIfSuccessConditionBroken() {
    boolean retval = false;
    if ( ( nrErrors > 0 && getSuccessCondition().equals( SUCCESS_IF_NO_ERRORS ) )
      || ( nrErrors >= limitFiles && getSuccessCondition().equals( SUCCESS_IF_ERRORS_LESS ) ) ) {
      retval = true;
    }
    return retval;
  }

  private boolean getSuccessStatus() {
    boolean retval = false;

    if ( ( nrErrors == 0 && getSuccessCondition().equals( SUCCESS_IF_NO_ERRORS ) )
      || ( nrSuccess >= limitFiles && getSuccessCondition().equals( SUCCESS_IF_AT_LEAST_X_FILES_UN_ZIPPED ) )
      || ( nrErrors <= limitFiles && getSuccessCondition().equals( SUCCESS_IF_ERRORS_LESS ) ) ) {
      retval = true;
    }

    return retval;
  }

  private boolean takeThisFile( FileObject sourceFile, String destinationFile ) throws FileSystemException {
    boolean retval = false;
    File destination = new File( destinationFile );
    if ( !destination.exists() ) {
      if ( log.isDebug() ) {
        logDebug( BaseMessages.getString( PKG, "JobUnZip.Log.CanNotFindFile", destinationFile ) );
      }
      return true;
    }
    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "JobUnZip.Log.FileExists", destinationFile ) );
    }
    if ( iffileexist == IF_FILE_EXISTS_SKIP ) {
      if ( log.isDebug() ) {
        logDebug( BaseMessages.getString( PKG, "JobUnZip.Log.FileSkip", destinationFile ) );
      }
      return false;
    }
    if ( iffileexist == IF_FILE_EXISTS_FAIL ) {
      updateErrors();
      logError( BaseMessages.getString( PKG, "JobUnZip.Log.FileError", destinationFile, "" + nrErrors ) );
      return false;
    }

    if ( iffileexist == IF_FILE_EXISTS_OVERWRITE ) {
      if ( log.isDebug() ) {
        logDebug( BaseMessages.getString( PKG, "JobUnZip.Log.FileOverwrite", destinationFile ) );
      }
      return true;
    }

    Long entrySize = sourceFile.getContent().getSize();
    Long destinationSize = destination.length();

    if ( iffileexist == IF_FILE_EXISTS_OVERWRITE_DIFF_SIZE ) {
      if ( entrySize != destinationSize ) {
        if ( log.isDebug() ) {
          logDebug( BaseMessages.getString(
            PKG, "JobUnZip.Log.FileDiffSize.Diff", sourceFile.getName().getURI(), "" + entrySize,
            destinationFile, "" + destinationSize ) );
        }
        return true;
      } else {
        if ( log.isDebug() ) {
          logDebug( BaseMessages.getString(
            PKG, "JobUnZip.Log.FileDiffSize.Same", sourceFile.getName().getURI(), "" + entrySize,
            destinationFile, "" + destinationSize ) );
        }
        return false;
      }
    }
    if ( iffileexist == IF_FILE_EXISTS_OVERWRITE_EQUAL_SIZE ) {
      if ( entrySize == destinationSize ) {
        if ( log.isDebug() ) {
          logDebug( BaseMessages.getString(
            PKG, "JobUnZip.Log.FileEqualSize.Same", sourceFile.getName().getURI(), "" + entrySize,
            destinationFile, "" + destinationSize ) );
        }
        return true;
      } else {
        if ( log.isDebug() ) {
          logDebug( BaseMessages.getString(
            PKG, "JobUnZip.Log.FileEqualSize.Diff", sourceFile.getName().getURI(), "" + entrySize,
            destinationFile, "" + destinationSize ) );
        }
        return false;
      }
    }
    if ( iffileexist == IF_FILE_EXISTS_OVERWRITE_ZIP_BIG ) {
      if ( entrySize > destinationSize ) {
        if ( log.isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "JobUnZip.Log.FileBigSize.Big", sourceFile.getName().getURI(), ""
            + entrySize, destinationFile, "" + destinationSize ) );
        }
        return true;
      } else {
        if ( log.isDebug() ) {
          logDebug( BaseMessages.getString(
            PKG, "JobUnZip.Log.FileBigSize.Small", sourceFile.getName().getURI(), "" + entrySize,
            destinationFile, "" + destinationSize ) );
        }
        return false;
      }
    }
    if ( iffileexist == IF_FILE_EXISTS_OVERWRITE_ZIP_BIG_EQUAL ) {
      if ( entrySize >= destinationSize ) {
        if ( log.isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "JobUnZip.Log.FileBigEqualSize.Big", sourceFile
            .getName().getURI(), "" + entrySize, destinationFile, "" + destinationSize ) );
        }
        return true;
      } else {
        if ( log.isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "JobUnZip.Log.FileBigEqualSize.Small", sourceFile
            .getName().getURI(), "" + entrySize, destinationFile, "" + destinationSize ) );
        }
        return false;
      }
    }
    if ( iffileexist == IF_FILE_EXISTS_OVERWRITE_ZIP_SMALL ) {
      if ( entrySize < destinationSize ) {
        if ( log.isDebug() ) {
          logDebug( BaseMessages.getString(
            PKG, "JobUnZip.Log.FileSmallSize.Small", sourceFile.getName().getURI(), "" + entrySize,
            destinationFile, "" + destinationSize ) );
        }
        return true;
      } else {
        if ( log.isDebug() ) {
          logDebug( BaseMessages.getString(
            PKG, "JobUnZip.Log.FileSmallSize.Big", sourceFile.getName().getURI(), "" + entrySize,
            destinationFile, "" + destinationSize ) );
        }
        return false;
      }
    }
    if ( iffileexist == IF_FILE_EXISTS_OVERWRITE_ZIP_SMALL_EQUAL ) {
      if ( entrySize <= destinationSize ) {
        if ( log.isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "JobUnZip.Log.FileSmallEqualSize.Small", sourceFile
            .getName().getURI(), "" + entrySize, destinationFile, "" + destinationSize ) );
        }
        return true;
      } else {
        if ( log.isDebug() ) {
          logDebug( BaseMessages.getString( PKG, "JobUnZip.Log.FileSmallEqualSize.Big", sourceFile
            .getName().getURI(), "" + entrySize, destinationFile, "" + destinationSize ) );
        }
        return false;
      }
    }
    if ( iffileexist == IF_FILE_EXISTS_UNIQ ) {
      // Create file with unique name
      return true;
    }

    return retval;
  }

  @Override public boolean isEvaluation() {
    return true;
  }

  public static final int getIfFileExistsInt( String desc ) {
    for ( int i = 0; i < typeIfFileExistsCode.length; i++ ) {
      if ( typeIfFileExistsCode[ i ].equalsIgnoreCase( desc ) ) {
        return i;
      }
    }
    return 0;
  }

  public static final String getIfFileExistsCode( int i ) {
    if ( i < 0 || i >= typeIfFileExistsCode.length ) {
      return null;
    }
    return typeIfFileExistsCode[ i ];
  }

  /**
   * @return Returns the iffileexist.
   */
  public int getIfFileExist() {
    return iffileexist;
  }

  public void setIfFileExists( int iffileexist ) {
    this.iffileexist = iffileexist;
  }

  public boolean isCreateMoveToDirectory() {
    return createMoveToDirectory;
  }

  public void setCreateMoveToDirectory( boolean createMoveToDirectory ) {
    this.createMoveToDirectory = createMoveToDirectory;
  }

  public void setZipFilename( String zipFilename ) {
    this.zipFilename = zipFilename;
  }

  public void setWildcard( String wildcard ) {
    this.wildcard = wildcard;
  }

  public void setWildcardExclude( String wildcardExclude ) {
    this.wildcardExclude = wildcardExclude;
  }

  public void setSourceDirectory( String targetdirectoryin ) {
    this.sourcedirectory = targetdirectoryin;
  }

  public void setMoveToDirectory( String movetodirectory ) {
    this.movetodirectory = movetodirectory;
  }

  public String getSourceDirectory() {
    return sourcedirectory;
  }

  public String getMoveToDirectory() {
    return movetodirectory;
  }

  public String getZipFilename() {
    return zipFilename;
  }

  public String getWildcardSource() {
    return wildcardSource;
  }

  public void setWildcardSource( String wildcardSource ) {
    this.wildcardSource = wildcardSource;
  }

  public String getWildcard() {
    return wildcard;
  }

  public String getWildcardExclude() {
    return wildcardExclude;
  }

  public void setAddFileToResult( boolean addfiletoresultin ) {
    this.addfiletoresult = addfiletoresultin;
  }

  public boolean isAddFileToResult() {
    return addfiletoresult;
  }

  public void setDateInFilename( boolean adddate ) {
    this.adddate = adddate;
  }

  public void setAddOriginalTimestamp( boolean addOriginalTimestamp ) {
    this.addOriginalTimestamp = addOriginalTimestamp;
  }

  public boolean isOriginalTimestamp() {
    return addOriginalTimestamp;
  }

  public void setOriginalModificationDate( boolean setOriginalModificationDate ) {
    this.setOriginalModificationDate = setOriginalModificationDate;
  }

  public boolean isOriginalModificationDate() {
    return setOriginalModificationDate;
  }

  public boolean isDateInFilename() {
    return adddate;
  }

  public void setTimeInFilename( boolean addtime ) {
    this.addtime = addtime;
  }

  public boolean isTimeInFilename() {
    return addtime;
  }

  public boolean isSpecifyFormat() {
    return specifyFormat;
  }

  public void setSpecifyFormat( boolean specifyFormat ) {
    this.specifyFormat = specifyFormat;
  }

  public String getDateTimeFormat() {
    return dateTimeFormat;
  }

  public void setDateTimeFormat( String dateTimeFormat ) {
    this.dateTimeFormat = dateTimeFormat;
  }

  public void setDatafromprevious( boolean isfromprevious ) {
    this.isfromprevious = isfromprevious;
  }

  public boolean getDatafromprevious() {
    return isfromprevious;
  }

  public void setCreateRootFolder( boolean rootzip ) {
    this.rootzip = rootzip;
  }

  public boolean isCreateRootFolder() {
    return rootzip;
  }

  public void setCreateFolder( boolean createfolder ) {
    this.createfolder = createfolder;
  }

  public boolean isCreateFolder() {
    return createfolder;
  }

  public void setLimit( String nrLimitin ) {
    this.nrLimit = nrLimitin;
  }

  public String getLimit() {
    return nrLimit;
  }

  public void setSuccessCondition( String successCondition ) {
    this.successCondition = successCondition;
  }

  public String getSuccessCondition() {
    return successCondition;
  }

  protected String getTargetFilename( FileObject file ) throws FileSystemException {

    String retval = "";
    String filename = file.getName().getPath();
    // Replace possible environment variables...
    if ( filename != null ) {
      retval = filename;
    }
    if ( file.getType() != FileType.FILE ) {
      return retval;
    }

    if ( !specifyFormat && !adddate && !addtime ) {
      return retval;
    }

    int lenstring = retval.length();
    int lastindexOfDot = retval.lastIndexOf( '.' );
    if ( lastindexOfDot == -1 ) {
      lastindexOfDot = lenstring;
    }

    retval = retval.substring( 0, lastindexOfDot );

    if ( daf == null ) {
      daf = new SimpleDateFormat();
    }

    Date timestamp = new Date();
    if ( addOriginalTimestamp ) {
      timestamp = new Date( file.getContent().getLastModifiedTime() );
    }

    if ( specifyFormat && !Utils.isEmpty( dateTimeFormat ) ) {
      if ( !dateFormatSet ) {
        daf.applyPattern( dateTimeFormat );
      }
      String dt = daf.format( timestamp );
      retval += dt;
    } else {

      if ( adddate ) {
        if ( !dateFormatSet ) {
          daf.applyPattern( "yyyyMMdd" );
        }
        String d = daf.format( timestamp );
        retval += "_" + d;
      }
      if ( addtime ) {
        if ( !dateFormatSet ) {
          daf.applyPattern( "HHmmssSSS" );
        }
        String t = daf.format( timestamp );
        retval += "_" + t;
      }
    }

    if ( daf != null ) {
      dateFormatSet = true;
    }

    retval += filename.substring( lastindexOfDot, lenstring );

    return retval;

  }

  @Override
  public void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
    ValidatorContext ctx1 = new ValidatorContext();
    AbstractFileValidator.putVariableSpace( ctx1, getVariables() );
    AndValidator.putValidators( ctx1, ActionValidatorUtils.notBlankValidator(),
      ActionValidatorUtils.fileDoesNotExistValidator() );

    ActionValidatorUtils.andValidator().validate( this, "zipFilename", remarks, ctx1 );

    if ( 2 == afterunzip ) {
      // setting says to move
      ActionValidatorUtils.andValidator().validate( this, "moveToDirectory", remarks,
        AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
    }

    ActionValidatorUtils.andValidator().validate( this, "sourceDirectory", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );

  }

}
