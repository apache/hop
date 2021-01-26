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

package org.apache.hop.workflow.actions.movefiles;

import org.apache.commons.vfs2.AllFileSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.validator.AbstractFileValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ValidatorContext;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.w3c.dom.Node;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This defines a 'move files' action.
 *
 * @author Samatar Hassan
 * @since 25-02-2008
 */

@Action(
  id = "MOVE_FILES",
  name = "i18n::ActionMoveFiles.Name",
  description = "i18n::ActionMoveFiles.Description",
  image = "MoveFiles.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
  documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/movefiles.html"
)
public class ActionMoveFiles extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionMoveFiles.class; // For Translator

  public boolean moveEmptyFolders;
  public boolean argFromPrevious;
  public boolean includeSubfolders;
  public boolean addResultFilenames;
  public boolean destinationIsAFile;
  public boolean createDestinationFolder;
  public String[] sourceFileFolder;
  public String[] destinationFileFolder;
  public String[] wildcard;
  private String nrErrorsLessThan;

  private String successCondition;
  public String SUCCESS_IF_AT_LEAST_X_FILES_UN_ZIPPED = "success_when_at_least";
  public String SUCCESS_IF_ERRORS_LESS = "success_if_errors_less";
  public String SUCCESS_IF_NO_ERRORS = "success_if_no_errors";

  private boolean addDate;
  private boolean addTime;
  private boolean specifyFormat;
  private String dateTimeFormat;
  private boolean addDateBeforeExtension;
  private boolean DoNotKeepFolderStructure;
  private String ifFileExists;
  private String destinationFolder;
  private String ifMovedFileExists;
  private String movedDateTimeFormat;
  private boolean AddMovedDateBeforeExtension;
  private boolean addMovedDate;
  private boolean addMovedTime;
  private boolean SpecifyMoveFormat;
  public boolean createMoveToFolder;
  public boolean simulate;

  int nrErrors = 0;
  int nrSuccess = 0;
  boolean successConditionBroken = false;
  boolean successConditionBrokenExit = false;
  int limitFiles = 0;

  public ActionMoveFiles( String n ) {
    super( n, "" );
    simulate = false;
    createMoveToFolder = false;
    SpecifyMoveFormat = false;
    addMovedDate = false;
    addMovedTime = false;
    AddMovedDateBeforeExtension = false;
    movedDateTimeFormat = null;
    ifMovedFileExists = "do_nothing";
    destinationFolder = null;
    DoNotKeepFolderStructure = false;
    moveEmptyFolders = true;
    argFromPrevious = false;
    sourceFileFolder = null;
    destinationFileFolder = null;
    wildcard = null;
    includeSubfolders = false;
    addResultFilenames = false;
    destinationIsAFile = false;
    createDestinationFolder = false;
    nrErrorsLessThan = "10";
    successCondition = SUCCESS_IF_NO_ERRORS;
    addDate = false;
    addTime = false;
    specifyFormat = false;
    dateTimeFormat = null;
    addDateBeforeExtension = false;
    ifFileExists = "do_nothing";
  }

  public ActionMoveFiles() {
    this( "" );
  }

  public void allocate( int nrFields ) {
    sourceFileFolder = new String[ nrFields ];
    destinationFileFolder = new String[ nrFields ];
    wildcard = new String[ nrFields ];
  }

  public Object clone() {
    ActionMoveFiles je = (ActionMoveFiles) super.clone();
    if ( sourceFileFolder != null ) {
      int nrFields = sourceFileFolder.length;
      je.allocate( nrFields );
      System.arraycopy( sourceFileFolder, 0, je.sourceFileFolder, 0, nrFields );
      System.arraycopy( wildcard, 0, je.wildcard, 0, nrFields );
      System.arraycopy( destinationFileFolder, 0, je.destinationFileFolder, 0, nrFields );
    }
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 600 );

    retval.append( super.getXml() );
    retval.append( "      " ).append( XmlHandler.addTagValue( "move_empty_folders", moveEmptyFolders ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "arg_from_previous", argFromPrevious ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "include_subfolders", includeSubfolders ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "add_result_filesname", addResultFilenames ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "destination_is_a_file", destinationIsAFile ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "create_destination_folder", createDestinationFolder ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "add_date", addDate ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "add_time", addTime ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "SpecifyFormat", specifyFormat ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "date_time_format", dateTimeFormat ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "nr_errors_less_than", nrErrorsLessThan ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "success_condition", successCondition ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "AddDateBeforeExtension", addDateBeforeExtension ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "DoNotKeepFolderStructure", DoNotKeepFolderStructure ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "iffileexists", ifFileExists ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "destinationFolder", destinationFolder ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "ifmovedfileexists", ifMovedFileExists ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "moved_date_time_format", movedDateTimeFormat ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "create_move_to_folder", createMoveToFolder ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "add_moved_date", addMovedDate ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "add_moved_time", addMovedTime ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "SpecifyMoveFormat", SpecifyMoveFormat ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "AddMovedDateBeforeExtension", AddMovedDateBeforeExtension ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "simulate", simulate ) );

    retval.append( "      <fields>" ).append( Const.CR );
    if ( sourceFileFolder != null ) {
      for ( int i = 0; i < sourceFileFolder.length; i++ ) {
        retval.append( "        <field>" ).append( Const.CR );
        retval.append( "          " ).append( XmlHandler.addTagValue( "source_filefolder", sourceFileFolder[ i ] ) );
        retval.append( "          " ).append(
          XmlHandler.addTagValue( "destination_filefolder", destinationFileFolder[ i ] ) );
        retval.append( "          " ).append( XmlHandler.addTagValue( "wildcard", wildcard[ i ] ) );
        retval.append( "        </field>" ).append( Const.CR );
      }
    }
    retval.append( "      </fields>" ).append( Const.CR );

    return retval.toString();
  }

  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider, IVariables variables ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      moveEmptyFolders = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "move_empty_folders" ) );
      argFromPrevious = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "arg_from_previous" ) );
      includeSubfolders = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "include_subfolders" ) );
      addResultFilenames = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "add_result_filesname" ) );
      destinationIsAFile = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "destination_is_a_file" ) );
      createDestinationFolder =
        "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "create_destination_folder" ) );
      addDate = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "add_date" ) );
      addTime = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "add_time" ) );
      specifyFormat = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "SpecifyFormat" ) );
      addDateBeforeExtension =
        "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "AddDateBeforeExtension" ) );
      DoNotKeepFolderStructure =
        "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "DoNotKeepFolderStructure" ) );
      dateTimeFormat = XmlHandler.getTagValue( entrynode, "date_time_format" );
      nrErrorsLessThan = XmlHandler.getTagValue( entrynode, "nr_errors_less_than" );
      successCondition = XmlHandler.getTagValue( entrynode, "success_condition" );
      ifFileExists = XmlHandler.getTagValue( entrynode, "iffileexists" );
      destinationFolder = XmlHandler.getTagValue( entrynode, "destinationFolder" );
      ifMovedFileExists = XmlHandler.getTagValue( entrynode, "ifmovedfileexists" );
      movedDateTimeFormat = XmlHandler.getTagValue( entrynode, "moved_date_time_format" );
      AddMovedDateBeforeExtension =
        "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "AddMovedDateBeforeExtension" ) );
      createMoveToFolder = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "create_move_to_folder" ) );
      addMovedDate = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "add_moved_date" ) );
      addMovedTime = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "add_moved_time" ) );
      SpecifyMoveFormat = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "SpecifyMoveFormat" ) );
      simulate = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "simulate" ) );

      Node fields = XmlHandler.getSubNode( entrynode, "fields" );

      // How many field arguments?
      int nrFields = XmlHandler.countNodes( fields, "field" );
      allocate( nrFields );

      // Read them all...
      for ( int i = 0; i < nrFields; i++ ) {
        Node fnode = XmlHandler.getSubNodeByNr( fields, "field", i );

        sourceFileFolder[ i ] = XmlHandler.getTagValue( fnode, "source_filefolder" );
        destinationFileFolder[ i ] = XmlHandler.getTagValue( fnode, "destination_filefolder" );
        wildcard[ i ] = XmlHandler.getTagValue( fnode, "wildcard" );
      }
    } catch ( HopXmlException xe ) {

      throw new HopXmlException(
        BaseMessages.getString( PKG, "JobMoveFiles.Error.Exception.UnableLoadXML" ), xe );
    }
  }

  public Result execute( Result previousResult, int nr ) throws HopException {
    Result result = previousResult;
    List<RowMetaAndData> rows = result.getRows();
    RowMetaAndData resultRow = null;
    result.setNrErrors( 1 );
    result.setResult( false );

    nrErrors = 0;
    nrSuccess = 0;
    successConditionBroken = false;
    successConditionBrokenExit = false;
    limitFiles = Const.toInt( resolve( getNrErrorsLessThan() ), 10 );

    if ( log.isDetailed() ) {
      if ( simulate ) {
        logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.SimulationOn" ) );
      }
      if ( includeSubfolders ) {
        logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.IncludeSubFoldersOn" ) );
      }
    }

    String MoveToFolder = resolve( destinationFolder );
    // Get source and destination files, also wildcard
    String[] vSourceFileFolder = sourceFileFolder;
    String[] vDestinationFileFolder = destinationFileFolder;
    String[] vwildcard = wildcard;

    if ( ifFileExists.equals( "move_file" ) ) {
      if ( Utils.isEmpty( MoveToFolder ) ) {
        logError( BaseMessages.getString( PKG, "JobMoveFiles.Log.Error.MoveToFolderMissing" ) );
        return result;
      }
      FileObject folder = null;
      try {
        folder = HopVfs.getFileObject( MoveToFolder );
        if ( !folder.exists() ) {
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.Error.FolderMissing", MoveToFolder ) );
          }
          if ( createMoveToFolder ) {
            folder.createFolder();
          } else {
            logError( BaseMessages.getString( PKG, "JobMoveFiles.Log.Error.FolderMissing", MoveToFolder ) );
            return result;
          }
        }
        if ( !folder.getType().equals( FileType.FOLDER ) ) {
          logError( BaseMessages.getString( PKG, "JobMoveFiles.Log.Error.NotFolder", MoveToFolder ) );
          return result;
        }
      } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "JobMoveFiles.Log.Error.GettingMoveToFolder", MoveToFolder, e
          .getMessage() ) );
        return result;
      } finally {
        if ( folder != null ) {
          try {
            folder.close();
          } catch ( IOException ex ) { /* Ignore */
          }
        }
      }
    }

    if ( argFromPrevious ) {
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.ArgFromPrevious.Found", ( rows != null ? rows
          .size() : 0 )
          + "" ) );
      }
    }
    if ( argFromPrevious && rows != null ) {
      for ( int iteration = 0; iteration < rows.size() && !parentWorkflow.isStopped(); iteration++ ) {
        // Success condition broken?
        if ( successConditionBroken ) {
          if ( !successConditionBrokenExit ) {
            logError( BaseMessages.getString( PKG, "JobMoveFiles.Error.SuccessConditionbroken", "" + nrErrors ) );
            successConditionBrokenExit = true;
          }
          result.setNrErrors( nrErrors );
          displayResults();
          return result;
        }

        resultRow = rows.get( iteration );

        // Get source and destination file names, also wildcard
        String vSourceFileFolderPrevious = resultRow.getString( 0, null );
        String vDestinationFileFolderPrevious = resultRow.getString( 1, null );
        String vWildcardPrevious = resultRow.getString( 2, null );

        if ( !Utils.isEmpty( vSourceFileFolderPrevious ) && !Utils.isEmpty( vDestinationFileFolderPrevious ) ) {
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString(
              PKG, "JobMoveFiles.Log.ProcessingRow", vSourceFileFolderPrevious,
              vDestinationFileFolderPrevious, vWildcardPrevious ) );
          }

          if ( !ProcessFileFolder(
            vSourceFileFolderPrevious, vDestinationFileFolderPrevious, vWildcardPrevious, parentWorkflow, result,
            MoveToFolder ) ) {
            // The move process fail
            // Update Errors
            updateErrors();
          }
        } else {
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString(
              PKG, "JobMoveFiles.Log.IgnoringRow", vSourceFileFolder[ iteration ],
              vDestinationFileFolder[ iteration ], vwildcard[ iteration ] ) );
          }
        }
      }
    } else if ( vSourceFileFolder != null && vDestinationFileFolder != null ) {
      for ( int i = 0; i < vSourceFileFolder.length && !parentWorkflow.isStopped(); i++ ) {
        // Success condition broken?
        if ( successConditionBroken ) {
          if ( !successConditionBrokenExit ) {
            logError( BaseMessages.getString( PKG, "JobMoveFiles.Error.SuccessConditionbroken", "" + nrErrors ) );
            successConditionBrokenExit = true;
          }
          result.setNrErrors( nrErrors );
          displayResults();
          return result;
        }

        if ( !Utils.isEmpty( vSourceFileFolder[ i ] ) && !Utils.isEmpty( vDestinationFileFolder[ i ] ) ) {
          // ok we can process this file/folder
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString(
              PKG, "JobMoveFiles.Log.ProcessingRow", vSourceFileFolder[ i ], vDestinationFileFolder[ i ],
              vwildcard[ i ] ) );
          }

          if ( !ProcessFileFolder(
            vSourceFileFolder[ i ], vDestinationFileFolder[ i ], vwildcard[ i ], parentWorkflow, result, MoveToFolder ) ) {
            // Update Errors
            updateErrors();
          }
        } else {
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages
              .getString(
                PKG, "JobMoveFiles.Log.IgnoringRow", vSourceFileFolder[ i ], vDestinationFileFolder[ i ],
                vwildcard[ i ] ) );
          }
        }
      }
    }

    // Success Condition
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
      logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.Info.FilesInError", "" + nrErrors ) );
      logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.Info.FilesInSuccess", "" + nrSuccess ) );
      logDetailed( "=======================================" );
    }
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

  private boolean ProcessFileFolder( String sourcefilefoldername, String destinationfilefoldername,
                                     String wildcard, IWorkflowEngine<WorkflowMeta> parentWorkflow, Result result, String MoveToFolder ) {
    boolean entrystatus = false;
    FileObject sourcefilefolder = null;
    FileObject destinationfilefolder = null;
    FileObject movetofolderfolder = null;
    FileObject Currentfile = null;

    // Get real source, destination file and wildcard
    String realSourceFilefoldername = resolve( sourcefilefoldername );
    String realDestinationFilefoldername = resolve( destinationfilefoldername );
    String realWildcard = resolve( wildcard );

    try {
      sourcefilefolder = HopVfs.getFileObject( realSourceFilefoldername );
      destinationfilefolder = HopVfs.getFileObject( realDestinationFilefoldername );
      if ( !Utils.isEmpty( MoveToFolder ) ) {
        movetofolderfolder = HopVfs.getFileObject( MoveToFolder );
      }

      if ( sourcefilefolder.exists() ) {

        // Check if destination folder/parent folder exists !
        // If user wanted and if destination folder does not exist
        // PDI will create it
        if ( createDestinationFolder( destinationfilefolder ) ) {

          // Basic Tests
          if ( sourcefilefolder.getType().equals( FileType.FOLDER ) && destinationIsAFile ) {
            // Source is a folder, destination is a file
            // WARNING !!! CAN NOT MOVE FOLDER TO FILE !!!

            log.logError( BaseMessages.getString( PKG, "JobMoveFiles.Log.Forbidden" ), BaseMessages.getString(
              PKG, "JobMoveFiles.Log.CanNotMoveFolderToFile", realSourceFilefoldername,
              realDestinationFilefoldername ) );

            // Update Errors
            updateErrors();
          } else {
            if ( destinationfilefolder.getType().equals( FileType.FOLDER )
              && sourcefilefolder.getType().equals( FileType.FILE ) ) {
              // Source is a file, destination is a folder
              // return destination short filename
              String shortfilename = sourcefilefolder.getName().getBaseName();

              try {
                shortfilename = getDestinationFilename( shortfilename );
              } catch ( Exception e ) {
                logError( BaseMessages.getString( PKG, BaseMessages.getString(
                  PKG, "JobMoveFiles.Error.GettingFilename", sourcefilefolder.getName().getBaseName(), e
                    .toString() ) ) );
                return entrystatus;
              }
              // Move the file to the destination folder

              String destinationfilenamefull =
                HopVfs.getFilename( destinationfilefolder ) + Const.FILE_SEPARATOR + shortfilename;
              FileObject destinationfile = HopVfs.getFileObject( destinationfilenamefull );

              entrystatus = MoveFile( shortfilename, sourcefilefolder, destinationfile, movetofolderfolder, parentWorkflow, result );
              return entrystatus;
            } else if ( sourcefilefolder.getType().equals( FileType.FILE ) && destinationIsAFile ) {
              // Source is a file, destination is a file

              FileObject destinationfile = HopVfs.getFileObject( realDestinationFilefoldername );

              // return destination short filename
              String shortfilename = destinationfile.getName().getBaseName();
              try {
                shortfilename = getDestinationFilename( shortfilename );
              } catch ( Exception e ) {
                logError( BaseMessages.getString( PKG, BaseMessages.getString(
                  PKG, "JobMoveFiles.Error.GettingFilename", sourcefilefolder.getName().getBaseName(), e
                    .toString() ) ) );
                return entrystatus;
              }

              String destinationfilenamefull =
                HopVfs.getFilename( destinationfile.getParent() ) + Const.FILE_SEPARATOR + shortfilename;
              destinationfile = HopVfs.getFileObject( destinationfilenamefull );

              entrystatus =
                MoveFile(
                  shortfilename, sourcefilefolder, destinationfile, movetofolderfolder, parentWorkflow, result );
              return entrystatus;
            } else {
              // Both source and destination are folders
              if ( log.isDetailed() ) {
                logDetailed( "  " );
                logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.FetchFolder", sourcefilefolder
                  .toString() ) );
              }

              FileObject[] fileObjects = sourcefilefolder.findFiles( new AllFileSelector() {
                public boolean traverseDescendents( FileSelectInfo info ) {
                  return true;
                }

                public boolean includeFile( FileSelectInfo info ) {
                  FileObject fileObject = info.getFile();
                  try {
                    if ( fileObject == null ) {
                      return false;
                    }
                  } catch ( Exception ex ) {
                    // Upon error don't process the file.
                    return false;
                  } finally {
                    if ( fileObject != null ) {
                      try {
                        fileObject.close();
                      } catch ( IOException ex ) { /* Ignore */
                      }
                    }

                  }
                  return true;
                }
              } );

              if ( fileObjects != null ) {
                for ( int j = 0; j < fileObjects.length && !parentWorkflow.isStopped(); j++ ) {
                  // Success condition broken?
                  if ( successConditionBroken ) {
                    if ( !successConditionBrokenExit ) {
                      logError( BaseMessages.getString( PKG, "JobMoveFiles.Error.SuccessConditionbroken", ""
                        + nrErrors ) );
                      successConditionBrokenExit = true;
                    }
                    return false;
                  }
                  // Fetch files in list one after one ...
                  Currentfile = fileObjects[ j ];

                  if ( !MoveOneFile(
                    Currentfile, sourcefilefolder, realDestinationFilefoldername, realWildcard, parentWorkflow,
                    result, movetofolderfolder ) ) {
                    // Update Errors
                    updateErrors();
                  }

                }
              }
            }

          }
          entrystatus = true;
        } else {
          // Destination Folder or Parent folder is missing
          logError( BaseMessages.getString(
            PKG, "JobMoveFiles.Error.DestinationFolderNotFound", realDestinationFilefoldername ) );
        }
      } else {
        logError( BaseMessages.getString( PKG, "JobMoveFiles.Error.SourceFileNotExists", realSourceFilefoldername ) );
      }
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "JobMoveFiles.Error.Exception.MoveProcess", realSourceFilefoldername
        .toString(), destinationfilefolder.toString(), e.getMessage() ) );
    } finally {
      if ( sourcefilefolder != null ) {
        try {
          sourcefilefolder.close();
        } catch ( IOException ex ) {
          /* Ignore */
        }
      }
      if ( destinationfilefolder != null ) {
        try {
          destinationfilefolder.close();
        } catch ( IOException ex ) {
          /* Ignore */
        }
      }
      if ( Currentfile != null ) {
        try {
          Currentfile.close();
        } catch ( IOException ex ) {
          /* Ignore */
        }
      }
      if ( movetofolderfolder != null ) {
        try {
          movetofolderfolder.close();
        } catch ( IOException ex ) {
          /* Ignore */
        }
      }
    }
    return entrystatus;
  }

  private boolean MoveFile( String shortfilename, FileObject sourcefilename, FileObject destinationfilename,
                            FileObject movetofolderfolder, IWorkflowEngine<WorkflowMeta> parentWorkflow, Result result ) {

    FileObject destinationfile = null;
    boolean retval = false;
    try {
      if ( !destinationfilename.exists() ) {
        if ( !simulate ) {
          sourcefilename.moveTo( destinationfilename );
        }
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.FileMoved", sourcefilename
            .getName().toString(), destinationfilename.getName().toString() ) );
        }

        // add filename to result filename
        if ( addResultFilenames && !ifFileExists.equals( "fail" ) && !ifFileExists.equals( "do_nothing" ) ) {
          addFileToResultFilenames( destinationfilename.toString(), result, parentWorkflow );
        }

        updateSuccess();
        retval = true;

      } else {
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.FileExists", destinationfilename.toString() ) );
        }
        if ( ifFileExists.equals( "overwrite_file" ) ) {
          if ( !simulate ) {
            sourcefilename.moveTo( destinationfilename );
          }
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.FileOverwrite", destinationfilename
              .getName().toString() ) );
          }

          // add filename to result filename
          if ( addResultFilenames && !ifFileExists.equals( "fail" ) && !ifFileExists.equals( "do_nothing" ) ) {
            addFileToResultFilenames( destinationfilename.toString(), result, parentWorkflow );
          }

          updateSuccess();
          retval = true;

        } else if ( ifFileExists.equals( "unique_name" ) ) {
          String shortFilename = shortfilename;

          // return destination short filename
          try {
            shortFilename = getMoveDestinationFilename( shortFilename, "ddMMyyyy_HHmmssSSS" );
          } catch ( Exception e ) {
            logError( BaseMessages.getString( PKG, BaseMessages.getString(
              PKG, "JobMoveFiles.Error.GettingFilename", shortFilename ) ), e );
            return retval;
          }

          String movetofilenamefull =
            destinationfilename.getParent().toString() + Const.FILE_SEPARATOR + shortFilename;
          destinationfile = HopVfs.getFileObject( movetofilenamefull );

          if ( !simulate ) {
            sourcefilename.moveTo( destinationfile );
          }
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.FileMoved", sourcefilename
              .getName().toString(), destinationfile.getName().toString() ) );
          }

          // add filename to result filename
          if ( addResultFilenames && !ifFileExists.equals( "fail" ) && !ifFileExists.equals( "do_nothing" ) ) {
            addFileToResultFilenames( destinationfile.toString(), result, parentWorkflow );
          }

          updateSuccess();
          retval = true;
        } else if ( ifFileExists.equals( "delete_file" ) ) {
          if ( !simulate ) {
            sourcefilename.delete();
          }
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.FileDeleted", destinationfilename
              .getName().toString() ) );
          }
          updateSuccess();
          retval = true;
        } else if ( ifFileExists.equals( "move_file" ) ) {
          String shortFilename = shortfilename;
          // return destination short filename
          try {
            shortFilename = getMoveDestinationFilename( shortFilename, null );
          } catch ( Exception e ) {
            logError( BaseMessages.getString( PKG, BaseMessages.getString(
              PKG, "JobMoveFiles.Error.GettingFilename", shortFilename ) ), e );
            return retval;
          }

          String movetofilenamefull = movetofolderfolder.toString() + Const.FILE_SEPARATOR + shortFilename;
          destinationfile = HopVfs.getFileObject( movetofilenamefull );
          if ( !destinationfile.exists() ) {
            if ( !simulate ) {
              sourcefilename.moveTo( destinationfile );
            }
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.FileMoved", sourcefilename
                .getName().toString(), destinationfile.getName().toString() ) );
            }

            // add filename to result filename
            if ( addResultFilenames && !ifFileExists.equals( "fail" ) && !ifFileExists.equals( "do_nothing" ) ) {
              addFileToResultFilenames( destinationfile.toString(), result, parentWorkflow );
            }

          } else {
            if ( ifMovedFileExists.equals( "overwrite_file" ) ) {
              if ( !simulate ) {
                sourcefilename.moveTo( destinationfile );
              }
              if ( log.isDetailed() ) {
                logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.FileOverwrite", destinationfile
                  .getName().toString() ) );
              }

              // add filename to result filename
              if ( addResultFilenames && !ifFileExists.equals( "fail" ) && !ifFileExists.equals( "do_nothing" ) ) {
                addFileToResultFilenames( destinationfile.toString(), result, parentWorkflow );
              }

              updateSuccess();
              retval = true;
            } else if ( ifMovedFileExists.equals( "unique_name" ) ) {
              SimpleDateFormat daf = new SimpleDateFormat();
              Date now = new Date();
              daf.applyPattern( "ddMMyyyy_HHmmssSSS" );
              String dt = daf.format( now );
              shortFilename += "_" + dt;

              String destinationfilenamefull =
                movetofolderfolder.toString() + Const.FILE_SEPARATOR + shortFilename;
              destinationfile = HopVfs.getFileObject( destinationfilenamefull );

              if ( !simulate ) {
                sourcefilename.moveTo( destinationfile );
              }
              if ( log.isDetailed() ) {
                logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.FileMoved", destinationfile
                  .getName().toString() ) );
              }

              // add filename to result filename
              if ( addResultFilenames && !ifFileExists.equals( "fail" ) && !ifFileExists.equals( "do_nothing" ) ) {
                addFileToResultFilenames( destinationfile.toString(), result, parentWorkflow );
              }

              updateSuccess();
              retval = true;
            } else if ( ifMovedFileExists.equals( "fail" ) ) {
              // Update Errors
              updateErrors();
            }
          }

        } else if ( ifFileExists.equals( "fail" ) ) {
          // Update Errors
          updateErrors();
        }
      }
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "JobMoveFiles.Error.Exception.MoveProcessError", sourcefilename
        .toString(), destinationfilename.toString(), e.getMessage() ) );
      updateErrors();
    } finally {
      if ( destinationfile != null ) {
        try {
          destinationfile.close();
        } catch ( IOException ex ) { /* Ignore */
        }
      }
    }
    return retval;
  }

  private boolean MoveOneFile( FileObject Currentfile, FileObject sourcefilefolder,
                               String realDestinationFilefoldername, String realWildcard, IWorkflowEngine<WorkflowMeta> parentWorkflow, Result result,
                               FileObject movetofolderfolder ) {
    boolean entrystatus = false;
    FileObject filename = null;

    try {
      if ( !Currentfile.toString().equals( sourcefilefolder.toString() ) ) {
        // Pass over the Base folder itself

        // return destination short filename
        String sourceshortfilename = Currentfile.getName().getBaseName();
        String shortfilename = sourceshortfilename;
        try {
          shortfilename = getDestinationFilename( sourceshortfilename );
        } catch ( Exception e ) {
          logError( BaseMessages.getString( PKG, BaseMessages.getString(
            PKG, "JobMoveFiles.Error.GettingFilename", Currentfile.getName().getBaseName(), e.toString() ) ) );
          return entrystatus;
        }

        int lenCurrent = sourceshortfilename.length();
        String shortFilenameFromBaseFolder = shortfilename;
        if ( !isDoNotKeepFolderStructure() ) {
          shortFilenameFromBaseFolder =
            Currentfile.toString().substring(
              sourcefilefolder.toString().length(), Currentfile.toString().length() );
        }
        shortFilenameFromBaseFolder =
          shortFilenameFromBaseFolder.substring( 0, shortFilenameFromBaseFolder.length() - lenCurrent )
            + shortfilename;

        // Built destination filename
        filename =
          HopVfs.getFileObject( realDestinationFilefoldername
            + Const.FILE_SEPARATOR + shortFilenameFromBaseFolder );

        if ( !Currentfile.getParent().toString().equals( sourcefilefolder.toString() ) ) {

          // Not in the Base Folder..Only if include sub folders
          if ( includeSubfolders ) {
            // Folders..only if include subfolders
            if ( Currentfile.getType() == FileType.FOLDER ) {
              if ( includeSubfolders && moveEmptyFolders && Utils.isEmpty( wildcard ) ) {
                entrystatus =
                  MoveFile( shortfilename, Currentfile, filename, movetofolderfolder, parentWorkflow, result );
              }
            } else {

              if ( GetFileWildcard( sourceshortfilename, realWildcard ) ) {
                entrystatus =
                  MoveFile( shortfilename, Currentfile, filename, movetofolderfolder, parentWorkflow, result );
              }
            }
          }
        } else {
          // In the Base Folder...
          // Folders..only if include subfolders
          if ( Currentfile.getType() == FileType.FOLDER ) {
            if ( includeSubfolders && moveEmptyFolders && Utils.isEmpty( wildcard ) ) {
              entrystatus =
                MoveFile( shortfilename, Currentfile, filename, movetofolderfolder, parentWorkflow, result );
            }
          } else {

            // file...Check if exists
            if ( GetFileWildcard( sourceshortfilename, realWildcard ) ) {
              entrystatus =
                MoveFile( shortfilename, Currentfile, filename, movetofolderfolder, parentWorkflow, result );

            }
          }

        }

      }
      entrystatus = true;

    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "JobMoveFiles.Log.Error", e.toString() ) );
    } finally {
      if ( filename != null ) {
        try {
          filename.close();

        } catch ( IOException ex ) { /* Ignore */
        }
      }

    }
    return entrystatus;
  }

  private void updateErrors() {
    nrErrors++;
    if ( checkIfSuccessConditionBroken() ) {
      // Success condition was broken
      successConditionBroken = true;
    }
  }

  private boolean checkIfSuccessConditionBroken() {
    boolean retval = false;
    if ( ( nrErrors > 0 && getSuccessCondition().equals( SUCCESS_IF_NO_ERRORS ) )
      || ( nrErrors >= limitFiles && getSuccessCondition().equals( SUCCESS_IF_ERRORS_LESS ) ) ) {
      retval = true;
    }
    return retval;
  }

  private void updateSuccess() {
    nrSuccess++;
  }

  private void addFileToResultFilenames( String fileaddentry, Result result, IWorkflowEngine<WorkflowMeta> parentWorkflow ) {
    try {
      ResultFile resultFile =
        new ResultFile( ResultFile.FILE_TYPE_GENERAL, HopVfs.getFileObject( fileaddentry ), parentWorkflow
          .getWorkflowName(), toString() );
      result.getResultFiles().put( resultFile.getFile().toString(), resultFile );

      if ( log.isDebug() ) {
        logDebug( " ------ " );
        logDebug( BaseMessages.getString( PKG, "JobMoveFiles.Log.FileAddedToResultFilesName", fileaddentry ) );
      }

    } catch ( Exception e ) {
      log.logError( BaseMessages.getString( PKG, "JobMoveFiles.Error.AddingToFilenameResult" ), fileaddentry
        + "" + e.getMessage() );
    }

  }

  private boolean createDestinationFolder( FileObject filefolder ) {
    FileObject folder = null;
    try {
      if ( destinationIsAFile ) {
        folder = filefolder.getParent();
      } else {
        folder = filefolder;
      }

      if ( !folder.exists() ) {
        if ( createDestinationFolder ) {
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.FolderNotExist", folder
              .getName().toString() ) );
          }
          folder.createFolder();
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.FolderWasCreated", folder
              .getName().toString() ) );
          }
        } else {
          logError( BaseMessages.getString( PKG, "JobMoveFiles.Log.FolderNotExist", folder.getName().toString() ) );
          return false;
        }
      }
      return true;
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "JobMoveFiles.Log.CanNotCreateParentFolder", folder
        .getName().toString() ), e );

    } finally {
      if ( folder != null ) {
        try {
          folder.close();
        } catch ( Exception ex ) { /* Ignore */
        }
      }
    }
    return false;
  }

  /**********************************************************
   *
   * @param selectedfile
   * @param wildcard
   * @return True if the selectedfile matches the wildcard
   **********************************************************/
  private boolean GetFileWildcard( String selectedfile, String wildcard ) {
    Pattern pattern = null;
    boolean getIt = true;

    if ( !Utils.isEmpty( wildcard ) ) {
      pattern = Pattern.compile( wildcard );
      // First see if the file matches the regular expression!
      if ( pattern != null ) {
        Matcher matcher = pattern.matcher( selectedfile );
        getIt = matcher.matches();
      }
    }

    return getIt;
  }

  private String getDestinationFilename( String shortsourcefilename ) throws Exception {
    String shortfilename = shortsourcefilename;
    int lenstring = shortsourcefilename.length();
    int lastindexOfDot = shortfilename.lastIndexOf( '.' );
    if ( lastindexOfDot == -1 ) {
      lastindexOfDot = lenstring;
    }

    if ( isAddDateBeforeExtension() ) {
      shortfilename = shortfilename.substring( 0, lastindexOfDot );
    }

    SimpleDateFormat daf = new SimpleDateFormat();
    Date now = new Date();

    if ( isSpecifyFormat() && !Utils.isEmpty( getDateTimeFormat() ) ) {
      daf.applyPattern( getDateTimeFormat() );
      String dt = daf.format( now );
      shortfilename += dt;
    } else {
      if ( isAddDate() ) {
        daf.applyPattern( "yyyyMMdd" );
        String d = daf.format( now );
        shortfilename += "_" + d;
      }
      if ( isAddTime() ) {
        daf.applyPattern( "HHmmssSSS" );
        String t = daf.format( now );
        shortfilename += "_" + t;
      }
    }
    if ( isAddDateBeforeExtension() ) {
      shortfilename += shortsourcefilename.substring( lastindexOfDot, lenstring );
    }

    return shortfilename;
  }

  private String getMoveDestinationFilename( String shortsourcefilename, String DateFormat ) throws Exception {
    String shortfilename = shortsourcefilename;
    int lenstring = shortsourcefilename.length();
    int lastindexOfDot = shortfilename.lastIndexOf( '.' );
    if ( lastindexOfDot == -1 ) {
      lastindexOfDot = lenstring;
    }

    if ( isAddMovedDateBeforeExtension() ) {
      shortfilename = shortfilename.substring( 0, lastindexOfDot );
    }

    SimpleDateFormat daf = new SimpleDateFormat();
    Date now = new Date();

    if ( DateFormat != null ) {
      daf.applyPattern( DateFormat );
      String dt = daf.format( now );
      shortfilename += dt;
    } else {

      if ( isSpecifyMoveFormat() && !Utils.isEmpty( getMovedDateTimeFormat() ) ) {
        daf.applyPattern( getMovedDateTimeFormat() );
        String dt = daf.format( now );
        shortfilename += dt;
      } else {
        if ( isAddMovedDate() ) {
          daf.applyPattern( "yyyyMMdd" );
          String d = daf.format( now );
          shortfilename += "_" + d;
        }
        if ( isAddMovedTime() ) {
          daf.applyPattern( "HHmmssSSS" );
          String t = daf.format( now );
          shortfilename += "_" + t;
        }
      }
    }
    if ( isAddMovedDateBeforeExtension() ) {
      shortfilename += shortsourcefilename.substring( lastindexOfDot, lenstring );
    }

    return shortfilename;
  }

  public void setAddDate( boolean adddate ) {
    this.addDate = adddate;
  }

  public boolean isAddDate() {
    return addDate;
  }

  public boolean isAddMovedDate() {
    return addMovedDate;
  }

  public void setAddMovedDate( boolean addMovedDate ) {
    this.addMovedDate = addMovedDate;
  }

  public boolean isAddMovedTime() {
    return addMovedTime;
  }

  public void setAddMovedTime( boolean addMovedTime ) {
    this.addMovedTime = addMovedTime;
  }

  public void setIfFileExists( String ifFileExists ) {
    this.ifFileExists = ifFileExists;
  }

  public String getIfFileExists() {
    return ifFileExists;
  }

  public void setIfMovedFileExists( String ifMovedFileExists ) {
    this.ifMovedFileExists = ifMovedFileExists;
  }

  public String getIfMovedFileExists() {
    return ifMovedFileExists;
  }

  public void setAddTime( boolean addtime ) {
    this.addTime = addtime;
  }

  public boolean isAddTime() {
    return addTime;
  }

  public void setAddDateBeforeExtension( boolean addDateBeforeExtension ) {
    this.addDateBeforeExtension = addDateBeforeExtension;
  }

  public void setAddMovedDateBeforeExtension( boolean AddMovedDateBeforeExtension ) {
    this.AddMovedDateBeforeExtension = AddMovedDateBeforeExtension;
  }

  public boolean isSpecifyFormat() {
    return specifyFormat;
  }

  public void setSpecifyFormat( boolean specifyFormat ) {
    this.specifyFormat = specifyFormat;
  }

  public void setSpecifyMoveFormat( boolean SpecifyMoveFormat ) {
    this.SpecifyMoveFormat = SpecifyMoveFormat;
  }

  public boolean isSpecifyMoveFormat() {
    return SpecifyMoveFormat;
  }

  public String getDateTimeFormat() {
    return dateTimeFormat;
  }

  public void setDateTimeFormat( String dateTimeFormat ) {
    this.dateTimeFormat = dateTimeFormat;
  }

  public String getMovedDateTimeFormat() {
    return movedDateTimeFormat;
  }

  public void setMovedDateTimeFormat( String movedDateTimeFormat ) {
    this.movedDateTimeFormat = movedDateTimeFormat;
  }

  public boolean isAddDateBeforeExtension() {
    return addDateBeforeExtension;
  }

  public boolean isAddMovedDateBeforeExtension() {
    return AddMovedDateBeforeExtension;
  }

  public boolean isDoNotKeepFolderStructure() {
    return DoNotKeepFolderStructure;
  }

  public void setDestinationFolder( String destinationFolder ) {
    this.destinationFolder = destinationFolder;
  }

  public String getDestinationFolder() {
    return destinationFolder;
  }

  public void setDoNotKeepFolderStructure( boolean DoNotKeepFolderStructure ) {
    this.DoNotKeepFolderStructure = DoNotKeepFolderStructure;
  }

  public void setMoveEmptyFolders( boolean moveEmptyFolders ) {
    this.moveEmptyFolders = moveEmptyFolders;
  }

  public void setIncludeSubfolders( boolean includeSubfolders ) {
    this.includeSubfolders = includeSubfolders;
  }

  public void setAddresultfilesname( boolean addResultFilenames ) {
    this.addResultFilenames = addResultFilenames;
  }

  public void setArgFromPrevious( boolean argfrompreviousin ) {
    this.argFromPrevious = argfrompreviousin;
  }

  public void setDestinationIsAFile( boolean destinationIsAFile ) {
    this.destinationIsAFile = destinationIsAFile;
  }

  public void setCreateDestinationFolder( boolean createDestinationFolder ) {
    this.createDestinationFolder = createDestinationFolder;
  }

  public void setCreateMoveToFolder( boolean createMoveToFolder ) {
    this.createMoveToFolder = createMoveToFolder;
  }

  public void setNrErrorsLessThan( String nrErrorsLessThan ) {
    this.nrErrorsLessThan = nrErrorsLessThan;
  }

  public String getNrErrorsLessThan() {
    return nrErrorsLessThan;
  }

  public void setSimulate( boolean simulate ) {
    this.simulate = simulate;
  }

  public void setSuccessCondition( String successCondition ) {
    this.successCondition = successCondition;
  }

  public String getSuccessCondition() {
    return successCondition;
  }

  public void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
    boolean res = ActionValidatorUtils.andValidator().validate( this, "arguments", remarks, AndValidator.putValidators( ActionValidatorUtils.notNullValidator() ) );

    if ( res == false ) {
      return;
    }

    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace( ctx, getVariables() );
    AndValidator.putValidators( ctx, ActionValidatorUtils.notNullValidator(), ActionValidatorUtils.fileExistsValidator() );

    for ( int i = 0; i < sourceFileFolder.length; i++ ) {
      ActionValidatorUtils.andValidator().validate( this, "arguments[" + i + "]", remarks, ctx );
    }
  }

  @Override public boolean isEvaluation() {
    return true;
  }

}
