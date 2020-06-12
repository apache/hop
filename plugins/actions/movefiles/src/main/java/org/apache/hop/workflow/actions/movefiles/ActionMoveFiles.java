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
  i18nPackageName = "org.apache.hop.workflow.actions.movefiles",
  name = "ActionMoveFiles.Name",
  description = "ActionMoveFiles.Description",
  image = "MoveFiles.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement"
)
public class ActionMoveFiles extends ActionBase implements Cloneable, IAction {
  private static Class<?> PKG = ActionMoveFiles.class; // for i18n purposes, needed by Translator!!

  public boolean move_empty_folders;
  public boolean arg_from_previous;
  public boolean include_subfolders;
  public boolean add_result_filesname;
  public boolean destination_is_a_file;
  public boolean create_destination_folder;
  public String[] source_filefolder;
  public String[] destination_filefolder;
  public String[] wildcard;
  private String nr_errors_less_than;

  private String success_condition;
  public String SUCCESS_IF_AT_LEAST_X_FILES_UN_ZIPPED = "success_when_at_least";
  public String SUCCESS_IF_ERRORS_LESS = "success_if_errors_less";
  public String SUCCESS_IF_NO_ERRORS = "success_if_no_errors";

  private boolean add_date;
  private boolean add_time;
  private boolean SpecifyFormat;
  private String date_time_format;
  private boolean AddDateBeforeExtension;
  private boolean DoNotKeepFolderStructure;
  private String iffileexists;
  private String destinationFolder;
  private String ifmovedfileexists;
  private String moved_date_time_format;
  private boolean AddMovedDateBeforeExtension;
  private boolean add_moved_date;
  private boolean add_moved_time;
  private boolean SpecifyMoveFormat;
  public boolean create_move_to_folder;
  public boolean simulate;

  int NrErrors = 0;
  int NrSuccess = 0;
  boolean successConditionBroken = false;
  boolean successConditionBrokenExit = false;
  int limitFiles = 0;

  public ActionMoveFiles( String n ) {
    super( n, "" );
    simulate = false;
    create_move_to_folder = false;
    SpecifyMoveFormat = false;
    add_moved_date = false;
    add_moved_time = false;
    AddMovedDateBeforeExtension = false;
    moved_date_time_format = null;
    ifmovedfileexists = "do_nothing";
    destinationFolder = null;
    DoNotKeepFolderStructure = false;
    move_empty_folders = true;
    arg_from_previous = false;
    source_filefolder = null;
    destination_filefolder = null;
    wildcard = null;
    include_subfolders = false;
    add_result_filesname = false;
    destination_is_a_file = false;
    create_destination_folder = false;
    nr_errors_less_than = "10";
    success_condition = SUCCESS_IF_NO_ERRORS;
    add_date = false;
    add_time = false;
    SpecifyFormat = false;
    date_time_format = null;
    AddDateBeforeExtension = false;
    iffileexists = "do_nothing";
  }

  public ActionMoveFiles() {
    this( "" );
  }

  public void allocate( int nrFields ) {
    source_filefolder = new String[ nrFields ];
    destination_filefolder = new String[ nrFields ];
    wildcard = new String[ nrFields ];
  }

  public Object clone() {
    ActionMoveFiles je = (ActionMoveFiles) super.clone();
    if ( source_filefolder != null ) {
      int nrFields = source_filefolder.length;
      je.allocate( nrFields );
      System.arraycopy( source_filefolder, 0, je.source_filefolder, 0, nrFields );
      System.arraycopy( wildcard, 0, je.wildcard, 0, nrFields );
      System.arraycopy( destination_filefolder, 0, je.destination_filefolder, 0, nrFields );
    }
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 600 );

    retval.append( super.getXml() );
    retval.append( "      " ).append( XmlHandler.addTagValue( "move_empty_folders", move_empty_folders ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "arg_from_previous", arg_from_previous ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "include_subfolders", include_subfolders ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "add_result_filesname", add_result_filesname ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "destination_is_a_file", destination_is_a_file ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "create_destination_folder", create_destination_folder ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "add_date", add_date ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "add_time", add_time ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "SpecifyFormat", SpecifyFormat ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "date_time_format", date_time_format ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "nr_errors_less_than", nr_errors_less_than ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "success_condition", success_condition ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "AddDateBeforeExtension", AddDateBeforeExtension ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "DoNotKeepFolderStructure", DoNotKeepFolderStructure ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "iffileexists", iffileexists ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "destinationFolder", destinationFolder ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "ifmovedfileexists", ifmovedfileexists ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "moved_date_time_format", moved_date_time_format ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "create_move_to_folder", create_move_to_folder ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "add_moved_date", add_moved_date ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "add_moved_time", add_moved_time ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "SpecifyMoveFormat", SpecifyMoveFormat ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "AddMovedDateBeforeExtension", AddMovedDateBeforeExtension ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "simulate", simulate ) );

    retval.append( "      <fields>" ).append( Const.CR );
    if ( source_filefolder != null ) {
      for ( int i = 0; i < source_filefolder.length; i++ ) {
        retval.append( "        <field>" ).append( Const.CR );
        retval.append( "          " ).append( XmlHandler.addTagValue( "source_filefolder", source_filefolder[ i ] ) );
        retval.append( "          " ).append(
          XmlHandler.addTagValue( "destination_filefolder", destination_filefolder[ i ] ) );
        retval.append( "          " ).append( XmlHandler.addTagValue( "wildcard", wildcard[ i ] ) );
        retval.append( "        </field>" ).append( Const.CR );
      }
    }
    retval.append( "      </fields>" ).append( Const.CR );

    return retval.toString();
  }

  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      move_empty_folders = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "move_empty_folders" ) );
      arg_from_previous = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "arg_from_previous" ) );
      include_subfolders = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "include_subfolders" ) );
      add_result_filesname = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "add_result_filesname" ) );
      destination_is_a_file = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "destination_is_a_file" ) );
      create_destination_folder =
        "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "create_destination_folder" ) );
      add_date = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "add_date" ) );
      add_time = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "add_time" ) );
      SpecifyFormat = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "SpecifyFormat" ) );
      AddDateBeforeExtension =
        "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "AddDateBeforeExtension" ) );
      DoNotKeepFolderStructure =
        "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "DoNotKeepFolderStructure" ) );
      date_time_format = XmlHandler.getTagValue( entrynode, "date_time_format" );
      nr_errors_less_than = XmlHandler.getTagValue( entrynode, "nr_errors_less_than" );
      success_condition = XmlHandler.getTagValue( entrynode, "success_condition" );
      iffileexists = XmlHandler.getTagValue( entrynode, "iffileexists" );
      destinationFolder = XmlHandler.getTagValue( entrynode, "destinationFolder" );
      ifmovedfileexists = XmlHandler.getTagValue( entrynode, "ifmovedfileexists" );
      moved_date_time_format = XmlHandler.getTagValue( entrynode, "moved_date_time_format" );
      AddMovedDateBeforeExtension =
        "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "AddMovedDateBeforeExtension" ) );
      create_move_to_folder = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "create_move_to_folder" ) );
      add_moved_date = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "add_moved_date" ) );
      add_moved_time = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "add_moved_time" ) );
      SpecifyMoveFormat = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "SpecifyMoveFormat" ) );
      simulate = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "simulate" ) );

      Node fields = XmlHandler.getSubNode( entrynode, "fields" );

      // How many field arguments?
      int nrFields = XmlHandler.countNodes( fields, "field" );
      allocate( nrFields );

      // Read them all...
      for ( int i = 0; i < nrFields; i++ ) {
        Node fnode = XmlHandler.getSubNodeByNr( fields, "field", i );

        source_filefolder[ i ] = XmlHandler.getTagValue( fnode, "source_filefolder" );
        destination_filefolder[ i ] = XmlHandler.getTagValue( fnode, "destination_filefolder" );
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

    NrErrors = 0;
    NrSuccess = 0;
    successConditionBroken = false;
    successConditionBrokenExit = false;
    limitFiles = Const.toInt( environmentSubstitute( getNrErrorsLessThan() ), 10 );

    if ( log.isDetailed() ) {
      if ( simulate ) {
        logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.SimulationOn" ) );
      }
      if ( include_subfolders ) {
        logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.IncludeSubFoldersOn" ) );
      }
    }

    String MoveToFolder = environmentSubstitute( destinationFolder );
    // Get source and destination files, also wildcard
    String[] vsourcefilefolder = source_filefolder;
    String[] vdestinationfilefolder = destination_filefolder;
    String[] vwildcard = wildcard;

    if ( iffileexists.equals( "move_file" ) ) {
      if ( Utils.isEmpty( MoveToFolder ) ) {
        logError( BaseMessages.getString( PKG, "JobMoveFiles.Log.Error.MoveToFolderMissing" ) );
        return result;
      }
      FileObject folder = null;
      try {
        folder = HopVfs.getFileObject( MoveToFolder, this );
        if ( !folder.exists() ) {
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.Error.FolderMissing", MoveToFolder ) );
          }
          if ( create_move_to_folder ) {
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

    if ( arg_from_previous ) {
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.ArgFromPrevious.Found", ( rows != null ? rows
          .size() : 0 )
          + "" ) );
      }
    }
    if ( arg_from_previous && rows != null ) {
      for ( int iteration = 0; iteration < rows.size() && !parentWorkflow.isStopped(); iteration++ ) {
        // Success condition broken?
        if ( successConditionBroken ) {
          if ( !successConditionBrokenExit ) {
            logError( BaseMessages.getString( PKG, "JobMoveFiles.Error.SuccessConditionbroken", "" + NrErrors ) );
            successConditionBrokenExit = true;
          }
          result.setNrErrors( NrErrors );
          displayResults();
          return result;
        }

        resultRow = rows.get( iteration );

        // Get source and destination file names, also wildcard
        String vsourcefilefolder_previous = resultRow.getString( 0, null );
        String vdestinationfilefolder_previous = resultRow.getString( 1, null );
        String vwildcard_previous = resultRow.getString( 2, null );

        if ( !Utils.isEmpty( vsourcefilefolder_previous ) && !Utils.isEmpty( vdestinationfilefolder_previous ) ) {
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString(
              PKG, "JobMoveFiles.Log.ProcessingRow", vsourcefilefolder_previous,
              vdestinationfilefolder_previous, vwildcard_previous ) );
          }

          if ( !ProcessFileFolder(
            vsourcefilefolder_previous, vdestinationfilefolder_previous, vwildcard_previous, parentWorkflow, result,
            MoveToFolder ) ) {
            // The move process fail
            // Update Errors
            updateErrors();
          }
        } else {
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString(
              PKG, "JobMoveFiles.Log.IgnoringRow", vsourcefilefolder[ iteration ],
              vdestinationfilefolder[ iteration ], vwildcard[ iteration ] ) );
          }
        }
      }
    } else if ( vsourcefilefolder != null && vdestinationfilefolder != null ) {
      for ( int i = 0; i < vsourcefilefolder.length && !parentWorkflow.isStopped(); i++ ) {
        // Success condition broken?
        if ( successConditionBroken ) {
          if ( !successConditionBrokenExit ) {
            logError( BaseMessages.getString( PKG, "JobMoveFiles.Error.SuccessConditionbroken", "" + NrErrors ) );
            successConditionBrokenExit = true;
          }
          result.setNrErrors( NrErrors );
          displayResults();
          return result;
        }

        if ( !Utils.isEmpty( vsourcefilefolder[ i ] ) && !Utils.isEmpty( vdestinationfilefolder[ i ] ) ) {
          // ok we can process this file/folder
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString(
              PKG, "JobMoveFiles.Log.ProcessingRow", vsourcefilefolder[ i ], vdestinationfilefolder[ i ],
              vwildcard[ i ] ) );
          }

          if ( !ProcessFileFolder(
            vsourcefilefolder[ i ], vdestinationfilefolder[ i ], vwildcard[ i ], parentWorkflow, result, MoveToFolder ) ) {
            // Update Errors
            updateErrors();
          }
        } else {
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages
              .getString(
                PKG, "JobMoveFiles.Log.IgnoringRow", vsourcefilefolder[ i ], vdestinationfilefolder[ i ],
                vwildcard[ i ] ) );
          }
        }
      }
    }

    // Success Condition
    result.setNrErrors( NrErrors );
    result.setNrLinesWritten( NrSuccess );
    if ( getSuccessStatus() ) {
      result.setResult( true );
    }

    displayResults();

    return result;
  }

  private void displayResults() {
    if ( log.isDetailed() ) {
      logDetailed( "=======================================" );
      logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.Info.FilesInError", "" + NrErrors ) );
      logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.Info.FilesInSuccess", "" + NrSuccess ) );
      logDetailed( "=======================================" );
    }
  }

  private boolean getSuccessStatus() {
    boolean retval = false;

    if ( ( NrErrors == 0 && getSuccessCondition().equals( SUCCESS_IF_NO_ERRORS ) )
      || ( NrSuccess >= limitFiles && getSuccessCondition().equals( SUCCESS_IF_AT_LEAST_X_FILES_UN_ZIPPED ) )
      || ( NrErrors <= limitFiles && getSuccessCondition().equals( SUCCESS_IF_ERRORS_LESS ) ) ) {
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
    String realSourceFilefoldername = environmentSubstitute( sourcefilefoldername );
    String realDestinationFilefoldername = environmentSubstitute( destinationfilefoldername );
    String realWildcard = environmentSubstitute( wildcard );

    try {
      sourcefilefolder = HopVfs.getFileObject( realSourceFilefoldername, this );
      destinationfilefolder = HopVfs.getFileObject( realDestinationFilefoldername, this );
      if ( !Utils.isEmpty( MoveToFolder ) ) {
        movetofolderfolder = HopVfs.getFileObject( MoveToFolder, this );
      }

      if ( sourcefilefolder.exists() ) {

        // Check if destination folder/parent folder exists !
        // If user wanted and if destination folder does not exist
        // PDI will create it
        if ( CreateDestinationFolder( destinationfilefolder ) ) {

          // Basic Tests
          if ( sourcefilefolder.getType().equals( FileType.FOLDER ) && destination_is_a_file ) {
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
              FileObject destinationfile = HopVfs.getFileObject( destinationfilenamefull, this );

              entrystatus = MoveFile( shortfilename, sourcefilefolder, destinationfile, movetofolderfolder, parentWorkflow, result );
              return entrystatus;
            } else if ( sourcefilefolder.getType().equals( FileType.FILE ) && destination_is_a_file ) {
              // Source is a file, destination is a file

              FileObject destinationfile = HopVfs.getFileObject( realDestinationFilefoldername, this );

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
              destinationfile = HopVfs.getFileObject( destinationfilenamefull, this );

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
                        + NrErrors ) );
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
        if ( add_result_filesname && !iffileexists.equals( "fail" ) && !iffileexists.equals( "do_nothing" ) ) {
          addFileToResultFilenames( destinationfilename.toString(), result, parentWorkflow );
        }

        updateSuccess();
        retval = true;

      } else {
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.FileExists", destinationfilename.toString() ) );
        }
        if ( iffileexists.equals( "overwrite_file" ) ) {
          if ( !simulate ) {
            sourcefilename.moveTo( destinationfilename );
          }
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.FileOverwrite", destinationfilename
              .getName().toString() ) );
          }

          // add filename to result filename
          if ( add_result_filesname && !iffileexists.equals( "fail" ) && !iffileexists.equals( "do_nothing" ) ) {
            addFileToResultFilenames( destinationfilename.toString(), result, parentWorkflow );
          }

          updateSuccess();
          retval = true;

        } else if ( iffileexists.equals( "unique_name" ) ) {
          String short_filename = shortfilename;

          // return destination short filename
          try {
            short_filename = getMoveDestinationFilename( short_filename, "ddMMyyyy_HHmmssSSS" );
          } catch ( Exception e ) {
            logError( BaseMessages.getString( PKG, BaseMessages.getString(
              PKG, "JobMoveFiles.Error.GettingFilename", short_filename ) ), e );
            return retval;
          }

          String movetofilenamefull =
            destinationfilename.getParent().toString() + Const.FILE_SEPARATOR + short_filename;
          destinationfile = HopVfs.getFileObject( movetofilenamefull, this );

          if ( !simulate ) {
            sourcefilename.moveTo( destinationfile );
          }
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.FileMoved", sourcefilename
              .getName().toString(), destinationfile.getName().toString() ) );
          }

          // add filename to result filename
          if ( add_result_filesname && !iffileexists.equals( "fail" ) && !iffileexists.equals( "do_nothing" ) ) {
            addFileToResultFilenames( destinationfile.toString(), result, parentWorkflow );
          }

          updateSuccess();
          retval = true;
        } else if ( iffileexists.equals( "delete_file" ) ) {
          if ( !simulate ) {
            sourcefilename.delete();
          }
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.FileDeleted", destinationfilename
              .getName().toString() ) );
          }
          updateSuccess();
          retval = true;
        } else if ( iffileexists.equals( "move_file" ) ) {
          String short_filename = shortfilename;
          // return destination short filename
          try {
            short_filename = getMoveDestinationFilename( short_filename, null );
          } catch ( Exception e ) {
            logError( BaseMessages.getString( PKG, BaseMessages.getString(
              PKG, "JobMoveFiles.Error.GettingFilename", short_filename ) ), e );
            return retval;
          }

          String movetofilenamefull = movetofolderfolder.toString() + Const.FILE_SEPARATOR + short_filename;
          destinationfile = HopVfs.getFileObject( movetofilenamefull, this );
          if ( !destinationfile.exists() ) {
            if ( !simulate ) {
              sourcefilename.moveTo( destinationfile );
            }
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.FileMoved", sourcefilename
                .getName().toString(), destinationfile.getName().toString() ) );
            }

            // add filename to result filename
            if ( add_result_filesname && !iffileexists.equals( "fail" ) && !iffileexists.equals( "do_nothing" ) ) {
              addFileToResultFilenames( destinationfile.toString(), result, parentWorkflow );
            }

          } else {
            if ( ifmovedfileexists.equals( "overwrite_file" ) ) {
              if ( !simulate ) {
                sourcefilename.moveTo( destinationfile );
              }
              if ( log.isDetailed() ) {
                logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.FileOverwrite", destinationfile
                  .getName().toString() ) );
              }

              // add filename to result filename
              if ( add_result_filesname && !iffileexists.equals( "fail" ) && !iffileexists.equals( "do_nothing" ) ) {
                addFileToResultFilenames( destinationfile.toString(), result, parentWorkflow );
              }

              updateSuccess();
              retval = true;
            } else if ( ifmovedfileexists.equals( "unique_name" ) ) {
              SimpleDateFormat daf = new SimpleDateFormat();
              Date now = new Date();
              daf.applyPattern( "ddMMyyyy_HHmmssSSS" );
              String dt = daf.format( now );
              short_filename += "_" + dt;

              String destinationfilenamefull =
                movetofolderfolder.toString() + Const.FILE_SEPARATOR + short_filename;
              destinationfile = HopVfs.getFileObject( destinationfilenamefull, this );

              if ( !simulate ) {
                sourcefilename.moveTo( destinationfile );
              }
              if ( log.isDetailed() ) {
                logDetailed( BaseMessages.getString( PKG, "JobMoveFiles.Log.FileMoved", destinationfile
                  .getName().toString() ) );
              }

              // add filename to result filename
              if ( add_result_filesname && !iffileexists.equals( "fail" ) && !iffileexists.equals( "do_nothing" ) ) {
                addFileToResultFilenames( destinationfile.toString(), result, parentWorkflow );
              }

              updateSuccess();
              retval = true;
            } else if ( ifmovedfileexists.equals( "fail" ) ) {
              // Update Errors
              updateErrors();
            }
          }

        } else if ( iffileexists.equals( "fail" ) ) {
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
    FileObject file_name = null;

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
        String short_filename_from_basefolder = shortfilename;
        if ( !isDoNotKeepFolderStructure() ) {
          short_filename_from_basefolder =
            Currentfile.toString().substring(
              sourcefilefolder.toString().length(), Currentfile.toString().length() );
        }
        short_filename_from_basefolder =
          short_filename_from_basefolder.substring( 0, short_filename_from_basefolder.length() - lenCurrent )
            + shortfilename;

        // Built destination filename
        file_name =
          HopVfs.getFileObject( realDestinationFilefoldername
            + Const.FILE_SEPARATOR + short_filename_from_basefolder, this );

        if ( !Currentfile.getParent().toString().equals( sourcefilefolder.toString() ) ) {

          // Not in the Base Folder..Only if include sub folders
          if ( include_subfolders ) {
            // Folders..only if include subfolders
            if ( Currentfile.getType() == FileType.FOLDER ) {
              if ( include_subfolders && move_empty_folders && Utils.isEmpty( wildcard ) ) {
                entrystatus =
                  MoveFile( shortfilename, Currentfile, file_name, movetofolderfolder, parentWorkflow, result );
              }
            } else {

              if ( GetFileWildcard( sourceshortfilename, realWildcard ) ) {
                entrystatus =
                  MoveFile( shortfilename, Currentfile, file_name, movetofolderfolder, parentWorkflow, result );
              }
            }
          }
        } else {
          // In the Base Folder...
          // Folders..only if include subfolders
          if ( Currentfile.getType() == FileType.FOLDER ) {
            if ( include_subfolders && move_empty_folders && Utils.isEmpty( wildcard ) ) {
              entrystatus =
                MoveFile( shortfilename, Currentfile, file_name, movetofolderfolder, parentWorkflow, result );
            }
          } else {

            // file...Check if exists
            if ( GetFileWildcard( sourceshortfilename, realWildcard ) ) {
              entrystatus =
                MoveFile( shortfilename, Currentfile, file_name, movetofolderfolder, parentWorkflow, result );

            }
          }

        }

      }
      entrystatus = true;

    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "JobMoveFiles.Log.Error", e.toString() ) );
    } finally {
      if ( file_name != null ) {
        try {
          file_name.close();

        } catch ( IOException ex ) { /* Ignore */
        }
      }

    }
    return entrystatus;
  }

  private void updateErrors() {
    NrErrors++;
    if ( checkIfSuccessConditionBroken() ) {
      // Success condition was broken
      successConditionBroken = true;
    }
  }

  private boolean checkIfSuccessConditionBroken() {
    boolean retval = false;
    if ( ( NrErrors > 0 && getSuccessCondition().equals( SUCCESS_IF_NO_ERRORS ) )
      || ( NrErrors >= limitFiles && getSuccessCondition().equals( SUCCESS_IF_ERRORS_LESS ) ) ) {
      retval = true;
    }
    return retval;
  }

  private void updateSuccess() {
    NrSuccess++;
  }

  private void addFileToResultFilenames( String fileaddentry, Result result, IWorkflowEngine<WorkflowMeta> parentWorkflow ) {
    try {
      ResultFile resultFile =
        new ResultFile( ResultFile.FILE_TYPE_GENERAL, HopVfs.getFileObject( fileaddentry, this ), parentWorkflow
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

  private boolean CreateDestinationFolder( FileObject filefolder ) {
    FileObject folder = null;
    try {
      if ( destination_is_a_file ) {
        folder = filefolder.getParent();
      } else {
        folder = filefolder;
      }

      if ( !folder.exists() ) {
        if ( create_destination_folder ) {
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
    this.add_date = adddate;
  }

  public boolean isAddDate() {
    return add_date;
  }

  public boolean isAddMovedDate() {
    return add_moved_date;
  }

  public void setAddMovedDate( boolean add_moved_date ) {
    this.add_moved_date = add_moved_date;
  }

  public boolean isAddMovedTime() {
    return add_moved_time;
  }

  public void setAddMovedTime( boolean add_moved_time ) {
    this.add_moved_time = add_moved_time;
  }

  public void setIfFileExists( String iffileexists ) {
    this.iffileexists = iffileexists;
  }

  public String getIfFileExists() {
    return iffileexists;
  }

  public void setIfMovedFileExists( String ifmovedfileexists ) {
    this.ifmovedfileexists = ifmovedfileexists;
  }

  public String getIfMovedFileExists() {
    return ifmovedfileexists;
  }

  public void setAddTime( boolean addtime ) {
    this.add_time = addtime;
  }

  public boolean isAddTime() {
    return add_time;
  }

  public void setAddDateBeforeExtension( boolean AddDateBeforeExtension ) {
    this.AddDateBeforeExtension = AddDateBeforeExtension;
  }

  public void setAddMovedDateBeforeExtension( boolean AddMovedDateBeforeExtension ) {
    this.AddMovedDateBeforeExtension = AddMovedDateBeforeExtension;
  }

  public boolean isSpecifyFormat() {
    return SpecifyFormat;
  }

  public void setSpecifyFormat( boolean SpecifyFormat ) {
    this.SpecifyFormat = SpecifyFormat;
  }

  public void setSpecifyMoveFormat( boolean SpecifyMoveFormat ) {
    this.SpecifyMoveFormat = SpecifyMoveFormat;
  }

  public boolean isSpecifyMoveFormat() {
    return SpecifyMoveFormat;
  }

  public String getDateTimeFormat() {
    return date_time_format;
  }

  public void setDateTimeFormat( String date_time_format ) {
    this.date_time_format = date_time_format;
  }

  public String getMovedDateTimeFormat() {
    return moved_date_time_format;
  }

  public void setMovedDateTimeFormat( String moved_date_time_format ) {
    this.moved_date_time_format = moved_date_time_format;
  }

  public boolean isAddDateBeforeExtension() {
    return AddDateBeforeExtension;
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

  public void setMoveEmptyFolders( boolean move_empty_foldersin ) {
    this.move_empty_folders = move_empty_foldersin;
  }

  public void setIncludeSubfolders( boolean include_subfoldersin ) {
    this.include_subfolders = include_subfoldersin;
  }

  public void setAddresultfilesname( boolean add_result_filesnamein ) {
    this.add_result_filesname = add_result_filesnamein;
  }

  public void setArgFromPrevious( boolean argfrompreviousin ) {
    this.arg_from_previous = argfrompreviousin;
  }

  public void setDestinationIsAFile( boolean destination_is_a_file ) {
    this.destination_is_a_file = destination_is_a_file;
  }

  public void setCreateDestinationFolder( boolean create_destination_folder ) {
    this.create_destination_folder = create_destination_folder;
  }

  public void setCreateMoveToFolder( boolean create_move_to_folder ) {
    this.create_move_to_folder = create_move_to_folder;
  }

  public void setNrErrorsLessThan( String nr_errors_less_than ) {
    this.nr_errors_less_than = nr_errors_less_than;
  }

  public String getNrErrorsLessThan() {
    return nr_errors_less_than;
  }

  public void setSimulate( boolean simulate ) {
    this.simulate = simulate;
  }

  public void setSuccessCondition( String success_condition ) {
    this.success_condition = success_condition;
  }

  public String getSuccessCondition() {
    return success_condition;
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

    for ( int i = 0; i < source_filefolder.length; i++ ) {
      ActionValidatorUtils.andValidator().validate( this, "arguments[" + i + "]", remarks, ctx );
    }
  }

  public boolean evaluates() {
    return true;
  }

}
