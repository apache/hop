/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.workflow.actions.pgpencryptfiles;

import org.apache.commons.vfs2.AllFileSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This defines a 'PGP decrypt files' action.
 *
 * @author Samatar Hassan
 * @since 25-02-2008
 */

@Action(
  id = "PGP_ENCRYPT_FILES",
  i18nPackageName = "org.apache.hop.workflow.actions.pgpencryptfiles",
  name = "ActionPGPEncryptFiles.Name",
  description = "ActionPGPEncryptFiles.Description",
  image = "PGPEncryptFiles.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileEncryption",
  documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/pgpencryptfiles.html"
)
public class ActionPGPEncryptFiles extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionPGPEncryptFiles.class; // Needed by Translator

  public static final String[] actionTypeDesc = new String[] {
    BaseMessages.getString( PKG, "JobPGPEncryptFiles.ActionsType.Encrypt.Label" ),
    BaseMessages.getString( PKG, "JobPGPEncryptFiles.ActionsType.Sign.Label" ),
    BaseMessages.getString( PKG, "JobPGPEncryptFiles.ActionsType.SignAndEncrypt.Label" ), };
  public static final String[] actionTypeCodes = new String[] { "encrypt", "sign", "signandencrypt" };
  public static final int ACTION_TYPE_ENCRYPT = 0;
  public static final int ACTION_TYPE_SIGN = 1;
  public static final int ACTION_TYPE_SIGN_AND_ENCRYPT = 2;

  private SimpleDateFormat daf;
  private GPG gpg;

  public boolean argFromPrevious;
  public boolean includeSubFolders;
  public boolean addResultFileNames;
  public boolean destinationIsAFile;
  public boolean createDestinationFolder;

  public int[] actionType;
  public String[] sourceFileFolder;
  public String[] userId;
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
  private boolean doNotKeepFolderStructure;
  private String ifFileExists;
  private String destinationFolder;
  private String ifMovedFileExists;
  private String movedDateTimeFormat;
  private boolean addMovedDateBeforeExtension;
  private boolean addMovedDate;
  private boolean addMovedTime;
  private boolean specifyMoveFormat;
  public boolean createMoveToFolder;
  private String gpgLocation;

  private boolean asciiMode;

  private int nrErrors = 0;
  private int nrSuccess = 0;
  private boolean successConditionBroken = false;
  private boolean successConditionBrokenExit = false;
  private int limitFiles = 0;

  public ActionPGPEncryptFiles( String n ) {
    super( n, "" );
    createMoveToFolder = false;
    specifyMoveFormat = false;
    addMovedDate = false;
    addMovedTime = false;
    addMovedDateBeforeExtension = false;
    movedDateTimeFormat = null;
    gpgLocation = null;
    ifMovedFileExists = "do_nothing";
    destinationFolder = null;
    doNotKeepFolderStructure = false;
    argFromPrevious = false;
    sourceFileFolder = null;
    userId = null;
    destinationFileFolder = null;
    wildcard = null;
    includeSubFolders = false;
    addResultFileNames = false;
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
    asciiMode = false;
  }

  public ActionPGPEncryptFiles() {
    this( "" );
  }

  public void allocate( int nrFields ) {
    actionType = new int[ nrFields ];
    sourceFileFolder = new String[ nrFields ];
    userId = new String[ nrFields ];
    destinationFileFolder = new String[ nrFields ];
    wildcard = new String[ nrFields ];
  }

  public Object clone() {
    ActionPGPEncryptFiles je = (ActionPGPEncryptFiles) super.clone();
    if ( actionType != null ) {
      int nrFields = actionType.length;
      je.allocate( nrFields );
      System.arraycopy( actionType, 0, je.actionType, 0, nrFields );
      System.arraycopy( sourceFileFolder, 0, je.sourceFileFolder, 0, nrFields );
      System.arraycopy( userId, 0, je.userId, 0, nrFields );
      System.arraycopy( destinationFileFolder, 0, je.destinationFileFolder, 0, nrFields );
      System.arraycopy( wildcard, 0, je.wildcard, 0, nrFields );
    }
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 450 );

    retval.append( super.getXml() );
    retval.append( "      " ).append( XmlHandler.addTagValue( "gpglocation", gpgLocation ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "arg_from_previous", argFromPrevious ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "include_subfolders", includeSubFolders ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "add_result_filesname", addResultFileNames ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "destination_is_a_file", destinationIsAFile ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "create_destination_folder", createDestinationFolder ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "add_date", addDate ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "add_time", addTime ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "SpecifyFormat", specifyFormat ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "date_time_format", dateTimeFormat ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "nr_errors_less_than", nrErrorsLessThan ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "success_condition", successCondition ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "AddDateBeforeExtension", addDateBeforeExtension ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "DoNotKeepFolderStructure", doNotKeepFolderStructure ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "iffileexists", ifFileExists ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "destinationFolder", destinationFolder ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "ifmovedfileexists", ifMovedFileExists ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "moved_date_time_format", movedDateTimeFormat ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "create_move_to_folder", createMoveToFolder ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "add_moved_date", addMovedDate ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "add_moved_time", addMovedTime ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "SpecifyMoveFormat", specifyMoveFormat ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "AddMovedDateBeforeExtension", addMovedDateBeforeExtension ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "asciiMode", asciiMode ) );

    retval.append( "      <fields>" ).append( Const.CR );
    if ( sourceFileFolder != null ) {
      for ( int i = 0; i < sourceFileFolder.length; i++ ) {
        retval.append( "        <field>" ).append( Const.CR );
        retval.append( "          " ).append( XmlHandler.addTagValue( "action_type", getActionTypeCode( actionType[ i ] ) ) );
        retval.append( "          " ).append( XmlHandler.addTagValue( "source_filefolder", sourceFileFolder[ i ] ) );
        retval.append( "          " ).append( XmlHandler.addTagValue( "userid", userId[ i ] ) );
        retval.append( "          " ).append( XmlHandler.addTagValue( "destination_filefolder", destinationFileFolder[ i ] ) );
        retval.append( "          " ).append( XmlHandler.addTagValue( "wildcard", wildcard[ i ] ) );
        retval.append( "        </field>" ).append( Const.CR );
      }
    }
    retval.append( "      </fields>" ).append( Const.CR );

    return retval.toString();
  }

  public static String getActionTypeCode( int i ) {
    if ( i < 0 || i >= actionTypeCodes.length ) {
      return actionTypeCodes[ 0 ];
    }
    return actionTypeCodes[ i ];
  }

  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      gpgLocation = XmlHandler.getTagValue( entrynode, "gpglocation" );
      argFromPrevious = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "arg_from_previous" ) );
      includeSubFolders = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "include_subfolders" ) );
      addResultFileNames = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "add_result_filesname" ) );
      destinationIsAFile = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "destination_is_a_file" ) );
      createDestinationFolder =
        "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "create_destination_folder" ) );
      addDate = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "add_date" ) );
      addTime = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "add_time" ) );
      specifyFormat = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "SpecifyFormat" ) );
      addDateBeforeExtension =
        "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "AddDateBeforeExtension" ) );
      doNotKeepFolderStructure =
        "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "DoNotKeepFolderStructure" ) );
      dateTimeFormat = XmlHandler.getTagValue( entrynode, "date_time_format" );
      nrErrorsLessThan = XmlHandler.getTagValue( entrynode, "nr_errors_less_than" );
      successCondition = XmlHandler.getTagValue( entrynode, "success_condition" );
      ifFileExists = XmlHandler.getTagValue( entrynode, "iffileexists" );
      destinationFolder = XmlHandler.getTagValue( entrynode, "destinationFolder" );
      ifMovedFileExists = XmlHandler.getTagValue( entrynode, "ifmovedfileexists" );
      movedDateTimeFormat = XmlHandler.getTagValue( entrynode, "moved_date_time_format" );
      addMovedDateBeforeExtension =
        "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "AddMovedDateBeforeExtension" ) );
      createMoveToFolder = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "create_move_to_folder" ) );
      addMovedDate = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "add_moved_date" ) );
      addMovedTime = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "add_moved_time" ) );
      specifyMoveFormat = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "SpecifyMoveFormat" ) );
      asciiMode = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "asciiMode" ) );

      Node fields = XmlHandler.getSubNode( entrynode, "fields" );

      // How many field arguments?
      int nrFields = XmlHandler.countNodes( fields, "field" );
      allocate( nrFields );

      // Read them all...
      for ( int i = 0; i < nrFields; i++ ) {
        Node fnode = XmlHandler.getSubNodeByNr( fields, "field", i );
        actionType[ i ] = getActionTypeByCode( Const.NVL( XmlHandler.getTagValue( fnode, "action_type" ), "" ) );
        sourceFileFolder[ i ] = XmlHandler.getTagValue( fnode, "source_filefolder" );
        userId[ i ] = XmlHandler.getTagValue( fnode, "userid" );
        destinationFileFolder[ i ] = XmlHandler.getTagValue( fnode, "destination_filefolder" );
        wildcard[ i ] = XmlHandler.getTagValue( fnode, "wildcard" );
      }
    } catch ( HopXmlException xe ) {

      throw new HopXmlException( BaseMessages.getString(
        PKG, "JobPGPEncryptFiles.Error.Exception.UnableLoadXML" ), xe );
    }
  }

  private static int getActionTypeByCode( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < actionTypeCodes.length; i++ ) {
      if ( actionTypeCodes[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }
    return 0;
  }

  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    List<RowMetaAndData> rows = result.getRows();
    RowMetaAndData resultRow = null;
    result.setNrErrors( 1 );
    result.setResult( false );

    try {

      nrErrors = 0;
      nrSuccess = 0;
      successConditionBroken = false;
      successConditionBrokenExit = false;
      limitFiles = Const.toInt( environmentSubstitute( getNrErrorsLessThan() ), 10 );

      if ( includeSubFolders ) {
        if ( isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Log.IncludeSubFoldersOn" ) );
        }
      }

      String MoveToFolder = environmentSubstitute( destinationFolder );
      // Get source and destination files, also wildcard
      String[] vSourceFileFolder = sourceFileFolder;
      String[] vuserid = userId;
      String[] vDestinationFileFolder = destinationFileFolder;
      String[] vwildcard = wildcard;

      if ( ifFileExists.equals( "move_file" ) ) {
        if ( Utils.isEmpty( MoveToFolder ) ) {
          logError( toString(), BaseMessages.getString( PKG, "JobPGPEncryptFiles.Log.Error.MoveToFolderMissing" ) );
          return result;
        }
        FileObject folder = null;
        try {
          folder = HopVfs.getFileObject( MoveToFolder );
          if ( !folder.exists() ) {
            if ( isDetailed() ) {
              logDetailed( BaseMessages
                .getString( PKG, "JobPGPEncryptFiles.Log.Error.FolderMissing", MoveToFolder ) );
            }
            if ( createMoveToFolder ) {
              folder.createFolder();
            } else {
              logError( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Log.Error.FolderMissing", MoveToFolder ) );
              return result;
            }
          }
          if ( !folder.getType().equals( FileType.FOLDER ) ) {
            logError( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Log.Error.NotFolder", MoveToFolder ) );
            return result;
          }
        } catch ( Exception e ) {
          logError( BaseMessages.getString(
            PKG, "JobPGPEncryptFiles.Log.Error.GettingMoveToFolder", MoveToFolder, e.getMessage() ) );
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

      gpg = new GPG( environmentSubstitute( gpgLocation ), log );

      if ( argFromPrevious ) {
        if ( isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Log.ArgFromPrevious.Found", ( rows != null
            ? rows.size() : 0 )
            + "" ) );
        }
      }
      if ( argFromPrevious && rows != null ) {
        for ( int iteration = 0; iteration < rows.size(); iteration++ ) {
          // Success condition broken?
          if ( successConditionBroken ) {
            if ( !successConditionBrokenExit ) {
              logError( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Error.SuccessConditionbroken", ""
                + nrErrors ) );
              successConditionBrokenExit = true;
            }
            result.setNrErrors( nrErrors );
            displayResults();
            return result;
          }

          resultRow = rows.get( iteration );

          // Get source and destination file names, also wildcard
          int vactionTypePrevious = getActionTypeByCode( resultRow.getString( 0, null ) );
          String vSourceFileFolderPrevious = resultRow.getString( 1, null );
          String vWildcardPrevious = environmentSubstitute( resultRow.getString( 2, null ) );
          String vuseridPrevious = resultRow.getString( 3, null );
          String vDestinationFileFolderPrevious = resultRow.getString( 4, null );

          if ( !Utils.isEmpty( vSourceFileFolderPrevious ) && !Utils.isEmpty( vDestinationFileFolderPrevious ) ) {
            if ( isDetailed() ) {
              logDetailed( BaseMessages.getString(
                PKG, "JobPGPEncryptFiles.Log.ProcessingRow", vSourceFileFolderPrevious,
                vDestinationFileFolderPrevious, vWildcardPrevious ) );
            }

            if ( !ProcessFileFolder(
              vactionTypePrevious, vSourceFileFolderPrevious, vuseridPrevious,
              vDestinationFileFolderPrevious, vWildcardPrevious, parentWorkflow, result, MoveToFolder ) ) {
              // The process fail
              // Update Errors
              updateErrors();
            }
          } else {
            if ( isDetailed() ) {
              logDetailed( BaseMessages.getString(
                PKG, "JobPGPEncryptFiles.Log.IgnoringRow", vSourceFileFolder[ iteration ],
                vDestinationFileFolder[ iteration ], vwildcard[ iteration ] ) );
            }
          }
        }
      } else if ( vSourceFileFolder != null && vDestinationFileFolder != null ) {
        for ( int i = 0; i < vSourceFileFolder.length && !parentWorkflow.isStopped(); i++ ) {
          // Success condition broken?
          if ( successConditionBroken ) {
            if ( !successConditionBrokenExit ) {
              logError( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Error.SuccessConditionbroken", ""
                + nrErrors ) );
              successConditionBrokenExit = true;
            }
            result.setNrErrors( nrErrors );
            displayResults();
            return result;
          }

          if ( !Utils.isEmpty( vSourceFileFolder[ i ] ) && !Utils.isEmpty( vDestinationFileFolder[ i ] ) ) {
            // ok we can process this file/folder
            if ( isDetailed() ) {
              logDetailed( BaseMessages.getString(
                PKG, "JobPGPEncryptFiles.Log.ProcessingRow", vSourceFileFolder[ i ], vDestinationFileFolder[ i ],
                vwildcard[ i ] ) );
            }

            if ( !ProcessFileFolder(
              actionType[ i ], vSourceFileFolder[ i ], vuserid[ i ], vDestinationFileFolder[ i ], vwildcard[ i ],
              parentWorkflow, result, MoveToFolder ) ) {
              // Update Errors
              updateErrors();
            }
          } else {
            if ( isDetailed() ) {
              logDetailed( BaseMessages.getString(
                PKG, "JobPGPEncryptFiles.Log.IgnoringRow", vSourceFileFolder[ i ], vDestinationFileFolder[ i ],
                vwildcard[ i ] ) );
            }
          }
        }
      }

    } catch ( Exception e ) {
      updateErrors();
      logError( BaseMessages.getString( "JobPGPEncryptFiles.Error", e.getMessage() ) );
    } finally {
      if ( sourceFileFolder != null ) {
        sourceFileFolder = null;
      }
      if ( destinationFileFolder != null ) {
        destinationFileFolder = null;
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
    if ( isDetailed() ) {
      logDetailed( "=======================================" );
      logDetailed( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Log.Info.FilesInError", "" + nrErrors ) );
      logDetailed( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Log.Info.FilesInSuccess", "" + nrSuccess ) );
      logDetailed( "=======================================" );
    }
  }

  public static int getActionTypeByDesc( String tt ) {
    if ( tt == null ) {
      return 0;
    }

    for ( int i = 0; i < actionTypeDesc.length; i++ ) {
      if ( actionTypeDesc[ i ].equalsIgnoreCase( tt ) ) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getActionTypeByCode( tt );
  }

  public static String getActionTypeDesc( int i ) {
    if ( i < 0 && i > ACTION_TYPE_SIGN_AND_ENCRYPT ) {
      return actionTypeDesc[ 0 ];
    }
    return actionTypeDesc[ i ];
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

  private boolean ProcessFileFolder( int actionType, String sourcefilefoldername, String userID,
                                     String destinationfilefoldername, String wildcard, IWorkflowEngine<WorkflowMeta> parentWorkflow, Result result, String MoveToFolder ) {
    boolean entrystatus = false;
    FileObject sourcefilefolder = null;
    FileObject destinationfilefolder = null;
    FileObject movetofolderfolder = null;
    FileObject Currentfile = null;

    // Get real source, destination file and wildcard
    String realSourceFilefoldername = environmentSubstitute( sourcefilefoldername );
    String realuserID = environmentSubstitute( userID );
    String realDestinationFilefoldername = environmentSubstitute( destinationfilefoldername );
    String realWildcard = environmentSubstitute( wildcard );

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

            logError( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Log.Forbidden" ), BaseMessages.getString(
              PKG, "JobPGPEncryptFiles.Log.CanNotMoveFolderToFile", realSourceFilefoldername,
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
                shortfilename = getDestinationFilename( sourcefilefolder.getName().getBaseName() );
              } catch ( Exception e ) {
                logError( BaseMessages.getString(
                  PKG, "JobPGPEncryptFiles.Error.GettingFilename", sourcefilefolder.getName().getBaseName(), e
                    .toString() ) );
                return entrystatus;
              }
              // Move the file to the destination folder

              String destinationfilenamefull =
                destinationfilefolder.toString() + Const.FILE_SEPARATOR + shortfilename;
              FileObject destinationfile = HopVfs.getFileObject( destinationfilenamefull );

              entrystatus = EncryptFile(
                  actionType, shortfilename, sourcefilefolder, realuserID, destinationfile,
                  movetofolderfolder, parentWorkflow, result );

            } else if ( sourcefilefolder.getType().equals( FileType.FILE ) && destinationIsAFile ) {

              // Source is a file, destination is a file

              FileObject destinationfile = HopVfs.getFileObject( realDestinationFilefoldername );

              // return destination short filename
              String shortfilename = destinationfile.getName().getBaseName();
              try {
                shortfilename = getDestinationFilename( destinationfile.getName().getBaseName() );
              } catch ( Exception e ) {
                logError( BaseMessages.getString(
                  PKG, "JobPGPEncryptFiles.Error.GettingFilename", sourcefilefolder.getName().getBaseName(), e
                    .toString() ) );
                return entrystatus;
              }

              String destinationfilenamefull =
                destinationfilefolder.getParent().toString() + Const.FILE_SEPARATOR + shortfilename;
              destinationfile = HopVfs.getFileObject( destinationfilenamefull );

              entrystatus =
                EncryptFile(
                  actionType, shortfilename, sourcefilefolder, realuserID, destinationfile,
                  movetofolderfolder, parentWorkflow, result );

            } else {
              // Both source and destination are folders
              if ( isDetailed() ) {
                logDetailed( "  " );
                logDetailed( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Log.FetchFolder", sourcefilefolder
                  .toString() ) );
              }

              FileObject[] fileObjects = sourcefilefolder.findFiles( new AllFileSelector() {
                public boolean traverseDescendents( FileSelectInfo info ) {
                  return info.getDepth() == 0 || includeSubFolders;
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
                        fileObject = null;
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
                      logError( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Error.SuccessConditionbroken", ""
                        + nrErrors ) );
                      successConditionBrokenExit = true;
                    }
                    return false;
                  }
                  // Fetch files in list one after one ...
                  Currentfile = fileObjects[ j ];

                  if ( !EncryptOneFile(
                    actionType, Currentfile, sourcefilefolder, realuserID, realDestinationFilefoldername,
                    realWildcard, parentWorkflow, result, movetofolderfolder ) ) {
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
            PKG, "JobPGPEncryptFiles.Error.DestinationFolderNotFound", realDestinationFilefoldername ) );
        }
      } else {
        logError( BaseMessages.getString(
          PKG, "JobPGPEncryptFiles.Error.SourceFileNotExists", realSourceFilefoldername ) );
      }
    } catch ( Exception e ) {
      logError( BaseMessages.getString(
        PKG, "JobPGPEncryptFiles.Error.Exception.MoveProcess", realSourceFilefoldername.toString(),
        destinationfilefolder.toString(), e.getMessage() ) );
      // Update Errors
      updateErrors();
    } finally {
      if ( sourcefilefolder != null ) {
        try {
          sourcefilefolder.close();
        } catch ( IOException ex ) { /* Ignore */
        }
      }
      if ( destinationfilefolder != null ) {
        try {
          destinationfilefolder.close();
        } catch ( IOException ex ) { /* Ignore */
        }
      }
      if ( Currentfile != null ) {
        try {
          Currentfile.close();
        } catch ( IOException ex ) { /* Ignore */
        }
      }
      if ( movetofolderfolder != null ) {
        try {
          movetofolderfolder.close();
        } catch ( IOException ex ) { /* Ignore */
        }
      }
    }
    return entrystatus;
  }

  private boolean EncryptFile( int actionType, String shortfilename, FileObject sourcefilename, String userID,
                               FileObject destinationfilename, FileObject movetofolderfolder, IWorkflowEngine<WorkflowMeta> parentWorkflow, Result result ) {

    FileObject destinationfile = null;
    boolean retval = false;
    try {
      if ( !destinationfilename.exists() ) {

        doJob( actionType, sourcefilename, userID, destinationfilename );
        if ( isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Log.FileEncrypted", sourcefilename
            .getName().toString(), destinationfilename.getName().toString() ) );
        }

        // add filename to result filename
        if ( addResultFileNames && !ifFileExists.equals( "fail" ) && !ifFileExists.equals( "do_nothing" ) ) {
          addFileToResultFilenames( destinationfilename.toString(), result, parentWorkflow );
        }

        updateSuccess();

      } else {
        if ( isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Log.FileExists", destinationfilename
            .toString() ) );
        }
        if ( ifFileExists.equals( "overwrite_file" ) ) {
          doJob( actionType, sourcefilename, userID, destinationfilename );
          if ( isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Log.FileOverwrite", destinationfilename
              .getName().toString() ) );
          }

          // add filename to result filename
          if ( addResultFileNames && !ifFileExists.equals( "fail" ) && !ifFileExists.equals( "do_nothing" ) ) {
            addFileToResultFilenames( destinationfilename.toString(), result, parentWorkflow );
          }

          updateSuccess();

        } else if ( ifFileExists.equals( "unique_name" ) ) {
          String shortFilename = shortfilename;

          // return destination short filename
          try {
            shortFilename = getMoveDestinationFilename( shortFilename, "ddMMyyyy_HHmmssSSS" );
          } catch ( Exception e ) {
            logError( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Error.GettingFilename", shortFilename ), e );
            return retval;
          }

          String movetofilenamefull =
            destinationfilename.getParent().toString() + Const.FILE_SEPARATOR + shortFilename;
          destinationfile = HopVfs.getFileObject( movetofilenamefull );

          doJob( actionType, sourcefilename, userID, destinationfilename );
          if ( isDetailed() ) {
            logDetailed( toString(), BaseMessages.getString(
              PKG, "JobPGPEncryptFiles.Log.FileEncrypted", sourcefilename.getName().toString(), destinationfile
                .getName().toString() ) );
          }

          // add filename to result filename
          if ( addResultFileNames && !ifFileExists.equals( "fail" ) && !ifFileExists.equals( "do_nothing" ) ) {
            addFileToResultFilenames( destinationfile.toString(), result, parentWorkflow );
          }

          updateSuccess();
        } else if ( ifFileExists.equals( "delete_file" ) ) {
          destinationfilename.delete();
          if ( isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Log.FileDeleted", destinationfilename
              .getName().toString() ) );
          }
        } else if ( ifFileExists.equals( "move_file" ) ) {
          String shortFilename = shortfilename;
          // return destination short filename
          try {
            shortFilename = getMoveDestinationFilename( shortFilename, null );
          } catch ( Exception e ) {
            logError( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Error.GettingFilename", shortFilename ), e );
            return retval;
          }

          String movetofilenamefull = movetofolderfolder.toString() + Const.FILE_SEPARATOR + shortFilename;
          destinationfile = HopVfs.getFileObject( movetofilenamefull );
          if ( !destinationfile.exists() ) {
            sourcefilename.moveTo( destinationfile );
            if ( isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Log.FileEncrypted", sourcefilename
                .getName().toString(), destinationfile.getName().toString() ) );
            }

            // add filename to result filename
            if ( addResultFileNames && !ifFileExists.equals( "fail" ) && !ifFileExists.equals( "do_nothing" ) ) {
              addFileToResultFilenames( destinationfile.toString(), result, parentWorkflow );
            }

          } else {
            if ( ifMovedFileExists.equals( "overwrite_file" ) ) {
              sourcefilename.moveTo( destinationfile );
              if ( isDetailed() ) {
                logDetailed( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Log.FileOverwrite", destinationfile
                  .getName().toString() ) );
              }

              // add filename to result filename
              if ( addResultFileNames && !ifFileExists.equals( "fail" ) && !ifFileExists.equals( "do_nothing" ) ) {
                addFileToResultFilenames( destinationfile.toString(), result, parentWorkflow );
              }

              updateSuccess();
            } else if ( ifMovedFileExists.equals( "unique_name" ) ) {
              SimpleDateFormat daf = new SimpleDateFormat();
              Date now = new Date();
              daf.applyPattern( "ddMMyyyy_HHmmssSSS" );
              String dt = daf.format( now );
              shortFilename += "_" + dt;

              String destinationfilenamefull =
                movetofolderfolder.toString() + Const.FILE_SEPARATOR + shortFilename;
              destinationfile = HopVfs.getFileObject( destinationfilenamefull );

              sourcefilename.moveTo( destinationfile );
              if ( isDetailed() ) {
                logDetailed( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Log.FileEncrypted", destinationfile
                  .getName().toString() ) );
              }

              // add filename to result filename
              if ( addResultFileNames && !ifFileExists.equals( "fail" ) && !ifFileExists.equals( "do_nothing" ) ) {
                addFileToResultFilenames( destinationfile.toString(), result, parentWorkflow );
              }

              updateSuccess();
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
      updateErrors();
      logError( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Error.Exception.MoveProcessError", sourcefilename
        .toString(), destinationfilename.toString(), e.getMessage() ) );
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

  private boolean EncryptOneFile( int actionType, FileObject Currentfile, FileObject sourcefilefolder,
                                  String userID, String realDestinationFilefoldername, String realWildcard, IWorkflowEngine<WorkflowMeta> parentWorkflow, Result result,
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
          logError( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Error.GettingFilename", Currentfile
            .getName().getBaseName(), e.toString() ) );
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
          if ( includeSubFolders ) {
            // Folders..only if include subfolders
            if ( Currentfile.getType() != FileType.FOLDER ) {

              if ( GetFileWildcard( sourceshortfilename, realWildcard ) ) {
                entrystatus =
                  EncryptFile(
                    actionType, shortfilename, Currentfile, userID, filename, movetofolderfolder, parentWorkflow,
                    result );
              }
            }
          }
        } else {
          // In the Base Folder...
          // Folders..only if include subfolders
          if ( Currentfile.getType() != FileType.FOLDER ) {
            // file...Check if exists
            if ( GetFileWildcard( sourceshortfilename, realWildcard ) ) {
              entrystatus =
                EncryptFile(
                  actionType, shortfilename, Currentfile, userID, filename, movetofolderfolder, parentWorkflow,
                  result );

            }
          }

        }

      }
      entrystatus = true;

    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Log.Error", e.toString() ) );
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

      if ( isDebug() ) {
        logDebug( " ------ " );
        logDebug( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Log.FileAddedToResultFilesName", fileaddentry ) );
      }

    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Error.AddingToFilenameResult" ), fileaddentry
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
          if ( isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Log.FolderNotExist", folder
              .getName().toString() ) );
          }
          folder.createFolder();
          if ( isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Log.FolderWasCreated", folder
              .getName().toString() ) );
          }
        } else {
          logError( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Log.FolderNotExist", folder
            .getName().toString() ) );
          return false;
        }
      }
      return true;
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Log.CanNotCreateParentFolder", folder
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

    if ( daf == null ) {
      daf = new SimpleDateFormat();
    }
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

  public void setAsciiMode( boolean asciiMode ) {
    this.asciiMode = asciiMode;
  }

  public boolean isAsciiMode() {
    return asciiMode;
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
    this.addMovedDateBeforeExtension = AddMovedDateBeforeExtension;
  }

  public boolean isSpecifyFormat() {
    return specifyFormat;
  }

  public void setSpecifyFormat( boolean specifyFormat ) {
    this.specifyFormat = specifyFormat;
  }

  public void setSpecifyMoveFormat( boolean SpecifyMoveFormat ) {
    this.specifyMoveFormat = SpecifyMoveFormat;
  }

  public boolean isSpecifyMoveFormat() {
    return specifyMoveFormat;
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
    return addMovedDateBeforeExtension;
  }

  public boolean isDoNotKeepFolderStructure() {
    return doNotKeepFolderStructure;
  }

  public void setDestinationFolder( String destinationFolder ) {
    this.destinationFolder = destinationFolder;
  }

  public String getDestinationFolder() {
    return destinationFolder;
  }

  public void setGpgLocation( String gpgLocation ) {
    this.gpgLocation = gpgLocation;
  }

  public String getGpgLocation() {
    return gpgLocation;
  }

  public void setDoNotKeepFolderStructure( boolean DoNotKeepFolderStructure ) {
    this.doNotKeepFolderStructure = DoNotKeepFolderStructure;
  }

  public void setIncludeSubFolders( boolean includeSubFolders ) {
    this.includeSubFolders = includeSubFolders;
  }

  public void setAddResultFileNames( boolean addResultFileNames ) {
    this.addResultFileNames = addResultFileNames;
  }

  /**
   * Gets addResultFilenames
   *
   * @return value of addResultFilenames
   */
  public boolean isAddResultFileNames() {
    return addResultFileNames;
  }

  public void setArgFromPrevious( boolean argFromPrevious ) {
    this.argFromPrevious = argFromPrevious;
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

  public void setSuccessCondition( String successCondition ) {
    this.successCondition = successCondition;
  }

  public String getSuccessCondition() {
    return successCondition;
  }

  public void doJob( int actionType, FileObject sourceFile, String userID, FileObject destinationFile ) throws HopException {

    switch ( actionType ) {
      case ActionPGPEncryptFiles.ACTION_TYPE_SIGN:
        gpg.signFile( sourceFile, userID, destinationFile, isAsciiMode() );
        break;
      case ActionPGPEncryptFiles.ACTION_TYPE_SIGN_AND_ENCRYPT:
        gpg.signAndEncryptFile( sourceFile, userID, destinationFile, isAsciiMode() );
        break;
      default:
        gpg.encryptFile( sourceFile, userID, destinationFile, isAsciiMode() );
        break;
    }
  }

  public boolean evaluates() {
    return true;
  }

  public void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
    boolean res = ActionValidatorUtils.andValidator().validate( this, "arguments", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notNullValidator() ) );

    if ( res == false ) {
      return;
    }

    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace( ctx, getVariables() );
    AndValidator.putValidators( ctx, ActionValidatorUtils.notNullValidator(),
      ActionValidatorUtils.fileExistsValidator() );

    for ( int i = 0; i < sourceFileFolder.length; i++ ) {
      ActionValidatorUtils.andValidator().validate( this, "arguments[" + i + "]", remarks, ctx );
    }
  }

}
