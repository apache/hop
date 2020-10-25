/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.workflow.actions.copyfiles;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.NameScope;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This defines a 'copy files' action.
 *
 * @author Samatar Hassan
 * @since 06-05-2007
 */

@Action(
		id = "COPY_FILES",
		i18nPackageName = "org.apache.hop.workflow.actions.copyfiles",
		name = "ActionCopyFiles.Name",
		description = "ActionCopyFiles.Description",
		image = "CopyFiles.svg",
		categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
		documentationUrl = "https://www.project-hop.org/manual/latest/plugins/actions/copyfiles.html"
)
public class ActionCopyFiles extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionCopyFiles.class; // for i18n purposes, needed by Translator!!

  public static final String SOURCE_CONFIGURATION_NAME = "source_configuration_name";
  public static final String SOURCE_FILE_FOLDER = "source_filefolder";

  public static final String DESTINATION_CONFIGURATION_NAME = "destination_configuration_name";
  public static final String DESTINATION_FILE_FOLDER = "destination_filefolder";

  public static final String LOCAL_SOURCE_FILE = "LOCAL-SOURCE-FILE-";
  public static final String LOCAL_DEST_FILE = "LOCAL-DEST-FILE-";

  public static final String STATIC_SOURCE_FILE = "STATIC-SOURCE-FILE-";
  public static final String STATIC_DEST_FILE = "STATIC-DEST-FILE-";

  public static final String DEST_URL = "EMPTY_DEST_URL-";
  public static final String SOURCE_URL = "EMPTY_SOURCE_URL-";

  public boolean copy_empty_folders;
  public boolean arg_from_previous;
  public boolean overwrite_files;
  public boolean include_subfolders;
  public boolean add_result_filesname;
  public boolean remove_source_files;
  public boolean destination_is_a_file;
  public boolean create_destination_folder;
  public String[] source_filefolder;
  public String[] destination_filefolder;
  public String[] wildcard;
  HashSet<String> list_files_remove = new HashSet<>();
  HashSet<String> list_add_result = new HashSet<>();
  int NbrFail = 0;

  private Map<String, String> configurationMappings = new HashMap<>();

  public ActionCopyFiles( String n ) {
    super( n, "" );
    copy_empty_folders = true;
    arg_from_previous = false;
    source_filefolder = null;
    remove_source_files = false;
    destination_filefolder = null;
    wildcard = null;
    overwrite_files = false;
    include_subfolders = false;
    add_result_filesname = false;
    destination_is_a_file = false;
    create_destination_folder = false;
  }

  public ActionCopyFiles() {
    this( "" );
  }

  public void allocate( int nrFields ) {
    source_filefolder = new String[ nrFields ];
    destination_filefolder = new String[ nrFields ];
    wildcard = new String[ nrFields ];
  }

  public Object clone() {
    ActionCopyFiles je = (ActionCopyFiles) super.clone();
    if ( source_filefolder != null ) {
      int nrFields = source_filefolder.length;
      je.allocate( nrFields );
      System.arraycopy( source_filefolder, 0, je.source_filefolder, 0, nrFields );
      System.arraycopy( destination_filefolder, 0, je.destination_filefolder, 0, nrFields );
      System.arraycopy( wildcard, 0, je.wildcard, 0, nrFields );
    }
    return je;
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( super.getXml() );
    retval.append( "      " ).append( XmlHandler.addTagValue( "copy_empty_folders", copy_empty_folders ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "arg_from_previous", arg_from_previous ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "overwrite_files", overwrite_files ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "include_subfolders", include_subfolders ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "remove_source_files", remove_source_files ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "add_result_filesname", add_result_filesname ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "destination_is_a_file", destination_is_a_file ) );
    retval.append( "      " ).append(
      XmlHandler.addTagValue( "create_destination_folder", create_destination_folder ) );

    retval.append( "      <fields>" ).append( Const.CR );

    // Get source and destination files, also wildcard
    String[] vsourcefilefolder = preprocessfilefilder( source_filefolder );
    String[] vdestinationfilefolder = preprocessfilefilder( destination_filefolder );
    if ( source_filefolder != null ) {
      for ( int i = 0; i < source_filefolder.length; i++ ) {
        retval.append( "        <field>" ).append( Const.CR );
        saveSource( retval, source_filefolder[ i ] );
        saveDestination( retval, destination_filefolder[ i ] );
        retval.append( "          " ).append( XmlHandler.addTagValue( "wildcard", wildcard[ i ] ) );
        retval.append( "        </field>" ).append( Const.CR );
      }
    }
    retval.append( "      </fields>" ).append( Const.CR );

    return retval.toString();
  }

  @Override
  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      copy_empty_folders = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "copy_empty_folders" ) );
      arg_from_previous = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "arg_from_previous" ) );
      overwrite_files = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "overwrite_files" ) );
      include_subfolders = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "include_subfolders" ) );
      remove_source_files = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "remove_source_files" ) );
      add_result_filesname = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "add_result_filesname" ) );
      destination_is_a_file = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "destination_is_a_file" ) );
      create_destination_folder =
        "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "create_destination_folder" ) );

      Node fields = XmlHandler.getSubNode( entrynode, "fields" );

      // How many field arguments?
      int nrFields = XmlHandler.countNodes( fields, "field" );
      allocate( nrFields );

      // Read them all...
      for ( int i = 0; i < nrFields; i++ ) {
        Node fnode = XmlHandler.getSubNodeByNr( fields, "field", i );
        source_filefolder[ i ] = loadSource( fnode );
        destination_filefolder[ i ] = loadDestination( fnode );
        wildcard[ i ] = XmlHandler.getTagValue( fnode, "wildcard" );
      }
    } catch ( HopXmlException xe ) {

      throw new HopXmlException(
        BaseMessages.getString( PKG, "JobCopyFiles.Error.Exception.UnableLoadXML" ), xe );
    }
  }

  protected String loadSource( Node fnode ) {
    String source_filefolder = XmlHandler.getTagValue( fnode, SOURCE_FILE_FOLDER );
    String ncName = XmlHandler.getTagValue( fnode, SOURCE_CONFIGURATION_NAME );
    return loadURL( source_filefolder, ncName, getMetadataProvider(), configurationMappings );
  }

  protected String loadDestination( Node fnode ) {
    String destination_filefolder = XmlHandler.getTagValue( fnode, DESTINATION_FILE_FOLDER );
    String ncName = XmlHandler.getTagValue( fnode, DESTINATION_CONFIGURATION_NAME );
    return loadURL( destination_filefolder, ncName, getMetadataProvider(), configurationMappings );
  }

  protected void saveSource( StringBuilder retval, String source ) {
    String namedCluster = configurationMappings.get( source );
    retval.append( "          " ).append( XmlHandler.addTagValue( SOURCE_FILE_FOLDER, source ) );
    retval.append( "          " ).append( XmlHandler.addTagValue( SOURCE_CONFIGURATION_NAME, namedCluster ) );
  }

  protected void saveDestination( StringBuilder retval, String destination ) {
    String namedCluster = configurationMappings.get( destination );
    retval.append( "          " ).append( XmlHandler.addTagValue( DESTINATION_FILE_FOLDER, destination ) );
    retval.append( "          " ).append( XmlHandler.addTagValue( DESTINATION_CONFIGURATION_NAME, namedCluster ) );
  }

  String[] preprocessfilefilder( String[] folders ) {
    List<String> nfolders = new ArrayList<>();
    if ( folders != null ) {
      for ( int i = 0; i < folders.length; i++ ) {
        nfolders.add( folders[ i ].replace( ActionCopyFiles.SOURCE_URL + i + "-", "" )
          .replace( ActionCopyFiles.DEST_URL + i + "-", "" ) );
      }
    }
    return nfolders.toArray( new String[ nfolders.size() ] );
  }

  @Override
  public Result execute( Result previousResult, int nr ) throws HopException {
    Result result = previousResult;

    List<RowMetaAndData> rows = result.getRows();
    RowMetaAndData resultRow = null;

    int NbrFail = 0;

    NbrFail = 0;

    if ( isBasic() ) {
      logBasic( BaseMessages.getString( PKG, "JobCopyFiles.Log.Starting" ) );
    }

    try {
      // Get source and destination files, also wildcard
      String[] vsourcefilefolder = preprocessfilefilder( source_filefolder );
      String[] vdestinationfilefolder = preprocessfilefilder( destination_filefolder );
      String[] vwildcard = wildcard;

      result.setResult( false );
      result.setNrErrors( 1 );

      if ( arg_from_previous ) {
        if ( isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "JobCopyFiles.Log.ArgFromPrevious.Found", ( rows != null ? rows
            .size() : 0 )
            + "" ) );
        }
      }

      if ( arg_from_previous && rows != null ) { // Copy the input row to the (command line) arguments
        for ( int iteration = 0; iteration < rows.size() && !parentWorkflow.isStopped(); iteration++ ) {
          resultRow = rows.get( iteration );

          // Get source and destination file names, also wildcard
          String vsourcefilefolder_previous = resultRow.getString( 0, null );
          String vdestinationfilefolder_previous = resultRow.getString( 1, null );
          String vwildcard_previous = resultRow.getString( 2, null );

          if ( !Utils.isEmpty( vsourcefilefolder_previous ) && !Utils.isEmpty( vdestinationfilefolder_previous ) ) {
            if ( isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "JobCopyFiles.Log.ProcessingRow",
                HopVfs.getFriendlyURI( environmentSubstitute( vsourcefilefolder_previous ) ),
                HopVfs.getFriendlyURI( environmentSubstitute( vdestinationfilefolder_previous ) ),
                environmentSubstitute( vwildcard_previous ) ) );
            }

            if ( !processFileFolder( vsourcefilefolder_previous, vdestinationfilefolder_previous, vwildcard_previous,
              parentWorkflow, result ) ) {
              // The copy process fail
              NbrFail++;
            }
          } else {
            if ( isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "JobCopyFiles.Log.IgnoringRow",
                HopVfs.getFriendlyURI( environmentSubstitute( vsourcefilefolder[ iteration ] ) ),
                HopVfs.getFriendlyURI( environmentSubstitute( vdestinationfilefolder[ iteration ] ) ),
                vwildcard[ iteration ] ) );
            }
          }
        }
      } else if ( vsourcefilefolder != null && vdestinationfilefolder != null ) {
        for ( int i = 0; i < vsourcefilefolder.length && !parentWorkflow.isStopped(); i++ ) {
          if ( !Utils.isEmpty( vsourcefilefolder[ i ] ) && !Utils.isEmpty( vdestinationfilefolder[ i ] ) ) {

            // ok we can process this file/folder

            if ( isBasic() ) {
              logBasic( BaseMessages.getString( PKG, "JobCopyFiles.Log.ProcessingRow",
                HopVfs.getFriendlyURI( environmentSubstitute( vsourcefilefolder[ i ] ) ),
                HopVfs.getFriendlyURI( environmentSubstitute( vdestinationfilefolder[ i ] ) ),
                environmentSubstitute( vwildcard[ i ] ) ) );
            }

            if ( !processFileFolder( vsourcefilefolder[ i ], vdestinationfilefolder[ i ], vwildcard[ i ], parentWorkflow, result ) ) {
              // The copy process fail
              NbrFail++;
            }
          } else {
            if ( isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "JobCopyFiles.Log.IgnoringRow",
                HopVfs.getFriendlyURI( environmentSubstitute( vsourcefilefolder[ i ] ) ),
                HopVfs.getFriendlyURI( environmentSubstitute( vdestinationfilefolder[ i ] ) ), vwildcard[ i ] ) );
            }
          }
        }
      }
    } finally {
      list_add_result = null;
      list_files_remove = null;
    }

    // Check if all files was process with success
    if ( NbrFail == 0 ) {
      result.setResult( true );
      result.setNrErrors( 0 );
    } else {
      result.setNrErrors( NbrFail );
    }

    return result;
  }

  boolean processFileFolder( String sourcefilefoldername, String destinationfilefoldername, String wildcard,
                             IWorkflowEngine<WorkflowMeta> parentWorkflow, Result result ) {
    boolean entrystatus = false;
    FileObject sourcefilefolder = null;
    FileObject destinationfilefolder = null;

    // Clear list files to remove after copy process
    // This list is also added to result files name
    list_files_remove.clear();
    list_add_result.clear();

    // Get real source, destination file and wildcard
    String realSourceFilefoldername = environmentSubstitute( sourcefilefoldername );
    String realDestinationFilefoldername = environmentSubstitute( destinationfilefoldername );
    String realWildcard = environmentSubstitute( wildcard );

    try {
      sourcefilefolder = HopVfs.getFileObject( realSourceFilefoldername );
      destinationfilefolder = HopVfs.getFileObject( realDestinationFilefoldername );

      if ( sourcefilefolder.exists() ) {

        // Check if destination folder/parent folder exists !
        // If user wanted and if destination folder does not exist
        // PDI will create it
        if ( CreateDestinationFolder( destinationfilefolder ) ) {

          // Basic Tests
          if ( sourcefilefolder.getType().equals( FileType.FOLDER ) && destination_is_a_file ) {
            // Source is a folder, destination is a file
            // WARNING !!! CAN NOT COPY FOLDER TO FILE !!!

            logError( BaseMessages.getString(
              PKG, "JobCopyFiles.Log.CanNotCopyFolderToFile", HopVfs.getFriendlyURI( realSourceFilefoldername ),
              HopVfs.getFriendlyURI( realDestinationFilefoldername ) ) );

            NbrFail++;

          } else {

            if ( destinationfilefolder.getType().equals( FileType.FOLDER )
              && sourcefilefolder.getType().equals( FileType.FILE ) ) {
              // Source is a file, destination is a folder
              // Copy the file to the destination folder

              destinationfilefolder.copyFrom( sourcefilefolder.getParent(), new TextOneFileSelector(
                sourcefilefolder.getParent().toString(), sourcefilefolder.getName().getBaseName(),
                destinationfilefolder.toString() ) );
              if ( isDetailed() ) {
                logDetailed( BaseMessages.getString( PKG, "JobCopyFiles.Log.FileCopied", HopVfs.getFriendlyURI( sourcefilefolder ),
                  HopVfs.getFriendlyURI( destinationfilefolder ) ) );
              }

            } else if ( sourcefilefolder.getType().equals( FileType.FILE ) && destination_is_a_file ) {
              // Source is a file, destination is a file

              destinationfilefolder.copyFrom( sourcefilefolder, new TextOneToOneFileSelector(
                destinationfilefolder ) );
            } else {
              // Both source and destination are folders
              if ( isDetailed() ) {
                logDetailed( "  " );
                logDetailed( BaseMessages.getString( PKG, "JobCopyFiles.Log.FetchFolder", HopVfs.getFriendlyURI( sourcefilefolder ) ) );

              }

              TextFileSelector textFileSelector =
                new TextFileSelector( sourcefilefolder, destinationfilefolder, realWildcard, parentWorkflow );
              try {
                destinationfilefolder.copyFrom( sourcefilefolder, textFileSelector );
              } finally {
                textFileSelector.shutdown();
              }
            }

            // Remove Files if needed
            if ( remove_source_files && !list_files_remove.isEmpty() ) {
              String sourceFilefoldername = sourcefilefolder.toString();
              int trimPathLength = sourceFilefoldername.length() + 1;
              FileObject removeFile;

              for ( Iterator<String> iter = list_files_remove.iterator(); iter.hasNext() && !parentWorkflow.isStopped(); ) {
                String fileremoventry = iter.next();
                removeFile = null; // re=null each iteration
                // Try to get the file relative to the existing connection
                if ( fileremoventry.startsWith( sourceFilefoldername ) ) {
                  if ( trimPathLength < fileremoventry.length() ) {
                    removeFile = sourcefilefolder.getChild( fileremoventry.substring( trimPathLength ) );
                  }
                }

                // Unable to retrieve file through existing connection; Get the file through a new VFS connection
                if ( removeFile == null ) {
                  removeFile = HopVfs.getFileObject( fileremoventry );
                }

                // Remove ONLY Files
                if ( removeFile.getType() == FileType.FILE ) {
                  boolean deletefile = removeFile.delete();
                  logBasic( " ------ " );
                  if ( !deletefile ) {
                    logError( "      "
                      + BaseMessages.getString(
                      PKG, "JobCopyFiles.Error.Exception.CanRemoveFileFolder", HopVfs.getFriendlyURI( fileremoventry ) ) );
                  } else {
                    if ( isDetailed() ) {
                      logDetailed( "      "
                        + BaseMessages.getString( PKG, "JobCopyFiles.Log.FileFolderRemoved", HopVfs.getFriendlyURI( fileremoventry ) ) );
                    }
                  }
                }
              }
            }

            // Add files to result files name
            if ( add_result_filesname && !list_add_result.isEmpty() ) {
              String destinationFilefoldername = destinationfilefolder.toString();
              int trimPathLength = destinationFilefoldername.length() + 1;
              FileObject addFile;

              for ( Iterator<String> iter = list_add_result.iterator(); iter.hasNext(); ) {
                String fileaddentry = iter.next();
                addFile = null; // re=null each iteration

                // Try to get the file relative to the existing connection
                if ( fileaddentry.startsWith( destinationFilefoldername ) ) {
                  if ( trimPathLength < fileaddentry.length() ) {
                    addFile = destinationfilefolder.getChild( fileaddentry.substring( trimPathLength ) );
                  }
                }

                // Unable to retrieve file through existing connection; Get the file through a new VFS connection
                if ( addFile == null ) {
                  addFile = HopVfs.getFileObject( fileaddentry );
                }

                // Add ONLY Files
                if ( addFile.getType() == FileType.FILE ) {
                  ResultFile resultFile =
                    new ResultFile( ResultFile.FILE_TYPE_GENERAL, addFile, parentWorkflow.getWorkflowName(), toString() );
                  result.getResultFiles().put( resultFile.getFile().toString(), resultFile );
                  if ( isDetailed() ) {
                    logDetailed( " ------ " );
                    logDetailed( "      "
                      + BaseMessages
                      .getString( PKG, "JobCopyFiles.Log.FileAddedToResultFilesName", HopVfs.getFriendlyURI( fileaddentry ) ) );
                  }
                }
              }
            }
          }
          entrystatus = true;
        } else {
          // Destination Folder or Parent folder is missing
          logError( BaseMessages.getString(
            PKG, "JobCopyFiles.Error.DestinationFolderNotFound", HopVfs.getFriendlyURI( realDestinationFilefoldername ) ) );
        }
      } else {
        logError( BaseMessages.getString( PKG, "JobCopyFiles.Error.SourceFileNotExists", HopVfs.getFriendlyURI( realSourceFilefoldername ) ) );

      }
    } catch ( FileSystemException fse ) {
      logError( BaseMessages.getString( PKG, "JobCopyFiles.Error.Exception.CopyProcessFileSystemException", fse
        .getMessage() ) );
      Throwable throwable = fse.getCause();
      while ( throwable != null ) {
        logError( BaseMessages.getString( PKG, "JobCopyFiles.Log.CausedBy", throwable.getMessage() ) );
        throwable = throwable.getCause();
      }
    } catch ( Exception e ) {
      logError( BaseMessages.getString(
        PKG, "JobCopyFiles.Error.Exception.CopyProcess", HopVfs.getFriendlyURI( realSourceFilefoldername ),
        HopVfs.getFriendlyURI( realDestinationFilefoldername ), e.getMessage() ), e );
    } finally {
      if ( sourcefilefolder != null ) {
        try {
          sourcefilefolder.close();
          sourcefilefolder = null;
        } catch ( IOException ex ) { /* Ignore */
        }
      }
      if ( destinationfilefolder != null ) {
        try {
          destinationfilefolder.close();
          destinationfilefolder = null;
        } catch ( IOException ex ) { /* Ignore */
        }
      }
    }

    return entrystatus;
  }

  private class TextOneToOneFileSelector implements FileSelector {
    FileObject destfile = null;

    public TextOneToOneFileSelector( FileObject destinationfile ) {

      if ( destinationfile != null ) {
        destfile = destinationfile;
      }
    }

    public boolean includeFile( FileSelectInfo info ) {
      boolean resultat = false;
      String fil_name = null;

      try {
        // check if the destination file exists

        if ( destfile.exists() ) {
          if ( isDetailed() ) {
            logDetailed( "      "
              + BaseMessages.getString( PKG, "JobCopyFiles.Log.FileExists", HopVfs.getFriendlyURI( destfile ) ) );
          }

          if ( overwrite_files ) {
            if ( isDetailed() ) {
              logDetailed( "      "
                + BaseMessages.getString( PKG, "JobCopyFiles.Log.FileOverwrite", HopVfs.getFriendlyURI( destfile ) ) );
            }

            resultat = true;
          }
        } else {
          if ( isDetailed() ) {
            logDetailed( "      "
              + BaseMessages.getString( PKG, "JobCopyFiles.Log.FileCopied", HopVfs.getFriendlyURI( info.getFile() ), HopVfs.getFriendlyURI( destfile ) ) );
          }

          resultat = true;
        }

        if ( resultat && remove_source_files ) {
          // add this folder/file to remove files
          // This list will be fetched and all entries files
          // will be removed
          list_files_remove.add( info.getFile().toString() );
        }

        if ( resultat && add_result_filesname ) {
          // add this folder/file to result files name
          list_add_result.add( destfile.toString() );
        }

      } catch ( Exception e ) {

        logError( BaseMessages.getString( PKG, "JobCopyFiles.Error.Exception.CopyProcess", HopVfs.getFriendlyURI( info
          .getFile() ), fil_name, e.getMessage() ) );

      }

      return resultat;

    }

    public boolean traverseDescendents( FileSelectInfo info ) {
      return false;
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
          if ( isDetailed() ) {
            logDetailed( "Folder  " + HopVfs.getFriendlyURI( folder ) + " does not exist !" );
          }
          folder.createFolder();
          if ( isDetailed() ) {
            logDetailed( "Folder parent was created." );
          }
        } else {
          logError( "Folder  " + HopVfs.getFriendlyURI( folder ) + " does not exist !" );
          return false;
        }
      }
      return true;
    } catch ( Exception e ) {
      logError( "Couldn't created parent folder " + HopVfs.getFriendlyURI( folder ), e );
    } finally {
      if ( folder != null ) {
        try {
          folder.close();
          folder = null;
        } catch ( Exception ex ) { /* Ignore */
        }
      }
    }
    return false;
  }

  private class TextFileSelector implements FileSelector {
    String fileWildcard = null;
    String sourceFolder = null;
    String destinationFolder = null;
    IWorkflowEngine<WorkflowMeta> parentjob;
    Pattern pattern;
    private int traverseCount;

    // Store connection to destination source for improved performance to remote hosts
    FileObject destinationFolderObject = null;

    /**********************************************************
     *
     * @param selectedfile
     * @return True if the selectedfile matches the wildcard
     **********************************************************/
    private boolean GetFileWildcard( String selectedfile ) {
      boolean getIt = true;
      // First see if the file matches the regular expression!
      if ( pattern != null ) {
        Matcher matcher = pattern.matcher( selectedfile );
        getIt = matcher.matches();
      }
      return getIt;
    }

    public TextFileSelector( FileObject sourcefolderin, FileObject destinationfolderin, String filewildcard,
                             IWorkflowEngine<WorkflowMeta> parentWorkflow ) {

      if ( sourcefolderin != null ) {
        sourceFolder = sourcefolderin.toString();
      }
      if ( destinationfolderin != null ) {
        destinationFolderObject = destinationfolderin;
        destinationFolder = destinationFolderObject.toString();
      }
      if ( !Utils.isEmpty( filewildcard ) ) {
        fileWildcard = filewildcard;
        pattern = Pattern.compile( fileWildcard );
      }
      parentjob = parentWorkflow;
    }

    public boolean includeFile( FileSelectInfo info ) {
      boolean returncode = false;
      FileObject file_name = null;
      String addFileNameString = null;
      try {

        if ( !info.getFile().toString().equals( sourceFolder ) && !parentjob.isStopped() ) {
          // Pass over the Base folder itself

          String short_filename = info.getFile().getName().getBaseName();
          // Built destination filename
          if ( destinationFolderObject == null ) {
            // Resolve the destination folder
            destinationFolderObject = HopVfs.getFileObject( destinationFolder );
          }

          String fullName = info.getFile().toString();
          String baseFolder = info.getBaseFolder().toString();
          String path = fullName.substring( fullName.indexOf( baseFolder ) + baseFolder.length() + 1 );
          file_name = destinationFolderObject.resolveFile( path, NameScope.DESCENDENT );

          if ( !info.getFile().getParent().equals( info.getBaseFolder() ) ) {

            // Not in the Base Folder..Only if include sub folders
            if ( include_subfolders ) {
              // Folders..only if include subfolders
              if ( info.getFile().getType() == FileType.FOLDER ) {
                if ( include_subfolders && copy_empty_folders && Utils.isEmpty( fileWildcard ) ) {
                  if ( ( file_name == null ) || ( !file_name.exists() ) ) {
                    if ( isDetailed() ) {
                      logDetailed( " ------ " );
                      logDetailed( "      "
                        + BaseMessages.getString( PKG, "JobCopyFiles.Log.FolderCopied", HopVfs.getFriendlyURI( info
                        .getFile() ), file_name != null ? HopVfs.getFriendlyURI( file_name ) : "" ) );
                    }
                    returncode = true;
                  } else {
                    if ( isDetailed() ) {
                      logDetailed( " ------ " );
                      logDetailed( "      "
                        + BaseMessages.getString( PKG, "JobCopyFiles.Log.FolderExists", HopVfs.getFriendlyURI( file_name ) ) );
                    }
                    if ( overwrite_files ) {
                      if ( isDetailed() ) {
                        logDetailed( "      "
                          + BaseMessages.getString( PKG, "JobCopyFiles.Log.FolderOverwrite", HopVfs.getFriendlyURI( info
                          .getFile() ), HopVfs.getFriendlyURI( file_name ) ) );
                      }
                      returncode = true;
                    }
                  }
                }

              } else {
                if ( GetFileWildcard( short_filename ) ) {
                  // Check if the file exists
                  if ( ( file_name == null ) || ( !file_name.exists() ) ) {
                    if ( isDetailed() ) {
                      logDetailed( " ------ " );
                      logDetailed( "      "
                        + BaseMessages.getString(
                        PKG, "JobCopyFiles.Log.FileCopied", HopVfs.getFriendlyURI( info.getFile() ), file_name != null
                          ? HopVfs.getFriendlyURI( file_name ) : "" ) );
                    }
                    returncode = true;
                  } else {
                    if ( isDetailed() ) {
                      logDetailed( " ------ " );
                      logDetailed( "      "
                        + BaseMessages.getString( PKG, "JobCopyFiles.Log.FileExists", HopVfs.getFriendlyURI( file_name ) ) );
                    }
                    if ( overwrite_files ) {
                      if ( isDetailed() ) {
                        logDetailed( "       "
                          + BaseMessages.getString( PKG, "JobCopyFiles.Log.FileExists", HopVfs.getFriendlyURI( info
                          .getFile() ), HopVfs.getFriendlyURI( file_name ) ) );
                      }

                      returncode = true;
                    }
                  }
                }
              }
            }
          } else {
            // In the Base Folder...
            // Folders..only if include subfolders
            if ( info.getFile().getType() == FileType.FOLDER ) {
              if ( include_subfolders && copy_empty_folders && Utils.isEmpty( fileWildcard ) ) {
                if ( ( file_name == null ) || ( !file_name.exists() ) ) {
                  if ( isDetailed() ) {
                    logDetailed( "", " ------ " );
                    logDetailed( "      "
                      + BaseMessages.getString(
                      PKG, "JobCopyFiles.Log.FolderCopied", HopVfs.getFriendlyURI( info.getFile() ), file_name != null
                        ? HopVfs.getFriendlyURI( file_name ) : "" ) );
                  }

                  returncode = true;
                } else {
                  if ( isDetailed() ) {
                    logDetailed( " ------ " );
                    logDetailed( "      "
                      + BaseMessages.getString( PKG, "JobCopyFiles.Log.FolderExists", HopVfs.getFriendlyURI( file_name ) ) );
                  }
                  if ( overwrite_files ) {
                    if ( isDetailed() ) {
                      logDetailed( "      "
                        + BaseMessages.getString( PKG, "JobCopyFiles.Log.FolderOverwrite", HopVfs.getFriendlyURI( info
                        .getFile() ), HopVfs.getFriendlyURI( file_name ) ) );
                    }

                    returncode = true;
                  }
                }
              }
            } else {
              // file...Check if exists
              file_name = HopVfs.getFileObject( destinationFolder + Const.FILE_SEPARATOR + short_filename );

              if ( GetFileWildcard( short_filename ) ) {
                if ( ( file_name == null ) || ( !file_name.exists() ) ) {
                  if ( isDetailed() ) {
                    logDetailed( " ------ " );
                    logDetailed( "      "
                      + BaseMessages.getString(
                      PKG, "JobCopyFiles.Log.FileCopied", HopVfs.getFriendlyURI( info.getFile() ), file_name != null
                        ? HopVfs.getFriendlyURI( file_name ) : "" ) );
                  }
                  returncode = true;

                } else {
                  if ( isDetailed() ) {
                    logDetailed( " ------ " );
                    logDetailed( "      "
                      + BaseMessages.getString( PKG, "JobCopyFiles.Log.FileExists", HopVfs.getFriendlyURI( file_name ) ) );
                  }

                  if ( overwrite_files ) {
                    if ( isDetailed() ) {
                      logDetailed(
                        "      " + BaseMessages.getString( PKG, "JobCopyFiles.Log.FileExistsInfos" ),
                        BaseMessages.getString(
                          PKG, "JobCopyFiles.Log.FileExists", HopVfs.getFriendlyURI( info.getFile() ), HopVfs.getFriendlyURI( file_name ) ) );
                    }

                    returncode = true;
                  }
                }
              }
            }
          }
        }
      } catch ( Exception e ) {

        logError( BaseMessages.getString( PKG, "JobCopyFiles.Error.Exception.CopyProcess", HopVfs.getFriendlyURI( info
          .getFile() ), file_name != null ? HopVfs.getFriendlyURI( file_name ) : null, e.getMessage() ) );

        returncode = false;
      } finally {
        if ( file_name != null ) {
          try {
            if ( returncode && add_result_filesname ) {
              addFileNameString = file_name.toString();
            }
            file_name.close();
            file_name = null;
          } catch ( IOException ex ) { /* Ignore */
          }
        }
      }
      if ( returncode && remove_source_files ) {
        // add this folder/file to remove files
        // This list will be fetched and all entries files
        // will be removed
        list_files_remove.add( info.getFile().toString() );
      }

      if ( returncode && add_result_filesname ) {
        // add this folder/file to result files name
        list_add_result.add( addFileNameString ); // was a NPE before with the file_name=null above in the finally
      }

      return returncode;
    }

    public boolean traverseDescendents( FileSelectInfo info ) {
      return ( traverseCount++ == 0 || include_subfolders );
    }

    public void shutdown() {
      if ( destinationFolderObject != null ) {
        try {
          destinationFolderObject.close();

        } catch ( IOException ex ) { /* Ignore */
        }
      }
    }
  }

  private class TextOneFileSelector implements FileSelector {
    String filename = null;
    String foldername = null;
    String destfolder = null;
    private int traverseCount;

    public TextOneFileSelector( String sourcefolderin, String sourcefilenamein, String destfolderin ) {
      if ( !Utils.isEmpty( sourcefilenamein ) ) {
        filename = sourcefilenamein;
      }

      if ( !Utils.isEmpty( sourcefolderin ) ) {
        foldername = sourcefolderin;
      }
      if ( !Utils.isEmpty( destfolderin ) ) {
        destfolder = destfolderin;
      }
    }

    public boolean includeFile( FileSelectInfo info ) {
      boolean resultat = false;
      String fil_name = null;

      try {
        if ( info.getFile().getType() == FileType.FILE ) {
          if ( info.getFile().getName().getBaseName().equals( filename )
            && ( info.getFile().getParent().toString().equals( foldername ) ) ) {
            // check if the file exists
            fil_name = destfolder + Const.FILE_SEPARATOR + filename;

            if ( HopVfs.getFileObject( fil_name).exists() ) {
              if ( isDetailed() ) {
                logDetailed( "      " + BaseMessages.getString( PKG, "JobCopyFiles.Log.FileExists", HopVfs.getFriendlyURI( fil_name ) ) );
              }

              if ( overwrite_files ) {
                if ( isDetailed() ) {
                  logDetailed( "      "
                    + BaseMessages.getString(
                    PKG, "JobCopyFiles.Log.FileOverwrite", HopVfs.getFriendlyURI( info.getFile() ), HopVfs.getFriendlyURI( fil_name ) ) );
                }

                resultat = true;
              }
            } else {
              if ( isDetailed() ) {
                logDetailed( "      "
                  + BaseMessages.getString(
                  PKG, "JobCopyFiles.Log.FileCopied", HopVfs.getFriendlyURI( info.getFile() ), HopVfs.getFriendlyURI( fil_name ) ) );
              }

              resultat = true;
            }
          }

          if ( resultat && remove_source_files ) {
            // add this folder/file to remove files
            // This list will be fetched and all entries files
            // will be removed
            list_files_remove.add( info.getFile().toString() );
          }

          if ( resultat && add_result_filesname ) {
            // add this folder/file to result files name
            list_add_result.add( HopVfs.getFileObject( fil_name ).toString() );
          }
        }
      } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "JobCopyFiles.Error.Exception.CopyProcess", HopVfs.getFriendlyURI( info
          .getFile() ), HopVfs.getFriendlyURI( fil_name ), e.getMessage() ) );

        resultat = false;
      }

      return resultat;
    }

    public boolean traverseDescendents( FileSelectInfo info ) {
      return ( traverseCount++ == 0 || include_subfolders );
    }
  }

  public void setCopyEmptyFolders( boolean copy_empty_foldersin ) {
    this.copy_empty_folders = copy_empty_foldersin;
  }

  public boolean isCopyEmptyFolders() {
    return copy_empty_folders;
  }

  public void setoverwrite_files( boolean overwrite_filesin ) {
    this.overwrite_files = overwrite_filesin;
  }

  public boolean isoverwrite_files() {
    return overwrite_files;
  }

  public void setIncludeSubfolders( boolean include_subfoldersin ) {
    this.include_subfolders = include_subfoldersin;
  }

  public boolean isIncludeSubfolders() {
    return include_subfolders;
  }

  public void setAddresultfilesname( boolean add_result_filesnamein ) {
    this.add_result_filesname = add_result_filesnamein;
  }

  public boolean isAddresultfilesname() {
    return add_result_filesname;
  }

  public void setArgFromPrevious( boolean argfrompreviousin ) {
    this.arg_from_previous = argfrompreviousin;
  }

  public boolean isArgFromPrevious() {
    return arg_from_previous;
  }

  public void setRemoveSourceFiles( boolean remove_source_filesin ) {
    this.remove_source_files = remove_source_filesin;
  }

  public boolean isRemoveSourceFiles() {
    return remove_source_files;
  }

  public void setDestinationIsAFile( boolean destination_is_a_file ) {
    this.destination_is_a_file = destination_is_a_file;
  }

  public boolean isDestinationIsAFile() {
    return destination_is_a_file;
  }

  public void setCreateDestinationFolder( boolean create_destination_folder ) {
    this.create_destination_folder = create_destination_folder;
  }

  public boolean isCreateDestinationFolder() {
    return create_destination_folder;
  }

  public void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
    boolean res = ActionValidatorUtils.andValidator().validate( this, "arguments", remarks, AndValidator.putValidators( ActionValidatorUtils.notNullValidator() ) );

    if ( !res ) {
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

  public String loadURL( String url, String ncName, IHopMetadataProvider metadataProvider, Map<String, String> mappings ) {
    if ( !Utils.isEmpty( ncName ) && !Utils.isEmpty( url ) ) {
      mappings.put( url, ncName );
    }
    return url;
  }

  public void setConfigurationMappings( Map<String, String> mappings ) {
    this.configurationMappings = mappings;
  }

  public String getConfigurationBy( String url ) {
    return this.configurationMappings.get( url );
  }

  public String getUrlPath( String incomingURL ) {
    String path = null;
    try {
      String noVariablesURL = incomingURL.replaceAll( "[${}]", "/" );
      FileName fileName = HopVfs.getFileSystemManager().resolveURI( noVariablesURL );
      String root = fileName.getRootURI();
      path = incomingURL.substring( root.length() - 1 );
    } catch ( FileSystemException e ) {
      path = null;
    }
    return path;
  }
}
