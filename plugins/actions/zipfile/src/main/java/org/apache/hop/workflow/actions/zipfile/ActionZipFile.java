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

package org.apache.hop.workflow.actions.zipfile;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.AbstractFileValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.FileDoesNotExistValidator;
import org.apache.hop.workflow.action.validator.ValidatorContext;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workarounds.BufferedOutputStreamWithCloseDetection;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.w3c.dom.Node;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * This defines a 'zip file' action. Its main use would be to zip files in a directory and process zipped files
 * (deleted or move).
 *
 * @author Samatar Hassan
 * @since 27-02-2007
 */

@Action(
  id = "ZIP_FILE",
  name = "i18n::ActionZipFile.Name",
  description = "i18n::ActionZipFile.Description",
  image = "Zip.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
  documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/zipfile.html"
)
public class ActionZipFile extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionZipFile.class; // For Translator

  private String zipFilename;
  public int compressionRate;
  public int ifZipFileExists;
  public int afterZip;
  private String wildCard;
  private String excludeWildCard;
  private String sourceDirectory;
  private String movetoDirectory;
  private boolean addFileToResult;
  private boolean isFromPrevious;
  private boolean createParentFolder;
  private boolean addDate;
  private boolean addTime;
  private boolean specifyFormat;
  private String dateTimeFormat;
  private boolean createMoveToDirectory;
  private boolean includingSubFolders;
  private String storedSourcePathDepth;

  /**
   * Default constructor.
   */
  public ActionZipFile( String n ) {
    super( n, "" );
    dateTimeFormat = null;
    zipFilename = null;
    ifZipFileExists = 2;
    afterZip = 0;
    compressionRate = 1;
    wildCard = null;
    excludeWildCard = null;
    sourceDirectory = null;
    movetoDirectory = null;
    addFileToResult = false;
    isFromPrevious = false;
    createParentFolder = false;
    addDate = false;
    addTime = false;
    specifyFormat = false;
    createMoveToDirectory = false;
    includingSubFolders = true;
    storedSourcePathDepth = "1";
  }

  public ActionZipFile() {
    this( "" );
  }

  public Object clone() {
    ActionZipFile je = (ActionZipFile) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 500 );

    retval.append( super.getXml() );
    retval.append( "      " ).append( XmlHandler.addTagValue( "zipfilename", zipFilename ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "compressionrate", compressionRate ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "ifzipfileexists", ifZipFileExists ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "wildcard", wildCard ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "wildcardexclude", excludeWildCard ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "sourcedirectory", sourceDirectory ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "movetodirectory", movetoDirectory ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "afterzip", afterZip ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "addfiletoresult", addFileToResult ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "isfromprevious", isFromPrevious ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "createparentfolder", createParentFolder ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "adddate", addDate ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "addtime", addTime ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "SpecifyFormat", specifyFormat ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "date_time_format", dateTimeFormat ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "createMoveToDirectory", createMoveToDirectory ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "include_subfolders", includingSubFolders ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "stored_source_path_depth", storedSourcePathDepth ) );

    return retval.toString();
  }

  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider, IVariables variables ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      zipFilename = XmlHandler.getTagValue( entrynode, "zipfilename" );
      compressionRate = Const.toInt( XmlHandler.getTagValue( entrynode, "compressionrate" ), -1 );
      ifZipFileExists = Const.toInt( XmlHandler.getTagValue( entrynode, "ifzipfileexists" ), -1 );
      afterZip = Const.toInt( XmlHandler.getTagValue( entrynode, "afterzip" ), -1 );
      wildCard = XmlHandler.getTagValue( entrynode, "wildcard" );
      excludeWildCard = XmlHandler.getTagValue( entrynode, "wildcardexclude" );
      sourceDirectory = XmlHandler.getTagValue( entrynode, "sourcedirectory" );
      movetoDirectory = XmlHandler.getTagValue( entrynode, "movetodirectory" );
      addFileToResult = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "addfiletoresult" ) );
      isFromPrevious = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "isfromprevious" ) );
      createParentFolder = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "createparentfolder" ) );
      addDate = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "adddate" ) );
      addTime = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "addtime" ) );
      specifyFormat = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "SpecifyFormat" ) );
      dateTimeFormat = XmlHandler.getTagValue( entrynode, "date_time_format" );
      createMoveToDirectory = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "createMoveToDirectory" ) );
      includingSubFolders = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "include_subfolders" ) );
      storedSourcePathDepth = XmlHandler.getTagValue( entrynode, "stored_source_path_depth" );
    } catch ( HopXmlException xe ) {
      throw new HopXmlException( BaseMessages.getString( PKG, "ActionZipFile.UnableLoadActionXML" ), xe );
    }
  }

  private boolean createParentFolder( String filename ) {
    // Check for parent folder
    FileObject parentfolder = null;

    boolean result = false;
    try {
      // Get parent folder
      parentfolder = HopVfs.getFileObject( filename ).getParent();

      if ( !parentfolder.exists() ) {
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "ActionZipFile.CanNotFindFolder", ""
            + parentfolder.getName() ) );
        }
        parentfolder.createFolder();
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "ActionZipFile.FolderCreated", "" + parentfolder.getName() ) );
        }
      } else {
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "ActionZipFile.FolderExists", "" + parentfolder.getName() ) );
        }
      }
      result = true;
    } catch ( Exception e ) {
      logError(
        BaseMessages.getString( PKG, "ActionZipFile.CanNotCreateFolder", "" + parentfolder.getName() ), e );
    } finally {
      if ( parentfolder != null ) {
        try {
          parentfolder.close();
        } catch ( Exception ex ) {
          // Ignore
        }
      }
    }
    return result;
  }

  public boolean processRowFile( IWorkflowEngine<WorkflowMeta> parentWorkflow, Result result, String realZipfilename, String realWildcard,
                                 String realWildcardExclude, String realSourceDirectoryOrFile, String realMovetodirectory,
                                 boolean createparentfolder ) {
    boolean Fileexists = false;
    File tempFile = null;
    File fileZip;
    boolean resultat = false;
    boolean renameOk = false;
    boolean orginExist = false;

    // Check if target file/folder exists!
    FileObject originFile = null;
    ZipInputStream zin = null;
    byte[] buffer;
    OutputStream dest = null;
    BufferedOutputStreamWithCloseDetection buff = null;
    ZipOutputStream out = null;
    ZipEntry entry;
    String localSourceFilename = realSourceDirectoryOrFile;

    try {
      originFile = HopVfs.getFileObject( realSourceDirectoryOrFile );
      localSourceFilename = HopVfs.getFilename( originFile );
      orginExist = originFile.exists();
    } catch ( Exception e ) {
      // Ignore errors
    } finally {
      if ( originFile != null ) {
        try {
          originFile.close();
        } catch ( IOException ex ) {
          logError( "Error closing file '" + originFile.toString() + "'", ex );
        }
      }
    }

    String localrealZipfilename = realZipfilename;
    if ( realZipfilename != null && orginExist ) {

      FileObject fileObject = null;
      try {
        fileObject = HopVfs.getFileObject( localrealZipfilename );
        localrealZipfilename = HopVfs.getFilename( fileObject );
        // Check if Zip File exists
        if ( fileObject.exists() ) {
          Fileexists = true;
          if ( log.isDebug() ) {
            logDebug( BaseMessages.getString( PKG, "JobZipFiles.Zip_FileExists1.Label" )
              + localrealZipfilename + BaseMessages.getString( PKG, "JobZipFiles.Zip_FileExists2.Label" ) );
          }
        }
        // Let's see if we need to create parent folder of destination zip filename
        if ( createparentfolder ) {
          createParentFolder( localrealZipfilename );
        }

        // Let's start the process now
        if ( ifZipFileExists == 3 && Fileexists ) {
          // the zip file exists and user want to Fail
          resultat = false;
        } else if ( ifZipFileExists == 2 && Fileexists ) {
          // the zip file exists and user want to do nothing
          if ( addFileToResult ) {
            // Add file to result files name
            ResultFile resultFile =
              new ResultFile( ResultFile.FILE_TYPE_GENERAL, fileObject, parentWorkflow.getWorkflowName(), toString() );
            result.getResultFiles().put( resultFile.getFile().toString(), resultFile );
          }
          resultat = true;
        } else if ( afterZip == 2 && realMovetodirectory == null ) {
          // After Zip, Move files..User must give a destination Folder
          resultat = false;
          logError( BaseMessages.getString( PKG, "JobZipFiles.AfterZip_No_DestinationFolder_Defined.Label" ) );
        } else {
          // After Zip, Move files..User must give a destination Folder

          // Let's see if we deal with file or folder
          FileObject[] fileList;

          FileObject sourceFileOrFolder = HopVfs.getFileObject( localSourceFilename );
          boolean isSourceDirectory = sourceFileOrFolder.getType().equals( FileType.FOLDER );
          final Pattern pattern;
          final Pattern patternExclude;

          if ( isSourceDirectory ) {
            // Let's prepare the pattern matcher for performance reasons.
            // We only do this if the target is a folder !
            //
            if ( !Utils.isEmpty( realWildcard ) ) {
              pattern = Pattern.compile( realWildcard );
            } else {
              pattern = null;
            }
            if ( !Utils.isEmpty( realWildcardExclude ) ) {
              patternExclude = Pattern.compile( realWildcardExclude );
            } else {
              patternExclude = null;
            }

            // Target is a directory
            // Get all the files in the directory...
            //
            if ( includingSubFolders ) {
              fileList = sourceFileOrFolder.findFiles( new ZipJobEntryPatternFileSelector( pattern, patternExclude ) );
            } else {
              fileList = sourceFileOrFolder.getChildren();
            }
          } else {
            pattern = null;
            patternExclude = null;

            // Target is a file
            fileList = new FileObject[] { sourceFileOrFolder };
          }

          if ( fileList.length == 0 ) {
            resultat = false;
            logError( BaseMessages.getString( PKG, "JobZipFiles.Log.FolderIsEmpty", localSourceFilename ) );
          } else if ( !checkContainsFile( localSourceFilename, fileList, isSourceDirectory ) ) {
            resultat = false;
            logError( BaseMessages.getString( PKG, "JobZipFiles.Log.NoFilesInFolder", localSourceFilename ) );
          } else {
            if ( ifZipFileExists == 0 && Fileexists ) {
              // the zip file exists and user want to create new one with unique name
              // Format Date

              // do we have already a .zip at the end?
              if ( localrealZipfilename.toLowerCase().endsWith( ".zip" ) ) {
                // strip this off
                localrealZipfilename = localrealZipfilename.substring( 0, localrealZipfilename.length() - 4 );
              }

              localrealZipfilename += "_" + StringUtil.getFormattedDateTimeNow( true ) + ".zip";
              if ( log.isDebug() ) {
                logDebug( BaseMessages.getString( PKG, "JobZipFiles.Zip_FileNameChange1.Label" )
                  + localrealZipfilename + BaseMessages.getString( PKG, "JobZipFiles.Zip_FileNameChange1.Label" ) );
              }
            } else if ( ifZipFileExists == 1 && Fileexists ) {
              // the zip file exists and user want to append
              // get a temp file
              fileZip = getFile( localrealZipfilename );
              tempFile = File.createTempFile( fileZip.getName(), null );

              // delete it, otherwise we cannot rename existing zip to it.
              tempFile.delete();

              renameOk = fileZip.renameTo( tempFile );

              if ( !renameOk ) {
                logError( BaseMessages.getString( PKG, "JobZipFiles.Cant_Rename_Temp1.Label" )
                  + fileZip.getAbsolutePath()
                  + BaseMessages.getString( PKG, "JobZipFiles.Cant_Rename_Temp2.Label" )
                  + tempFile.getAbsolutePath()
                  + BaseMessages.getString( PKG, "JobZipFiles.Cant_Rename_Temp3.Label" ) );
              }
              if ( log.isDebug() ) {
                logDebug( BaseMessages.getString( PKG, "JobZipFiles.Zip_FileAppend1.Label" )
                  + localrealZipfilename + BaseMessages.getString( PKG, "JobZipFiles.Zip_FileAppend2.Label" ) );
              }
            }

            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "JobZipFiles.Files_Found1.Label" )
                + fileList.length + BaseMessages.getString( PKG, "JobZipFiles.Files_Found2.Label" )
                + localSourceFilename + BaseMessages.getString( PKG, "JobZipFiles.Files_Found3.Label" ) );
            }

            // Prepare Zip File
            buffer = new byte[ 18024 ];
            dest = HopVfs.getOutputStream( localrealZipfilename, false );
            buff = new BufferedOutputStreamWithCloseDetection( dest );
            out = new ZipOutputStream( buff );

            HashSet<String> fileSet = new HashSet<>();

            if ( renameOk ) {
              // User want to append files to existing Zip file
              // The idea is to rename the existing zip file to a temporary file
              // and then adds all entries in the existing zip along with the new files,
              // excluding the zip entries that have the same name as one of the new files.

              zin = new ZipInputStream( new FileInputStream( tempFile ) );
              entry = zin.getNextEntry();

              while ( entry != null ) {
                String name = entry.getName();

                if ( !fileSet.contains( name ) ) {

                  // Add ZIP entry to output stream.
                  out.putNextEntry( new ZipEntry( name ) );
                  // Transfer bytes from the ZIP file to the output file
                  int len;
                  while ( ( len = zin.read( buffer ) ) > 0 ) {
                    out.write( buffer, 0, len );
                  }

                  fileSet.add( name );
                }
                entry = zin.getNextEntry();
              }
              // Close the streams
              zin.close();
            }

            // Set the method
            out.setMethod( ZipOutputStream.DEFLATED );
            // Set the compression level
            if ( compressionRate == 0 ) {
              out.setLevel( Deflater.NO_COMPRESSION );
            } else if ( compressionRate == 1 ) {
              out.setLevel( Deflater.DEFAULT_COMPRESSION );
            }
            if ( compressionRate == 2 ) {
              out.setLevel( Deflater.BEST_COMPRESSION );
            }
            if ( compressionRate == 3 ) {
              out.setLevel( Deflater.BEST_SPEED );
            }
            // Specify Zipped files (After that we will move,delete them...)
            FileObject[] zippedFiles = new FileObject[ fileList.length ];
            int fileNum = 0;

            // Get the files in the list...
            for ( int i = 0; i < fileList.length && !parentWorkflow.isStopped(); i++ ) {
              boolean getIt = true;
              boolean getItexclude = false;

              // First see if the file matches the regular expression!
              // ..only if target is a folder !
              if ( isSourceDirectory ) {
                // If we include sub-folders, we match on the whole name, not just the basename
                //
                String filename;
                if ( includingSubFolders ) {
                  filename = fileList[ i ].getName().getPath();
                } else {
                  filename = fileList[ i ].getName().getBaseName();
                }
                if ( pattern != null ) {
                  // Matches the base name of the file (backward compatible!)
                  //
                  Matcher matcher = pattern.matcher( filename );
                  getIt = matcher.matches();
                }

                if ( patternExclude != null ) {
                  Matcher matcherexclude = patternExclude.matcher( filename );
                  getItexclude = matcherexclude.matches();
                }
              }

              // Get processing File
              String targetFilename = HopVfs.getFilename( fileList[ i ] );
              if ( sourceFileOrFolder.getType().equals( FileType.FILE ) ) {
                targetFilename = localSourceFilename;
              }

              FileObject file = HopVfs.getFileObject( targetFilename );
              boolean isTargetDirectory = file.exists() && file.getType().equals( FileType.FOLDER );

              if ( getIt && !getItexclude && !isTargetDirectory && !fileSet.contains( targetFilename ) ) {
                // We can add the file to the Zip Archive
                if ( log.isDebug() ) {
                  logDebug( BaseMessages.getString( PKG, "JobZipFiles.Add_FilesToZip1.Label" )
                    + fileList[ i ] + BaseMessages.getString( PKG, "JobZipFiles.Add_FilesToZip2.Label" )
                    + localSourceFilename + BaseMessages.getString( PKG, "JobZipFiles.Add_FilesToZip3.Label" ) );
                }

                // Associate a file input stream for the current file
                InputStream in = HopVfs.getInputStream( file );

                // Add ZIP entry to output stream.
                //
                String relativeName;
                String fullName = fileList[ i ].getName().getPath();
                String basePath = sourceFileOrFolder.getName().getPath();
                if ( isSourceDirectory ) {
                  if ( fullName.startsWith( basePath ) ) {
                    relativeName = fullName.substring( basePath.length() + 1 );
                  } else {
                    relativeName = fullName;
                  }
                } else if ( isFromPrevious ) {
                  int depth = determineDepth( resolve( storedSourcePathDepth ) );
                  relativeName = determineZipfilenameForDepth( fullName, depth );
                } else {
                  relativeName = fileList[ i ].getName().getBaseName();
                }
                out.putNextEntry( new ZipEntry( relativeName ) );

                int len;
                while ( ( len = in.read( buffer ) ) > 0 ) {
                  out.write( buffer, 0, len );
                }
                out.flush();
                out.closeEntry();

                // Close the current file input stream
                in.close();

                // Get Zipped File
                zippedFiles[ fileNum ] = fileList[ i ];
                fileNum = fileNum + 1;
              }
            }
            // Close the ZipOutPutStream
            out.close();
            buff.close();
            dest.close();

            if ( log.isBasic() ) {
              logBasic( BaseMessages.getString( PKG, "JobZipFiles.Log.TotalZippedFiles", "" + zippedFiles.length ) );
            }
            // Delete Temp File
            if ( tempFile != null ) {
              tempFile.delete();
            }

            // -----Get the list of Zipped Files and Move or Delete Them
            if ( afterZip == 1 || afterZip == 2 ) {
              // iterate through the array of Zipped files
              for ( int i = 0; i < zippedFiles.length; i++ ) {
                if ( zippedFiles[ i ] != null ) {
                  // Delete, Move File
                  FileObject fileObjectd = zippedFiles[ i ];
                  if ( !isSourceDirectory ) {
                    fileObjectd = HopVfs.getFileObject( localSourceFilename );
                  }

                  // Here we can move, delete files
                  if ( afterZip == 1 ) {
                    // Delete File
                    boolean deleted = fileObjectd.delete();
                    if ( !deleted ) {
                      resultat = false;
                      logError( BaseMessages.getString( PKG, "JobZipFiles.Cant_Delete_File1.Label" )
                        + localSourceFilename + Const.FILE_SEPARATOR + zippedFiles[ i ]
                        + BaseMessages.getString( PKG, "JobZipFiles.Cant_Delete_File2.Label" ) );

                    }
                    // File deleted
                    if ( log.isDebug() ) {
                      logDebug( BaseMessages.getString( PKG, "JobZipFiles.File_Deleted1.Label" )
                        + localSourceFilename + Const.FILE_SEPARATOR + zippedFiles[ i ]
                        + BaseMessages.getString( PKG, "JobZipFiles.File_Deleted2.Label" ) );
                    }
                  } else if ( afterZip == 2 ) {
                    // Move File
                    FileObject fileObjectm = null;
                    try {
                      fileObjectm =
                        HopVfs.getFileObject( realMovetodirectory
                          + Const.FILE_SEPARATOR + fileObjectd.getName().getBaseName() );
                      fileObjectd.moveTo( fileObjectm );
                    } catch ( IOException e ) {
                      logError( BaseMessages.getString( PKG, "JobZipFiles.Cant_Move_File1.Label" )
                        + zippedFiles[ i ] + BaseMessages.getString( PKG, "JobZipFiles.Cant_Move_File2.Label" )
                        + e.getMessage() );
                      resultat = false;
                    } finally {
                      try {
                        if ( fileObjectm != null ) {
                          fileObjectm.close();
                        }
                      } catch ( Exception e ) {
                        if ( fileObjectm != null ) {
                          logError( "Error closing file '" + fileObjectm.toString() + "'", e );
                        }
                      }
                    }
                    // File moved
                    if ( log.isDebug() ) {
                      logDebug( BaseMessages.getString( PKG, "JobZipFiles.File_Moved1.Label" )
                        + zippedFiles[ i ] + BaseMessages.getString( PKG, "JobZipFiles.File_Moved2.Label" ) );
                    }
                  }
                }
              }
            }

            if ( addFileToResult ) {
              // Add file to result files name
              ResultFile resultFile =
                new ResultFile( ResultFile.FILE_TYPE_GENERAL, fileObject, parentWorkflow.getWorkflowName(), toString() );
              result.getResultFiles().put( resultFile.getFile().toString(), resultFile );
            }

            resultat = true;
          }
        }
      } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "JobZipFiles.Cant_CreateZipFile1.Label" )
          + localrealZipfilename + BaseMessages.getString( PKG, "JobZipFiles.Cant_CreateZipFile2.Label" ), e );
        resultat = false;
      } finally {
        if ( fileObject != null ) {
          try {
            fileObject.close();
            fileObject = null;
          } catch ( IOException ex ) {
            logError( "Error closing file '" + fileObject.toString() + "'", ex );
          }
        }

        try {
          if ( out != null ) {
            out.close();
          }
          if ( buff != null ) {
            buff.close();
          }
          if ( dest != null ) {
            dest.close();
          }
          if ( zin != null ) {
            zin.close();
          }

        } catch ( IOException ex ) {
          logError( "Error closing zip file entry for file '" + originFile.toString() + "'", ex );
        }
      }
    } else {
      resultat = false;
      if ( localrealZipfilename == null ) {
        logError( BaseMessages.getString( PKG, "JobZipFiles.No_ZipFile_Defined.Label" ) );
      }
      if ( !orginExist ) {
        logError( BaseMessages.getString( PKG, "JobZipFiles.No_FolderCible_Defined.Label", localSourceFilename ) );
      }
    }
    // return a verifier
    return resultat;
  }

  private int determineDepth( String depthString ) throws HopException {
    DecimalFormat df = new DecimalFormat( "0" );
    ParsePosition pp = new ParsePosition( 0 );
    df.setParseIntegerOnly( true );
    try {
      Number n = df.parse( depthString, pp );
      if ( n == null ) {
        return 1; // default
      }
      if ( pp.getErrorIndex() == 0 ) {
        throw new HopException( "Unable to convert stored depth '"
          + depthString + "' to depth at position " + pp.getErrorIndex() );
      }
      return n.intValue();
    } catch ( Exception e ) {
      throw new HopException( "Unable to convert stored depth '" + depthString + "' to depth", e );
    }
  }

  /**
   * Get the requested part of the filename
   *
   * @param filename the filename (full) (/path/to/a/file.txt)
   * @param depth    the depth to get. 0 means: the complete filename, 1: the name only (file.txt), 2: one folder (a/file.txt)
   *                 3: two folders (to/a/file.txt) and so on.
   * @return the requested part of the file name up to a certain depth
   * @throws HopFileException
   */
  private String determineZipfilenameForDepth( String filename, int depth ) throws HopException {
    try {
      if ( Utils.isEmpty( filename ) ) {
        return null;
      }
      if ( depth == 0 ) {
        return filename;
      }
      FileObject fileObject = HopVfs.getFileObject( filename );
      FileObject folder = fileObject.getParent();
      String baseName = fileObject.getName().getBaseName();
      if ( depth == 1 ) {
        return baseName;
      }
      StringBuilder path = new StringBuilder( baseName );
      int d = 1;
      while ( d < depth && folder != null ) {
        path.insert( 0, '/' );
        path.insert( 0, folder.getName().getBaseName() );
        folder = folder.getParent();
        d++;
      }
      return path.toString();
    } catch ( Exception e ) {
      throw new HopException( "Unable to get zip filename '" + filename + "' to depth " + depth, e );
    }
  }

  private File getFile( final String filename ) {
    try {
      String uri = HopVfs.getFileObject( resolve( filename ) ).getName().getPath();
      return new File( uri );
    } catch ( HopFileException ex ) {
      logError( "Error in Fetching URI for File: " + filename, ex );
    }
    return new File( filename );
  }

  private boolean checkContainsFile( String realSourceDirectoryOrFile, FileObject[] filelist, boolean isDirectory ) throws FileSystemException {
    boolean retval = false;
    for ( int i = 0; i < filelist.length; i++ ) {
      FileObject file = filelist[ i ];
      if ( ( file.exists() && file.getType().equals( FileType.FILE ) ) ) {
        retval = true;
      }
    }
    return retval;
  }

  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    List<RowMetaAndData> rows = result.getRows();

    // reset values
    String realZipfilename;
    String realWildcard = null;
    String realWildcardExclude = null;
    String realTargetdirectory;
    String realMovetodirectory = resolve( movetoDirectory );

    // Sanity check
    boolean SanityControlOK = true;

    if ( afterZip == 2 ) {
      if ( Utils.isEmpty( realMovetodirectory ) ) {
        SanityControlOK = false;
        logError( BaseMessages.getString( PKG, "JobZipFiles.AfterZip_No_DestinationFolder_Defined.Label" ) );
      } else {
        FileObject moveToDirectory = null;
        try {
          moveToDirectory = HopVfs.getFileObject( realMovetodirectory );
          if ( moveToDirectory.exists() ) {
            if ( moveToDirectory.getType() == FileType.FOLDER ) {
              if ( log.isDetailed() ) {
                logDetailed( BaseMessages
                  .getString( PKG, "JobZipFiles.Log.MoveToFolderExist", realMovetodirectory ) );
              }
            } else {
              SanityControlOK = false;
              logError( BaseMessages.getString( PKG, "JobZipFiles.Log.MoveToFolderNotFolder", realMovetodirectory ) );
            }
          } else {
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString(
                PKG, "JobZipFiles.Log.MoveToFolderNotNotExist", realMovetodirectory ) );
            }
            if ( createMoveToDirectory ) {
              moveToDirectory.createFolder();
              if ( log.isDetailed() ) {
                logDetailed( BaseMessages.getString(
                  PKG, "JobZipFiles.Log.MoveToFolderCreaterd", realMovetodirectory ) );
              }
            } else {
              SanityControlOK = false;
              logError( BaseMessages.getString(
                PKG, "JobZipFiles.Log.MoveToFolderNotNotExist", realMovetodirectory ) );
            }
          }
        } catch ( Exception e ) {
          SanityControlOK = false;
          logError( BaseMessages
            .getString( PKG, "JobZipFiles.ErrorGettingMoveToFolder.Label", realMovetodirectory ), e );
        } finally {
          if ( moveToDirectory != null ) {
            realMovetodirectory = HopVfs.getFilename( moveToDirectory );
            try {
              moveToDirectory.close();
            } catch ( Exception e ) {
              logError( "Error moving to directory", e );
              SanityControlOK = false;
            }
          }
        }
      }
    }

    if ( !SanityControlOK ) {
      return errorResult( result );
    }

    // arguments from previous

    if ( isFromPrevious ) {
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "JobZipFiles.ArgFromPrevious.Found", ( rows != null ? rows
          .size() : 0 )
          + "" ) );
      }
    }
    if ( isFromPrevious && rows != null ) {
      try {
        for ( int iteration = 0; iteration < rows.size() && !parentWorkflow.isStopped(); iteration++ ) {
          // get arguments from previous action
          RowMetaAndData resultRow = rows.get( iteration );
          // get target directory
          realTargetdirectory = resultRow.getString( 0, null );
          if ( !Utils.isEmpty( realTargetdirectory ) ) {
            // get wildcard to include
            if ( !Utils.isEmpty( resultRow.getString( 1, null ) ) ) {
              realWildcard = resultRow.getString( 1, null );
            }
            // get wildcard to exclude
            if ( !Utils.isEmpty( resultRow.getString( 2, null ) ) ) {
              realWildcardExclude = resultRow.getString( 2, null );
            }

            // get destination zip file
            realZipfilename = resultRow.getString( 3, null );
            if ( !Utils.isEmpty( realZipfilename ) ) {
              if ( !processRowFile( parentWorkflow, result, realZipfilename, realWildcard, realWildcardExclude, realTargetdirectory,
                realMovetodirectory, createParentFolder ) ) {
                return errorResult( result );
              }
            } else {
              logError( "destination zip filename is empty! Ignoring row..." );
            }
          } else {
            logError( "Target directory is empty! Ignoring row..." );
          }
        }
      } catch ( Exception e ) {
        logError( "Erreur during process!", e );
        result.setResult( false );
        result.setNrErrors( 1 );
      }
    } else if ( !isFromPrevious ) {
      if ( !Utils.isEmpty( sourceDirectory ) ) {
        // get values from action
        realZipfilename =
          getFullFilename( resolve( zipFilename ), addDate, addTime, specifyFormat, dateTimeFormat );
        realWildcard = resolve( wildCard );
        realWildcardExclude = resolve( excludeWildCard );
        realTargetdirectory = resolve( sourceDirectory );

        boolean success = processRowFile( parentWorkflow, result, realZipfilename, realWildcard, realWildcardExclude,
          realTargetdirectory, realMovetodirectory, createParentFolder );
        if ( success ) {
          result.setResult( true );
        } else {
          errorResult( result );
        }
      } else {
        logError( "Source folder/file is empty! Ignoring row..." );
      }
    }

    // End
    return result;
  }

  private Result errorResult( Result result ) {
    result.setNrErrors( 1 );
    result.setResult( false );
    return result;
  }

  public String getFullFilename( String filename, boolean addDate, boolean addTime, boolean specifyFormat,
                                 String datetimeFolder ) {
    String retval;
    if ( Utils.isEmpty( filename ) ) {
      return null;
    }

    // Replace possible environment variables...
    String realfilename = resolve( filename );
    int lenstring = realfilename.length();
    int lastindexOfDot = realfilename.lastIndexOf( '.' );
    if ( lastindexOfDot == -1 ) {
      lastindexOfDot = lenstring;
    }

    retval = realfilename.substring( 0, lastindexOfDot );

    final SimpleDateFormat daf = new SimpleDateFormat();
    Date now = new Date();

    if ( specifyFormat && !Utils.isEmpty( datetimeFolder ) ) {
      daf.applyPattern( datetimeFolder );
      String dt = daf.format( now );
      retval += dt;
    } else {
      if ( addDate ) {
        daf.applyPattern( "yyyyMMdd" );
        String d = daf.format( now );
        retval += "_" + d;
      }
      if ( addTime ) {
        daf.applyPattern( "HHmmssSSS" );
        String t = daf.format( now );
        retval += "_" + t;
      }
    }
    retval += realfilename.substring( lastindexOfDot, lenstring );
    return retval;

  }

  @Override public boolean isEvaluation() {
    return true;
  }

  public void setZipFilename( String zipFilename ) {
    this.zipFilename = zipFilename;
  }

  public void setWildcard( String wildcard ) {
    this.wildCard = wildcard;
  }

  public void setWildcardExclude( String wildcardExclude ) {
    this.excludeWildCard = wildcardExclude;
  }

  public void setSourceDirectory( String sourcedirectory ) {
    this.sourceDirectory = sourcedirectory;
  }

  public void setMoveToDirectory( String movetodirectory ) {
    this.movetoDirectory = movetodirectory;
  }

  public String getSourceDirectory() {
    return sourceDirectory;
  }

  public String getMoveToDirectory() {
    return movetoDirectory;
  }

  public String getZipFilename() {
    return zipFilename;
  }

  public boolean isCreateMoveToDirectory() {
    return createMoveToDirectory;
  }

  public void setCreateMoveToDirectory( boolean createMoveToDirectory ) {
    this.createMoveToDirectory = createMoveToDirectory;
  }

  public String getWildcard() {
    return wildCard;
  }

  public String getWildcardExclude() {
    return excludeWildCard;
  }

  public void setAddFileToResult( boolean addfiletoresultin ) {
    this.addFileToResult = addfiletoresultin;
  }

  public boolean isAddFileToResult() {
    return addFileToResult;
  }

  public void setcreateparentfolder( boolean createparentfolder ) {
    this.createParentFolder = createparentfolder;
  }

  public void setDateInFilename( boolean adddate ) {
    this.addDate = adddate;
  }

  public boolean isDateInFilename() {
    return addDate;
  }

  public void setTimeInFilename( boolean addtime ) {
    this.addTime = addtime;
  }

  public boolean isTimeInFilename() {
    return addTime;
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

  public boolean getcreateparentfolder() {
    return createParentFolder;
  }

  public void setDatafromprevious( boolean isfromprevious ) {
    this.isFromPrevious = isfromprevious;
  }

  public boolean getDatafromprevious() {
    return isFromPrevious;
  }

  @Override
  public void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
    ValidatorContext ctx1 = new ValidatorContext();
    AbstractFileValidator.putVariableSpace( ctx1, getVariables() );
    AndValidator.putValidators( ctx1, ActionValidatorUtils.notBlankValidator(), ActionValidatorUtils.fileDoesNotExistValidator() );
    if ( 3 == ifZipFileExists ) {
      // execute method fails if the file already exists; we should too
      FileDoesNotExistValidator.putFailIfExists( ctx1, true );
    }
    ActionValidatorUtils.andValidator().validate( this, "zipFilename", remarks, ctx1 );

    if ( 2 == afterZip ) {
      // setting says to move
      ActionValidatorUtils.andValidator().validate( this, "moveToDirectory", remarks, AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
    }

    ActionValidatorUtils.andValidator().validate( this, "sourceDirectory", remarks, AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );

  }

  /**
   * @return true if the search for files to zip in a folder include sub-folders
   */
  public boolean isIncludingSubFolders() {
    return includingSubFolders;
  }

  /**
   * @param includesSubFolders Set to true if the search for files to zip in a folder needs to include sub-folders
   */
  public void setIncludingSubFolders( boolean includesSubFolders ) {
    this.includingSubFolders = includesSubFolders;
  }

  public String getStoredSourcePathDepth() {
    return storedSourcePathDepth;
  }

  public void setStoredSourcePathDepth( String storedSourcePathDepth ) {
    this.storedSourcePathDepth = storedSourcePathDepth;
  }

  /**
   * Helper class providing pattern restrictions for
   * file names to be zipped
   */
  public static class ZipJobEntryPatternFileSelector implements FileSelector {

    private Pattern pattern;
    private Pattern patternExclude;

    public ZipJobEntryPatternFileSelector( Pattern pattern, Pattern patternExclude ) {
      this.pattern = pattern;
      this.patternExclude = patternExclude;
    }

    public boolean traverseDescendents( FileSelectInfo fileInfo ) throws Exception {
      return true;
    }

    public boolean includeFile( FileSelectInfo fileInfo ) throws Exception {
      boolean include;

      // Only include files in the sub-folders...
      // When we include sub-folders we match the whole filename, not just the base-name
      //
      if ( fileInfo.getFile().getType().equals( FileType.FILE ) ) {
        include = true;
        if ( pattern != null ) {
          String name = fileInfo.getFile().getName().getBaseName();
          include = pattern.matcher( name ).matches();
        }
        if ( include && patternExclude != null ) {
          String name = fileInfo.getFile().getName().getBaseName();
          include = !patternExclude.matcher( name ).matches();
        }
      } else {
        include = false;
      }
      return include;
    }
  }

}
