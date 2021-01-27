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

package org.apache.hop.workflow.actions.folderscompare;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopFileException;
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
import org.w3c.dom.Node;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This defines a 'folder compare' action. It will compare 2 folders, and will either follow the true flow upon the
 * files being the same or the false flow otherwise.
 *
 * @author Samatar Hassan
 * @since 25-11-2007
 */

@Action(
  id = "FOLDERS_COMPARE",
  name = "i18n::ActionFoldersCompare.Name",
  description = "i18n::ActionFoldersCompare.Description",
  image = "FoldersCompare.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
  documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/folderscompare.html"
)
public class ActionFoldersCompare extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionFoldersCompare.class; // For Translator

  private String filename1;
  private String filename2;
  private String wildcard;
  private String compareonly;
  private boolean includesubfolders;
  private boolean comparefilecontent;
  private boolean comparefilesize;

  public ActionFoldersCompare( String n ) {

    super( n, "" );
    includesubfolders = false;
    comparefilesize = false;
    comparefilecontent = false;
    compareonly = "all";
    wildcard = null;
    filename1 = null;
    filename2 = null;
  }

  public void setCompareOnly( String comparevalue ) {
    this.compareonly = comparevalue;
  }

  public String getCompareOnly() {
    return compareonly;
  }

  public ActionFoldersCompare() {
    this( "" );
  }

  public Object clone() {
    ActionFoldersCompare je = (ActionFoldersCompare) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 200 ); // 133 chars in just spaces and tag names alone

    retval.append( super.getXml() );
    retval.append( "      " ).append( XmlHandler.addTagValue( "include_subfolders", includesubfolders ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "compare_filecontent", comparefilecontent ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "compare_filesize", comparefilesize ) );

    retval.append( "      " ).append( XmlHandler.addTagValue( "compareonly", compareonly ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "wildcard", wildcard ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "filename1", filename1 ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "filename2", filename2 ) );

    return retval.toString();
  }

  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider, IVariables variables ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      includesubfolders = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "include_subfolders" ) );
      comparefilecontent = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "compare_filecontent" ) );
      comparefilesize = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "compare_filesize" ) );

      compareonly = XmlHandler.getTagValue( entrynode, "compareonly" );
      wildcard = XmlHandler.getTagValue( entrynode, "wildcard" );
      filename1 = XmlHandler.getTagValue( entrynode, "filename1" );
      filename2 = XmlHandler.getTagValue( entrynode, "filename2" );
    } catch ( HopXmlException xe ) {
      throw new HopXmlException( BaseMessages.getString( PKG, "JobFoldersCompare.Meta.UnableLoadXML", xe
        .getMessage() ) );
    }
  }

  public void setIncludeSubfolders( boolean includeSubfolders ) {
    this.includesubfolders = includeSubfolders;
  }

  public boolean isIncludeSubfolders() {
    return includesubfolders;
  }

  public void setCompareFileContent( boolean comparefilecontent ) {
    this.comparefilecontent = comparefilecontent;
  }

  public boolean isCompareFileContent() {
    return comparefilecontent;
  }

  public void setCompareFileSize( boolean comparefilesize ) {
    this.comparefilesize = comparefilesize;
  }

  public boolean isCompareFileSize() {
    return comparefilesize;
  }

  public String getRealWildcard() {
    return resolve( getWildcard() );
  }

  public String getRealFilename1() {
    return resolve( getFilename1() );
  }

  public String getRealFilename2() {
    return resolve( getFilename2() );
  }

  /**
   * Check whether 2 files have the same contents.
   *
   * @param file1 first file to compare
   * @param file2 second file to compare
   * @return true if files are equal, false if they are not
   * @throws IOException upon IO problems
   */
  protected boolean equalFileContents( FileObject file1, FileObject file2 ) throws HopFileException {
    // Really read the contents and do comparisons
    DataInputStream in1 = null;
    DataInputStream in2 = null;
    try {
      // Really read the contents and do comparisons

      in1 = new DataInputStream( new BufferedInputStream( HopVfs.getInputStream( HopVfs.getFilename( file1 ) ) ) );
      in2 = new DataInputStream( new BufferedInputStream( HopVfs.getInputStream( HopVfs.getFilename( file2 ) ) ) );

      char ch1, ch2;
      while ( in1.available() != 0 && in2.available() != 0 ) {
        ch1 = (char) in1.readByte();
        ch2 = (char) in2.readByte();
        if ( ch1 != ch2 ) {
          return false;
        }
      }
      if ( in1.available() != in2.available() ) {
        return false;
      } else {
        return true;
      }
    } catch ( IOException e ) {
      throw new HopFileException( e );
    } finally {
      if ( in1 != null ) {
        try {
          in1.close();
        } catch ( IOException ignored ) {
          // Nothing to see here...
        }
      }
      if ( in2 != null ) {
        try {
          in2.close();
        } catch ( Exception ignored ) {
          // We can't do anything else here...
        }
      }
    }
  }

  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    result.setResult( false );
    boolean ok = true;

    String realFilename1 = getRealFilename1();
    String realFilename2 = getRealFilename2();

    FileObject folder1 = null;
    FileObject folder2 = null;
    FileObject filefolder1 = null;
    FileObject filefolder2 = null;

    try {
      if ( filename1 != null && filename2 != null ) {
        // Get Folders/Files to compare
        folder1 = HopVfs.getFileObject( realFilename1 );
        folder2 = HopVfs.getFileObject( realFilename2 );

        if ( folder1.exists() && folder2.exists() ) {
          if ( !folder1.getType().equals( folder2.getType() ) ) {
            // pb...we try to compare file with folder !!!
            logError( BaseMessages.getString( PKG, "JobFoldersCompare.Log.CanNotCompareFilesFolders" ) );

            if ( folder1.getType() == FileType.FILE ) {
              logError( BaseMessages.getString( PKG, "JobFoldersCompare.Log.IsAFile", realFilename1 ) );
            } else if ( folder1.getType() == FileType.FOLDER ) {
              logError( BaseMessages.getString( PKG, "JobFoldersCompare.Log.IsAFolder", realFilename1 ) );
            } else {
              logError( BaseMessages.getString( PKG, "JobFoldersCompare.Log.IsUnknownFileType", realFilename1 ) );
            }

            if ( folder2.getType() == FileType.FILE ) {
              logError( BaseMessages.getString( PKG, "JobFoldersCompare.Log.IsAFile", realFilename2 ) );
            } else if ( folder2.getType() == FileType.FOLDER ) {
              logError( BaseMessages.getString( PKG, "JobFoldersCompare.Log.IsAFolder", realFilename2 ) );
            } else {
              logError( BaseMessages.getString( PKG, "JobFoldersCompare.Log.IsUnknownFileType", realFilename2 ) );
            }

          } else {
            if ( folder1.getType() == FileType.FILE ) {
              // simply compare 2 files ..
              if ( equalFileContents( folder1, folder2 ) ) {
                result.setResult( true );
              } else {
                result.setResult( false );
              }
            } else if ( folder1.getType() == FileType.FOLDER ) {
              // We compare 2 folders ...

              FileObject[] list1 = folder1.findFiles( new TextFileSelector( folder1.toString() ) );
              FileObject[] list2 = folder2.findFiles( new TextFileSelector( folder2.toString() ) );

              int lenList1 = list1.length;
              int lenList2 = list2.length;

              if ( log.isDetailed() ) {
                logDetailed( BaseMessages.getString(
                  PKG, "JobFoldersCompare.Log.FolderContains", realFilename1, "" + lenList1 ) );
                logDetailed( BaseMessages.getString(
                  PKG, "JobFoldersCompare.Log.FolderContains", realFilename2, "" + lenList2 ) );
              }
              if ( lenList1 == lenList2 ) {

                HashMap<String, String> collection1 = new HashMap<>();
                HashMap<String, String> collection2 = new HashMap<>();

                for ( int i = 0; i < list1.length; i++ ) {
                  // Put files list1 in TreeMap collection1
                  collection1.put( list1[ i ].getName().getBaseName(), list1[ i ].toString() );
                }

                for ( int i = 0; i < list2.length; i++ ) {
                  // Put files list2 in TreeMap collection2
                  collection2.put( list2[ i ].getName().getBaseName(), list2[ i ].toString() );
                }

                // Let's now fetch Folder1
                // and for each entry, we will search it in Folder2
                // if the entry exists..we will compare file entry (file or folder?)
                // if the 2 entry are file (not folder), we will compare content
                Set<Map.Entry<String, String>> entrees = collection1.entrySet();
                Iterator<Map.Entry<String, String>> iterateur = entrees.iterator();

                while ( iterateur.hasNext() ) {
                  Map.Entry<String, String> entree = iterateur.next();
                  if ( !collection2.containsKey( entree.getKey() ) ) {
                    ok = false;
                    if ( log.isDetailed() ) {
                      logDetailed( BaseMessages.getString(
                        PKG, "JobFoldersCompare.Log.FileCanNotBeFoundIn", entree.getKey().toString(),
                        realFilename2 ) );
                    }
                  } else {
                    if ( log.isDebug() ) {
                      logDebug( BaseMessages.getString( PKG, "JobFoldersCompare.Log.FileIsFoundIn", entree
                        .getKey().toString(), realFilename2 ) );
                    }

                    filefolder1 = HopVfs.getFileObject( entree.getValue() );
                    filefolder2 = HopVfs.getFileObject( collection2.get( entree.getKey() ) );

                    if ( !filefolder2.getType().equals( filefolder1.getType() ) ) {
                      // The file1 exist in the folder2..but they don't have the same type
                      ok = false;
                      if ( log.isDetailed() ) {
                        logDetailed( BaseMessages.getString(
                          PKG, "JobFoldersCompare.Log.FilesNotSameType", filefolder1.toString(), filefolder2
                            .toString() ) );
                      }

                      if ( filefolder1.getType() == FileType.FILE ) {
                        logError( BaseMessages.getString( PKG, "JobFoldersCompare.Log.IsAFile", filefolder1
                          .toString() ) );
                      } else if ( filefolder1.getType() == FileType.FOLDER ) {
                        logError( BaseMessages.getString( PKG, "JobFoldersCompare.Log.IsAFolder", filefolder1
                          .toString() ) );
                      } else {
                        logError( BaseMessages.getString(
                          PKG, "JobFoldersCompare.Log.IsUnknownFileType", filefolder1.toString() ) );
                      }

                      if ( filefolder2.getType() == FileType.FILE ) {
                        logError( BaseMessages.getString( PKG, "JobFoldersCompare.Log.IsAFile", filefolder2
                          .toString() ) );
                      } else if ( filefolder2.getType() == FileType.FOLDER ) {
                        logError( BaseMessages.getString( PKG, "JobFoldersCompare.Log.IsAFolder", filefolder2
                          .toString() ) );
                      } else {
                        logError( BaseMessages.getString(
                          PKG, "JobFoldersCompare.Log.IsUnknownFileType", filefolder2.toString() ) );
                      }

                    } else {
                      // Files are the same type ...
                      if ( filefolder2.getType() == FileType.FILE ) {
                        // Let's compare file size
                        if ( comparefilesize ) {
                          long filefolder1_size = filefolder1.getContent().getSize();
                          long filefolder2_size = filefolder2.getContent().getSize();
                          if ( filefolder1_size != filefolder2_size ) {
                            ok = false;
                            if ( log.isDetailed() ) {
                              logDetailed( BaseMessages.getString(
                                PKG, "JobFoldersCompare.Log.FilesNotSameSize", filefolder1.toString(),
                                filefolder2.toString() ) );
                              logDetailed( BaseMessages.getString(
                                PKG, "JobFoldersCompare.Log.SizeFileIs", filefolder1.toString(), ""
                                  + filefolder1_size ) );
                              logDetailed( BaseMessages.getString(
                                PKG, "JobFoldersCompare.Log.SizeFileIs", filefolder2.toString(), ""
                                  + filefolder2_size ) );
                            }
                          }
                        }

                        if ( ok ) {
                          // Let's compare files content..
                          if ( comparefilecontent ) {
                            if ( !equalFileContents( filefolder1, filefolder2 ) ) {
                              ok = false;
                              if ( log.isDetailed() ) {
                                logDetailed( BaseMessages.getString(
                                  PKG, "JobFoldersCompare.Log.FilesNotSameContent", filefolder1.toString(),
                                  filefolder2.toString() ) );
                              }
                            }
                          }
                        }
                      }
                    }

                  }
                  // logBasic(entree.getKey() + " - " + entree.getValue());
                }

                result.setResult( ok );
              } else {
                // The 2 folders don't have the same files number
                if ( log.isDetailed() ) {
                  logDetailed( BaseMessages.getString(
                    PKG, "JobFoldersCompare.Log.FoldersDifferentFiles", realFilename1.toString(), realFilename2
                      .toString() ) );
                }
              }

            }
            // else: File type unknown !!
          }

        } else {
          if ( !folder1.exists() ) {
            logError( BaseMessages.getString( PKG, "JobFileCompare.Log.FileNotExist", realFilename1 ) );
          }
          if ( !folder2.exists() ) {
            logError( BaseMessages.getString( PKG, "JobFileCompare.Log.FileNotExist", realFilename2 ) );
          }
          result.setResult( false );
          result.setNrErrors( 1 );
        }
      } else {
        logError( BaseMessages.getString( PKG, "JobFoldersCompare.Log.Need2Files" ) );
      }
    } catch ( Exception e ) {
      result.setResult( false );
      result.setNrErrors( 1 );
      logError( BaseMessages.getString(
        PKG, "JobFoldersCompare.Log.ErrorComparing", realFilename2, realFilename2, e.getMessage() ) );
    } finally {
      try {
        if ( folder1 != null ) {
          folder1.close();
          folder1 = null;
        }
        if ( folder2 != null ) {
          folder2.close();
          folder2 = null;
        }
        if ( filefolder1 != null ) {
          filefolder1.close();
          filefolder1 = null;
        }
        if ( filefolder2 != null ) {
          filefolder2.close();
          filefolder2 = null;
        }
      } catch ( IOException e ) {
        // Ignore errors
      }
    }

    return result;
  }

  private class TextFileSelector implements FileSelector {
    String sourceFolder = null;

    public TextFileSelector( String sourcefolderin ) {
      if ( !Utils.isEmpty( sourcefolderin ) ) {
        sourceFolder = sourcefolderin;
      }

    }

    public boolean includeFile( FileSelectInfo info ) {
      boolean returncode = false;
      try {
        if ( !info.getFile().toString().equals( sourceFolder ) ) {
          // Pass over the Base folder itself
          String shortFilename = info.getFile().getName().getBaseName();

          if ( info.getFile().getParent().equals( info.getBaseFolder() ) ) {
            // In the Base Folder...
            if ( ( info.getFile().getType() == FileType.FILE && compareonly.equals( "only_files" ) )
              || ( info.getFile().getType() == FileType.FOLDER && compareonly.equals( "only_folders" ) )
              || ( GetFileWildcard( shortFilename ) && compareonly.equals( "specify" ) )
              || ( compareonly.equals( "all" ) ) ) {
              returncode = true;
            }
          } else {
            // Not in the Base Folder...Only if include sub folders
            if ( includesubfolders ) {
              if ( ( info.getFile().getType() == FileType.FILE && compareonly.equals( "only_files" ) )
                || ( info.getFile().getType() == FileType.FOLDER && compareonly.equals( "only_folders" ) )
                || ( GetFileWildcard( shortFilename ) && compareonly.equals( "specify" ) )
                || ( compareonly.equals( "all" ) ) ) {
                returncode = true;
              }
            }
          }

        }
      } catch ( Exception e ) {

        logError( "Error while finding files ... in ["
          + info.getFile().toString() + "]. Exception :" + e.getMessage() );
        returncode = false;
      }
      return returncode;
    }

    public boolean traverseDescendents( FileSelectInfo info ) {
      return true;
    }
  }

  /**********************************************************
   *
   * @param selectedfile
   * @return True if the selectedfile matches the wildcard
   **********************************************************/
  private boolean GetFileWildcard( String selectedfile ) {
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

  @Override public boolean isEvaluation() {
    return true;
  }

  public void setWildcard( String wildcard ) {
    this.wildcard = wildcard;
  }

  public String getWildcard() {
    return wildcard;
  }

  public void setFilename1( String filename ) {
    this.filename1 = filename;
  }

  public String getFilename1() {
    return filename1;
  }

  public void setFilename2( String filename ) {
    this.filename2 = filename;
  }

  public String getFilename2() {
    return filename2;
  }

  public void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace( ctx, getVariables() );
    AndValidator.putValidators( ctx, ActionValidatorUtils.notNullValidator(), ActionValidatorUtils.fileExistsValidator() );
    ActionValidatorUtils.andValidator().validate( this, "filename1", remarks, ctx );
    ActionValidatorUtils.andValidator().validate( this, "filename2", remarks, ctx );
  }
}
