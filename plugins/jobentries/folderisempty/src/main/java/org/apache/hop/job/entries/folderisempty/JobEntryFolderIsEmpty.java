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

package org.apache.hop.job.entries.folderisempty;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.JobEntry;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.JobEntryBase;
import org.apache.hop.job.entry.JobEntryInterface;
import org.apache.hop.job.entry.validator.AndValidator;
import org.apache.hop.job.entry.validator.JobEntryValidatorUtils;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This defines a 'create folder' job entry. Its main use would be to create empty folder that can be used to control
 * the flow in ETL cycles.
 *
 * @author Sven/Samatar
 * @since 18-10-2007
 */

@JobEntry(
  id = "FOLDER_IS_EMPTY",
  i18nPackageName = "org.apache.hop.job.entries.folderisempty",
  name = "JobEntryFolderIsEmpty.Name",
  description = "JobEntryFolderIsEmpty.Description",
  image = "FolderIsEmpty.svg",
  categoryDescription = "i18n:org.apache.hop.job:JobCategory.Category.Conditions"
)
public class JobEntryFolderIsEmpty extends JobEntryBase implements Cloneable, JobEntryInterface {
  private static Class<?> PKG = JobEntryFolderIsEmpty.class; // for i18n purposes, needed by Translator2!!

  private String foldername;
  private int filescount;
  private int folderscount;
  private boolean includeSubfolders;
  private boolean specifywildcard;
  private String wildcard;
  private Pattern pattern;

  public JobEntryFolderIsEmpty( String n ) {
    super( n, "" );
    foldername = null;
    wildcard = null;
    includeSubfolders = false;
    specifywildcard = false;
  }

  public JobEntryFolderIsEmpty() {
    this( "" );
  }

  public Object clone() {
    JobEntryFolderIsEmpty je = (JobEntryFolderIsEmpty) super.clone();
    return je;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 50 );

    retval.append( super.getXML() );
    retval.append( "      " ).append( XMLHandler.addTagValue( "foldername", foldername ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "include_subfolders", includeSubfolders ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "specify_wildcard", specifywildcard ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "wildcard", wildcard ) );

    return retval.toString();
  }

  public void loadXML( Node entrynode,
                       IMetaStore metaStore ) throws HopXMLException {
    try {
      super.loadXML( entrynode );
      foldername = XMLHandler.getTagValue( entrynode, "foldername" );
      includeSubfolders = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "include_subfolders" ) );
      specifywildcard = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "specify_wildcard" ) );
      wildcard = XMLHandler.getTagValue( entrynode, "wildcard" );
    } catch ( HopXMLException xe ) {
      throw new HopXMLException( "Unable to load job entry of type 'create folder' from XML node", xe );
    }
  }

  public void setSpecifyWildcard( boolean specifywildcard ) {
    this.specifywildcard = specifywildcard;
  }

  public boolean isSpecifyWildcard() {
    return specifywildcard;
  }

  public void setFoldername( String foldername ) {
    this.foldername = foldername;
  }

  public String getFoldername() {
    return foldername;
  }

  public String getRealFoldername() {
    return environmentSubstitute( getFoldername() );
  }

  public String getWildcard() {
    return wildcard;
  }

  public String getRealWildcard() {
    return environmentSubstitute( getWildcard() );
  }

  public void setWildcard( String wildcard ) {
    this.wildcard = wildcard;
  }

  public boolean isIncludeSubFolders() {
    return includeSubfolders;
  }

  public void setIncludeSubFolders( boolean includeSubfolders ) {
    this.includeSubfolders = includeSubfolders;
  }

  public Result execute( Result previousResult, int nr ) {
    // see PDI-10270 for details
    boolean oldBehavior =
      "Y".equalsIgnoreCase( getVariable( Const.HOP_COMPATIBILITY_SET_ERROR_ON_SPECIFIC_JOB_ENTRIES, "N" ) );

    Result result = previousResult;
    result.setResult( false );
    result.setNrErrors( oldBehavior ? 1 : 0 );

    filescount = 0;
    folderscount = 0;
    pattern = null;

    if ( !Utils.isEmpty( getWildcard() ) ) {
      pattern = Pattern.compile( getRealWildcard() );
    }

    if ( foldername != null ) {
      String realFoldername = getRealFoldername();
      FileObject folderObject = null;
      try {
        folderObject = HopVFS.getFileObject( realFoldername, this );
        if ( folderObject.exists() ) {
          // Check if it's a folder
          if ( folderObject.getType() == FileType.FOLDER ) {
            // File provided is a folder, so we can process ...
            try {
              folderObject.findFiles( new TextFileSelector( folderObject.toString() ) );
            } catch ( Exception ex ) {
              if ( !( ex.getCause() instanceof ExpectedException ) ) {
                throw ex;
              }
            }
            if ( log.isBasic() ) {
              log.logBasic( "Total files", "We found : " + filescount + " file(s)" );
            }
            if ( filescount == 0 ) {
              result.setResult( true );
              result.setNrLinesInput( folderscount );
            }
          } else {
            // Not a folder, fail
            log.logError( "[" + realFoldername + "] is not a folder, failing." );
            result.setNrErrors( 1 );
          }
        } else {
          // No Folder found
          if ( log.isBasic() ) {
            logBasic( "we can not find [" + realFoldername + "] !" );
          }
          result.setNrErrors( 1 );
        }
      } catch ( Exception e ) {
        logError( "Error checking folder [" + realFoldername + "]", e );
        result.setResult( false );
        result.setNrErrors( 1 );
      } finally {
        if ( folderObject != null ) {
          try {
            folderObject.close();
            folderObject = null;
          } catch ( IOException ex ) { /* Ignore */
          }
        }
      }
    } else {
      logError( "No Foldername is defined." );
      result.setNrErrors( 1 );
    }

    return result;
  }

  private class ExpectedException extends Exception {
    private static final long serialVersionUID = -692662556327569162L;
  }

  private class TextFileSelector implements FileSelector {
    String root_folder = null;

    public TextFileSelector( String rootfolder ) {
      if ( rootfolder != null ) {
        root_folder = rootfolder;
      }
    }

    public boolean includeFile( FileSelectInfo info ) throws ExpectedException {
      boolean returncode = false;
      FileObject file_name = null;
      boolean rethrow = false;
      try {
        if ( !info.getFile().toString().equals( root_folder ) ) {
          // Pass over the Base folder itself
          if ( ( info.getFile().getType() == FileType.FILE ) ) {
            if ( info.getFile().getParent().equals( info.getBaseFolder() ) ) {
              // We are in the Base folder
              if ( ( isSpecifyWildcard() && GetFileWildcard( info.getFile().getName().getBaseName() ) )
                || !isSpecifyWildcard() ) {
                if ( log.isDetailed() ) {
                  log.logDetailed( "We found file : " + info.getFile().toString() );
                }
                filescount++;
              }
            } else {
              // We are not in the base Folder...ONLY if Use sub folders
              // We are in the Base folder
              if ( isIncludeSubFolders() ) {
                if ( ( isSpecifyWildcard() && GetFileWildcard( info.getFile().getName().getBaseName() ) )
                  || !isSpecifyWildcard() ) {
                  if ( log.isDetailed() ) {
                    log.logDetailed( "We found file : " + info.getFile().toString() );
                  }
                  filescount++;
                }
              }
            }
          } else {
            folderscount++;
          }
        }
        if ( filescount > 0 ) {
          rethrow = true;
          throw new ExpectedException();
        }
        return true;

      } catch ( Exception e ) {
        if ( !rethrow ) {
          log.logError( BaseMessages.getString( PKG, "JobFolderIsEmpty.Error" ), BaseMessages.getString(
            PKG, "JobFolderIsEmpty.Error.Exception", info.getFile().toString(), e.getMessage() ) );
          returncode = false;
        } else {
          throw (ExpectedException) e;
        }
      } finally {
        if ( file_name != null ) {
          try {
            file_name.close();
            file_name = null;
          } catch ( IOException ex ) { /* Ignore */
          }
        }
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
    boolean getIt = true;

    // First see if the file matches the regular expression!
    if ( pattern != null ) {
      Matcher matcher = pattern.matcher( selectedfile );
      getIt = matcher.matches();
    }
    return getIt;
  }

  public boolean evaluates() {
    return true;
  }

  @Override
  public void check( List<CheckResultInterface> remarks, JobMeta jobMeta, VariableSpace space,
                     IMetaStore metaStore ) {
    JobEntryValidatorUtils.andValidator().validate( this, "filename", remarks,
      AndValidator.putValidators( JobEntryValidatorUtils.notBlankValidator() ) );
  }
}
