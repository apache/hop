/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.job.entries.addresultfilenames;

import org.apache.hop.job.entry.validator.AbstractFileValidator;
import org.apache.hop.job.entry.validator.AndValidator;
import org.apache.hop.job.entry.validator.JobEntryValidatorUtils;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.JobEntry;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.Job;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.JobEntryBase;
import org.apache.hop.job.entry.JobEntryInterface;
import org.apache.hop.job.entry.validator.ValidatorContext;

import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/**
 * This defines a 'add result filenames' job entry.
 *
 * @author Samatar Hassan
 * @since 06-05-2007
 */
@JobEntry(
  id = "ADD_RESULT_FILENAMES",
  i18nPackageName = "org.apache.hop.job.entries.addresultfilenames",
  name = "JobEntry.AddResultFilenames.TypeDesc",
  description = "JobEntry.AddResultFilenames.Tooltip",
  categoryDescription = "i18n:org.apache.hop.job:JobCategory.Category.FileManagement" )
public class JobEntryAddResultFilenames extends JobEntryBase implements Cloneable, JobEntryInterface {
  private static Class<?> PKG = JobEntryAddResultFilenames.class; // for i18n purposes, needed by Translator2!!

  public boolean argFromPrevious;

  public boolean deleteallbefore;

  public boolean includeSubfolders;

  public String[] arguments;

  public String[] filemasks;

  public JobEntryAddResultFilenames( String n ) {
    super( n, "" );
    argFromPrevious = false;
    deleteallbefore = false;
    arguments = null;

    includeSubfolders = false;
  }

  public JobEntryAddResultFilenames() {
    this( "" );
  }

  public Object clone() {
    JobEntryAddResultFilenames je = (JobEntryAddResultFilenames) super.clone();
    return je;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( super.getXML() );
    retval.append( "      " ).append( XMLHandler.addTagValue( "arg_from_previous", argFromPrevious ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "include_subfolders", includeSubfolders ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "delete_all_before", deleteallbefore ) );

    retval.append( "      <fields>" ).append( Const.CR );
    if ( arguments != null ) {
      for ( int i = 0; i < arguments.length; i++ ) {
        retval.append( "        <field>" ).append( Const.CR );
        retval.append( "          " ).append( XMLHandler.addTagValue( "name", arguments[i] ) );
        retval.append( "          " ).append( XMLHandler.addTagValue( "filemask", filemasks[i] ) );
        retval.append( "        </field>" ).append( Const.CR );
      }
    }
    retval.append( "      </fields>" ).append( Const.CR );

    return retval.toString();
  }

  public void loadXML( Node entrynode, List<SlaveServer> slaveServers,
    IMetaStore metaStore ) throws HopXMLException {
    try {
      super.loadXML( entrynode, slaveServers );
      argFromPrevious = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "arg_from_previous" ) );
      includeSubfolders = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "include_subfolders" ) );
      deleteallbefore = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "delete_all_before" ) );

      Node fields = XMLHandler.getSubNode( entrynode, "fields" );

      // How many field arguments?
      int nrFields = XMLHandler.countNodes( fields, "field" );
      arguments = new String[nrFields];
      filemasks = new String[nrFields];

      // Read them all...
      for ( int i = 0; i < nrFields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );

        arguments[i] = XMLHandler.getTagValue( fnode, "name" );
        filemasks[i] = XMLHandler.getTagValue( fnode, "filemask" );
      }
    } catch ( HopXMLException xe ) {
      throw new HopXMLException(
        BaseMessages.getString( PKG, "JobEntryAddResultFilenames.UnableToLoadFromXml" ), xe );
    }
  }

  public Result execute( Result result, int nr ) throws HopException {
    List<RowMetaAndData> rows = result.getRows();
    RowMetaAndData resultRow = null;

    int nrErrFiles = 0;
    result.setResult( true );

    if ( deleteallbefore ) {
      // clear result filenames
      int size = result.getResultFiles().size();
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "JobEntryAddResultFilenames.log.FilesFound", "" + size ) );
      }

      result.getResultFiles().clear();
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "JobEntryAddResultFilenames.log.DeletedFiles", "" + size ) );
      }
    }

    if ( argFromPrevious ) {
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "JobEntryAddResultFilenames.FoundPreviousRows", String
          .valueOf( ( rows != null ? rows.size() : 0 ) ) ) );
      }
    }

    if ( argFromPrevious && rows != null ) { // Copy the input row to the (command line) arguments
      for ( int iteration = 0; iteration < rows.size() && !parentJob.isStopped(); iteration++ ) {
        resultRow = rows.get( iteration );

        // Get values from previous result
        String filefolder_previous = resultRow.getString( 0, null );
        String fmasks_previous = resultRow.getString( 1, null );

        // ok we can process this file/folder
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString(
            PKG, "JobEntryAddResultFilenames.ProcessingRow", filefolder_previous, fmasks_previous ) );
        }

        if ( !processFile( filefolder_previous, fmasks_previous, parentJob, result ) ) {
          nrErrFiles++;
        }

      }
    } else if ( arguments != null ) {

      for ( int i = 0; i < arguments.length && !parentJob.isStopped(); i++ ) {

        // ok we can process this file/folder
        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString(
            PKG, "JobEntryAddResultFilenames.ProcessingArg", arguments[i], filemasks[i] ) );
        }
        if ( !processFile( arguments[i], filemasks[i], parentJob, result ) ) {
          nrErrFiles++;
        }
      }
    }

    if ( nrErrFiles > 0 ) {
      result.setResult( false );
      result.setNrErrors( nrErrFiles );
    }

    return result;
  }

  private boolean processFile( String filename, String wildcard, Job parentJob, Result result ) {

    boolean rcode = true;
    FileObject filefolder = null;
    String realFilefoldername = environmentSubstitute( filename );
    String realwildcard = environmentSubstitute( wildcard );

    try {
      filefolder = HopVFS.getFileObject( realFilefoldername, this );
      if ( filefolder.exists() ) {
        // the file or folder exists

        if ( filefolder.getType() == FileType.FILE ) {
          // Add filename to Resultfilenames ...
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobEntryAddResultFilenames.AddingFileToResult", filefolder
              .toString() ) );
          }
          ResultFile resultFile =
            new ResultFile(
              ResultFile.FILE_TYPE_GENERAL, HopVFS.getFileObject( filefolder.toString(), this ), parentJob
                .getJobname(), toString() );
          result.getResultFiles().put( resultFile.getFile().toString(), resultFile );
        } else {
          FileObject[] list = filefolder.findFiles( new TextFileSelector( filefolder.toString(), realwildcard ) );

          for ( int i = 0; i < list.length && !parentJob.isStopped(); i++ ) {
            // Add filename to Resultfilenames ...
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "JobEntryAddResultFilenames.AddingFileToResult", list[i]
                .toString() ) );
            }
            ResultFile resultFile =
              new ResultFile(
                ResultFile.FILE_TYPE_GENERAL, HopVFS.getFileObject( list[i].toString(), this ), parentJob
                  .getJobname(), toString() );
            result.getResultFiles().put( resultFile.getFile().toString(), resultFile );
          }
        }

      } else {
        // File can not be found
        if ( log.isBasic() ) {
          logBasic( BaseMessages.getString(
            PKG, "JobEntryAddResultFilenames.FileCanNotbeFound", realFilefoldername ) );
        }
        rcode = false;
      }
    } catch ( Exception e ) {
      rcode = false;
      logError( BaseMessages.getString( PKG, "JobEntryAddResultFilenames.CouldNotProcess", realFilefoldername, e
        .getMessage() ), e );
    } finally {
      if ( filefolder != null ) {
        try {
          filefolder.close();
          filefolder = null;
        } catch ( IOException ex ) {
          // Ignore
        }
      }
    }

    return rcode;
  }

  private class TextFileSelector implements FileSelector {
    String fileWildcard = null;
    String sourceFolder = null;

    public TextFileSelector( String sourcefolderin, String filewildcard ) {
      if ( !Utils.isEmpty( sourcefolderin ) ) {
        sourceFolder = sourcefolderin;
      }

      if ( !Utils.isEmpty( filewildcard ) ) {
        fileWildcard = filewildcard;
      }
    }

    public boolean includeFile( FileSelectInfo info ) {
      boolean returncode = false;
      try {
        if ( !info.getFile().toString().equals( sourceFolder ) ) {
          // Pass over the Base folder itself
          String short_filename = info.getFile().getName().getBaseName();

          if ( info.getFile().getParent().equals( info.getBaseFolder() )
            || ( !info.getFile().getParent().equals( info.getBaseFolder() ) && includeSubfolders ) ) {
            if ( ( info.getFile().getType() == FileType.FILE && fileWildcard == null )
              || ( info.getFile().getType() == FileType.FILE && fileWildcard != null && GetFileWildcard(
                short_filename, fileWildcard ) ) ) {
              returncode = true;
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

  public void setIncludeSubfolders( boolean includeSubfolders ) {
    this.includeSubfolders = includeSubfolders;
  }

  public void setArgumentsPrevious( boolean argFromPrevious ) {
    this.argFromPrevious = argFromPrevious;
  }

  public void setDeleteAllBefore( boolean deleteallbefore ) {
    this.deleteallbefore = deleteallbefore;
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isArgFromPrevious() {
    return argFromPrevious;
  }

  public boolean deleteAllBefore() {
    return deleteallbefore;
  }

  public String[] getArguments() {
    return arguments;
  }

  public String[] getFilemasks() {
    return filemasks;
  }

  public boolean isIncludeSubfolders() {
    return includeSubfolders;
  }

  public void check( List<CheckResultInterface> remarks, JobMeta jobMeta, VariableSpace space,
    IMetaStore metaStore ) {
    boolean res = JobEntryValidatorUtils.andValidator().validate( this, "arguments", remarks,
        AndValidator.putValidators( JobEntryValidatorUtils.notNullValidator() ) );

    if ( res == false ) {
      return;
    }

    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace( ctx, getVariables() );
    AndValidator.putValidators( ctx, JobEntryValidatorUtils.notNullValidator(), JobEntryValidatorUtils.fileExistsValidator() );

    for ( int i = 0; i < arguments.length; i++ ) {
      JobEntryValidatorUtils.andValidator().validate( this, "arguments[" + i + "]", remarks, ctx );
    }
  }

}
