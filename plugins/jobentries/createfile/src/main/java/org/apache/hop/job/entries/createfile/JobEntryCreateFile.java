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

package org.apache.hop.job.entries.createfile;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.annotations.JobEntry;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.Job;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.IJobEntry;
import org.apache.hop.job.entry.JobEntryBase;
import org.apache.hop.job.entry.validator.AbstractFileValidator;
import org.apache.hop.job.entry.validator.AndValidator;
import org.apache.hop.job.entry.validator.JobEntryValidatorUtils;
import org.apache.hop.job.entry.validator.ValidatorContext;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.io.IOException;
import java.util.List;

/**
 * This defines a 'create file' job entry. Its main use would be to create empty trigger files that can be used to
 * control the flow in ETL cycles.
 *
 * @author Sven Boden
 * @since 28-01-2007
 */

@JobEntry(
  id = "CREATE_FILE",
  i18nPackageName = "org.apache.hop.job.entries.createfile",
  name = "JobEntryCreateFile.Name",
  description = "JobEntryCreateFile.Description",
  image = "CreateFile.svg",
  categoryDescription = "i18n:org.apache.hop.job:JobCategory.Category.FileManagement"
)
public class JobEntryCreateFile extends JobEntryBase implements Cloneable, IJobEntry {
  private static Class<?> PKG = JobEntryCreateFile.class; // for i18n purposes, needed by Translator!!
  private String filename;

  private boolean failIfFileExists;
  private boolean addfilenameresult;

  public JobEntryCreateFile( String n ) {
    super( n, "" );
    filename = null;
    failIfFileExists = true;
    addfilenameresult = false;
  }

  public JobEntryCreateFile() {
    this( "" );
  }

  public Object clone() {
    JobEntryCreateFile je = (JobEntryCreateFile) super.clone();
    return je;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 50 );

    retval.append( super.getXML() );
    retval.append( "      " ).append( XMLHandler.addTagValue( "filename", filename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "fail_if_file_exists", failIfFileExists ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "add_filename_result", addfilenameresult ) );

    return retval.toString();
  }

  public void loadXML( Node entrynode,
                       IMetaStore metaStore ) throws HopXMLException {
    try {
      super.loadXML( entrynode );
      filename = XMLHandler.getTagValue( entrynode, "filename" );
      failIfFileExists = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "fail_if_file_exists" ) );
      addfilenameresult = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "add_filename_result" ) );

    } catch ( HopXMLException xe ) {
      throw new HopXMLException( "Unable to load job entry of type 'create file' from XML node", xe );
    }
  }

  public void setFilename( String filename ) {
    this.filename = filename;
  }

  public String getFilename() {
    return filename;
  }

  public String getRealFilename() {
    return environmentSubstitute( getFilename() );
  }

  public Result execute( Result previousResult, int nr ) throws HopException {
    Result result = previousResult;
    result.setResult( false );

    if ( filename != null ) {
      String realFilename = getRealFilename();
      FileObject fileObject = null;
      try {
        fileObject = HopVFS.getFileObject( realFilename, this );

        if ( fileObject.exists() ) {
          if ( isFailIfFileExists() ) {
            // File exists and fail flag is on.
            result.setResult( false );
            logError( "File [" + realFilename + "] exists, failing." );
          } else {
            // File already exists, no reason to try to create it
            result.setResult( true );
            logBasic( "File [" + realFilename + "] already exists, not recreating." );
          }
          // add filename to result filenames if needed
          if ( isAddFilenameToResult() ) {
            addFilenameToResult( realFilename, result, parentJob );
          }
        } else {
          // No file yet, create an empty file.
          fileObject.createFile();
          logBasic( "File [" + realFilename + "] created!" );
          // add filename to result filenames if needed
          if ( isAddFilenameToResult() ) {
            addFilenameToResult( realFilename, result, parentJob );
          }
          result.setResult( true );
        }
      } catch ( IOException e ) {
        logError( "Could not create file [" + realFilename + "], exception: " + e.getMessage() );
        result.setResult( false );
        result.setNrErrors( 1 );
      } finally {
        if ( fileObject != null ) {
          try {
            fileObject.close();
            fileObject = null;
          } catch ( IOException ex ) {
            // Ignore
          }
        }
      }
    } else {
      logError( "No filename is defined." );
    }

    return result;
  }

  private void addFilenameToResult( String targetFilename, Result result, Job parentJob ) throws HopException {
    FileObject targetFile = null;
    try {
      targetFile = HopVFS.getFileObject( targetFilename, this );

      // Add to the result files...
      ResultFile resultFile =
        new ResultFile( ResultFile.FILE_TYPE_GENERAL, targetFile, parentJob.getJobname(), toString() );
      resultFile.setComment( "" );
      result.getResultFiles().put( resultFile.getFile().toString(), resultFile );

      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "JobEntryCreateFile.FileAddedToResult", targetFilename ) );
      }
    } catch ( Exception e ) {
      throw new HopException( e );
    } finally {
      try {
        if ( targetFile != null ) {
          targetFile.close();
        }
      } catch ( Exception e ) {
        // Ignore close errors
      }
    }
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isFailIfFileExists() {
    return failIfFileExists;
  }

  public void setFailIfFileExists( boolean failIfFileExists ) {
    this.failIfFileExists = failIfFileExists;
  }

  public boolean isAddFilenameToResult() {
    return addfilenameresult;
  }

  public void setAddFilenameToResult( boolean addfilenameresult ) {
    this.addfilenameresult = addfilenameresult;
  }

  public void check( List<ICheckResult> remarks, JobMeta jobMeta, IVariables variables,
                     IMetaStore metaStore ) {
    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace( ctx, getVariables() );
    AndValidator.putValidators( ctx, JobEntryValidatorUtils.notNullValidator(), JobEntryValidatorUtils.fileDoesNotExistValidator() );
    JobEntryValidatorUtils.andValidator().validate( this, "filename", remarks, ctx );
  }

}
