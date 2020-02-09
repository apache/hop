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

package org.apache.hop.job.entries.deletefile;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.JobEntryBase;
import org.apache.hop.job.entry.JobEntryInterface;
import org.apache.hop.job.entry.validator.AbstractFileValidator;
import org.apache.hop.job.entry.validator.AndValidator;
import org.apache.hop.job.entry.validator.FileExistsValidator;
import org.apache.hop.job.entry.validator.JobEntryValidatorUtils;
import org.apache.hop.job.entry.validator.ValidatorContext;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.io.IOException;
import java.util.List;

/**
 * This defines a 'delete file' job entry. Its main use would be to delete trigger files, but it will delete any file.
 *
 * @author Sven Boden
 * @since 10-02-2007
 */
public class JobEntryDeleteFile extends JobEntryBase implements Cloneable, JobEntryInterface {
  private static Class<?> PKG = JobEntryDeleteFile.class; // for i18n purposes, needed by Translator2!!

  private String filename;
  private boolean failIfFileNotExists;

  public JobEntryDeleteFile( String n ) {
    super( n, "" );
    filename = null;
    failIfFileNotExists = false;
  }

  public JobEntryDeleteFile() {
    this( "" );
  }

  public Object clone() {
    JobEntryDeleteFile je = (JobEntryDeleteFile) super.clone();
    return je;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 50 );

    retval.append( super.getXML() );
    retval.append( "      " ).append( XMLHandler.addTagValue( "filename", filename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "fail_if_file_not_exists", failIfFileNotExists ) );

    return retval.toString();
  }

  public void loadXML( Node entrynode,
                       IMetaStore metaStore ) throws HopXMLException {
    try {
      super.loadXML( entrynode );
      filename = XMLHandler.getTagValue( entrynode, "filename" );
      failIfFileNotExists = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "fail_if_file_not_exists" ) );
    } catch ( HopXMLException xe ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "JobEntryDeleteFile.Error_0001_Unable_To_Load_Job_From_Xml_Node" ), xe );
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

  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    result.setResult( false );

    if ( filename != null ) {
      String realFilename = getRealFilename();

      FileObject fileObject = null;
      try {
        fileObject = HopVFS.getFileObject( realFilename, this );

        if ( !fileObject.exists() ) {
          if ( isFailIfFileNotExists() ) {
            // File doesn't exist and fail flag is on.
            result.setResult( false );
            logError( BaseMessages.getString(
              PKG, "JobEntryDeleteFile.ERROR_0004_File_Does_Not_Exist", realFilename ) );
          } else {
            // File already deleted, no reason to try to delete it
            result.setResult( true );
            if ( log.isBasic() ) {
              logBasic( BaseMessages.getString( PKG, "JobEntryDeleteFile.File_Already_Deleted", realFilename ) );
            }
          }
        } else {
          boolean deleted = fileObject.delete();
          if ( !deleted ) {
            logError( BaseMessages.getString(
              PKG, "JobEntryDeleteFile.ERROR_0005_Could_Not_Delete_File", realFilename ) );
            result.setResult( false );
            result.setNrErrors( 1 );
          }
          if ( log.isBasic() ) {
            logBasic( BaseMessages.getString( PKG, "JobEntryDeleteFile.File_Deleted", realFilename ) );
          }
          result.setResult( true );
        }
      } catch ( Exception e ) {
        logError( BaseMessages.getString(
          PKG, "JobEntryDeleteFile.ERROR_0006_Exception_Deleting_File", realFilename, e.getMessage() ), e );
        result.setResult( false );
        result.setNrErrors( 1 );
      } finally {
        if ( fileObject != null ) {
          try {
            fileObject.close();
          } catch ( IOException ex ) { /* Ignore */
          }
        }
      }
    } else {
      logError( BaseMessages.getString( PKG, "JobEntryDeleteFile.ERROR_0007_No_Filename_Is_Defined" ) );
    }

    return result;
  }

  public boolean isFailIfFileNotExists() {
    return failIfFileNotExists;
  }

  public void setFailIfFileNotExists( boolean failIfFileExists ) {
    this.failIfFileNotExists = failIfFileExists;
  }

  public boolean evaluates() {
    return true;
  }

  public List<ResourceReference> getResourceDependencies( JobMeta jobMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( jobMeta );
    if ( !Utils.isEmpty( filename ) ) {
      String realFileName = jobMeta.environmentSubstitute( filename );
      ResourceReference reference = new ResourceReference( this );
      reference.getEntries().add( new ResourceEntry( realFileName, ResourceType.FILE ) );
      references.add( reference );
    }
    return references;
  }

  public void check( List<CheckResultInterface> remarks, JobMeta jobMeta, VariableSpace space,
                     IMetaStore metaStore ) {
    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace( ctx, getVariables() );
    AndValidator.putValidators( ctx, JobEntryValidatorUtils.notNullValidator(), JobEntryValidatorUtils.fileExistsValidator() );
    if ( isFailIfFileNotExists() ) {
      FileExistsValidator.putFailIfDoesNotExist( ctx, true );
    }
    JobEntryValidatorUtils.andValidator().validate( this, "filename", remarks, ctx );
  }
}
