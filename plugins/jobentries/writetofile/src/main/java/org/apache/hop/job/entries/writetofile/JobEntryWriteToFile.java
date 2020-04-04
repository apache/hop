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

package org.apache.hop.job.entries.writetofile;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.JobEntry;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.JobEntryBase;
import org.apache.hop.job.entry.IJobEntry;
import org.apache.hop.job.entry.validator.AndValidator;
import org.apache.hop.job.entry.validator.JobEntryValidatorUtils;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.List;

/**
 * This defines a 'write to file' job entry. Its main use would be to create empty trigger files that can be used to
 * control the flow in ETL cycles.
 *
 * @author Samatar Hassan
 * @since 28-01-2007
 */

@JobEntry(
  id = "WRITE_TO_FILE",
  i18nPackageName = "org.apache.hop.job.entries.writetofile",
  name = "JobEntryWriteToFile.Name",
  description = "JobEntryWriteToFile.Description",
  image = "WriteToFile.svg",
  categoryDescription = "i18n:org.apache.hop.job:JobCategory.Category.FileManagement"
)
public class JobEntryWriteToFile extends JobEntryBase implements Cloneable, IJobEntry {
  private static Class<?> PKG = JobEntryWriteToFile.class; // for i18n purposes, needed by Translator!!

  private String filename;
  private boolean createParentFolder;
  private boolean appendFile;
  private String content;
  private String encoding;

  public JobEntryWriteToFile( String n ) {
    super( n, "" );
    filename = null;
    createParentFolder = false;
    appendFile = false;
    content = null;
    encoding = null;
  }

  public JobEntryWriteToFile() {
    this( "" );
  }

  public Object clone() {
    JobEntryWriteToFile je = (JobEntryWriteToFile) super.clone();
    return je;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 100 );

    retval.append( super.getXML() );
    retval.append( "      " ).append( XMLHandler.addTagValue( "filename", filename ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "createParentFolder", createParentFolder ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "appendFile", appendFile ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "content", content ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "encoding", encoding ) );

    return retval.toString();
  }

  public void loadXML( Node entrynode,
                       IMetaStore metaStore ) throws HopXMLException {
    try {
      super.loadXML( entrynode );
      filename = XMLHandler.getTagValue( entrynode, "filename" );
      createParentFolder = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "createParentFolder" ) );
      appendFile = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "appendFile" ) );
      content = XMLHandler.getTagValue( entrynode, "content" );
      encoding = XMLHandler.getTagValue( entrynode, "encoding" );
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

  public void setContent( String content ) {
    this.content = content;
  }

  public String getContent() {
    return content;
  }

  public void setEncoding( String encoding ) {
    this.encoding = encoding;
  }

  public String getEncoding() {
    return encoding;
  }

  public String getRealFilename() {
    return environmentSubstitute( getFilename() );
  }

  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    result.setResult( false );
    result.setNrErrors( 1 );

    String realFilename = getRealFilename();
    if ( !Utils.isEmpty( realFilename ) ) {

      String content = environmentSubstitute( getContent() );
      String encoding = environmentSubstitute( getEncoding() );

      OutputStreamWriter osw = null;
      OutputStream os = null;
      try {

        // Create parent folder if needed
        createParentFolder( realFilename );

        // Create / open file for writing
        os = HopVFS.getOutputStream( realFilename, this, isAppendFile() );

        if ( Utils.isEmpty( encoding ) ) {
          if ( isDebug() ) {
            logDebug( BaseMessages.getString( PKG, "JobWriteToFile.Log.WritingToFile", realFilename ) );
          }
          osw = new OutputStreamWriter( os );
        } else {
          if ( isDebug() ) {
            logDebug( BaseMessages.getString(
              PKG, "JobWriteToFile.Log.WritingToFileWithEncoding", realFilename, encoding ) );
          }
          osw = new OutputStreamWriter( os, encoding );
        }
        osw.write( content );

        result.setResult( true );
        result.setNrErrors( 0 );

      } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "JobWriteToFile.Error.WritingFile", realFilename, e.getMessage() ) );
      } finally {
        if ( osw != null ) {
          try {
            osw.flush();
            osw.close();
          } catch ( Exception ex ) { /* Ignore */
          }
        }
        if ( os != null ) {
          try {
            os.flush();
            os.close();
          } catch ( Exception ex ) { /* Ignore */
          }
        }
      }
    } else {
      logError( BaseMessages.getString( PKG, "JobWriteToFile.Error.MissinfgFile" ) );
    }

    return result;
  }

  private void createParentFolder( String realFilename ) throws HopException {
    FileObject parent = null;
    try {
      parent = HopVFS.getFileObject( realFilename, this ).getParent();
      if ( !parent.exists() ) {
        if ( isCreateParentFolder() ) {
          if ( isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobWriteToFile.Log.ParentFoldetNotExist", parent
              .getName().toString() ) );
          }
          parent.createFolder();
          if ( isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "JobWriteToFile.Log.ParentFolderCreated", parent
              .getName().toString() ) );
          }
        } else {
          throw new HopException( BaseMessages.getString(
            PKG, "JobWriteToFile.Log.ParentFoldetNotExist", parent.getName().toString() ) );
        }
      }
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString(
        PKG, "JobWriteToFile.Error.CheckingParentFolder", realFilename ), e );
    } finally {
      if ( parent != null ) {
        try {
          parent.close();
        } catch ( Exception e ) { /* Ignore */
        }
      }
    }
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isAppendFile() {
    return appendFile;
  }

  public void setAppendFile( boolean appendFile ) {
    this.appendFile = appendFile;
  }

  public boolean isCreateParentFolder() {
    return createParentFolder;
  }

  public void setCreateParentFolder( boolean createParentFolder ) {
    this.createParentFolder = createParentFolder;
  }

  public List<ResourceReference> getResourceDependencies( JobMeta jobMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( jobMeta );
    if ( !Utils.isEmpty( getFilename() ) ) {
      String realFileName = jobMeta.environmentSubstitute( getFilename() );
      ResourceReference reference = new ResourceReference( this );
      reference.getEntries().add( new ResourceEntry( realFileName, ResourceType.FILE ) );
      references.add( reference );
    }
    return references;
  }

  @Override
  public void check( List<ICheckResult> remarks, JobMeta jobMeta, IVariables variables,
                     IMetaStore metaStore ) {
    JobEntryValidatorUtils.andValidator().validate( this, "filename", remarks,
      AndValidator.putValidators( JobEntryValidatorUtils.notBlankValidator() ) );
  }
}
