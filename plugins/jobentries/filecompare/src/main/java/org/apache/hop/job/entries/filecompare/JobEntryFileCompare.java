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

package org.apache.hop.job.entries.filecompare;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.annotations.JobEntry;
import org.apache.hop.core.exception.HopFileException;
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
import org.apache.hop.job.entry.validator.JobEntryValidatorUtils;
import org.apache.hop.job.entry.validator.ValidatorContext;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;

/**
 * This defines a 'file compare' job entry. It will compare 2 files in a binary way, and will either follow the true
 * flow upon the files being the same or the false flow otherwise.
 *
 * @author Sven Boden
 * @since 01-02-2007
 */

@JobEntry(
  id = "FILE_COMPARE",
  i18nPackageName = "org.apache.hop.job.entries.filecompare",
  name = "JobEntryFileCompare.Name",
  description = "JobEntryFileCompare.Description",
  image = "FileCompare.svg",
  categoryDescription = "i18n:org.apache.hop.job:JobCategory.Category.FileManagement"
)
public class JobEntryFileCompare extends JobEntryBase implements Cloneable, JobEntryInterface {
  private static Class<?> PKG = JobEntryFileCompare.class; // for i18n purposes, needed by Translator2!!

  private String filename1;
  private String filename2;
  private boolean addFilenameToResult;
  private JobMeta parentJobMeta;

  public JobEntryFileCompare( String n ) {
    super( n, "" );
    filename1 = null;
    filename2 = null;
    addFilenameToResult = false;
  }

  public JobEntryFileCompare() {
    this( "" );
  }

  public Object clone() {
    JobEntryFileCompare je = (JobEntryFileCompare) super.clone();
    return je;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 50 );

    retval.append( super.getXML() );
    retval.append( "      " ).append( XMLHandler.addTagValue( "filename1", filename1 ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "filename2", filename2 ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "add_filename_result", addFilenameToResult ) );

    return retval.toString();
  }

  public void loadXML( Node entrynode,
                       IMetaStore metaStore ) throws HopXMLException {
    try {
      super.loadXML( entrynode );
      filename1 = XMLHandler.getTagValue( entrynode, "filename1" );
      filename2 = XMLHandler.getTagValue( entrynode, "filename2" );
      addFilenameToResult = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "add_filename_result" ) );
    } catch ( HopXMLException xe ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "JobEntryFileCompare.ERROR_0001_Unable_To_Load_From_Xml_Node" ), xe );
    }
  }

  public String getRealFilename1() {
    return environmentSubstitute( getFilename1() );
  }

  public String getRealFilename2() {
    return environmentSubstitute( getFilename2() );
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
      in1 =
        new DataInputStream( new BufferedInputStream( HopVFS.getInputStream(
          HopVFS.getFilename( file1 ), this ) ) );
      in2 =
        new DataInputStream( new BufferedInputStream( HopVFS.getInputStream(
          HopVFS.getFilename( file2 ), this ) ) );

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
          // Nothing to do here
        }
      }
      if ( in2 != null ) {
        try {
          in2.close();
        } catch ( IOException ignored ) {
          // Nothing to see here...
        }
      }
    }
  }

  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    result.setResult( false );

    String realFilename1 = getRealFilename1();
    String realFilename2 = getRealFilename2();

    FileObject file1 = null;
    FileObject file2 = null;
    try {
      if ( filename1 != null && filename2 != null ) {

        file1 = HopVFS.getFileObject( realFilename1, this );
        file2 = HopVFS.getFileObject( realFilename2, this );

        if ( file1.exists() && file2.exists() ) {
          if ( equalFileContents( file1, file2 ) ) {
            result.setResult( true );
          } else {
            result.setResult( false );
          }

          // add filename to result filenames
          if ( addFilenameToResult && file1.getType() == FileType.FILE && file2.getType() == FileType.FILE ) {
            ResultFile resultFile =
              new ResultFile( ResultFile.FILE_TYPE_GENERAL, file1, parentJob.getJobname(), toString() );
            resultFile.setComment( BaseMessages.getString( PKG, "JobWaitForFile.FilenameAdded" ) );
            result.getResultFiles().put( resultFile.getFile().toString(), resultFile );
            resultFile = new ResultFile( ResultFile.FILE_TYPE_GENERAL, file2, parentJob.getJobname(), toString() );
            resultFile.setComment( BaseMessages.getString( PKG, "JobWaitForFile.FilenameAdded" ) );
            result.getResultFiles().put( resultFile.getFile().toString(), resultFile );
          }
        } else {
          if ( !file1.exists() ) {
            logError( BaseMessages.getString(
              PKG, "JobEntryFileCompare.ERROR_0004_File1_Does_Not_Exist", realFilename1 ) );
          }
          if ( !file2.exists() ) {
            logError( BaseMessages.getString(
              PKG, "JobEntryFileCompare.ERROR_0005_File2_Does_Not_Exist", realFilename2 ) );
          }
          result.setResult( false );
          result.setNrErrors( 1 );
        }
      } else {
        logError( BaseMessages.getString( PKG, "JobEntryFileCompare.ERROR_0006_Need_Two_Filenames" ) );
      }
    } catch ( Exception e ) {
      result.setResult( false );
      result.setNrErrors( 1 );
      logError( BaseMessages.getString(
        PKG, "JobEntryFileCompare.ERROR_0007_Comparing_Files", realFilename2, realFilename2, e.getMessage() ) );
    } finally {
      try {
        if ( file1 != null ) {
          file1.close();
          file1 = null;
        }

        if ( file2 != null ) {
          file2.close();
          file2 = null;
        }
      } catch ( IOException e ) {
        // Ignore errors
      }
    }

    return result;
  }

  public boolean evaluates() {
    return true;
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

  public boolean isAddFilenameToResult() {
    return addFilenameToResult;
  }

  public void setAddFilenameToResult( boolean addFilenameToResult ) {
    this.addFilenameToResult = addFilenameToResult;
  }

  public List<ResourceReference> getResourceDependencies( JobMeta jobMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( jobMeta );
    if ( ( !Utils.isEmpty( filename1 ) ) && ( !Utils.isEmpty( filename2 ) ) ) {
      String realFilename1 = jobMeta.environmentSubstitute( filename1 );
      String realFilename2 = jobMeta.environmentSubstitute( filename2 );
      ResourceReference reference = new ResourceReference( this );
      reference.getEntries().add( new ResourceEntry( realFilename1, ResourceType.FILE ) );
      reference.getEntries().add( new ResourceEntry( realFilename2, ResourceType.FILE ) );
      references.add( reference );
    }
    return references;
  }

  public void check( List<CheckResultInterface> remarks, JobMeta jobMeta, VariableSpace space,
                     IMetaStore metaStore ) {
    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace( ctx, getVariables() );
    AndValidator.putValidators( ctx, JobEntryValidatorUtils.notNullValidator(),
      JobEntryValidatorUtils.fileExistsValidator() );
    JobEntryValidatorUtils.andValidator().validate( this, "filename1", remarks, ctx );
    JobEntryValidatorUtils.andValidator().validate( this, "filename2", remarks, ctx );
  }

  @Override public JobMeta getParentJobMeta() {
    return parentJobMeta;
  }

  @Override public void setParentJobMeta( JobMeta parentJobMeta ) {
    this.parentJobMeta = parentJobMeta;
  }

}
