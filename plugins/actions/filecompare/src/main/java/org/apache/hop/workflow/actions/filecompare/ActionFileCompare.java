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

package org.apache.hop.workflow.actions.filecompare;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopXmlException;
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
import org.apache.hop.workflow.action.validator.ValidatorContext;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;

/**
 * This defines a 'file compare' action. It will compare 2 files in a binary way, and will either follow the true
 * flow upon the files being the same or the false flow otherwise.
 *
 * @author Sven Boden
 * @since 01-02-2007
 */

@Action(
  id = "FILE_COMPARE",
  name = "i18n::ActionFileCompare.Name",
  description = "i18n::ActionFileCompare.Description",
  image = "FileCompare.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
  documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/filecompare.html"
)
public class ActionFileCompare extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionFileCompare.class; // For Translator

  private String filename1;
  private String filename2;
  private boolean addFilenameToResult;
  private WorkflowMeta parentWorkflowMeta;

  public ActionFileCompare( String n ) {
    super( n, "" );
    filename1 = null;
    filename2 = null;
    addFilenameToResult = false;
  }

  public ActionFileCompare() {
    this( "" );
  }

  public Object clone() {
    ActionFileCompare je = (ActionFileCompare) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 50 );

    retval.append( super.getXml() );
    retval.append( "      " ).append( XmlHandler.addTagValue( "filename1", filename1 ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "filename2", filename2 ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "add_filename_result", addFilenameToResult ) );

    return retval.toString();
  }

  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider, IVariables variables ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      filename1 = XmlHandler.getTagValue( entrynode, "filename1" );
      filename2 = XmlHandler.getTagValue( entrynode, "filename2" );
      addFilenameToResult = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "add_filename_result" ) );
    } catch ( HopXmlException xe ) {
      throw new HopXmlException( BaseMessages.getString(
        PKG, "ActionFileCompare.ERROR_0001_Unable_To_Load_From_Xml_Node" ), xe );
    }
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

        file1 = HopVfs.getFileObject( realFilename1 );
        file2 = HopVfs.getFileObject( realFilename2 );

        if ( file1.exists() && file2.exists() ) {
          if ( equalFileContents( file1, file2 ) ) {
            result.setResult( true );
          } else {
            result.setResult( false );
          }

          // add filename to result filenames
          if ( addFilenameToResult && file1.getType() == FileType.FILE && file2.getType() == FileType.FILE ) {
            ResultFile resultFile =
              new ResultFile( ResultFile.FILE_TYPE_GENERAL, file1, parentWorkflow.getWorkflowName(), toString() );
            resultFile.setComment( BaseMessages.getString( PKG, "JobWaitForFile.FilenameAdded" ) );
            result.getResultFiles().put( resultFile.getFile().toString(), resultFile );
            resultFile = new ResultFile( ResultFile.FILE_TYPE_GENERAL, file2, parentWorkflow.getWorkflowName(), toString() );
            resultFile.setComment( BaseMessages.getString( PKG, "JobWaitForFile.FilenameAdded" ) );
            result.getResultFiles().put( resultFile.getFile().toString(), resultFile );
          }
        } else {
          if ( !file1.exists() ) {
            logError( BaseMessages.getString(
              PKG, "ActionFileCompare.ERROR_0004_File1_Does_Not_Exist", realFilename1 ) );
          }
          if ( !file2.exists() ) {
            logError( BaseMessages.getString(
              PKG, "ActionFileCompare.ERROR_0005_File2_Does_Not_Exist", realFilename2 ) );
          }
          result.setResult( false );
          result.setNrErrors( 1 );
        }
      } else {
        logError( BaseMessages.getString( PKG, "ActionFileCompare.ERROR_0006_Need_Two_Filenames" ) );
      }
    } catch ( Exception e ) {
      result.setResult( false );
      result.setNrErrors( 1 );
      logError( BaseMessages.getString(
        PKG, "ActionFileCompare.ERROR_0007_Comparing_Files", realFilename2, realFilename2, e.getMessage() ) );
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

  @Override public boolean isEvaluation() {
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

  public List<ResourceReference> getResourceDependencies( IVariables variables, WorkflowMeta workflowMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( variables, workflowMeta );
    if ( ( !Utils.isEmpty( filename1 ) ) && ( !Utils.isEmpty( filename2 ) ) ) {
      String realFilename1 = resolve( filename1 );
      String realFilename2 = resolve( filename2 );
      ResourceReference reference = new ResourceReference( this );
      reference.getEntries().add( new ResourceEntry( realFilename1, ResourceType.FILE ) );
      reference.getEntries().add( new ResourceEntry( realFilename2, ResourceType.FILE ) );
      references.add( reference );
    }
    return references;
  }

  public void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace( ctx, getVariables() );
    AndValidator.putValidators( ctx, ActionValidatorUtils.notNullValidator(),
      ActionValidatorUtils.fileExistsValidator() );
    ActionValidatorUtils.andValidator().validate( this, "filename1", remarks, ctx );
    ActionValidatorUtils.andValidator().validate( this, "filename2", remarks, ctx );
  }

  @Override public WorkflowMeta getParentWorkflowMeta() {
    return parentWorkflowMeta;
  }

  @Override public void setParentWorkflowMeta( WorkflowMeta parentWorkflowMeta ) {
    this.parentWorkflowMeta = parentWorkflowMeta;
  }

}
