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

package org.apache.hop.workflow.actions.writetofile;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.List;

/**
 * This defines a 'write to file' action. Its main use would be to create empty trigger files that can be used to
 * control the flow in ETL cycles.
 *
 * @author Samatar Hassan
 * @since 28-01-2007
 */

@Action(
  id = "WRITE_TO_FILE",
  name = "i18n::ActionWriteToFile.Name",
  description = "i18n::ActionWriteToFile.Description",
  image = "WriteToFile.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
  documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/writetofile.html"
)
public class ActionWriteToFile extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionWriteToFile.class; // For Translator

  private String filename;
  private boolean createParentFolder;
  private boolean appendFile;
  private String content;
  private String encoding;

  public ActionWriteToFile( String n ) {
    super( n, "" );
    filename = null;
    createParentFolder = false;
    appendFile = false;
    content = null;
    encoding = null;
  }

  public ActionWriteToFile() {
    this( "" );
  }

  public Object clone() {
    ActionWriteToFile je = (ActionWriteToFile) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 100 );

    retval.append( super.getXml() );
    retval.append( "      " ).append( XmlHandler.addTagValue( "filename", filename ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "createParentFolder", createParentFolder ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "appendFile", appendFile ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "content", content ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "encoding", encoding ) );

    return retval.toString();
  }

  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider, IVariables variables ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      filename = XmlHandler.getTagValue( entrynode, "filename" );
      createParentFolder = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "createParentFolder" ) );
      appendFile = "Y".equalsIgnoreCase( XmlHandler.getTagValue( entrynode, "appendFile" ) );
      content = XmlHandler.getTagValue( entrynode, "content" );
      encoding = XmlHandler.getTagValue( entrynode, "encoding" );
    } catch ( HopXmlException xe ) {
      throw new HopXmlException( "Unable to load action of type 'create file' from XML node", xe );
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
    return resolve( getFilename() );
  }

  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    result.setResult( false );
    result.setNrErrors( 1 );

    String realFilename = getRealFilename();
    if ( !Utils.isEmpty( realFilename ) ) {

      String content = resolve( getContent() );
      String encoding = resolve( getEncoding() );

      OutputStreamWriter osw = null;
      OutputStream os = null;
      try {

        // Create parent folder if needed
        createParentFolder( realFilename );

        // Create / open file for writing
        os = HopVfs.getOutputStream( realFilename, isAppendFile() );

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
      parent = HopVfs.getFileObject( realFilename ).getParent();
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

  @Override public boolean isEvaluation() {
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

  public List<ResourceReference> getResourceDependencies( IVariables variables, WorkflowMeta workflowMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( variables, workflowMeta );
    if ( !Utils.isEmpty( getFilename() ) ) {
      String realFileName = resolve( getFilename() );
      ResourceReference reference = new ResourceReference( this );
      reference.getEntries().add( new ResourceEntry( realFileName, ResourceType.FILE ) );
      references.add( reference );
    }
    return references;
  }

  @Override
  public void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
    ActionValidatorUtils.andValidator().validate( this, "filename", remarks,
      AndValidator.putValidators( ActionValidatorUtils.notBlankValidator() ) );
  }
}
