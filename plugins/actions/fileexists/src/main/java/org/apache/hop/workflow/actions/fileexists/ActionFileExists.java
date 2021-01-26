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

package org.apache.hop.workflow.actions.fileexists;

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
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.util.List;
import java.util.Map;

/**
 * This defines an SQL action.
 *
 * @author Matt
 * @since 05-11-2003
 */

@Action(
  id = "FILE_EXISTS",
  name = "i18n::ActionFileExists.Name",
  description = "i18n::ActionFileExists.Description",
  image = "FileExists.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Conditions",
  documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/fileexists.html"
)
public class ActionFileExists extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionFileExists.class; // For Translator

  private String filename;

  public ActionFileExists( String n ) {
    super( n, "" );
    filename = null;
  }

  public ActionFileExists() {
    this( "" );
  }

  public Object clone() {
    ActionFileExists je = (ActionFileExists) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 20 );

    retval.append( super.getXml() );
    retval.append( "      " ).append( XmlHandler.addTagValue( "filename", filename ) );

    return retval.toString();
  }

  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider, IVariables variables ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      filename = XmlHandler.getTagValue( entrynode, "filename" );
    } catch ( HopXmlException xe ) {
      throw new HopXmlException( BaseMessages.getString(
        PKG, "ActionFileExists.ERROR_0001_Cannot_Load_Job_Entry_From_Xml_Node" ), xe );
    }
  }

  public void setFilename( String filename ) {
    this.filename = filename;
  }

  public String getFilename() {
    return filename;
  }

  public String getRealFilename() {
    return resolve( getFilename() );
  }

  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    result.setResult( false );
    result.setNrErrors( 0 );

    if ( filename != null ) {

      String realFilename = getRealFilename();
      try {
        FileObject file = HopVfs.getFileObject( realFilename );
        if ( file.exists() && file.isReadable() ) {
          logDetailed( BaseMessages.getString( PKG, "ActionFileExists.File_Exists", realFilename ) );
          result.setResult( true );
        } else {
          logDetailed( BaseMessages.getString( PKG, "ActionFileExists.File_Does_Not_Exist", realFilename ) );
        }
      } catch ( Exception e ) {
        result.setNrErrors( 1 );
        logError( BaseMessages.getString( PKG, "ActionFileExists.ERROR_0004_IO_Exception", e.getMessage() ), e );
      }
    } else {
      result.setNrErrors( 1 );
      logError( BaseMessages.getString( PKG, "ActionFileExists.ERROR_0005_No_Filename_Defined" ) );
    }

    return result;
  }

  @Override public boolean isEvaluation() {
    return true;
  }

  public List<ResourceReference> getResourceDependencies( IVariables variables, WorkflowMeta workflowMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( variables, workflowMeta );
    if ( !Utils.isEmpty( filename ) ) {
      String realFileName = resolve( filename );
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

  /**
   * Exports the object to a flat-file system, adding content with filename keys to a set of definitions. The supplied
   * resource naming interface allows the object to name appropriately without worrying about those parts of the
   * implementation specific details.
   *
   * @param variables           The variable variables to resolve (environment) variables with.
   * @param definitions     The map containing the filenames and content
   * @param namingInterface The resource naming interface allows the object to be named appropriately
   * @param metadataProvider       the metadataProvider to load external metadata from
   * @return The filename for this object. (also contained in the definitions map)
   * @throws HopException in case something goes wrong during the export
   */
  public String exportResources( IVariables variables, Map<String, ResourceDefinition> definitions,
                                 IResourceNaming namingInterface, IHopMetadataProvider metadataProvider ) throws HopException {
    try {
      // The object that we're modifying here is a copy of the original!
      // So let's change the filename from relative to absolute by grabbing the file object...
      // In case the name of the file comes from previous transforms, forget about this!
      //
      if ( !Utils.isEmpty( filename ) ) {
        // From : ${FOLDER}/../foo/bar.csv
        // To : /home/matt/test/files/foo/bar.csv
        //
        FileObject fileObject = HopVfs.getFileObject( variables.resolve( filename ) );

        // If the file doesn't exist, forget about this effort too!
        //
        if ( fileObject.exists() ) {
          // Convert to an absolute path...
          //
          filename = namingInterface.nameResource( fileObject, variables, true );

          return filename;
        }
      }
      return null;
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }
}
