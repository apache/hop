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

package org.apache.hop.workflow.actions.filesexist;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.w3c.dom.Node;

import java.io.IOException;
import java.util.List;

/**
 * This defines a Files exist action.
 *
 * @author Samatar
 * @since 10-12-2007
 */

@Action(
  id = "FILES_EXIST",
  name = "i18n::ActionFilesExist.Name",
  description = "i18n::ActionFilesExist.Description",
  image = "FilesExist.svg",
  categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Conditions",
  documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/filesexist.html"
)
public class ActionFilesExist extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionFilesExist.class; // For Translator

  private String filename; // TODO: looks like it is not used: consider deleting

  private String[] arguments;

  public ActionFilesExist( String n ) {
    super( n, "" );
    filename = null;
  }

  public ActionFilesExist() {
    this( "" );
  }

  public void allocate( int nrFields ) {
    arguments = new String[ nrFields ];
  }

  public Object clone() {
    ActionFilesExist je = (ActionFilesExist) super.clone();
    if ( arguments != null ) {
      int nrFields = arguments.length;
      je.allocate( nrFields );
      System.arraycopy( arguments, 0, je.arguments, 0, nrFields );
    }
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( 30 );

    retval.append( super.getXml() );

    retval.append( "      " ).append( XmlHandler.addTagValue( "filename", filename ) );

    retval.append( "      <fields>" ).append( Const.CR );
    if ( arguments != null ) {
      for ( int i = 0; i < arguments.length; i++ ) {
        retval.append( "        <field>" ).append( Const.CR );
        retval.append( "          " ).append( XmlHandler.addTagValue( "name", arguments[ i ] ) );
        retval.append( "        </field>" ).append( Const.CR );
      }
    }
    retval.append( "      </fields>" ).append( Const.CR );

    return retval.toString();
  }

  public void loadXml( Node entrynode,
                       IHopMetadataProvider metadataProvider, IVariables variables ) throws HopXmlException {
    try {
      super.loadXml( entrynode );
      filename = XmlHandler.getTagValue( entrynode, "filename" );

      Node fields = XmlHandler.getSubNode( entrynode, "fields" );

      // How many field arguments?
      int nrFields = XmlHandler.countNodes( fields, "field" );
      allocate( nrFields );

      // Read them all...
      for ( int i = 0; i < nrFields; i++ ) {
        Node fnode = XmlHandler.getSubNodeByNr( fields, "field", i );

        arguments[ i ] = XmlHandler.getTagValue( fnode, "name" );

      }
    } catch ( HopXmlException xe ) {
      throw new HopXmlException( BaseMessages.getString(
        PKG, "ActionFilesExist.ERROR_0001_Cannot_Load_Job_Entry_From_Xml_Node", xe.getMessage() ) );
    }
  }

  public void setFilename( String filename ) {
    this.filename = filename;
  }

  public String getFilename() {
    return filename;
  }

  public String[] getArguments() {
    return arguments;
  }

  public void setArguments( String[] arguments ) {
    this.arguments = arguments;
  }

  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;
    result.setResult( false );
    result.setNrErrors( 0 );
    int missingfiles = 0;
    int nrErrors = 0;

    // see PDI-10270 for details
    boolean oldBehavior =
      "Y".equalsIgnoreCase( getVariable( Const.HOP_COMPATIBILITY_SET_ERROR_ON_SPECIFIC_WORKFLOW_ACTIONS, "N" ) );

    if ( arguments != null ) {
      for ( int i = 0; i < arguments.length && !parentWorkflow.isStopped(); i++ ) {
        FileObject file = null;

        try {
          String realFilefoldername = resolve( arguments[ i ] );
          file = HopVfs.getFileObject( realFilefoldername );

          if ( file.exists() && file.isReadable() ) { // TODO: is it needed to check file for readability?
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "ActionFilesExist.File_Exists", realFilefoldername ) );
            }
          } else {
            missingfiles++;
            if ( log.isDetailed() ) {
              logDetailed( BaseMessages.getString(
                PKG, "ActionFilesExist.File_Does_Not_Exist", realFilefoldername ) );
            }
          }

        } catch ( Exception e ) {
          nrErrors++;
          missingfiles++;
          logError( BaseMessages.getString( PKG, "ActionFilesExist.ERROR_0004_IO_Exception", e.toString() ), e );
        } finally {
          if ( file != null ) {
            try {
              file.close();
              file = null;
            } catch ( IOException ex ) { /* Ignore */
            }
          }
        }
      }

    }

    result.setNrErrors( nrErrors );

    if ( oldBehavior ) {
      result.setNrErrors( missingfiles );
    }

    if ( missingfiles == 0 ) {
      result.setResult( true );
    }

    return result;
  }

  @Override public boolean isEvaluation() {
    return true;
  }

  @Override
  public void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
  }

}
