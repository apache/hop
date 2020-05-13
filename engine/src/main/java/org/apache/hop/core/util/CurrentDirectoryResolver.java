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

package org.apache.hop.core.util;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/**
 * This class resolve and update system variables
 * {@link org.apache.hop.core.Const#INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY}
 * {@link org.apache.hop.core.Const#INTERNAL_VARIABLE_WORKFLOW_FILENAME_DIRECTORY}
 * {@link org.apache.hop.core.Const#INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY}
 * {@link org.apache.hop.core.Const#INTERNAL_VARIABLE_WORKFLOW_FILENAME_NAME}
 */
public class CurrentDirectoryResolver {

  public CurrentDirectoryResolver() {
  }

  /**
   * The logic of this method:
   * <p>
   * We return the child var space with directory extracted from filename
   * if we do not have a filename we will return the child var space without updates
   *
   * @param parentVariables - parent variable space which can be inherited
   * @param filename        - is file which we use at this moment
   * @return new var space if inherit was set false or child var space with updated system variables
   */
  public IVariables resolveCurrentDirectory( IVariables parentVariables, String filename ) {
    Variables tmpSpace = new Variables();
    tmpSpace.setParentVariableSpace( parentVariables );
    tmpSpace.initializeVariablesFrom( parentVariables );

    if ( filename != null ) {
      try {
        FileObject fileObject = HopVfs.getFileObject( filename, tmpSpace );

        if ( !fileObject.exists() ) {
          // don't set variables if the file doesn't exist
          return tmpSpace;
        }

        FileName fileName = fileObject.getName();

        // The filename of the pipeline
        tmpSpace.setVariable( Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_NAME, fileName.getBaseName() );

        // The directory of the pipeline
        FileName fileDir = fileName.getParent();
        tmpSpace.setVariable( Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY, fileDir.getURI() );
        tmpSpace.setVariable( Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_DIRECTORY, fileDir.getURI() );
        tmpSpace.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY, fileDir.getURI() );
      } catch ( Exception e ) {
        throw new RuntimeException( "Unable to figure out the current directory", e );
      }
    }
    return tmpSpace;
  }

  public IVariables resolveCurrentDirectory( IVariables parentVariables, TransformMeta transformMeta, String filename ) {
    if ( transformMeta != null && transformMeta.getParentPipelineMeta() != null ) {
      filename = transformMeta.getParentPipelineMeta().getFilename();
    } else if ( transformMeta != null && transformMeta.getParentPipelineMeta() != null && filename == null ) {
      filename = transformMeta.getParentPipelineMeta().getFilename();
    }
    return resolveCurrentDirectory( parentVariables, filename );
  }

  public IVariables resolveCurrentDirectory( IVariables parentVariables, IWorkflowEngine<WorkflowMeta> workflow, String filename ) {
    if ( workflow != null && filename == null ) {
      filename = workflow.getFilename();
    } else if ( WorkflowMeta.class.isAssignableFrom( parentVariables.getClass() ) ) {
      // additional fallback protection for design mode
      WorkflowMeta realParent = null;
      realParent = (WorkflowMeta) parentVariables;
      filename = realParent.getFilename();
    }
    return resolveCurrentDirectory( parentVariables, filename );
  }

  public String normalizeSlashes( String str ) {
    if ( StringUtils.isBlank( str ) ) {
      return str;
    }
    while ( str.contains( "\\" ) ) {
      str = str.replace( "\\", "/" );
    }
    while ( str.contains( "//" ) ) {
      str = str.replace( "//", "/" );
    }
    return str;
  }

}
