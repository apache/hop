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

package org.apache.hop.pipeline;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.parameters.DuplicateParamException;
import org.apache.hop.core.parameters.INamedParams;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.util.CurrentDirectoryResolver;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.util.serialization.BaseSerializingMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.hop.core.Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY;
import static org.apache.hop.core.Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY;
import static org.apache.hop.core.Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_DIRECTORY;
import static org.apache.hop.core.Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_NAME;

/**
 * This class is supposed to use in transforms where the mapping to sub pipelines takes place
 *
 * @author Yury Bakhmutski
 * @since 02-jan-2017
 */
public abstract class TransformWithMappingMeta<Main extends ITransform, Data extends ITransformData>
  extends BaseSerializingMeta<Main, Data> implements ITransformMeta<Main, Data> {
  //default value
  private static Class<?> PKG = TransformWithMappingMeta.class;

  protected String filename;

  public static PipelineMeta loadMappingMeta( TransformWithMappingMeta mappingMeta, IMetaStore metaStore, IVariables variables ) throws HopException {
    return loadMappingMeta( mappingMeta, metaStore, variables, true );
  }

  /**
   * @return new var space with follow vars from parent space or just new space if parent was null
   * <p>
   * {@link org.apache.hop.core.Const#INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY}
   * {@link org.apache.hop.core.Const#INTERNAL_VARIABLE_WORKFLOW_FILENAME_DIRECTORY}
   * {@link org.apache.hop.core.Const#INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY}
   * {@link org.apache.hop.core.Const#INTERNAL_VARIABLE_WORKFLOW_FILENAME_NAME}
   */
  private static IVariables getVarSpaceOnlyWithRequiredParentVars( IVariables parentSpace ) {
    Variables tmpSpace = new Variables();
    if ( parentSpace != null ) {
      tmpSpace.setVariable( INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY, parentSpace.getVariable( INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY ) );
      tmpSpace.setVariable( INTERNAL_VARIABLE_WORKFLOW_FILENAME_DIRECTORY, parentSpace.getVariable( INTERNAL_VARIABLE_WORKFLOW_FILENAME_DIRECTORY ) );
      tmpSpace.setVariable( INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY, parentSpace.getVariable( INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY ) );
      tmpSpace.setVariable( INTERNAL_VARIABLE_WORKFLOW_FILENAME_NAME, parentSpace.getVariable( INTERNAL_VARIABLE_WORKFLOW_FILENAME_NAME ) );
    }
    return tmpSpace;
  }

  public static synchronized PipelineMeta loadMappingMeta( TransformWithMappingMeta executorMeta,
                                                           IMetaStore metaStore, IVariables variables, boolean share ) throws HopException {
    PipelineMeta mappingPipelineMeta = null;

    CurrentDirectoryResolver r = new CurrentDirectoryResolver();
    // send restricted parentVariables with several important options
    // Otherwise we destroy child variables and the option "Inherit all variables from the pipeline" is enabled always.
    IVariables tmpSpace = r.resolveCurrentDirectory( getVarSpaceOnlyWithRequiredParentVars( variables ),
      executorMeta.getParentTransformMeta(), executorMeta.getFilename() );

    String realFilename = tmpSpace.environmentSubstitute( executorMeta.getFilename() );
    if ( variables != null ) {
      // This is a parent pipeline and parent variable should work here. A child file name can be resolved via parent space.
      realFilename = variables.environmentSubstitute( realFilename );
    }
    try {
      // OK, load the meta-data from file...
      // Don't set internal variables: they belong to the parent thread!
      if ( mappingPipelineMeta == null ) {
        mappingPipelineMeta = new PipelineMeta( realFilename, metaStore, true, tmpSpace );
        LogChannel.GENERAL.logDetailed( "Loading pipeline", "Pipeline was loaded from XML file [" + realFilename + "]" );
      }
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG, "TransformWithMappingMeta.Exception.UnableToLoadPipeline" ), e );
    }

    if ( mappingPipelineMeta == null ) {  //skip warning
      return null;
    }


    if ( share ) {
      //  When the child parameter does exist in the parent parameters, overwrite the child parameter by the
      // parent parameter.
      replaceVariableValues( mappingPipelineMeta, variables );
      // All other parent parameters need to get copied into the child parameters  (when the 'Inherit all
      // variables from the pipeline?' option is checked)
      addMissingVariables( mappingPipelineMeta, variables );
    }
    mappingPipelineMeta.setMetaStore( metaStore );
    mappingPipelineMeta.setFilename( mappingPipelineMeta.getFilename() );

    return mappingPipelineMeta;
  }

  public static void activateParams( IVariables childVariableSpace, INamedParams childNamedParams, IVariables parent, String[] listParameters,
                                     String[] mappingVariables, String[] inputFields ) {
    activateParams( childVariableSpace, childNamedParams, parent, listParameters, mappingVariables, inputFields, true );
  }

  public static void activateParams( IVariables childVariableSpace, INamedParams childNamedParams, IVariables parent, String[] listParameters,
                                     String[] mappingVariables, String[] inputFields, boolean isPassingAllParameters ) {
    Map<String, String> parameters = new HashMap<>();
    Set<String> subPipelineParameters = new HashSet<>( Arrays.asList( listParameters ) );

    if ( mappingVariables != null ) {
      for ( int i = 0; i < mappingVariables.length; i++ ) {
        parameters.put( mappingVariables[ i ], parent.environmentSubstitute( inputFields[ i ] ) );
        //If inputField value is not empty then create it in variables of transform(Parent)
        if ( !Utils.isEmpty( Const.trim( parent.environmentSubstitute( inputFields[ i ] ) ) ) ) {
          parent.setVariable( mappingVariables[ i ], parent.environmentSubstitute( inputFields[ i ] ) );
        }
      }
    }

    for ( String variableName : parent.listVariables() ) {
      // When the child parameter does exist in the parent parameters, overwrite the child parameter by the
      // parent parameter.
      if ( parameters.containsKey( variableName ) ) {
        parameters.put( variableName, parent.getVariable( variableName ) );
        // added  isPassingAllParameters check since we don't need to overwrite the child value if the
        // isPassingAllParameters is not checked
      } else if ( ArrayUtils.contains( listParameters, variableName ) && isPassingAllParameters ) {
        // there is a definition only in Pipeline properties - params tab
        parameters.put( variableName, parent.getVariable( variableName ) );
      }
    }

    for ( Map.Entry<String, String> entry : parameters.entrySet() ) {
      String key = entry.getKey();
      String value = Const.NVL( entry.getValue(), "" );
      if ( subPipelineParameters.contains( key ) ) {
        try {
          childNamedParams.setParameterValue( key, Const.NVL( entry.getValue(), "" ) );
        } catch ( UnknownParamException e ) {
          // this is explicitly checked for up front
        }
      } else {
        try {
          childNamedParams.addParameterDefinition( key, "", "" );
          childNamedParams.setParameterValue( key, value );
        } catch ( DuplicateParamException e ) {
          // this was explicitly checked before
        } catch ( UnknownParamException e ) {
          // this is explicitly checked for up front
        }

        childVariableSpace.setVariable( key, value );
      }
    }

    childNamedParams.activateParameters();
  }


  /**
   * @return the fileName
   */
  public String getFilename() {
    return filename;
  }

  /**
   * @param filename the fileName to set
   */
  public void setFilename( String filename ) {
    this.filename = filename;
  }

  /**
   * @param fileName the fileName to set
   */
  public void replaceFileName( String fileName ) {
    this.filename = fileName;
  }

  @Override
  public String exportResources( IVariables variables, Map<String, ResourceDefinition> definitions,
                                 IResourceNaming iResourceNaming,
                                 IMetaStore metaStore ) throws HopException {
    try {
      // Try to load the pipeline from a file.
      // Modify this recursively too...
      //
      // NOTE: there is no need to clone this transform because the caller is
      // responsible for this.
      //
      // First load the mapping pipeline...
      //
      PipelineMeta mappingPipelineMeta = loadMappingMeta( this, metaStore, variables );

      // Also go down into the mapping pipeline and export the files
      // there. (mapping recursively down)
      //
      String proposedNewFilename =
        mappingPipelineMeta.exportResources(
          mappingPipelineMeta, definitions, iResourceNaming, metaStore );

      // To get a relative path to it, we inject
      // ${Internal.Entry.Current.Directory}
      //
      String newFilename = "${" + INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY + "}/" + proposedNewFilename;

      // Set the correct filename inside the XML.
      //
      mappingPipelineMeta.setFilename( newFilename );

      // change it in the action
      //
      replaceFileName( newFilename );

      return proposedNewFilename;
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG, "TransformWithMappingMeta.Exception.UnableToLoadPipeline", filename ) );
    }
  }

  public static void addMissingVariables( IVariables fromSpace, IVariables toSpace ) {
    if ( toSpace == null ) {
      return;
    }
    String[] variableNames = toSpace.listVariables();
    for ( String variable : variableNames ) {
      if ( fromSpace.getVariable( variable ) == null ) {
        fromSpace.setVariable( variable, toSpace.getVariable( variable ) );
      }
    }
  }

  public static void replaceVariableValues( IVariables childPipelineMeta, IVariables replaceBy, String type ) {
    if ( replaceBy == null ) {
      return;
    }
    String[] variableNames = replaceBy.listVariables();
    for ( String variableName : variableNames ) {
      if ( childPipelineMeta.getVariable( variableName ) != null && !isInternalVariable( variableName, type ) ) {
        childPipelineMeta.setVariable( variableName, replaceBy.getVariable( variableName ) );
      }
    }
  }

  public static void replaceVariableValues( IVariables childPipelineMeta, IVariables replaceBy ) {
    replaceVariableValues( childPipelineMeta, replaceBy, "" );
  }

  private static boolean isInternalVariable( String variableName, String type ) {
    return type.equals( "Pipeline" ) ? isPipelineInternalVariable( variableName )
      : type.equals( "Workflow" ) ? isJobInternalVariable( variableName )
      : isJobInternalVariable( variableName ) || isPipelineInternalVariable( variableName );
  }

  private static boolean isPipelineInternalVariable( String variableName ) {
    return ( Arrays.asList( Const.INTERNAL_PIPELINE_VARIABLES ).contains( variableName ) );
  }

  private static boolean isJobInternalVariable( String variableName ) {
    return ( Arrays.asList( Const.INTERNAL_WORKFLOW_VARIABLES ).contains( variableName ) );
  }

}
