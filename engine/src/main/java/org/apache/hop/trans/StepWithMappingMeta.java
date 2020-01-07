/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.trans;

import static org.apache.hop.core.Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY;
import static org.apache.hop.core.Const.INTERNAL_VARIABLE_JOB_FILENAME_DIRECTORY;
import static org.apache.hop.core.Const.INTERNAL_VARIABLE_TRANSFORMATION_FILENAME_DIRECTORY;
import static org.apache.hop.core.Const.INTERNAL_VARIABLE_JOB_FILENAME_NAME;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.parameters.DuplicateParamException;
import org.apache.hop.core.parameters.NamedParams;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.util.CurrentDirectoryResolver;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.util.serialization.BaseSerializingMeta;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceNamingInterface;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.trans.steps.mapping.MappingIODefinition;
import org.apache.hop.metastore.api.IMetaStore;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class is supposed to use in steps where the mapping to sub transformations takes place
 *
 * @since 02-jan-2017
 * @author Yury Bakhmutski
 */
public abstract class StepWithMappingMeta extends BaseSerializingMeta implements StepMetaInterface {
  //default value
  private static Class<?> PKG = StepWithMappingMeta.class;

  protected String fileName;

  public static TransMeta loadMappingMeta( StepWithMappingMeta mappingMeta, IMetaStore metaStore, VariableSpace space ) throws HopException {
    return loadMappingMeta( mappingMeta, metaStore, space, true );
  }

  /**
   * @return new var space with follow vars from parent space or just new space if parent was null
   * 
   * {@link org.apache.hop.core.Const#INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY}
   * {@link org.apache.hop.core.Const#INTERNAL_VARIABLE_JOB_FILENAME_DIRECTORY}
   * {@link org.apache.hop.core.Const#INTERNAL_VARIABLE_TRANSFORMATION_FILENAME_DIRECTORY}
   * {@link org.apache.hop.core.Const#INTERNAL_VARIABLE_JOB_FILENAME_NAME}
   */
  private static VariableSpace getVarSpaceOnlyWithRequiredParentVars( VariableSpace parentSpace ) {
    Variables tmpSpace = new Variables();
    if ( parentSpace != null ) {
      tmpSpace.setVariable( INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY, parentSpace.getVariable( INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY ) );
      tmpSpace.setVariable( INTERNAL_VARIABLE_JOB_FILENAME_DIRECTORY, parentSpace.getVariable( INTERNAL_VARIABLE_JOB_FILENAME_DIRECTORY ) );
      tmpSpace.setVariable( INTERNAL_VARIABLE_TRANSFORMATION_FILENAME_DIRECTORY, parentSpace.getVariable( INTERNAL_VARIABLE_TRANSFORMATION_FILENAME_DIRECTORY ) );
      tmpSpace.setVariable( INTERNAL_VARIABLE_JOB_FILENAME_NAME, parentSpace.getVariable( INTERNAL_VARIABLE_JOB_FILENAME_NAME ) );
    }
    return tmpSpace;
  }

  public static synchronized TransMeta loadMappingMeta( StepWithMappingMeta executorMeta,
                                                        IMetaStore metaStore, VariableSpace space, boolean share ) throws HopException {
    TransMeta mappingTransMeta = null;

    CurrentDirectoryResolver r = new CurrentDirectoryResolver();
    // send restricted parentVariables with several important options
    // Otherwise we destroy child variables and the option "Inherit all variables from the transformation" is enabled always.
    VariableSpace tmpSpace = r.resolveCurrentDirectory( getVarSpaceOnlyWithRequiredParentVars( space ),
        executorMeta.getParentStepMeta(), executorMeta.getFileName() );

        String realFilename = tmpSpace.environmentSubstitute( executorMeta.getFileName() );
        if ( space != null ) {
          // This is a parent transformation and parent variable should work here. A child file name can be resolved via parent space.
          realFilename = space.environmentSubstitute( realFilename );
        }
        try {
          // OK, load the meta-data from file...
          // Don't set internal variables: they belong to the parent thread!
          if ( mappingTransMeta == null ) {
            mappingTransMeta = new TransMeta( realFilename, metaStore, true, tmpSpace );
            LogChannel.GENERAL.logDetailed( "Loading transformation from repository", "Transformation was loaded from XML file [" + realFilename + "]" );
          }
        } catch ( Exception e ) {
          throw new HopException( BaseMessages.getString( PKG, "StepWithMappingMeta.Exception.UnableToLoadTrans" ), e );
        }

    if ( mappingTransMeta == null ) {  //skip warning
      return null;
    }


    if ( share ) {
      //  When the child parameter does exist in the parent parameters, overwrite the child parameter by the
      // parent parameter.
      replaceVariableValues( mappingTransMeta, space );
      // All other parent parameters need to get copied into the child parameters  (when the 'Inherit all
      // variables from the transformation?' option is checked)
      addMissingVariables( mappingTransMeta, space );
    }
    mappingTransMeta.setMetaStore( metaStore );
    mappingTransMeta.setFilename( mappingTransMeta.getFilename() );

    return mappingTransMeta;
  }

  public static void activateParams( VariableSpace childVariableSpace, NamedParams childNamedParams, VariableSpace parent, String[] listParameters,
                                     String[] mappingVariables, String[] inputFields ) {
    activateParams(  childVariableSpace,  childNamedParams,  parent, listParameters, mappingVariables,  inputFields, true );
  }

  public static void activateParams( VariableSpace childVariableSpace, NamedParams childNamedParams, VariableSpace parent, String[] listParameters,
                                     String[] mappingVariables, String[] inputFields, boolean isPassingAllParameters ) {
    Map<String, String> parameters = new HashMap<>();
    Set<String> subTransParameters = new HashSet<>( Arrays.asList( listParameters ) );

    if ( mappingVariables != null ) {
      for ( int i = 0; i < mappingVariables.length; i++ ) {
        parameters.put( mappingVariables[ i ], parent.environmentSubstitute( inputFields[ i ] ) );
        //If inputField value is not empty then create it in variableSpace of step(Parent)
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
        // there is a definition only in Transformation properties - params tab
        parameters.put( variableName, parent.getVariable( variableName ) );
      }
    }

    for ( Map.Entry<String, String> entry : parameters.entrySet() ) {
      String key = entry.getKey();
      String value = Const.NVL( entry.getValue(), "" );
      if ( subTransParameters.contains( key ) ) {
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
  public String getFileName() {
    return fileName;
  }

  /**
   * @param fileName the fileName to set
   */
  public void setFileName( String fileName ) {
    this.fileName = fileName;
  }
  /**
   * @param fileName the fileName to set
   */
  public void replaceFileName( String fileName ) {
    this.fileName = fileName;
  }

  @Override
  public String exportResources( VariableSpace space, Map<String, ResourceDefinition> definitions,
                                 ResourceNamingInterface resourceNamingInterface,
                                 IMetaStore metaStore ) throws HopException {
    try {
      // Try to load the transformation from repository or file.
      // Modify this recursively too...
      //
      // NOTE: there is no need to clone this step because the caller is
      // responsible for this.
      //
      // First load the mapping transformation...
      //
      TransMeta mappingTransMeta = loadMappingMeta( this, metaStore, space );

      // Also go down into the mapping transformation and export the files
      // there. (mapping recursively down)
      //
      String proposedNewFilename =
              mappingTransMeta.exportResources(
                      mappingTransMeta, definitions, resourceNamingInterface, metaStore );

      // To get a relative path to it, we inject
      // ${Internal.Entry.Current.Directory}
      //
      String newFilename = "${" + INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY + "}/" + proposedNewFilename;

      // Set the correct filename inside the XML.
      //
      mappingTransMeta.setFilename( newFilename );

      // change it in the job entry
      //
      replaceFileName( newFilename );

      return proposedNewFilename;
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG, "StepWithMappingMeta.Exception.UnableToLoadTrans", fileName ) );
    }
  }

  public static  void addMissingVariables( VariableSpace fromSpace, VariableSpace toSpace ) {
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

  public static void replaceVariableValues( VariableSpace childTransMeta, VariableSpace replaceBy, String type ) {
    if ( replaceBy == null ) {
      return;
    }
    String[] variableNames = replaceBy.listVariables();
    for ( String variableName : variableNames ) {
      if ( childTransMeta.getVariable( variableName ) != null && !isInternalVariable( variableName, type ) ) {
        childTransMeta.setVariable( variableName, replaceBy.getVariable( variableName ) );
      }
    }
  }

  public static void replaceVariableValues( VariableSpace childTransMeta, VariableSpace replaceBy ) {
    replaceVariableValues(  childTransMeta,  replaceBy, "" );
  }

  private static boolean isInternalVariable( String variableName, String type ) {
    return type.equals( "Trans" ) ? isTransInternalVariable( variableName )
      : type.equals( "Job" ) ? isJobInternalVariable( variableName )
      : isJobInternalVariable( variableName ) || isTransInternalVariable( variableName );
  }

  private static boolean isTransInternalVariable( String variableName ) {
    return ( Arrays.asList( Const.INTERNAL_TRANS_VARIABLES ).contains( variableName ) );
  }

  private static boolean isJobInternalVariable( String variableName ) {
    return ( Arrays.asList( Const.INTERNAL_JOB_VARIABLES ).contains( variableName ) );
  }

  /**
   * @return the inputMappings
   */
  public List<MappingIODefinition> getInputMappings() {
    return Collections.emptyList();
  }

  /**
   * @return the outputMappings
   */
  public List<MappingIODefinition> getOutputMappings() {
    return Collections.emptyList();
  }
}
