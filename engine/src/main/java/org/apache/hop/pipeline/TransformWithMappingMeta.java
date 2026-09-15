/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline;

import static org.apache.hop.core.Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER;
import static org.apache.hop.core.Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY;
import static org.apache.hop.core.Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER;
import static org.apache.hop.core.Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_NAME;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.parameters.INamedParameters;
import org.apache.hop.core.parameters.SubExecutionParameters;
import org.apache.hop.core.util.CurrentDirectoryResolver;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;

/** This class is supposed to use in transforms where the mapping to sub pipelines takes place */
public abstract class TransformWithMappingMeta<Main extends ITransform, Data extends ITransformData>
    extends BaseTransformMeta<Main, Data> {

  private static final Class<?> PKG = TransformWithMappingMeta.class;

  @HopMetadataProperty(hopMetadataPropertyType = HopMetadataPropertyType.PIPELINE_FILE)
  protected String filename;

  public TransformWithMappingMeta() {
    super();
  }

  public TransformWithMappingMeta(TransformWithMappingMeta m) {
    this();
    this.filename = m.filename;
  }

  /**
   * @return new var variables with follow vars from parent variables or just new variables if
   *     parent was null
   *     <p>{@link Const#INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER} {@link
   *     Const#INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER} {@link
   *     Const#INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY} {@link
   *     Const#INTERNAL_VARIABLE_WORKFLOW_FILENAME_NAME}
   */
  private static IVariables getVarSpaceOnlyWithRequiredParentVars(IVariables parentSpace) {
    Variables tmpSpace = new Variables();
    if (parentSpace != null) {
      tmpSpace.setVariable(
          INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER,
          parentSpace.getVariable(INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER));
      tmpSpace.setVariable(
          INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER,
          parentSpace.getVariable(INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER));
      tmpSpace.setVariable(
          INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY,
          parentSpace.getVariable(INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY));
      tmpSpace.setVariable(
          INTERNAL_VARIABLE_WORKFLOW_FILENAME_NAME,
          parentSpace.getVariable(INTERNAL_VARIABLE_WORKFLOW_FILENAME_NAME));
    }
    return tmpSpace;
  }

  public static synchronized PipelineMeta loadMappingMeta(
      TransformWithMappingMeta executorMeta,
      IHopMetadataProvider metadataProvider,
      IVariables variables)
      throws HopException {
    return loadMappingMeta(executorMeta, null, metadataProvider, variables);
  }

  /**
   * Loads child pipeline metadata from a file.
   *
   * @param explicitPipelineFilename when non-empty, this path is loaded and resolved instead of the
   *     filename stored on {@code executorMeta}. Use when the pipeline path is taken from an
   *     incoming row at runtime so multiple transform copies must not mutate shared meta.
   */
  public static synchronized PipelineMeta loadMappingMeta(
      TransformWithMappingMeta executorMeta,
      String explicitPipelineFilename,
      IHopMetadataProvider metadataProvider,
      IVariables variables)
      throws HopException {
    PipelineMeta mappingPipelineMeta = null;

    String filenameToUse =
        !Utils.isEmpty(explicitPipelineFilename)
            ? explicitPipelineFilename
            : executorMeta.getFilename();

    CurrentDirectoryResolver r = new CurrentDirectoryResolver();
    // send restricted parentVariables with several important options
    // Otherwise we destroy child variables and the option "Inherit all variables from the pipeline"
    // is enabled always.
    IVariables tmpSpace =
        r.resolveCurrentDirectory(
            getVarSpaceOnlyWithRequiredParentVars(variables),
            executorMeta.getParentTransformMeta(),
            filenameToUse);

    String realFilename = tmpSpace.resolve(filenameToUse);
    if (variables != null) {
      // This is a parent pipeline and parent variable should work here. A child file name can be
      // resolved via parent variables.
      realFilename = variables.resolve(realFilename);
    }
    try {
      // OK, load the meta-data from file...
      // Don't set internal variables: they belong to the parent thread!
      if (mappingPipelineMeta == null) {
        mappingPipelineMeta = new PipelineMeta(realFilename, metadataProvider, tmpSpace);
        LogChannel.GENERAL.logDetailed(
            "Loading pipeline", "Pipeline was loaded from XML file [" + realFilename + "]");
      }
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "TransformWithMappingMeta.Exception.UnableToLoadPipeline"),
          e);
    }

    if (mappingPipelineMeta == null) { // skip warning
      return null;
    }

    mappingPipelineMeta.setMetadataProvider(metadataProvider);
    mappingPipelineMeta.setFilename(mappingPipelineMeta.getFilename());

    return mappingPipelineMeta;
  }

  public static void activateParams(
      IVariables childVariableSpace,
      INamedParameters childNamedParams,
      IVariables parent,
      String[] listParameters,
      String[] mappingVariables,
      String[] inputFields) {
    activateParams(
        childVariableSpace,
        childNamedParams,
        parent,
        listParameters,
        mappingVariables,
        inputFields,
        true);
  }

  public static void activateParams(
      IVariables childVariableSpace,
      INamedParameters childNamedParams,
      IVariables parent,
      String[] listParameters,
      String[] mappingVariables,
      String[] inputFields,
      boolean isPassingAllParameters) {
    // Kept for plugins that call this directly. All executors share one implementation so a
    // sub-pipeline and a sub-workflow resolve their parameters identically.
    //
    // This signature has no way to say "the row names a parameter but configures no value", so a
    // blank value is read as exactly that and left to the option and the child's own default.
    //
    List<String> names = new ArrayList<>();
    List<String> values = new ArrayList<>();
    if (mappingVariables != null) {
      for (int i = 0; i < mappingVariables.length; i++) {
        String value = inputFields == null || i >= inputFields.length ? null : inputFields[i];
        if (Utils.isEmpty(Const.trim(value))) {
          continue;
        }
        names.add(mappingVariables[i]);
        values.add(value);
      }
    }

    SubExecutionParameters.activate(
        childVariableSpace,
        childNamedParams,
        parent,
        listParameters,
        names.toArray(new String[0]),
        values.toArray(new String[0]),
        isPassingAllParameters,
        true);
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
  public void setFilename(String filename) {
    this.filename = filename;
  }

  /**
   * @param fileName the fileName to set
   */
  public void replaceFileName(String fileName) {
    this.filename = fileName;
  }

  @Override
  public String exportResources(
      IVariables variables,
      Map<String, ResourceDefinition> definitions,
      IResourceNaming iResourceNaming,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    try {
      // Try to load the pipeline from a file.
      // Modify this recursively too...
      //
      // NOTE: there is no need to clone this transform because the caller is
      // responsible for this.
      //
      // First load the mapping pipeline...
      //
      PipelineMeta mappingPipelineMeta = loadMappingMeta(this, metadataProvider, variables);

      // Also go down into the mapping pipeline and export the files
      // there. (mapping recursively down)
      //
      String proposedNewFilename =
          mappingPipelineMeta.exportResources(
              variables, definitions, iResourceNaming, metadataProvider);

      // To get a relative path to it, we inject
      // ${Internal.Entry.Current.Directory}
      //
      String newFilename =
          "${" + INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER + "}/" + proposedNewFilename;

      // Set the correct filename inside the XML.
      //
      mappingPipelineMeta.setFilename(newFilename);

      // change it in the action
      //
      replaceFileName(newFilename);

      return proposedNewFilename;
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "TransformWithMappingMeta.Exception.UnableToLoadPipeline", filename));
    }
  }

  public static void addMissingVariables(IVariables fromSpace, IVariables toSpace) {
    if (toSpace == null) {
      return;
    }
    String[] variableNames = toSpace.getVariableNames();
    for (String variable : variableNames) {
      // Skip null/empty names: Set.contains(null) throws NPE and empty keys are invalid
      if (Utils.isEmpty(variable)) {
        continue;
      }
      if (fromSpace.getVariable(variable) == null) {
        fromSpace.setVariable(variable, toSpace.getVariable(variable));
      }
    }
  }

  public static void replaceVariableValues(
      IVariables childPipelineMeta, IVariables replaceBy, String type) {
    if (replaceBy == null) {
      return;
    }
    String[] variableNames = replaceBy.getVariableNames();
    for (String variableName : variableNames) {
      // Skip null/empty names: Immutable Set.contains(null) throws NPE (issue #7067)
      if (Utils.isEmpty(variableName)) {
        continue;
      }
      if (childPipelineMeta.getVariable(variableName) != null
          && !isInternalVariable(variableName, type)) {
        childPipelineMeta.setVariable(variableName, replaceBy.getVariable(variableName));
      }
    }
  }

  public static void replaceVariableValues(IVariables childPipelineMeta, IVariables replaceBy) {
    replaceVariableValues(childPipelineMeta, replaceBy, "");
  }

  private static boolean isInternalVariable(String variableName, String type) {
    return switch (type) {
      case "Pipeline" -> isPipelineInternalVariable(variableName);
      case "Workflow" -> isWorkflowInternalVariable(variableName);
      default ->
          isWorkflowInternalVariable(variableName) || isPipelineInternalVariable(variableName);
    };
  }

  private static boolean isPipelineInternalVariable(String variableName) {
    return variableName != null && Const.INTERNAL_PIPELINE_VARIABLES.contains(variableName);
  }

  private static boolean isWorkflowInternalVariable(String variableName) {
    return variableName != null && Const.INTERNAL_WORKFLOW_VARIABLES.contains(variableName);
  }
}
