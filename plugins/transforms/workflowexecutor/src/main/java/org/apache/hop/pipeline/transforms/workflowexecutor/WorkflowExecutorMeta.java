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

package org.apache.hop.pipeline.transforms.workflowexecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNone;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.CurrentDirectoryResolver;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transform.stream.IStream.StreamType;
import org.apache.hop.pipeline.transform.stream.Stream;
import org.apache.hop.pipeline.transform.stream.StreamIcon;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;

/** Meta-data for the Workflow executor transform. */
@Transform(
    id = "WorkflowExecutor",
    image = "ui/images/workflowexecutor.svg",
    name = "i18n::WorkflowExecutor.Name",
    description = "i18n::WorkflowExecutor.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Flow",
    documentationUrl = "/pipeline/transforms/workflow-executor.html",
    keywords = "i18n::WorkflowExecutorMeta.keyword",
    actionTransformTypes = {ActionTransformType.HOP_FILE, ActionTransformType.HOP_WORKFLOW})
@Getter
@Setter
public class WorkflowExecutorMeta
    extends BaseTransformMeta<WorkflowExecutor, WorkflowExecutorData> {
  private static final Class<?> PKG = WorkflowExecutorMeta.class;

  /** The name of the workflow run configuration to execute with */
  @HopMetadataProperty(key = "run_configuration")
  private String runConfigurationName;

  @HopMetadataProperty(
      key = "filename",
      hopMetadataPropertyType = HopMetadataPropertyType.WORKFLOW_FILE)
  private String filename;

  /**
   * The number of input rows that are sent as result rows to the workflow in one go, defaults to
   * "1"
   */
  @HopMetadataProperty(key = "group_size")
  private String groupSize;

  /**
   * Optional name of a field to group rows together that are sent together to the workflow as
   * result rows (empty default)
   */
  @HopMetadataProperty(key = "group_field")
  private String groupField;

  /**
   * Optional time in ms that is spent waiting and accumulating rows before they are given to the
   * workflow as result rows (empty default, "0")
   */
  @HopMetadataProperty(key = "group_time")
  private String groupTime;

  @HopMetadataProperty(key = "variablemapping", groupKey = "parameters")
  private List<WorkflowExecutorParameters> parameters;

  @HopMetadataProperty(key = "execution_result_target_transform")
  private String executionResultTargetTransform;

  private TransformMeta executionResultTargetTransformMeta;

  /**
   * The optional name of the output field that will contain the execution time of the workflow in
   * (Integer in ms)
   */
  @HopMetadataProperty(key = "execution_time_field")
  private String executionTimeField;

  /** The optional name of the output field that will contain the execution result (Boolean) */
  @HopMetadataProperty(key = "execution_result_field")
  private String executionResultField;

  /** The optional name of the output field that will contain the number of errors (Integer) */
  @HopMetadataProperty(key = "execution_errors_field")
  private String executionNrErrorsField;

  /** The optional name of the output field that will contain the number of rows read (Integer) */
  @HopMetadataProperty(key = "execution_lines_read_field")
  private String executionLinesReadField;

  /**
   * The optional name of the output field that will contain the number of rows written (Integer)
   */
  @HopMetadataProperty(key = "execution_lines_written_field")
  private String executionLinesWrittenField;

  /** The optional name of the output field that will contain the number of rows input (Integer) */
  @HopMetadataProperty(key = "execution_lines_input_field")
  private String executionLinesInputField;

  /** The optional name of the output field that will contain the number of rows output (Integer) */
  @HopMetadataProperty(key = "execution_lines_output_field")
  private String executionLinesOutputField;

  /**
   * The optional name of the output field that will contain the number of rows rejected (Integer)
   */
  @HopMetadataProperty(key = "execution_lines_rejected_field")
  private String executionLinesRejectedField;

  /**
   * The optional name of the output field that will contain the number of rows updated (Integer)
   */
  @HopMetadataProperty(key = "execution_lines_updated_field")
  private String executionLinesUpdatedField;

  /**
   * The optional name of the output field that will contain the number of rows deleted (Integer)
   */
  @HopMetadataProperty(key = "execution_lines_deleted_field")
  private String executionLinesDeletedField;

  /**
   * The optional name of the output field that will contain the number of files retrieved (Integer)
   */
  @HopMetadataProperty(key = "execution_files_retrieved_field")
  private String executionFilesRetrievedField;

  /**
   * The optional name of the output field that will contain the exit status of the last executed
   * shell script (Integer)
   */
  @HopMetadataProperty(key = "execution_exit_status_field")
  private String executionExitStatusField;

  /**
   * The optional name of the output field that will contain the log text of the workflow execution
   * (String)
   */
  @HopMetadataProperty(key = "execution_log_text_field")
  private String executionLogTextField;

  /**
   * The optional name of the output field that will contain the log channel ID of the workflow
   * execution (String)
   */
  @HopMetadataProperty(key = "execution_log_channelid_field")
  private String executionLogChannelIdField;

  /** The optional transform to send the result rows to */
  @HopMetadataProperty(key = "result_rows_target_transform")
  private String resultRowsTargetTransform;

  private TransformMeta resultRowsTargetTransformMeta;

  @HopMetadataProperty(
      key = "result_rows_field",
      inlineListTags = {"name", "type", "length", "precision"})
  private List<WorkflowExecutorResultRows> resultRowsField;

  /** The optional transform to send the result files to */
  @HopMetadataProperty(key = "result_files_target_transform")
  private String resultFilesTargetTransform;

  private TransformMeta resultFilesTargetTransformMeta;

  @HopMetadataProperty(key = "result_files_file_name_field")
  private String resultFilesFileNameField;

  /** This flag causes the workflow to inherit all variables from the parent pipeline */
  @HopMetadataProperty(key = "inherit_all_vars")
  private boolean inheritingAllVariables;

  private IHopMetadataProvider metadataProvider;

  public WorkflowExecutorMeta() {
    super(); // allocate BaseTransformMeta
  }

  @Override
  public void setDefault() {
    parameters = new ArrayList<>();
    resultRowsField = new ArrayList<>();
    inheritingAllVariables = true;

    groupSize = "1";
    groupField = "";
    groupTime = "";

    executionTimeField = "ExecutionTime";
    executionResultField = "ExecutionResult";
    executionNrErrorsField = "ExecutionNrErrors";
    executionLinesReadField = "ExecutionLinesRead";
    executionLinesWrittenField = "ExecutionLinesWritten";
    executionLinesInputField = "ExecutionLinesInput";
    executionLinesOutputField = "ExecutionLinesOutput";
    executionLinesRejectedField = "ExecutionLinesRejected";
    executionLinesUpdatedField = "ExecutionLinesUpdated";
    executionLinesDeletedField = "ExecutionLinesDeleted";
    executionFilesRetrievedField = "ExecutionFilesRetrieved";
    executionExitStatusField = "ExecutionExitStatus";
    executionLogTextField = "ExecutionLogText";
    executionLogChannelIdField = "ExecutionLogChannelId";

    resultFilesFileNameField = "FileName";
  }

  @Override
  public void getFields(
      IRowMeta row,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    row.clear();

    if (nextTransform != null && nextTransform.equals(resultRowsTargetTransformMeta)) {
      for (int i = 0; i < resultRowsField.size(); i++) {
        IValueMeta value;
        try {
          value =
              ValueMetaFactory.createValueMeta(
                  resultRowsField.get(i).getName(),
                  ValueMetaFactory.getIdForValueMeta(resultRowsField.get(i).getType()),
                  resultRowsField.get(i).getLength(),
                  resultRowsField.get(i).getPrecision());
        } catch (HopPluginException e) {
          value = new ValueMetaNone(resultRowsField.get(i).getName());
          value.setLength(
              resultRowsField.get(i).getLength(), resultRowsField.get(i).getPrecision());
        }
        row.addValueMeta(value);
      }
    } else if (nextTransform != null && nextTransform.equals(resultFilesTargetTransformMeta)) {
      if (!Utils.isEmpty(resultFilesFileNameField)) {
        IValueMeta value = new ValueMetaString("filename", 255, 0);
        row.addValueMeta(value);
      }
    } else if (nextTransform != null && nextTransform.equals(executionResultTargetTransformMeta)) {
      if (!Utils.isEmpty(executionTimeField)) {
        IValueMeta value = new ValueMetaInteger(executionTimeField, 15, 0);
        row.addValueMeta(value);
      }
      if (!Utils.isEmpty(executionResultField)) {
        IValueMeta value = new ValueMetaBoolean(executionResultField);
        row.addValueMeta(value);
      }
      if (!Utils.isEmpty(executionNrErrorsField)) {
        IValueMeta value = new ValueMetaInteger(executionNrErrorsField, 9, 0);
        row.addValueMeta(value);
      }
      if (!Utils.isEmpty(executionLinesReadField)) {
        IValueMeta value = new ValueMetaInteger(executionLinesReadField, 9, 0);
        row.addValueMeta(value);
      }
      if (!Utils.isEmpty(executionLinesWrittenField)) {
        IValueMeta value = new ValueMetaInteger(executionLinesWrittenField, 9, 0);
        row.addValueMeta(value);
      }
      if (!Utils.isEmpty(executionLinesInputField)) {
        IValueMeta value = new ValueMetaInteger(executionLinesInputField, 9, 0);
        row.addValueMeta(value);
      }
      if (!Utils.isEmpty(executionLinesOutputField)) {
        IValueMeta value = new ValueMetaInteger(executionLinesOutputField, 9, 0);
        row.addValueMeta(value);
      }
      if (!Utils.isEmpty(executionLinesRejectedField)) {
        IValueMeta value = new ValueMetaInteger(executionLinesRejectedField, 9, 0);
        row.addValueMeta(value);
      }
      if (!Utils.isEmpty(executionLinesUpdatedField)) {
        IValueMeta value = new ValueMetaInteger(executionLinesUpdatedField, 9, 0);
        row.addValueMeta(value);
      }
      if (!Utils.isEmpty(executionLinesDeletedField)) {
        IValueMeta value = new ValueMetaInteger(executionLinesDeletedField, 9, 0);
        row.addValueMeta(value);
      }
      if (!Utils.isEmpty(executionFilesRetrievedField)) {
        IValueMeta value = new ValueMetaInteger(executionFilesRetrievedField, 9, 0);
        row.addValueMeta(value);
      }
      if (!Utils.isEmpty(executionExitStatusField)) {
        IValueMeta value = new ValueMetaInteger(executionExitStatusField, 3, 0);
        row.addValueMeta(value);
      }
      if (!Utils.isEmpty(executionLogTextField)) {
        IValueMeta value = new ValueMetaString(executionLogTextField);
        value.setLargeTextField(true);
        row.addValueMeta(value);
      }
      if (!Utils.isEmpty(executionLogChannelIdField)) {
        IValueMeta value = new ValueMetaString(executionLogChannelIdField, 50, 0);
        row.addValueMeta(value);
      }
    }
  }

  public String[] getInfoTransforms() {
    String[] infoTransforms = getTransformIOMeta().getInfoTransformNames();
    // Return null instead of empty array to preserve existing behavior
    return infoTransforms.length == 0 ? null : infoTransforms;
  }

  public String[] getTargetTransforms() {

    List<String> targetTransforms = new ArrayList<>();

    if (!Utils.isEmpty(resultFilesTargetTransform)) {
      targetTransforms.add(resultFilesTargetTransform);
    }
    if (!Utils.isEmpty(resultRowsTargetTransform)) {
      targetTransforms.add(resultRowsTargetTransform);
    }

    if (targetTransforms.isEmpty()) {
      return null;
    }

    return targetTransforms.toArray(new String[targetTransforms.size()]);
  }

  public static final synchronized WorkflowMeta loadWorkflowMeta(
      WorkflowExecutorMeta executorMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    return loadWorkflowMeta(executorMeta, metadataProvider, variables);
  }

  public static final synchronized WorkflowMeta loadWorkflowMeta(
      WorkflowExecutorMeta executorMeta,
      IHopMetadataProvider metadataProvider,
      IVariables variables)
      throws HopException {
    WorkflowMeta mappingWorkflowMeta = null;

    CurrentDirectoryResolver r = new CurrentDirectoryResolver();
    IVariables tmpSpace =
        r.resolveCurrentDirectory(
            variables, executorMeta.getParentTransformMeta(), executorMeta.getFilename());

    String realFilename = tmpSpace.resolve(executorMeta.getFilename());

    // OK, load the meta-data from file...
    //
    // Don't set internal variables: they belong to the parent thread!
    //
    mappingWorkflowMeta = new WorkflowMeta(variables, realFilename, metadataProvider);
    LogChannel.GENERAL.logDetailed(
        "Loaded workflow", "Workflow was loaded from XML file [" + realFilename + "]");

    // Pass some important information to the mapping pipeline metadata:
    //
    mappingWorkflowMeta.setMetadataProvider(metadataProvider);
    mappingWorkflowMeta.setFilename(mappingWorkflowMeta.getFilename());

    return mappingWorkflowMeta;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    CheckResult cr;
    if (prev == null || prev.size() == 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "WorkflowExecutorMeta.CheckResult.NotReceivingAnyFields"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "WorkflowExecutorMeta.CheckResult.TransformReceivingFields",
                  prev.size() + ""),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "WorkflowExecutorMeta.CheckResult.TransformReceivingFieldsFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "WorkflowExecutorMeta.CheckResult.NoInputReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, TransformMeta transformMeta) {
    List<ResourceReference> references = new ArrayList<>(5);
    String realFilename = variables.resolve(filename);
    ResourceReference reference = new ResourceReference(transformMeta);
    references.add(reference);

    if (StringUtils.isNotEmpty(realFilename)) {
      // Add the filename to the references, including a reference to this transform
      // meta data.
      //
      reference.getEntries().add(new ResourceEntry(realFilename, ResourceType.ACTIONFILE));
    }
    return references;
  }

  /**
   * This method was created exclusively for tests to bypass the mock static final method without
   * using PowerMock
   *
   * @param executorMeta
   * @param metadataProvider
   * @param variables
   * @return WorkflowMeta
   * @throws HopException
   */
  WorkflowMeta loadWorkflowMetaProxy(
      WorkflowExecutorMeta executorMeta,
      IHopMetadataProvider metadataProvider,
      IVariables variables)
      throws HopException {
    return loadWorkflowMeta(executorMeta, metadataProvider, variables);
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
      // First load the executor workflow metadata...
      //
      WorkflowMeta executorWorkflowMeta = loadWorkflowMetaProxy(this, metadataProvider, variables);

      // Also go down into the mapping pipeline and export the files
      // there. (mapping recursively down)
      //
      String proposedNewFilename =
          executorWorkflowMeta.exportResources(
              variables, definitions, iResourceNaming, metadataProvider);

      // To get a relative path to it, we inject
      // ${Internal.Entry.Current.Directory}
      //
      String newFilename =
          "${" + Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER + "}/" + proposedNewFilename;

      // Set the correct filename inside the XML.
      //
      executorWorkflowMeta.setFilename(newFilename);

      // change it in the action
      //
      filename = newFilename;

      return proposedNewFilename;
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "WorkflowExecutorMeta.Exception.UnableToLoadWorkflow", filename));
    }
  }

  @Override
  public ITransformIOMeta getTransformIOMeta() {
    ITransformIOMeta ioMeta = super.getTransformIOMeta(false);
    if (ioMeta == null) {

      ioMeta = new TransformIOMeta(true, true, true, false, true, false);

      ioMeta.addStream(
          new Stream(
              StreamType.TARGET,
              executionResultTargetTransformMeta,
              BaseMessages.getString(PKG, "WorkflowExecutorMeta.ResultStream.Description"),
              StreamIcon.TARGET,
              null));
      ioMeta.addStream(
          new Stream(
              StreamType.TARGET,
              resultRowsTargetTransformMeta,
              BaseMessages.getString(PKG, "WorkflowExecutorMeta.ResultRowsStream.Description"),
              StreamIcon.TARGET,
              null));
      ioMeta.addStream(
          new Stream(
              StreamType.TARGET,
              resultFilesTargetTransformMeta,
              BaseMessages.getString(PKG, "WorkflowExecutorMeta.ResultFilesStream.Description"),
              StreamIcon.TARGET,
              null));
      setTransformIOMeta(ioMeta);
    }
    return ioMeta;
  }

  /**
   * When an optional stream is selected, this method is called to handled the ETL metadata
   * implications of that.
   *
   * @param stream The optional stream to handle.
   */
  @Override
  public void handleStreamSelection(IStream stream) {
    // This transform targets another transform.
    // Make sure that we don't specify the same transform for more than 1 target...
    //
    List<IStream> targets = getTransformIOMeta().getTargetStreams();
    int index = targets.indexOf(stream);
    TransformMeta transform = targets.get(index).getTransformMeta();
    switch (index) {
      case 0:
        setExecutionResultTargetTransformMeta(transform);
        break;
      case 1:
        setResultRowsTargetTransformMeta(transform);
        break;
      case 2:
        setResultFilesTargetTransformMeta(transform);
        break;
      default:
        break;
    }
  }

  /** Remove the cached {@link TransformIOMeta} so it is recreated when it is next accessed. */
  @Override
  public void resetTransformIoMeta() {
    // Do Nothing
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    executionResultTargetTransformMeta =
        TransformMeta.findTransform(transforms, executionResultTargetTransform);
    resultRowsTargetTransformMeta =
        TransformMeta.findTransform(transforms, resultRowsTargetTransform);
    resultFilesTargetTransformMeta =
        TransformMeta.findTransform(transforms, resultFilesTargetTransform);
  }

  @Override
  public PipelineType[] getSupportedPipelineTypes() {
    return new PipelineType[] {
      PipelineType.Normal,
    };
  }

  @Override
  public boolean excludeFromCopyDistributeVerification() {
    return true;
  }

  /**
   * @return The objects referenced in the transform, like a mapping, a pipeline, a workflow, ...
   */
  @Override
  public String[] getReferencedObjectDescriptions() {
    return new String[] {
      BaseMessages.getString(PKG, "WorkflowExecutorMeta.ReferencedObject.Description"),
    };
  }

  private boolean isJobDefined() {
    return StringUtils.isNotEmpty(filename);
  }

  @Override
  public boolean[] isReferencedObjectEnabled() {
    return new boolean[] {
      isJobDefined(),
    };
  }

  /**
   * Load the referenced object
   *
   * @param index the object index to load
   * @param metadataProvider the metadataProvider
   * @param variables the variable variables to use
   * @return the referenced object once loaded
   * @throws HopException
   */
  @Override
  public IHasFilename loadReferencedObject(
      int index, IHopMetadataProvider metadataProvider, IVariables variables) throws HopException {
    return loadWorkflowMeta(this, metadataProvider, variables);
  }

  @Override
  public boolean cleanAfterHopFromRemove() {
    setExecutionResultTargetTransformMeta(null);
    setResultRowsTargetTransformMeta(null);
    setResultFilesTargetTransformMeta(null);
    return true;
  }

  @Override
  public boolean cleanAfterHopFromRemove(TransformMeta toTransform) {
    if (null == toTransform || null == toTransform.getName()) {
      return false;
    }

    boolean hasChanged = false;
    String toTransformName = toTransform.getName();

    if (getExecutionResultTargetTransformMeta() != null
        && toTransformName.equals(getExecutionResultTargetTransformMeta().getName())) {
      setExecutionResultTargetTransformMeta(null);
      hasChanged = true;
    } else if (getResultRowsTargetTransformMeta() != null
        && toTransformName.equals(getResultRowsTargetTransformMeta().getName())) {
      setResultRowsTargetTransformMeta(null);
      hasChanged = true;
    } else if (getResultFilesTargetTransformMeta() != null
        && toTransformName.equals(getResultFilesTargetTransformMeta().getName())) {
      setResultFilesTargetTransformMeta(null);
      hasChanged = true;
    }
    return hasChanged;
  }
}
