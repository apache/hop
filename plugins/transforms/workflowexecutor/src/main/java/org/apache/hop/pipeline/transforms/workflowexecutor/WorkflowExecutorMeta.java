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
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
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
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
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
import org.w3c.dom.Node;

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
public class WorkflowExecutorMeta
    extends BaseTransformMeta<WorkflowExecutor, WorkflowExecutorData> {
  private static final Class<?> PKG = WorkflowExecutorMeta.class;
  public static final String CONST_FILENAME = "filename";
  public static final String CONST_RESULT_ROWS_FIELD = "result_rows_field";
  public static final String CONST_SPACES = "      ";

  /** The name of the workflow run configuration to execute with */
  private String runConfigurationName;

  private String filename;

  /**
   * The number of input rows that are sent as result rows to the workflow in one go, defaults to
   * "1"
   */
  private String groupSize;

  /**
   * Optional name of a field to group rows together that are sent together to the workflow as
   * result rows (empty default)
   */
  private String groupField;

  /**
   * Optional time in ms that is spent waiting and accumulating rows before they are given to the
   * workflow as result rows (empty default, "0")
   */
  private String groupTime;

  private WorkflowExecutorParameters parameters;

  private String executionResultTargetTransform;
  private TransformMeta executionResultTargetTransformMeta;

  /**
   * The optional name of the output field that will contain the execution time of the workflow in
   * (Integer in ms)
   */
  private String executionTimeField;

  /** The optional name of the output field that will contain the execution result (Boolean) */
  private String executionResultField;

  /** The optional name of the output field that will contain the number of errors (Integer) */
  private String executionNrErrorsField;

  /** The optional name of the output field that will contain the number of rows read (Integer) */
  private String executionLinesReadField;

  /**
   * The optional name of the output field that will contain the number of rows written (Integer)
   */
  private String executionLinesWrittenField;

  /** The optional name of the output field that will contain the number of rows input (Integer) */
  private String executionLinesInputField;

  /** The optional name of the output field that will contain the number of rows output (Integer) */
  private String executionLinesOutputField;

  /**
   * The optional name of the output field that will contain the number of rows rejected (Integer)
   */
  private String executionLinesRejectedField;

  /**
   * The optional name of the output field that will contain the number of rows updated (Integer)
   */
  private String executionLinesUpdatedField;

  /**
   * The optional name of the output field that will contain the number of rows deleted (Integer)
   */
  private String executionLinesDeletedField;

  /**
   * The optional name of the output field that will contain the number of files retrieved (Integer)
   */
  private String executionFilesRetrievedField;

  /**
   * The optional name of the output field that will contain the exit status of the last executed
   * shell script (Integer)
   */
  private String executionExitStatusField;

  /**
   * The optional name of the output field that will contain the log text of the workflow execution
   * (String)
   */
  private String executionLogTextField;

  /**
   * The optional name of the output field that will contain the log channel ID of the workflow
   * execution (String)
   */
  private String executionLogChannelIdField;

  /** The optional transform to send the result rows to */
  private String resultRowsTargetTransform;

  private TransformMeta resultRowsTargetTransformMeta;
  private String[] resultRowsField;
  private int[] resultRowsType;
  private int[] resultRowsLength;
  private int[] resultRowsPrecision;

  /** The optional transform to send the result files to */
  private String resultFilesTargetTransform;

  private TransformMeta resultFilesTargetTransformMeta;
  private String resultFilesFileNameField;

  private IHopMetadataProvider metadataProvider;

  public WorkflowExecutorMeta() {
    super(); // allocate BaseTransformMeta

    parameters = new WorkflowExecutorParameters();
    resultRowsField = new String[0];
  }

  @Override
  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(300);

    // Export a little bit of extra information regarding the reference
    //
    retval.append("    ").append(XmlHandler.addTagValue("run_configuration", runConfigurationName));
    retval.append("    ").append(XmlHandler.addTagValue(CONST_FILENAME, filename));
    retval.append("    ").append(XmlHandler.addTagValue("group_size", groupSize));
    retval.append("    ").append(XmlHandler.addTagValue("group_field", groupField));
    retval.append("    ").append(XmlHandler.addTagValue("group_time", groupTime));

    // Add the mapping parameters too
    //
    retval.append(parameters.getXml());

    // The output side...
    //
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "execution_result_target_transform",
                executionResultTargetTransformMeta == null
                    ? null
                    : executionResultTargetTransformMeta.getName()));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("execution_time_field", executionTimeField));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("execution_result_field", executionResultField));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("execution_errors_field", executionNrErrorsField));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("execution_lines_read_field", executionLinesReadField));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue("execution_lines_written_field", executionLinesWrittenField));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("execution_lines_input_field", executionLinesInputField));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("execution_lines_output_field", executionLinesOutputField));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue("execution_lines_rejected_field", executionLinesRejectedField));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue("execution_lines_updated_field", executionLinesUpdatedField));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue("execution_lines_deleted_field", executionLinesDeletedField));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "execution_files_retrieved_field", executionFilesRetrievedField));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("execution_exit_status_field", executionExitStatusField));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("execution_log_text_field", executionLogTextField));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue("execution_log_channelid_field", executionLogChannelIdField));

    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "result_rows_target_transform",
                resultRowsTargetTransformMeta == null
                    ? null
                    : resultRowsTargetTransformMeta.getName()));
    for (int i = 0; i < resultRowsField.length; i++) {
      retval.append("    ").append(XmlHandler.openTag(CONST_RESULT_ROWS_FIELD)).append(Const.CR);
      retval.append(CONST_SPACES).append(XmlHandler.addTagValue("name", resultRowsField[i]));
      retval
          .append(CONST_SPACES)
          .append(
              XmlHandler.addTagValue("type", ValueMetaFactory.getValueMetaName(resultRowsType[i])));
      retval.append(CONST_SPACES).append(XmlHandler.addTagValue("length", resultRowsLength[i]));
      retval
          .append(CONST_SPACES)
          .append(XmlHandler.addTagValue("precision", resultRowsPrecision[i]));
      retval.append("    ").append(XmlHandler.closeTag(CONST_RESULT_ROWS_FIELD)).append(Const.CR);
    }

    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "result_files_target_transform",
                resultFilesTargetTransformMeta == null
                    ? null
                    : resultFilesTargetTransformMeta.getName()));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("result_files_file_name_field", resultFilesFileNameField));

    return retval.toString();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      runConfigurationName = XmlHandler.getTagValue(transformNode, "run_configuration");
      filename = XmlHandler.getTagValue(transformNode, CONST_FILENAME);

      groupSize = XmlHandler.getTagValue(transformNode, "group_size");
      groupField = XmlHandler.getTagValue(transformNode, "group_field");
      groupTime = XmlHandler.getTagValue(transformNode, "group_time");

      // Load the mapping parameters too..
      //
      Node mappingParametersNode =
          XmlHandler.getSubNode(transformNode, WorkflowExecutorParameters.XML_TAG);
      parameters = new WorkflowExecutorParameters(mappingParametersNode);

      // The output side...
      //
      executionResultTargetTransform =
          XmlHandler.getTagValue(transformNode, "execution_result_target_transform");
      executionTimeField = XmlHandler.getTagValue(transformNode, "execution_time_field");
      executionResultField = XmlHandler.getTagValue(transformNode, "execution_result_field");
      executionNrErrorsField = XmlHandler.getTagValue(transformNode, "execution_errors_field");
      executionLinesReadField = XmlHandler.getTagValue(transformNode, "execution_lines_read_field");
      executionLinesWrittenField =
          XmlHandler.getTagValue(transformNode, "execution_lines_written_field");
      executionLinesInputField =
          XmlHandler.getTagValue(transformNode, "execution_lines_input_field");
      executionLinesOutputField =
          XmlHandler.getTagValue(transformNode, "execution_lines_output_field");
      executionLinesRejectedField =
          XmlHandler.getTagValue(transformNode, "execution_lines_rejected_field");
      executionLinesUpdatedField =
          XmlHandler.getTagValue(transformNode, "execution_lines_updated_field");
      executionLinesDeletedField =
          XmlHandler.getTagValue(transformNode, "execution_lines_deleted_field");
      executionFilesRetrievedField =
          XmlHandler.getTagValue(transformNode, "execution_files_retrieved_field");
      executionExitStatusField =
          XmlHandler.getTagValue(transformNode, "execution_exit_status_field");
      executionLogTextField = XmlHandler.getTagValue(transformNode, "execution_log_text_field");
      executionLogChannelIdField =
          XmlHandler.getTagValue(transformNode, "execution_log_channelid_field");

      resultRowsTargetTransform =
          XmlHandler.getTagValue(transformNode, "result_rows_target_transform");

      int nrFields = XmlHandler.countNodes(transformNode, CONST_RESULT_ROWS_FIELD);
      resultRowsField = new String[nrFields];
      resultRowsType = new int[nrFields];
      resultRowsLength = new int[nrFields];
      resultRowsPrecision = new int[nrFields];

      for (int i = 0; i < nrFields; i++) {

        Node fieldNode = XmlHandler.getSubNodeByNr(transformNode, CONST_RESULT_ROWS_FIELD, i);

        resultRowsField[i] = XmlHandler.getTagValue(fieldNode, "name");
        resultRowsType[i] =
            ValueMetaFactory.getIdForValueMeta(XmlHandler.getTagValue(fieldNode, "type"));
        resultRowsLength[i] = Const.toInt(XmlHandler.getTagValue(fieldNode, "length"), -1);
        resultRowsPrecision[i] = Const.toInt(XmlHandler.getTagValue(fieldNode, "precision"), -1);
      }

      resultFilesTargetTransform =
          XmlHandler.getTagValue(transformNode, "result_files_target_transform");
      resultFilesFileNameField =
          XmlHandler.getTagValue(transformNode, "result_files_file_name_field");
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG, "WorkflowExecutorMeta.Exception.ErrorLoadingJobExecutorDetailsFromXML"),
          e);
    }
  }

  @Override
  public void setDefault() {
    parameters = new WorkflowExecutorParameters();
    parameters.setInheritingAllVariables(true);

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
      for (int i = 0; i < resultRowsField.length; i++) {
        IValueMeta value;
        try {
          value =
              ValueMetaFactory.createValueMeta(
                  resultRowsField[i],
                  resultRowsType[i],
                  resultRowsLength[i],
                  resultRowsPrecision[i]);
        } catch (HopPluginException e) {
          value = new ValueMetaNone(resultRowsField[i]);
          value.setLength(resultRowsLength[i], resultRowsPrecision[i]);
        }
        row.addValueMeta(value);
      }
    } else if (nextTransform != null && nextTransform.equals(resultFilesTargetTransformMeta)) {
      if (!Utils.isEmpty(resultFilesFileNameField)) {
        IValueMeta value = new ValueMetaString(CONST_FILENAME, 255, 0);
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
   * @return the mappingParameters
   */
  public WorkflowExecutorParameters getMappingParameters() {
    return parameters;
  }

  /**
   * @param mappingParameters the mappingParameters to set
   */
  public void setMappingParameters(WorkflowExecutorParameters mappingParameters) {
    this.parameters = mappingParameters;
  }

  /**
   * @return the parameters
   */
  public WorkflowExecutorParameters getParameters() {
    return parameters;
  }

  /**
   * @param parameters the parameters to set
   */
  public void setParameters(WorkflowExecutorParameters parameters) {
    this.parameters = parameters;
  }

  /**
   * @return the executionTimeField
   */
  public String getExecutionTimeField() {
    return executionTimeField;
  }

  /**
   * @param executionTimeField the executionTimeField to set
   */
  public void setExecutionTimeField(String executionTimeField) {
    this.executionTimeField = executionTimeField;
  }

  /**
   * @return the executionResultField
   */
  public String getExecutionResultField() {
    return executionResultField;
  }

  /**
   * @param executionResultField the executionResultField to set
   */
  public void setExecutionResultField(String executionResultField) {
    this.executionResultField = executionResultField;
  }

  /**
   * @return the executionNrErrorsField
   */
  public String getExecutionNrErrorsField() {
    return executionNrErrorsField;
  }

  /**
   * @param executionNrErrorsField the executionNrErrorsField to set
   */
  public void setExecutionNrErrorsField(String executionNrErrorsField) {
    this.executionNrErrorsField = executionNrErrorsField;
  }

  /**
   * @return the executionLinesReadField
   */
  public String getExecutionLinesReadField() {
    return executionLinesReadField;
  }

  /**
   * @param executionLinesReadField the executionLinesReadField to set
   */
  public void setExecutionLinesReadField(String executionLinesReadField) {
    this.executionLinesReadField = executionLinesReadField;
  }

  /**
   * @return the executionLinesWrittenField
   */
  public String getExecutionLinesWrittenField() {
    return executionLinesWrittenField;
  }

  /**
   * @param executionLinesWrittenField the executionLinesWrittenField to set
   */
  public void setExecutionLinesWrittenField(String executionLinesWrittenField) {
    this.executionLinesWrittenField = executionLinesWrittenField;
  }

  /**
   * @return the executionLinesInputField
   */
  public String getExecutionLinesInputField() {
    return executionLinesInputField;
  }

  /**
   * @param executionLinesInputField the executionLinesInputField to set
   */
  public void setExecutionLinesInputField(String executionLinesInputField) {
    this.executionLinesInputField = executionLinesInputField;
  }

  /**
   * @return the executionLinesOutputField
   */
  public String getExecutionLinesOutputField() {
    return executionLinesOutputField;
  }

  /**
   * @param executionLinesOutputField the executionLinesOutputField to set
   */
  public void setExecutionLinesOutputField(String executionLinesOutputField) {
    this.executionLinesOutputField = executionLinesOutputField;
  }

  /**
   * @return the executionLinesRejectedField
   */
  public String getExecutionLinesRejectedField() {
    return executionLinesRejectedField;
  }

  /**
   * @param executionLinesRejectedField the executionLinesRejectedField to set
   */
  public void setExecutionLinesRejectedField(String executionLinesRejectedField) {
    this.executionLinesRejectedField = executionLinesRejectedField;
  }

  /**
   * @return the executionLinesUpdatedField
   */
  public String getExecutionLinesUpdatedField() {
    return executionLinesUpdatedField;
  }

  /**
   * @param executionLinesUpdatedField the executionLinesUpdatedField to set
   */
  public void setExecutionLinesUpdatedField(String executionLinesUpdatedField) {
    this.executionLinesUpdatedField = executionLinesUpdatedField;
  }

  /**
   * @return the executionLinesDeletedField
   */
  public String getExecutionLinesDeletedField() {
    return executionLinesDeletedField;
  }

  /**
   * @param executionLinesDeletedField the executionLinesDeletedField to set
   */
  public void setExecutionLinesDeletedField(String executionLinesDeletedField) {
    this.executionLinesDeletedField = executionLinesDeletedField;
  }

  /**
   * @return the executionFilesRetrievedField
   */
  public String getExecutionFilesRetrievedField() {
    return executionFilesRetrievedField;
  }

  /**
   * @param executionFilesRetrievedField the executionFilesRetrievedField to set
   */
  public void setExecutionFilesRetrievedField(String executionFilesRetrievedField) {
    this.executionFilesRetrievedField = executionFilesRetrievedField;
  }

  /**
   * @return the executionExitStatusField
   */
  public String getExecutionExitStatusField() {
    return executionExitStatusField;
  }

  /**
   * @param executionExitStatusField the executionExitStatusField to set
   */
  public void setExecutionExitStatusField(String executionExitStatusField) {
    this.executionExitStatusField = executionExitStatusField;
  }

  /**
   * @return the executionLogTextField
   */
  public String getExecutionLogTextField() {
    return executionLogTextField;
  }

  /**
   * @param executionLogTextField the executionLogTextField to set
   */
  public void setExecutionLogTextField(String executionLogTextField) {
    this.executionLogTextField = executionLogTextField;
  }

  /**
   * @return the executionLogChannelIdField
   */
  public String getExecutionLogChannelIdField() {
    return executionLogChannelIdField;
  }

  /**
   * @param executionLogChannelIdField the executionLogChannelIdField to set
   */
  public void setExecutionLogChannelIdField(String executionLogChannelIdField) {
    this.executionLogChannelIdField = executionLogChannelIdField;
  }

  /**
   * @return the resultRowsTargetTransform
   */
  public String getResultRowsTargetTransform() {
    return resultRowsTargetTransform;
  }

  /**
   * @param resultRowsTargetTransform the result Rows Target Transform to set
   */
  public void setResultRowsTargetTransform(String resultRowsTargetTransform) {
    this.resultRowsTargetTransform = resultRowsTargetTransform;
  }

  /**
   * @return the resultRowsField
   */
  public String[] getResultRowsField() {
    return resultRowsField;
  }

  /**
   * @param resultRowsField the resultRowsField to set
   */
  public void setResultRowsField(String[] resultRowsField) {
    this.resultRowsField = resultRowsField;
  }

  /**
   * @return the resultRowsType
   */
  public int[] getResultRowsType() {
    return resultRowsType;
  }

  /**
   * @param resultRowsType the resultRowsType to set
   */
  public void setResultRowsType(int[] resultRowsType) {
    this.resultRowsType = resultRowsType;
  }

  /**
   * @return the resultRowsLength
   */
  public int[] getResultRowsLength() {
    return resultRowsLength;
  }

  /**
   * @param resultRowsLength the resultRowsLength to set
   */
  public void setResultRowsLength(int[] resultRowsLength) {
    this.resultRowsLength = resultRowsLength;
  }

  /**
   * @return the resultRowsPrecision
   */
  public int[] getResultRowsPrecision() {
    return resultRowsPrecision;
  }

  /**
   * @param resultRowsPrecision the resultRowsPrecision to set
   */
  public void setResultRowsPrecision(int[] resultRowsPrecision) {
    this.resultRowsPrecision = resultRowsPrecision;
  }

  /**
   * @return the result Files Target Transform
   */
  public String getResultFilesTargetTransform() {
    return resultFilesTargetTransform;
  }

  /**
   * @param resultFilesTargetTransform the result Files Target Transform to set
   */
  public void setResultFilesTargetTransform(String resultFilesTargetTransform) {
    this.resultFilesTargetTransform = resultFilesTargetTransform;
  }

  /**
   * @return the resultRowsTargetTransformMeta
   */
  public TransformMeta getResultRowsTargetTransformMeta() {
    return resultRowsTargetTransformMeta;
  }

  /**
   * @param resultRowsTargetTransformMeta the resultRowsTargetTransformMeta to set
   */
  public void setResultRowsTargetTransformMeta(TransformMeta resultRowsTargetTransformMeta) {
    this.resultRowsTargetTransformMeta = resultRowsTargetTransformMeta;
  }

  /**
   * @return the resultFilesTargetTransformMeta
   */
  public TransformMeta getResultFilesTargetTransformMeta() {
    return resultFilesTargetTransformMeta;
  }

  /**
   * @param resultFilesTargetTransformMeta the resultFilesTargetTransformMeta to set
   */
  public void setResultFilesTargetTransformMeta(TransformMeta resultFilesTargetTransformMeta) {
    this.resultFilesTargetTransformMeta = resultFilesTargetTransformMeta;
  }

  /**
   * @return the groupSize
   */
  public String getGroupSize() {
    return groupSize;
  }

  /**
   * @param groupSize the groupSize to set
   */
  public void setGroupSize(String groupSize) {
    this.groupSize = groupSize;
  }

  /**
   * @return the groupField
   */
  public String getGroupField() {
    return groupField;
  }

  /**
   * @param groupField the groupField to set
   */
  public void setGroupField(String groupField) {
    this.groupField = groupField;
  }

  /**
   * @return the groupTime
   */
  public String getGroupTime() {
    return groupTime;
  }

  /**
   * @param groupTime the groupTime to set
   */
  public void setGroupTime(String groupTime) {
    this.groupTime = groupTime;
  }

  @Override
  public boolean excludeFromCopyDistributeVerification() {
    return true;
  }

  /**
   * @return the execution Result Target Transform
   */
  public String getExecutionResultTargetTransform() {
    return executionResultTargetTransform;
  }

  /**
   * @param executionResultTargetTransform the executionResultTargetTransform to set
   */
  public void setExecutionResultTargetTransform(String executionResultTargetTransform) {
    this.executionResultTargetTransform = executionResultTargetTransform;
  }

  /**
   * @return the executionResultTargetTransformMeta
   */
  public TransformMeta getExecutionResultTargetTransformMeta() {
    return executionResultTargetTransformMeta;
  }

  /**
   * @param executionResultTargetTransformMeta the executionResultTargetTransformMeta to set
   */
  public void setExecutionResultTargetTransformMeta(
      TransformMeta executionResultTargetTransformMeta) {
    this.executionResultTargetTransformMeta = executionResultTargetTransformMeta;
  }

  /**
   * @return the resultFilesFileNameField
   */
  public String getResultFilesFileNameField() {
    return resultFilesFileNameField;
  }

  /**
   * @param resultFilesFileNameField the resultFilesFileNameField to set
   */
  public void setResultFilesFileNameField(String resultFilesFileNameField) {
    this.resultFilesFileNameField = resultFilesFileNameField;
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

  public void setMetadataProvider(IHopMetadataProvider metadataProvider) {
    this.metadataProvider = metadataProvider;
  }

  public IHopMetadataProvider getMetadataProvider() {
    return metadataProvider;
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

  /**
   * Gets runConfigurationName
   *
   * @return value of runConfigurationName
   */
  public String getRunConfigurationName() {
    return runConfigurationName;
  }

  /**
   * @param runConfigurationName The runConfigurationName to set
   */
  public void setRunConfigurationName(String runConfigurationName) {
    this.runConfigurationName = runConfigurationName;
  }
}
