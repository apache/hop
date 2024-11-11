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

package org.apache.hop.pipeline.transforms.pipelineexecutor;

import java.util.ArrayList;
import java.util.List;
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
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.ISubPipelineAwareMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.TransformWithMappingMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transform.stream.IStream.StreamType;
import org.apache.hop.pipeline.transform.stream.Stream;
import org.apache.hop.pipeline.transform.stream.StreamIcon;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

/** Meta-data for the Pipeline Executor transform. */
@Transform(
    id = "PipelineExecutor",
    image = "ui/images/pipelineexecutor.svg",
    name = "i18n::PipelineExecutor.Name",
    description = "i18n::PipelineExecutor.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Flow",
    documentationUrl = "/pipeline/transforms/pipeline-executor.html",
    keywords = "i18n::PipelineExecutorMeta.keyword",
    actionTransformTypes = {ActionTransformType.HOP_FILE, ActionTransformType.HOP_PIPELINE})
public class PipelineExecutorMeta
    extends TransformWithMappingMeta<PipelineExecutor, PipelineExecutorData>
    implements ISubPipelineAwareMeta {

  private static final Class<?> PKG = PipelineExecutorMeta.class;

  static final String F_EXECUTION_RESULT_TARGET_TRANSFORM = "execution_result_target_transform";
  static final String F_RESULT_FILE_TARGET_TRANSFORM = "result_files_target_transform";
  static final String F_EXECUTOR_OUTPUT_TRANSFORM = "executors_output_transform";
  public static final String CONST_RESULT_ROWS_FIELD = "result_rows_field";

  /** The name of the pipeline run configuration with which we want to execute the pipeline. */
  private String runConfigurationName;

  /** Flag that indicate that pipeline name is specified in a stream's field */
  private boolean filenameInField;

  /** Name of the field containing the pipeline file's name */
  private String filenameField;

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

  private PipelineExecutorParameters parameters;

  private String executionResultTargetTransform;

  private TransformMeta executionResultTargetTransformMeta;

  /**
   * The optional name of the output field that will contain the execution time of the pipeline in
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
   * The optional name of the output field that will contain the log text of the pipeline execution
   * (String)
   */
  private String executionLogTextField;

  /**
   * The optional name of the output field that will contain the log channel ID of the pipeline
   * execution (String)
   */
  private String executionLogChannelIdField;

  /** The optional transform to send the result rows to */
  private String outputRowsSourceTransform;

  private TransformMeta outputRowsSourceTransformMeta;

  private String[] outputRowsField;

  private int[] outputRowsType;

  private int[] outputRowsLength;

  private int[] outputRowsPrecision;

  /** The optional transform to send the result files to */
  private String resultFilesTargetTransform;

  private TransformMeta resultFilesTargetTransformMeta;

  private String resultFilesFileNameField;

  /**
   * These fields are related to executor transform's "basic" output stream, where a copy of input
   * data will be placed
   */
  private String executorsOutputTransform;

  private TransformMeta executorsOutputTransformMeta;

  private IHopMetadataProvider metadataProvider;

  public PipelineExecutorMeta() {
    super(); // allocate BaseTransformMeta

    parameters = new PipelineExecutorParameters();
    this.allocate(0);
  }

  public void allocate(int nrFields) {
    outputRowsField = new String[nrFields];
    outputRowsType = new int[nrFields];
    outputRowsLength = new int[nrFields];
    outputRowsPrecision = new int[nrFields];
  }

  @Override
  public Object clone() {
    PipelineExecutorMeta retval = (PipelineExecutorMeta) super.clone();
    int nrFields = outputRowsField.length;
    retval.allocate(nrFields);
    System.arraycopy(outputRowsField, 0, retval.outputRowsField, 0, nrFields);
    System.arraycopy(outputRowsType, 0, retval.outputRowsType, 0, nrFields);
    System.arraycopy(outputRowsLength, 0, retval.outputRowsLength, 0, nrFields);
    System.arraycopy(outputRowsPrecision, 0, retval.outputRowsPrecision, 0, nrFields);
    return retval;
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(300);

    retval.append("    ").append(XmlHandler.addTagValue("run_configuration", runConfigurationName));
    retval.append("    ").append(XmlHandler.addTagValue("filename", filename));
    retval.append("    ").append(XmlHandler.addTagValue("filenameInField", filenameInField));
    retval.append("    ").append(XmlHandler.addTagValue("filenameField", filenameField));

    retval.append("    ").append(XmlHandler.addTagValue("group_size", groupSize));
    retval.append("    ").append(XmlHandler.addTagValue("group_field", groupField));
    retval.append("    ").append(XmlHandler.addTagValue("group_time", groupTime));

    // Add the mapping parameters too
    //
    retval.append("      ").append(parameters.getXml()).append(Const.CR);

    // The output side...
    //
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                F_EXECUTION_RESULT_TARGET_TRANSFORM,
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
                outputRowsSourceTransformMeta == null
                    ? null
                    : outputRowsSourceTransformMeta.getName()));
    for (int i = 0; i < outputRowsField.length; i++) {
      retval.append("      ").append(XmlHandler.openTag(CONST_RESULT_ROWS_FIELD));
      retval.append(XmlHandler.addTagValue("name", outputRowsField[i], false));
      retval.append(
          XmlHandler.addTagValue(
              "type", ValueMetaFactory.getValueMetaName(outputRowsType[i]), false));
      retval.append(XmlHandler.addTagValue("length", outputRowsLength[i], false));
      retval.append(XmlHandler.addTagValue("precision", outputRowsPrecision[i], false));
      retval.append(XmlHandler.closeTag(CONST_RESULT_ROWS_FIELD)).append(Const.CR);
    }

    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                F_RESULT_FILE_TARGET_TRANSFORM,
                resultFilesTargetTransformMeta == null
                    ? null
                    : resultFilesTargetTransformMeta.getName()));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("result_files_file_name_field", resultFilesFileNameField));

    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                F_EXECUTOR_OUTPUT_TRANSFORM,
                executorsOutputTransformMeta == null
                    ? null
                    : executorsOutputTransformMeta.getName()));

    return retval.toString();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      runConfigurationName = XmlHandler.getTagValue(transformNode, "run_configuration");
      filename = XmlHandler.getTagValue(transformNode, "filename");
      filenameInField =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "filenameInField"));
      filenameField = XmlHandler.getTagValue(transformNode, "filenameField");

      groupSize = XmlHandler.getTagValue(transformNode, "group_size");
      groupField = XmlHandler.getTagValue(transformNode, "group_field");
      groupTime = XmlHandler.getTagValue(transformNode, "group_time");

      // Load the mapping parameters too..
      //
      Node mappingParametersNode =
          XmlHandler.getSubNode(transformNode, PipelineExecutorParameters.XML_TAG);
      parameters = new PipelineExecutorParameters(mappingParametersNode);

      // The output side...
      //
      executionResultTargetTransform =
          XmlHandler.getTagValue(transformNode, F_EXECUTION_RESULT_TARGET_TRANSFORM);
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

      outputRowsSourceTransform =
          XmlHandler.getTagValue(transformNode, "result_rows_target_transform");

      int nrFields = XmlHandler.countNodes(transformNode, CONST_RESULT_ROWS_FIELD);
      allocate(nrFields);

      for (int i = 0; i < nrFields; i++) {

        Node fieldNode = XmlHandler.getSubNodeByNr(transformNode, CONST_RESULT_ROWS_FIELD, i);

        outputRowsField[i] = XmlHandler.getTagValue(fieldNode, "name");
        outputRowsType[i] =
            ValueMetaFactory.getIdForValueMeta(XmlHandler.getTagValue(fieldNode, "type"));
        outputRowsLength[i] = Const.toInt(XmlHandler.getTagValue(fieldNode, "length"), -1);
        outputRowsPrecision[i] = Const.toInt(XmlHandler.getTagValue(fieldNode, "precision"), -1);
      }

      resultFilesTargetTransform =
          XmlHandler.getTagValue(transformNode, F_RESULT_FILE_TARGET_TRANSFORM);
      resultFilesFileNameField =
          XmlHandler.getTagValue(transformNode, "result_files_file_name_field");
      executorsOutputTransform = XmlHandler.getTagValue(transformNode, F_EXECUTOR_OUTPUT_TRANSFORM);
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG, "PipelineExecutorMeta.Exception.ErrorLoadingPipelineExecutorDetailsFromXML"),
          e);
    }
  }

  @Override
  public void setDefault() {
    parameters = new PipelineExecutorParameters();
    parameters.setInheritingAllVariables(true);

    filenameInField = false;

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

  void prepareExecutionResultsFields(IRowMeta row, TransformMeta nextTransform)
      throws HopTransformException {
    if (nextTransform != null && executionResultTargetTransformMeta != null) {
      addFieldToRow(row, executionTimeField, IValueMeta.TYPE_INTEGER, 15, 0);
      addFieldToRow(row, executionResultField, IValueMeta.TYPE_BOOLEAN);
      addFieldToRow(row, executionNrErrorsField, IValueMeta.TYPE_INTEGER, 9, 0);
      addFieldToRow(row, executionLinesReadField, IValueMeta.TYPE_INTEGER, 9, 0);
      addFieldToRow(row, executionLinesWrittenField, IValueMeta.TYPE_INTEGER, 9, 0);
      addFieldToRow(row, executionLinesInputField, IValueMeta.TYPE_INTEGER, 9, 0);
      addFieldToRow(row, executionLinesOutputField, IValueMeta.TYPE_INTEGER, 9, 0);
      addFieldToRow(row, executionLinesRejectedField, IValueMeta.TYPE_INTEGER, 9, 0);
      addFieldToRow(row, executionLinesUpdatedField, IValueMeta.TYPE_INTEGER, 9, 0);
      addFieldToRow(row, executionLinesDeletedField, IValueMeta.TYPE_INTEGER, 9, 0);
      addFieldToRow(row, executionFilesRetrievedField, IValueMeta.TYPE_INTEGER, 9, 0);
      addFieldToRow(row, executionExitStatusField, IValueMeta.TYPE_INTEGER, 3, 0);
      addFieldToRow(row, executionLogTextField, IValueMeta.TYPE_STRING);
      addFieldToRow(row, executionLogChannelIdField, IValueMeta.TYPE_STRING, 50, 0);
    }
  }

  protected void addFieldToRow(IRowMeta row, String fieldName, int type)
      throws HopTransformException {
    addFieldToRow(row, fieldName, type, -1, -1);
  }

  protected void addFieldToRow(IRowMeta row, String fieldName, int type, int length, int precision)
      throws HopTransformException {
    if (!Utils.isEmpty(fieldName)) {
      try {
        IValueMeta value = ValueMetaFactory.createValueMeta(fieldName, type, length, precision);
        value.setOrigin(getParentTransformMeta().getName());
        row.addValueMeta(value);
      } catch (HopPluginException e) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "PipelineExecutorMeta.ValueMetaInterfaceCreation", fieldName),
            e);
      }
    }
  }

  void prepareExecutionResultsFileFields(IRowMeta row, TransformMeta nextTransform)
      throws HopTransformException {
    if (nextTransform != null
        && resultFilesTargetTransformMeta != null
        && nextTransform.equals(resultFilesTargetTransformMeta)) {
      addFieldToRow(row, resultFilesFileNameField, IValueMeta.TYPE_STRING);
    }
  }

  void prepareResultsRowsFields(IRowMeta row) throws HopTransformException {
    for (int i = 0; i < outputRowsField.length; i++) {
      addFieldToRow(
          row, outputRowsField[i], outputRowsType[i], outputRowsLength[i], outputRowsPrecision[i]);
    }
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    if (nextTransform != null) {
      if (nextTransform.equals(executionResultTargetTransformMeta)) {
        inputRowMeta.clear();
        prepareExecutionResultsFields(inputRowMeta, nextTransform);
      } else if (nextTransform.equals(resultFilesTargetTransformMeta)) {
        inputRowMeta.clear();
        prepareExecutionResultsFileFields(inputRowMeta, nextTransform);
      } else if (nextTransform.equals(outputRowsSourceTransformMeta)) {
        inputRowMeta.clear();
        prepareResultsRowsFields(inputRowMeta);
      }
      // else don't call clear on inputRowMeta, it's the main output and should mimic the input
    }
  }

  public String[] getInfoTransforms() {
    String[] infoTransforms = getTransformIOMeta().getInfoTransformNames();
    // Return null instead of empty array to preserve existing behavior
    return infoTransforms.length == 0 ? null : infoTransforms;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transforminfo,
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
              BaseMessages.getString(PKG, "PipelineExecutorMeta.CheckResult.NotReceivingAnyFields"),
              transforminfo);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "PipelineExecutorMeta.CheckResult.TransformReceivingFields",
                  prev.size() + ""),
              transforminfo);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "PipelineExecutorMeta.CheckResult.TransformReceivingFieldsFromOtherTransforms"),
              transforminfo);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "PipelineExecutorMeta.CheckResult.NoInputReceived"),
              transforminfo);
      remarks.add(cr);
    }
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, TransformMeta transformMeta) {
    List<ResourceReference> references = new ArrayList<>(5);
    String realFilename = variables.resolve(filename);
    ResourceReference reference = new ResourceReference(transformMeta);

    if (StringUtils.isNotEmpty(realFilename)) {
      // Add the filename to the references, including a reference to this transform
      // meta data.
      //
      reference.getEntries().add(new ResourceEntry(realFilename, ResourceType.ACTIONFILE));
    }
    references.add(reference);
    return references;
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
              BaseMessages.getString(PKG, "PipelineExecutorMeta.ResultStream.Description"),
              StreamIcon.TARGET,
              null));
      ioMeta.addStream(
          new Stream(
              StreamType.TARGET,
              outputRowsSourceTransformMeta,
              BaseMessages.getString(PKG, "PipelineExecutorMeta.ResultRowsStream.Description"),
              StreamIcon.TARGET,
              null));
      ioMeta.addStream(
          new Stream(
              StreamType.TARGET,
              resultFilesTargetTransformMeta,
              BaseMessages.getString(PKG, "PipelineExecutorMeta.ResultFilesStream.Description"),
              StreamIcon.TARGET,
              null));
      ioMeta.addStream(
          new Stream(
              StreamType.TARGET,
              executorsOutputTransformMeta,
              BaseMessages.getString(PKG, "PipelineExecutorMeta.ExecutorOutputStream.Description"),
              StreamIcon.OUTPUT,
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
    List<IStream> targets = getTransformIOMeta().getTargetStreams();
    int index = targets.indexOf(stream);
    TransformMeta transform = targets.get(index).getTransformMeta();
    switch (index) {
      case 0:
        setExecutionResultTargetTransformMeta(transform);
        break;
      case 1:
        setOutputRowsSourceTransformMeta(transform);
        break;
      case 2:
        setResultFilesTargetTransformMeta(transform);
        break;
      case 3:
        setExecutorsOutputTransformMeta(transform);
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
    outputRowsSourceTransformMeta =
        TransformMeta.findTransform(transforms, outputRowsSourceTransform);
    resultFilesTargetTransformMeta =
        TransformMeta.findTransform(transforms, resultFilesTargetTransform);
    executorsOutputTransformMeta =
        TransformMeta.findTransform(transforms, executorsOutputTransform);
  }

  @Override
  public PipelineType[] getSupportedPipelineTypes() {
    return new PipelineType[] {
      PipelineType.Normal,
    };
  }

  /**
   * @return the mappingParameters
   */
  public PipelineExecutorParameters getMappingParameters() {
    return parameters;
  }

  /**
   * @param mappingParameters the mappingParameters to set
   */
  public void setMappingParameters(PipelineExecutorParameters mappingParameters) {
    this.parameters = mappingParameters;
  }

  /**
   * @return the parameters
   */
  public PipelineExecutorParameters getParameters() {
    return parameters;
  }

  /**
   * @param parameters the parameters to set
   */
  public void setParameters(PipelineExecutorParameters parameters) {
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
   * @return the executionResultTargetTransform
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
   * @return The objects referenced in the transform, like a mapping, a pipeline, ...
   */
  @Override
  public String[] getReferencedObjectDescriptions() {
    return new String[] {
      BaseMessages.getString(PKG, "PipelineExecutorMeta.ReferencedObject.Description"),
    };
  }

  private boolean isPipelineDefined() {
    return StringUtils.isNotEmpty(filename);
  }

  @Override
  public boolean[] isReferencedObjectEnabled() {
    return new boolean[] {
      isPipelineDefined(),
    };
  }

  /**
   * Load the referenced object
   *
   * @param index the object index to load
   * @param variables the variable variables to use
   * @return the referenced object once loaded
   * @throws HopException
   */
  @Override
  public IHasFilename loadReferencedObject(
      int index, IHopMetadataProvider metadataProvider, IVariables variables) throws HopException {
    return loadMappingMeta(this, metadataProvider, variables);
  }

  public IHopMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  public void setMetadataProvider(IHopMetadataProvider metadataProvider) {
    this.metadataProvider = metadataProvider;
  }

  public String getOutputRowsSourceTransform() {
    return outputRowsSourceTransform;
  }

  public void setOutputRowsSourceTransform(String outputRowsSourceTransform) {
    this.outputRowsSourceTransform = outputRowsSourceTransform;
  }

  public TransformMeta getOutputRowsSourceTransformMeta() {
    return outputRowsSourceTransformMeta;
  }

  public void setOutputRowsSourceTransformMeta(TransformMeta outputRowsSourceTransformMeta) {
    this.outputRowsSourceTransformMeta = outputRowsSourceTransformMeta;
  }

  public String[] getOutputRowsField() {
    return outputRowsField;
  }

  public void setOutputRowsField(String[] outputRowsField) {
    this.outputRowsField = outputRowsField;
  }

  public int[] getOutputRowsType() {
    return outputRowsType;
  }

  public void setOutputRowsType(int[] outputRowsType) {
    this.outputRowsType = outputRowsType;
  }

  public int[] getOutputRowsLength() {
    return outputRowsLength;
  }

  public void setOutputRowsLength(int[] outputRowsLength) {
    this.outputRowsLength = outputRowsLength;
  }

  public int[] getOutputRowsPrecision() {
    return outputRowsPrecision;
  }

  public void setOutputRowsPrecision(int[] outputRowsPrecision) {
    this.outputRowsPrecision = outputRowsPrecision;
  }

  public String getResultFilesTargetTransform() {
    return resultFilesTargetTransform;
  }

  public void setResultFilesTargetTransform(String resultFilesTargetTransform) {
    this.resultFilesTargetTransform = resultFilesTargetTransform;
  }

  public TransformMeta getResultFilesTargetTransformMeta() {
    return resultFilesTargetTransformMeta;
  }

  public void setResultFilesTargetTransformMeta(TransformMeta resultFilesTargetTransformMeta) {
    this.resultFilesTargetTransformMeta = resultFilesTargetTransformMeta;
  }

  public String getExecutorsOutputTransform() {
    return executorsOutputTransform;
  }

  public void setExecutorsOutputTransform(String executorsOutputTransform) {
    this.executorsOutputTransform = executorsOutputTransform;
  }

  public TransformMeta getExecutorsOutputTransformMeta() {
    return executorsOutputTransformMeta;
  }

  public void setExecutorsOutputTransformMeta(TransformMeta executorsOutputTransformMeta) {
    this.executorsOutputTransformMeta = executorsOutputTransformMeta;
  }

  @Override
  public boolean cleanAfterHopFromRemove() {

    setExecutionResultTargetTransformMeta(null);
    setOutputRowsSourceTransformMeta(null);
    setResultFilesTargetTransformMeta(null);
    setExecutorsOutputTransformMeta(null);
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
    } else if (getOutputRowsSourceTransformMeta() != null
        && toTransformName.equals(getOutputRowsSourceTransformMeta().getName())) {
      setOutputRowsSourceTransformMeta(null);
      hasChanged = true;
    } else if (getResultFilesTargetTransformMeta() != null
        && toTransformName.equals(getResultFilesTargetTransformMeta().getName())) {
      setResultFilesTargetTransformMeta(null);
      hasChanged = true;
    } else if (getExecutorsOutputTransformMeta() != null
        && toTransformName.equals(getExecutorsOutputTransformMeta().getName())) {
      setExecutorsOutputTransformMeta(null);
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

  public boolean isFilenameInField() {
    return filenameInField;
  }

  public void setFilenameInField(boolean filenameInField) {
    this.filenameInField = filenameInField;
  }

  public String getFilenameField() {
    return filenameField;
  }

  public void setFilenameField(String filenameField) {
    this.filenameField = filenameField;
  }
}
