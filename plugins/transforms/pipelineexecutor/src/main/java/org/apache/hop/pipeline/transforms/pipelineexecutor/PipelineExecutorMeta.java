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
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
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
import org.apache.hop.metadata.api.HopMetadataProperty;
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
    image = "pipelineexecutor.svg",
    name = "i18n::PipelineExecutor.Name",
    description = "i18n::PipelineExecutor.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Flow",
    documentationUrl = "/pipeline/transforms/pipeline-executor.html",
    keywords = "i18n::PipelineExecutorMeta.keyword",
    actionTransformTypes = {ActionTransformType.HOP_FILE, ActionTransformType.HOP_PIPELINE})
@Getter
@Setter
public class PipelineExecutorMeta
    extends TransformWithMappingMeta<PipelineExecutor, PipelineExecutorData>
    implements ISubPipelineAwareMeta {

  private static final Class<?> PKG = PipelineExecutorMeta.class;

  /** The name of the pipeline run configuration with which we want to execute the pipeline. */
  @HopMetadataProperty(key = "run_configuration")
  private String runConfigurationName;

  /** Flag that indicate that pipeline name is specified in a stream's field */
  @HopMetadataProperty(key = "filenameInField")
  private boolean filenameInField;

  /** Name of the field containing the pipeline file's name */
  @HopMetadataProperty(key = "filenameField")
  private String filenameField;

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

  @HopMetadataProperty(key = "variable_mapping", groupKey = "parameters")
  private List<PipelineExecutorParameters> parameters;

  /** This flag causes the workflow to inherit all variables from the parent pipeline */
  @HopMetadataProperty(key = "inherit_all_vars")
  private boolean inheritingAllVariables;

  @HopMetadataProperty(key = "execution_result_target_transform")
  private String executionResultTargetTransform;

  private TransformMeta executionResultTargetTransformMeta;

  /**
   * The optional name of the output field that will contain the execution time of the pipeline in
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
   * The optional name of the output field that will contain the log text of the pipeline execution
   * (String)
   */
  @HopMetadataProperty(key = "execution_log_text_field")
  private String executionLogTextField;

  /**
   * The optional name of the output field that will contain the log channel ID of the pipeline
   * execution (String)
   */
  @HopMetadataProperty(key = "execution_log_channelid_field")
  private String executionLogChannelIdField;

  /** The optional transform to send the result rows to */
  @HopMetadataProperty(key = "result_rows_target_transform")
  private String outputRowsSourceTransform;

  private TransformMeta outputRowsSourceTransformMeta;

  @HopMetadataProperty(
      key = "result_rows_field",
      inlineListTags = {"name", "type", "length", "precision"})
  private List<PipelineExecutorResultRows> resultRows;

  /** The optional transform to send the result files to */
  @HopMetadataProperty(key = "result_files_target_transform")
  private String resultFilesTargetTransform;

  private TransformMeta resultFilesTargetTransformMeta;

  @HopMetadataProperty(key = "result_files_file_name_field")
  private String resultFilesFileNameField;

  /**
   * These fields are related to executor transform's "basic" output stream, where a copy of input
   * data will be placed
   */
  @HopMetadataProperty(key = "executors_output_transform")
  private String executorsOutputTransform;

  private TransformMeta executorsOutputTransformMeta;

  private IHopMetadataProvider metadataProvider;

  public PipelineExecutorMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * @deprecated Added for backwards compatibility for old parameter style
   * @param transformNode Transform node XML
   * @param metadataProvider Metadata provider
   * @throws HopXmlException when unable to parse XML
   */
  @Override
  @Deprecated(since = "2.13")
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    super.loadXml(transformNode, metadataProvider);
    try {
      // Load inherit_all_vars
      //
      String value =
          XmlHandler.getTagValue(
              XmlHandler.getSubNode(transformNode, "parameters"), "inherit_all_vars");
      if (value != null) {
        setInheritingAllVariables("Y".equalsIgnoreCase(value));
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG, "PipelineExecutorMeta.Exception.ErrorLoadingPipelineExecutorDetailsFromXML"),
          e);
    }
  }

  @Override
  public void setDefault() {
    parameters = new ArrayList<>();
    resultRows = new ArrayList<>();
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
    for (PipelineExecutorResultRows pipelineExecutorResultRows : resultRows) {
      addFieldToRow(
          row,
          pipelineExecutorResultRows.getName(),
          ValueMetaFactory.getIdForValueMeta(pipelineExecutorResultRows.getType()),
          pipelineExecutorResultRows.getLength(),
          pipelineExecutorResultRows.getPrecision());
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
    if (prev == null || prev.isEmpty()) {
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
        setExecutionResultTargetTransform(transform.getName());
        break;
      case 1:
        setOutputRowsSourceTransformMeta(transform);
        setOutputRowsSourceTransform(transform.getName());
        break;
      case 2:
        setResultFilesTargetTransformMeta(transform);
        setResultFilesTargetTransform(transform.getName());
        break;
      case 3:
        setExecutorsOutputTransformMeta(transform);
        setExecutorsOutputTransform(transform.getName());
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

  @Override
  public boolean excludeFromCopyDistributeVerification() {
    return true;
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

  @Override
  public boolean cleanAfterHopFromRemove() {

    setExecutionResultTargetTransformMeta(null);
    setExecutionResultTargetTransform(null);
    setOutputRowsSourceTransformMeta(null);
    setOutputRowsSourceTransform(null);
    setResultFilesTargetTransformMeta(null);
    setResultFilesTargetTransform(null);
    setExecutorsOutputTransformMeta(null);
    setExecutorsOutputTransform(null);
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
      setExecutionResultTargetTransform(null);
      hasChanged = true;
    } else if (getOutputRowsSourceTransformMeta() != null
        && toTransformName.equals(getOutputRowsSourceTransformMeta().getName())) {
      setOutputRowsSourceTransformMeta(null);
      setOutputRowsSourceTransform(null);
      hasChanged = true;
    } else if (getResultFilesTargetTransformMeta() != null
        && toTransformName.equals(getResultFilesTargetTransformMeta().getName())) {
      setResultFilesTargetTransformMeta(null);
      setResultFilesTargetTransform(null);
      hasChanged = true;
    } else if (getExecutorsOutputTransformMeta() != null
        && toTransformName.equals(getExecutorsOutputTransformMeta().getName())) {
      setExecutorsOutputTransformMeta(null);
      setExecutorsOutputTransform(null);
      hasChanged = true;
    }
    return hasChanged;
  }

  @Override
  public boolean supportsDrillDown() {
    return true;
  }
}
