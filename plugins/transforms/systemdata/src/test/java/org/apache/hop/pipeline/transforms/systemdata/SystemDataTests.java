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

package org.apache.hop.pipeline.transforms.systemdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import org.apache.hop.core.Const;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;

/** Unit test for {@link SystemData} */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class SystemDataTests {

  private static final Date PIPELINE_START = new Date(1_700_000_000_000L);
  private static final Date WORKFLOW_START = new Date(1_700_100_000_000L);
  private static final Date PREVIOUS_EXECUTION_START = new Date(1_699_000_000_000L);

  private TransformMockHelper<SystemDataMeta, SystemDataData> helper;
  private SystemDataMeta meta;
  private SystemDataData data;
  private IHopMetadataProvider metadataProvider;
  private IExecutionInfoLocation executionInfoLocation;

  @BeforeEach
  void setUp() throws Exception {
    helper = new TransformMockHelper<>("SystemData", SystemDataMeta.class, SystemDataData.class);
    when(helper.pipeline.isRunning()).thenReturn(true);
    when(helper.pipeline.isStopped()).thenReturn(false);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);

    meta = new SystemDataMeta();
    data = new SystemDataData();
    metadataProvider = mock(IHopMetadataProvider.class);

    when(helper.pipeline.getExecutionStartDate()).thenReturn(PIPELINE_START);
    when(helper.pipeline.getPipelineMeta()).thenReturn(helper.pipelineMeta);
    when(helper.pipelineMeta.getName()).thenReturn("pipeline-name");
    when(helper.pipelineMeta.getModifiedUser()).thenReturn("tester");
    when(helper.pipelineMeta.getModifiedDate()).thenReturn(new Date(1_701_000_000_000L));
    when(helper.pipelineMeta.getFilename()).thenReturn("pipeline-file.hpl");

    PipelineRunConfiguration pipelineRunConfiguration = new PipelineRunConfiguration();
    pipelineRunConfiguration.setExecutionInfoLocationName("pipeline-location");
    when(helper.pipeline.getPipelineRunConfiguration()).thenReturn(pipelineRunConfiguration);
    @SuppressWarnings("unchecked")
    IWorkflowEngine<WorkflowMeta> parentWorkflow = mock(IWorkflowEngine.class);
    when(helper.pipeline.getParentWorkflow()).thenReturn(parentWorkflow);
    when(parentWorkflow.getExecutionStartDate()).thenReturn(WORKFLOW_START);
    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    when(workflowMeta.getName()).thenReturn("workflow-name");
    when(parentWorkflow.getWorkflowMeta()).thenReturn(workflowMeta);
    WorkflowRunConfiguration workflowRunConfiguration = new WorkflowRunConfiguration();
    workflowRunConfiguration.setExecutionInfoLocationName("workflow-location");
    when(parentWorkflow.getWorkflowRunConfiguration()).thenReturn(workflowRunConfiguration);

    Result previousResult = createPreviousResult();
    when(helper.pipeline.getPreviousResult()).thenReturn(previousResult);

    executionInfoLocation = mock(IExecutionInfoLocation.class);
    Execution execution = new Execution();
    execution.setExecutionStartDate(PREVIOUS_EXECUTION_START);
    when(executionInfoLocation.findPreviousSuccessfulExecution(any(), any())).thenReturn(execution);

    @SuppressWarnings("unchecked")
    IHopMetadataSerializer<ExecutionInfoLocation> serializer = mock(IHopMetadataSerializer.class);
    when(metadataProvider.getSerializer(ExecutionInfoLocation.class)).thenReturn(serializer);
    when(serializer.load(any()))
        .thenAnswer(
            invocation -> {
              ExecutionInfoLocation location = new ExecutionInfoLocation();
              location.setExecutionInfoLocation(executionInfoLocation);
              return location;
            });
  }

  @Test
  void processRow_readsInputAndEmitsAllConfiguredSystemFields() throws Exception {
    List<SystemDataMeta.SystemInfoField> fields = new ArrayList<>();
    for (SystemDataType type : SystemDataType.values()) {
      if (type == SystemDataType.HOSTNAME
          || type == SystemDataType.HOSTNAME_REAL
          || type == SystemDataType.IP_ADDRESS) {
        continue;
      }
      SystemDataMeta.SystemInfoField field = new SystemDataMeta.SystemInfoField();
      field.setFieldName("f_" + type.name());
      field.setFieldType(type);
      fields.add(field);
    }
    meta.setFields(fields);

    when(helper.pipelineMeta.findPreviousTransforms(any()))
        .thenReturn(List.of(mock(TransformMeta.class)));
    SystemData transform = newTransform();
    IRowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("input"));
    transform.setInputRowMeta(inputMeta);
    transform.addRowSetToInputRowSets(helper.getMockInputRowSet(new Object[][] {{"seed"}}));
    QueueRowSet outputRowSet = new QueueRowSet();
    transform.addRowSetToOutputRowSets(outputRowSet);

    assertTrue(transform.init());
    transform.processRow();

    Object[] output = outputRowSet.getRow();
    assertNotNull(output);
    assertTrue(output.length >= fields.size());
    assertEquals("seed", output[0]);

    assertEquals(
        PIPELINE_START, value(outputRowSet.getRowMeta(), output, SystemDataType.SYSTEM_START));
    assertEquals(
        "pipeline-name", value(outputRowSet.getRowMeta(), output, SystemDataType.PIPELINE_NAME));
    assertEquals("tester", value(outputRowSet.getRowMeta(), output, SystemDataType.MODIFIED_USER));
    assertEquals(
        "pipeline-file.hpl", value(outputRowSet.getRowMeta(), output, SystemDataType.FILENAME));
    assertEquals(
        0L,
        ((Number)
                Objects.requireNonNull(
                    value(outputRowSet.getRowMeta(), output, SystemDataType.COPYNR)))
            .longValue());
    assertEquals(
        true, value(outputRowSet.getRowMeta(), output, SystemDataType.PREVIOUS_RESULT_RESULT));
    assertEquals(
        11L,
        ((Number)
                Objects.requireNonNull(
                    value(
                        outputRowSet.getRowMeta(),
                        output,
                        SystemDataType.PREVIOUS_RESULT_EXIT_STATUS)))
            .longValue());
    assertEquals(
        "log-text",
        value(outputRowSet.getRowMeta(), output, SystemDataType.PREVIOUS_RESULT_LOG_TEXT));
    assertNull(value(outputRowSet.getRowMeta(), output, SystemDataType.NONE));
    assertInstanceOf(
        Date.class, value(outputRowSet.getRowMeta(), output, SystemDataType.THIS_DAY_START));
  }

  @Test
  @Tag("slow")
  void processRow_networkFieldsAreFastAndCoveredWithStaticMocking() throws Exception {
    List<SystemDataMeta.SystemInfoField> fields = new ArrayList<>();
    fields.add(field("f_HOSTNAME", SystemDataType.HOSTNAME));
    fields.add(field("f_HOSTNAME_REAL", SystemDataType.HOSTNAME_REAL));
    fields.add(field("f_IP_ADDRESS", SystemDataType.IP_ADDRESS));
    meta.setFields(fields);

    when(helper.pipelineMeta.findPreviousTransforms(any()))
        .thenReturn(List.of(mock(TransformMeta.class)));
    SystemData transform = newTransform();
    transform.setInputRowMeta(new RowMeta());
    transform.addRowSetToInputRowSets(helper.getMockInputRowSet(new Object[][] {{}}));
    QueueRowSet outputRowSet = new QueueRowSet();
    transform.addRowSetToOutputRowSets(outputRowSet);

    try (MockedStatic<Const> constMock = mockStatic(Const.class, CALLS_REAL_METHODS)) {
      constMock.when(Const::getHostname).thenReturn("host");
      constMock.when(Const::getHostnameReal).thenReturn("lance");
      constMock.when(Const::getIPAddress).thenReturn("127.0.0.1");

      assertTrue(transform.init());
      transform.processRow();
    }

    Object[] output = outputRowSet.getRow();
    assertNotNull(output);
    assertEquals("host", valueByFieldName(outputRowSet.getRowMeta(), output, "f_HOSTNAME"));
    assertEquals("lance", valueByFieldName(outputRowSet.getRowMeta(), output, "f_HOSTNAME_REAL"));
    assertEquals("127.0.0.1", valueByFieldName(outputRowSet.getRowMeta(), output, "f_IP_ADDRESS"));
  }

  @Test
  void processRow_workflowDatesAreNullWhenParentWorkflowMissing() throws Exception {
    meta.setFields(
        List.of(
            field("workflow_from", SystemDataType.WORKFLOW_DATE_FROM),
            field("workflow_to", SystemDataType.WORKFLOW_DATE_TO)));
    when(helper.pipeline.getParentWorkflow()).thenReturn(null);
    when(helper.pipelineMeta.findPreviousTransforms(any())).thenReturn(new ArrayList<>());

    SystemData transform = newTransform();
    transform.setInputRowMeta(new RowMeta());
    transform.addRowSetToOutputRowSets(new QueueRowSet());
    assertTrue(transform.init());
    assertFalse(transform.processRow());

    IRowSet outputRowSet = transform.getOutputRowSets().getFirst();
    Object[] output = outputRowSet.getRow();
    assertNull(valueByFieldName(outputRowSet.getRowMeta(), output, "workflow_from"));
    assertNull(valueByFieldName(outputRowSet.getRowMeta(), output, "workflow_to"));
  }

  @Test
  void processRow_pipelineDateFromFallsBackTo1900WhenNoPreviousExecution() throws Exception {
    meta.setFields(List.of(field("pipeline_from", SystemDataType.PIPELINE_DATE_FROM)));
    when(helper.pipelineMeta.findPreviousTransforms(any())).thenReturn(new ArrayList<>());
    when(executionInfoLocation.findPreviousSuccessfulExecution(any(), any())).thenReturn(null);

    SystemData transform = newTransform();
    transform.setInputRowMeta(new RowMeta());
    transform.addRowSetToOutputRowSets(new QueueRowSet());
    assertTrue(transform.init());
    assertFalse(transform.processRow());

    IRowSet outputRowSet = transform.getOutputRowSets().getFirst();
    Object[] output = outputRowSet.getRow();
    Date value = (Date) valueByFieldName(outputRowSet.getRowMeta(), output, "pipeline_from");
    assertNotNull(value);
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(value);
    assertEquals(1900, calendar.get(Calendar.YEAR));
  }

  @Test
  void processRow_ipAddressFailureThrowsHopException() throws Exception {
    meta.setFields(List.of(field("ip", SystemDataType.IP_ADDRESS)));
    when(helper.pipelineMeta.findPreviousTransforms(any())).thenReturn(new ArrayList<>());
    SystemData transform = newTransform();
    transform.setInputRowMeta(new RowMeta());
    transform.addRowSetToOutputRowSets(new QueueRowSet());
    assertTrue(transform.init());

    try (MockedStatic<Const> constMock = mockStatic(Const.class, CALLS_REAL_METHODS)) {
      constMock
          .when(Const::getIPAddress)
          .thenThrow(new UnknownHostException("expected test exception"));
      assertThrows(HopException.class, transform::processRow);
    }
  }

  @Test
  void processRow_singleRowModeEmitsDefaultsWhenPreviousResultMissing() throws Exception {
    List<SystemDataMeta.SystemInfoField> fields = new ArrayList<>();
    fields.add(field("nr_errors", SystemDataType.PREVIOUS_RESULT_NR_ERRORS));
    fields.add(field("result", SystemDataType.PREVIOUS_RESULT_RESULT));
    fields.add(field("log", SystemDataType.PREVIOUS_RESULT_LOG_TEXT));
    fields.add(field("pipeline_from", SystemDataType.PIPELINE_DATE_FROM));
    meta.setFields(fields);

    when(helper.pipelineMeta.findPreviousTransforms(any())).thenReturn(new ArrayList<>());
    when(helper.pipeline.getPreviousResult()).thenReturn(null);
    @SuppressWarnings("unchecked")
    IHopMetadataSerializer<ExecutionInfoLocation> serializer = mock(IHopMetadataSerializer.class);
    when(metadataProvider.getSerializer(ExecutionInfoLocation.class)).thenReturn(serializer);
    when(serializer.load(any())).thenReturn(null);

    SystemData transform = newTransform();
    transform.setInputRowMeta(new RowMeta());
    transform.addRowSetToOutputRowSets(new QueueRowSet());

    assertTrue(transform.init());
    assertFalse(transform.processRow());

    IRowSet outputRowSet = transform.getOutputRowSets().getFirst();
    Object[] output = outputRowSet.getRow();
    assertNotNull(output);
    assertTrue(output.length >= 4);
    assertEquals(
        0L,
        ((Number)
                Objects.requireNonNull(
                    valueByFieldName(outputRowSet.getRowMeta(), output, "nr_errors")))
            .longValue());
    assertEquals(false, valueByFieldName(outputRowSet.getRowMeta(), output, "result"));
    assertNull(valueByFieldName(outputRowSet.getRowMeta(), output, "log"));
    assertNull(valueByFieldName(outputRowSet.getRowMeta(), output, "pipeline_from"));
  }

  private SystemData newTransform() {
    TransformMeta tm = helper.transformMeta;
    PipelineMeta pm = helper.pipelineMeta;
    SystemData input = new SystemData(tm, meta, data, 0, pm, helper.pipeline);
    input.setMetadataProvider(metadataProvider);
    return input;
  }

  private static SystemDataMeta.SystemInfoField field(String name, SystemDataType type) {
    SystemDataMeta.SystemInfoField field = new SystemDataMeta.SystemInfoField();
    field.setFieldName(name);
    field.setFieldType(type);
    return field;
  }

  private static Object value(IRowMeta rowMeta, Object[] output, SystemDataType type) {
    int index = rowMeta.indexOfValue("f_" + type.name());
    if (index < 0) {
      return null;
    }
    return output[index];
  }

  private static Object valueByFieldName(IRowMeta rowMeta, Object[] output, String fieldName) {
    int index = rowMeta.indexOfValue(fieldName);
    if (index < 0) {
      return null;
    }
    return output[index];
  }

  private static Result createPreviousResult() {
    Result previousResult = new Result();
    previousResult.setResult(true);
    previousResult.setExitStatus(11);
    previousResult.setEntryNr(12);
    previousResult.setNrFilesRetrieved(13);
    previousResult.setNrLinesDeleted(14);
    previousResult.setNrLinesInput(15);
    previousResult.setNrLinesOutput(16);
    previousResult.setNrLinesRead(17);
    previousResult.setNrLinesRejected(18);
    previousResult.setNrLinesUpdated(19);
    previousResult.setNrLinesWritten(20);
    previousResult.setStopped(true);
    previousResult.setNrErrors(21);
    previousResult.setLogText("log-text");
    previousResult.getRows().add(null);
    previousResult.getResultFiles().put("f1", mock(ResultFile.class));
    return previousResult;
  }
}
