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

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.util.List;

/**
 * @author Matt
 * @since 18-mar-2013
 */
public class PipelineExecutorData extends BaseTransformData implements ITransformData {
  private IPipelineEngine<PipelineMeta> executorPipeline;
  private PipelineMeta executorPipelineMeta;

  private IRowMeta inputRowMeta;

  private IRowMeta executorTransformOutputRowMeta;
  private IRowMeta resultRowsOutputRowMeta;
  private IRowMeta executionResultsOutputRowMeta;
  private IRowMeta resultFilesOutputRowMeta;

  public List<RowMetaAndData> groupBuffer;
  public int groupSize;
  public int groupTime;
  public long groupTimeStart;
  public String groupField;
  public int groupFieldIndex;
  public IValueMeta groupFieldMeta;

  public Object prevGroupFieldData;

  private IRowSet executorTransformOutputRowSet;
  private IRowSet resultRowsRowSet;
  private IRowSet resultFilesRowSet;
  private IRowSet executionResultRowSet;

  public PipelineExecutorData() {
    super();
  }

  /**
   * Gets executorPipeline
   *
   * @return value of executorPipeline
   */
  public IPipelineEngine<PipelineMeta> getExecutorPipeline() {
    return executorPipeline;
  }

  /**
   * @param executorPipeline The executorPipeline to set
   */
  public void setExecutorPipeline( IPipelineEngine<PipelineMeta> executorPipeline ) {
    this.executorPipeline = executorPipeline;
  }

  public PipelineMeta getExecutorPipelineMeta() {
    return executorPipelineMeta;
  }

  public void setExecutorPipelineMeta( PipelineMeta executorPipelineMeta ) {
    this.executorPipelineMeta = executorPipelineMeta;
  }

  public IRowMeta getInputRowMeta() {
    return inputRowMeta;
  }

  public void setInputRowMeta( IRowMeta inputRowMeta ) {
    this.inputRowMeta = inputRowMeta;
  }

  public IRowMeta getExecutorTransformOutputRowMeta() {
    return executorTransformOutputRowMeta;
  }

  public void setExecutorTransformOutputRowMeta( IRowMeta executorTransformOutputRowMeta ) {
    this.executorTransformOutputRowMeta = executorTransformOutputRowMeta;
  }

  public IRowMeta getResultRowsOutputRowMeta() {
    return resultRowsOutputRowMeta;
  }

  public void setResultRowsOutputRowMeta( IRowMeta resultRowsOutputRowMeta ) {
    this.resultRowsOutputRowMeta = resultRowsOutputRowMeta;
  }

  public IRowMeta getExecutionResultsOutputRowMeta() {
    return executionResultsOutputRowMeta;
  }

  public void setExecutionResultsOutputRowMeta( IRowMeta executionResultsOutputRowMeta ) {
    this.executionResultsOutputRowMeta = executionResultsOutputRowMeta;
  }

  public IRowMeta getResultFilesOutputRowMeta() {
    return resultFilesOutputRowMeta;
  }

  public void setResultFilesOutputRowMeta( IRowMeta resultFilesOutputRowMeta ) {
    this.resultFilesOutputRowMeta = resultFilesOutputRowMeta;
  }

  public IRowSet getExecutorTransformOutputRowSet() {
    return executorTransformOutputRowSet;
  }

  public void setExecutorTransformOutputRowSet( IRowSet executorTransformOutputRowSet ) {
    this.executorTransformOutputRowSet = executorTransformOutputRowSet;
  }

  public IRowSet getResultRowsRowSet() {
    return resultRowsRowSet;
  }

  public void setResultRowsRowSet( IRowSet resultRowsRowSet ) {
    this.resultRowsRowSet = resultRowsRowSet;
  }

  public IRowSet getResultFilesRowSet() {
    return resultFilesRowSet;
  }

  public void setResultFilesRowSet( IRowSet resultFilesRowSet ) {
    this.resultFilesRowSet = resultFilesRowSet;
  }

  public IRowSet getExecutionResultRowSet() {
    return executionResultRowSet;
  }

  public void setExecutionResultRowSet( IRowSet executionResultRowSet ) {
    this.executionResultRowSet = executionResultRowSet;
  }
}
