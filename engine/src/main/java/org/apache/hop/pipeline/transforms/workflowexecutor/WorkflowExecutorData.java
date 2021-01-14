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

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.workflow.engine.IWorkflowEngine;

import java.util.List;

/**
 * @author Matt
 * @since 24-jan-2005
 */
public class WorkflowExecutorData extends BaseTransformData implements ITransformData {
  public IWorkflowEngine<WorkflowMeta> executorWorkflow;
  public WorkflowMeta executorWorkflowMeta;
  public IRowMeta inputRowMeta;
  public IRowMeta executionResultsOutputRowMeta;
  public IRowMeta resultRowsOutputRowMeta;
  public IRowMeta resultFilesOutputRowMeta;

  public List<RowMetaAndData> groupBuffer;
  public int groupSize;
  public int groupTime;
  public long groupTimeStart;
  public String groupField;
  public int groupFieldIndex;
  public IValueMeta groupFieldMeta;
  public Object prevGroupFieldData;
  public IRowSet resultRowsRowSet;
  public IRowSet resultFilesRowSet;
  public IRowSet executionResultRowSet;

  public WorkflowExecutorData() {
    super();
  }

}
