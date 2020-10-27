/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.workflow;

import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.workflow.engine.IWorkflowEngine;

public class DelegationAdapter implements IDelegationListener {

  @Override public void jobDelegationStarted( IWorkflowEngine<WorkflowMeta> delegatedWorkflow, WorkflowExecutionConfiguration workflowExecutionConfiguration ) {

  }

  @Override public void pipelineDelegationStarted( IPipelineEngine<PipelineMeta> delegatedPipeline, PipelineExecutionConfiguration pipelineExecutionConfiguration ) {

  }
}
