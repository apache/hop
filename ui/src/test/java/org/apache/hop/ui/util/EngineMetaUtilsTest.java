/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.util;

import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class EngineMetaUtilsTest {

  @Test
  public void isJobOrPipeline_withJob() {
    WorkflowMeta jobInstance = new WorkflowMeta();
    assertTrue( EngineMetaUtils.isJobOrPipeline( jobInstance ) );
  }

  @Test
  public void isJobOrPipeline_withPipeline() {
    PipelineMeta pipelineInstance = new PipelineMeta();
    assertTrue( EngineMetaUtils.isJobOrPipeline( pipelineInstance ) );
  }

}
