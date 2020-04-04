/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
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

package org.apache.hop.core.listeners;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.job.Job;
import org.apache.hop.pipeline.Pipeline;

public interface ISubComponentExecutionListener {

  /**
   * This method is called right before a sub-pipeline, mapping, single threader template, ... is to be executed
   * in a parent job or pipeline.
   *
   * @param pipeline The pipeline that is about to be executed.
   * @throws HopException In case something goes wrong
   */
  void beforePipelineExecution( Pipeline pipeline ) throws HopException;

  /**
   * This method is called right after a sub-pipeline, mapping, single threader template, ... was executed in a
   * parent job or pipeline.
   *
   * @param pipeline The pipeline that was just executed.
   * @throws HopException In case something goes wrong
   */
  void afterPipelineExecution( Pipeline pipeline ) throws HopException;

  /**
   * This method is called right before a job is to be executed in a parent job or pipeline (Job job-entry, Job
   * Executor transform).
   *
   * @param job The job that is about to be executed.
   * @throws HopException In case something goes wrong
   */
  void beforeJobExecution( Job job ) throws HopException;

  /**
   * This method is called right after a job was executed in a parent job or pipeline (Job job-entry, Job Executor
   * transform).
   *
   * @param job The job that was executed.
   * @throws HopException In case something goes wrong
   */
  void afterJobExecution( Job job ) throws HopException;
}
