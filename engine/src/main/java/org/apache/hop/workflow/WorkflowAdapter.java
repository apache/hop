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

package org.apache.hop.workflow;

import org.apache.hop.core.exception.HopException;

/**
 * Utility class to allow only certain methods of IWorkflowListener to be overridden.
 *
 * @author Marc
 */

public class WorkflowAdapter implements IWorkflowListener {

  @Override
  public void jobFinished( Workflow workflow ) throws HopException {
    // NoOp

  }

  @Override
  public void jobStarted( Workflow workflow ) throws HopException {
    // NoOp

  }

}
