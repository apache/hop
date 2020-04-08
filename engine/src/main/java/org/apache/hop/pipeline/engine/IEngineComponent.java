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

package org.apache.hop.pipeline.engine;

/**
 * An identifiable component of an execution engine {@link IPipelineEngine}
 * In a pipeline engine this would be a transform
 */
public interface IEngineComponent {

  /**
   * @return The component name
   */
  String getName();

  /**
   * @return The copy number (0 of higher for parallel runs)
   */
  int getCopyNr();


  /**
   * @return The log channel ID or null if there is no separate log channel.
   */
  String getLogChannelId();

  /**
   * Retrieve the logging text of this component in the engine
   *
   * @return logging text
   */
  String getLogText();

  /**
   * @return true if this component is running/active
   */
  boolean isRunning();

  /**
   * @return true if the component is selected in the user interface
   */
  boolean isSelected();

  /**
   * @return The number of errors in this component
   */
  long getErrors();
}
