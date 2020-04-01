/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.apache.hop.pipeline.step;

import org.apache.hop.pipeline.Pipeline;

/**
 * This listener informs the audience of the various states of a step.
 *
 * @author matt
 */
public interface StepListener {

  /**
   * This method is called when a step goes from being idle to being active.
   *
   * @param pipeline
   * @param stepMeta
   * @param step
   */
  public void stepActive( Pipeline pipeline, StepMeta stepMeta, StepInterface step );

  /**
   * This method is called when a step completes all work and is finished.
   *
   * @param pipeline
   * @param stepMeta
   * @param step
   */
  public void stepFinished( Pipeline pipeline, StepMeta stepMeta, StepInterface step );
}
