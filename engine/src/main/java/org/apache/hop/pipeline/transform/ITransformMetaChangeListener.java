/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2015 - 2017 by Hitachi Vantara : http://www.pentaho.com
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
package org.apache.hop.pipeline.transform;

import org.apache.hop.pipeline.PipelineMeta;

public interface ITransformMetaChangeListener {
  /**
   * This method is called when a transform was changed
   *
   * @param pipelineMeta PipelineMeta which include this transforms
   * @param oldMeta   the previous meta, which changed
   * @param newMeta   the updated meta with new variables values
   */
  void onTransformChange( PipelineMeta pipelineMeta, TransformMeta oldMeta, TransformMeta newMeta );

}
