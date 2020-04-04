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

package org.apache.hop.ui.util;

import org.apache.hop.core.IEngineMeta;
import org.apache.hop.job.JobMeta;
import org.apache.hop.pipeline.PipelineMeta;

public class EngineMetaUtils {

  /**
   * Validates if {@code engineMetaInterface} is Job or Pipeline.
   *
   * @param engineMetaInterface
   * @return true if engineMetaInterface instance is Job or Pipeline, otherwise false.
   */
  public static boolean isJobOrPipeline( IEngineMeta engineMetaInterface ) {

    return ( engineMetaInterface instanceof PipelineMeta ) || ( engineMetaInterface instanceof JobMeta );
  }

}
