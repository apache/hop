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

package org.apache.hop.pipeline.transforms.switchcase;

import org.apache.hop.core.injection.Injection;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Utility class that contains the case value, the target transform name and the resolved target transform
 *
 * @author matt
 */
public class SwitchCaseTarget implements Cloneable {
  /**
   * The value to switch over
   */
  @Injection( name = "CASE_VALUE" )
  public String caseValue;

  /**
   * The case target transform name (only used during serialization)
   */
  @Injection( name = "CASE_TARGET_TRANSFORM_NAME" )
  public String caseTargetTransformName;

  /**
   * The case target transform
   */
  public TransformMeta caseTargetTransform;

  public SwitchCaseTarget() {
  }

  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

}
