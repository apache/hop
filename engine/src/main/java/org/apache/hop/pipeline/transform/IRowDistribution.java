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

package org.apache.hop.pipeline.transform;

import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.gui.IPrimitiveGC.EImage;
import org.apache.hop.core.row.IRowMeta;

public interface IRowDistribution {

  /**
   * @return The row distribution code (plugin id)
   */
  String getCode();

  /**
   * @return The row distribution description (plugin description)
   */
  String getDescription();

  /**
   * Do the actual row distribution in the transform
   *
   * @param rowMeta       the meta-data of the row to distribute
   * @param row           the data of the row data to distribute
   * @param iTransform The transform to distribute the rows in
   * @throws HopTransformException
   */
  void distributeRow( IRowMeta rowMeta, Object[] row, ITransform iTransform ) throws HopTransformException;

  /**
   * Which mini-icon needs to be shown on the hop?
   *
   * @return the available code EImage or null if the standard icon needs to be used.
   */
  EImage getDistributionImage();
}
