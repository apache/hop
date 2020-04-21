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

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Matt
 * @since 24-jan-2005
 */
public class SwitchCaseData extends BaseTransformData implements ITransformData {

  public IRowMeta outputRowMeta;
  public KeyToRowSetMap outputMap;
  public IValueMeta valueMeta;
  public final Set<IRowSet> nullRowSetSet = new HashSet<IRowSet>();
  public int fieldIndex;
  public IValueMeta inputValueMeta;
  // we expect only one default set for now
  public final Set<IRowSet> defaultRowSetSet = new HashSet<IRowSet>( 1, 1 );
  public IValueMeta stringValueMeta;

  public SwitchCaseData() {
    super();
  }
}
