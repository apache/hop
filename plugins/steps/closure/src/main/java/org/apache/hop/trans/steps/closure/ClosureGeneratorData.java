/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.trans.steps.closure;

import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.trans.step.BaseStepData;
import org.apache.hop.trans.step.StepDataInterface;

import java.util.Map;

/**
 * @author Matt
 * @since 18-Sep-2007
 */
public class ClosureGeneratorData extends BaseStepData implements StepDataInterface {
  public RowMetaInterface outputRowMeta;
  public int parentIndex;
  public int childIndex;
  public boolean reading;
  public ValueMetaInterface parentValueMeta;
  public ValueMetaInterface childValueMeta;
  public Map<Object, Object> map;
  public Map<Object, Long> parents;
  public Object topLevel;

  public ClosureGeneratorData() {
    super();
  }
}
