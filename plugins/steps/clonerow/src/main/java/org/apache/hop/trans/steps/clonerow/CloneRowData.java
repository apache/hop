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

package org.apache.hop.trans.steps.clonerow;

import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.trans.step.BaseStepData;
import org.apache.hop.trans.step.StepDataInterface;

/**
 * @author Samatar
 * @since 27-06-2008
 */
public class CloneRowData extends BaseStepData implements StepDataInterface {

  protected long nrclones;
  protected RowMetaInterface outputRowMeta;
  protected int indexOfNrCloneField;
  protected boolean addInfosToRow;
  protected int NrPrevFields;

  public CloneRowData() {
    super();
    nrclones = 0;
    indexOfNrCloneField = -1;
    addInfosToRow = false;
    NrPrevFields = 0;
  }

}
