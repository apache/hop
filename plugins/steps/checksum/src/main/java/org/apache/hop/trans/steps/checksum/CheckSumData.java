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

package org.apache.hop.trans.steps.checksum;

import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.trans.step.BaseStepData;
import org.apache.hop.trans.step.StepDataInterface;

import java.security.MessageDigest;

/**
 * Data class for the Cheksum class
 *
 * @author Samatar Hassan
 * @since 16-06-2008
 */
public class CheckSumData extends BaseStepData implements StepDataInterface {

  public int[] fieldnrs;

  public int fieldnr;

  public RowMetaInterface outputRowMeta;
  public int nrInfields;

  public MessageDigest digest;

  public CheckSumData() {
    super();
    this.nrInfields = 0;
  }

}
