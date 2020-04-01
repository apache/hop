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

package org.apache.hop.pipeline.steps.regexeval;

import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.pipeline.step.BaseStepData;
import org.apache.hop.pipeline.step.StepDataInterface;

import java.util.regex.Pattern;

/**
 * Runtime data for the RegexEval step.
 *
 * @author Samatar Hassan
 * @author Daniel Einspanjer
 * @since 27-03-2008
 */
public class RegexEvalData extends BaseStepData implements StepDataInterface {

  public RowMetaInterface outputRowMeta;
  public RowMetaInterface conversionRowMeta;
  public int indexOfFieldToEvaluate;
  public int indexOfResultField;

  public Pattern pattern;

  public int[] positions;

  public RegexEvalData() {
    super();

    indexOfFieldToEvaluate = -1;
    indexOfResultField = -1;
  }
}
