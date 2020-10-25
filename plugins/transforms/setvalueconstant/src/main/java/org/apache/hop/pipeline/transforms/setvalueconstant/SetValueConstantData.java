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

package org.apache.hop.pipeline.transforms.setvalueconstant;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

/**
 * @author Samatar
 * @since 16-06-2008
 */
public class SetValueConstantData extends BaseTransformData implements ITransformData {

  private IRowMeta outputRowMeta;
  private IRowMeta convertRowMeta;

  private String[] realReplaceByValues;
  private int[] fieldnrs;
  private int fieldnr;

  SetValueConstantData() {
    super();
  }

  IRowMeta getOutputRowMeta() {
    return outputRowMeta;
  }

  void setOutputRowMeta( IRowMeta outputRowMeta ) {
    this.outputRowMeta = outputRowMeta;
  }

  IRowMeta getConvertRowMeta() {
    return convertRowMeta;
  }

  void setConvertRowMeta( IRowMeta convertRowMeta ) {
    this.convertRowMeta = convertRowMeta;
  }

  String[] getRealReplaceByValues() {
    return realReplaceByValues;
  }

  void setRealReplaceByValues( String[] realReplaceByValues ) {
    this.realReplaceByValues = realReplaceByValues;
  }

  int[] getFieldnrs() {
    return fieldnrs;
  }

  void setFieldnrs( int[] fieldnrs ) {
    this.fieldnrs = fieldnrs;
  }

  int getFieldnr() {
    return fieldnr;
  }

  void setFieldnr( int fieldnr ) {
    this.fieldnr = fieldnr;
  }
}
