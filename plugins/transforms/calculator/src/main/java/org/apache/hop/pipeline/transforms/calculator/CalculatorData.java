/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.calculator;

import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Matt
 * @since 8-sep-2005
 */
public class CalculatorData extends BaseTransformData implements ITransformData {
  private IRowMeta outputRowMeta;
  private IRowMeta calcRowMeta;

  private Calculator.FieldIndexes[] fieldIndexes;

  private int[] tempIndexes;

  private final Map<Integer, IValueMeta> resultMetaMapping;

  public CalculatorData() {
    super();
    resultMetaMapping = new HashMap<>();
  }

  public IRowMeta getOutputRowMeta() {
    return outputRowMeta;
  }

  public void setOutputRowMeta( IRowMeta outputRowMeta ) {
    this.outputRowMeta = outputRowMeta;
  }

  public IRowMeta getCalcRowMeta() {
    return calcRowMeta;
  }

  public void setCalcRowMeta( IRowMeta calcRowMeta ) {
    this.calcRowMeta = calcRowMeta;
  }

  public Calculator.FieldIndexes[] getFieldIndexes() {
    return fieldIndexes;
  }

  public void setFieldIndexes( Calculator.FieldIndexes[] fieldIndexes ) {
    this.fieldIndexes = fieldIndexes;
  }

  public int[] getTempIndexes() {
    return tempIndexes;
  }

  public void setTempIndexes( int[] tempIndexes ) {
    this.tempIndexes = tempIndexes;
  }

  public IValueMeta getValueMetaFor( int resultType, String name ) throws HopPluginException {
    // don't need any synchronization as data instance belongs only to one transform instance
    IValueMeta meta = resultMetaMapping.get( resultType );
    if ( meta == null ) {
      meta = ValueMetaFactory.createValueMeta( name, resultType );
      resultMetaMapping.put( resultType, meta );
    }
    return meta;
  }

  public void clearValuesMetaMapping() {
    resultMetaMapping.clear();
  }
}
