/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.calculator.calculations.math;

import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.ValueDataUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transforms.calculator.CalculationInput;
import org.apache.hop.pipeline.transforms.calculator.CalculationOutput;
import org.apache.hop.pipeline.transforms.calculator.ICalculation;

public class Remainder implements ICalculation {
  private static final String description =
      BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.Remainder");

  @Override
  public String getCode() {
    return "REMAINDER";
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public int getDefaultResultType() {
    return IValueMeta.TYPE_NUMBER;
  }

  @Override
  public CalculationOutput calculate(CalculationInput in) throws HopValueException {
    if (in.targetMeta.getType() != in.metaA.getType()
        || in.targetMeta.getType() != in.metaB.getType()) {

      Object dataA = in.targetMeta.convertData(in.metaA, in.dataA);
      IValueMeta metaA = in.targetMeta.clone();
      Object dataB = in.targetMeta.convertData(in.metaB, in.dataB);
      IValueMeta metaB = in.targetMeta.clone();

      return new CalculationOutput(
          in.targetMeta.getType(), ValueDataUtil.remainder(metaA, dataA, metaB, dataB));
    }

    return new CalculationOutput(
        in.targetMeta.getType(), ValueDataUtil.remainder(in.metaA, in.dataA, in.metaB, in.dataB));
  }
}
