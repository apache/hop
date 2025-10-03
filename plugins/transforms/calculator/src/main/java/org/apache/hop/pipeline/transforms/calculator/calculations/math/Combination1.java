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
import org.apache.hop.pipeline.transforms.calculator.CalculationInput;
import org.apache.hop.pipeline.transforms.calculator.CalculationOutput;
import org.apache.hop.pipeline.transforms.calculator.ICalculation;

public class Combination1 implements ICalculation {
  @Override
  public String getCode() {
    return "COMBINATION_1";
  }

  @Override
  public String getDescription() {
    return "A + B * C";
  }

  @Override
  public int getDefaultResultType() {
    return IValueMeta.TYPE_NUMBER;
  }

  @Override
  public CalculationOutput calculate(CalculationInput in) throws HopValueException {
    return new CalculationOutput(
        ICalculation.getResultType(in.metaA),
        ValueDataUtil.combination1(in.metaA, in.dataA, in.metaB, in.dataB, in.metaC, in.dataC));
  }
}
