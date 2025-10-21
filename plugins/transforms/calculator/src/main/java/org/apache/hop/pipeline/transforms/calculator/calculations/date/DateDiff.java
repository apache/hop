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

package org.apache.hop.pipeline.transforms.calculator.calculations.date;

import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.ValueDataUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transforms.calculator.CalculationInput;
import org.apache.hop.pipeline.transforms.calculator.CalculationOutput;
import org.apache.hop.pipeline.transforms.calculator.ICalculation;

public class DateDiff implements ICalculation {
  private final String code;
  private final String resultType;
  private final String description;

  public DateDiff(INTERVAL interval) {
    this.code = interval.name();
    this.resultType = interval.resultType;
    this.description = BaseMessages.getString(PKG, interval.messageKey);
  }

  @Override
  public String getCode() {
    return code;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public int getDefaultResultType() {
    return IValueMeta.TYPE_INTEGER;
  }

  @Override
  public CalculationOutput calculate(CalculationInput in) throws HopValueException {
    return new CalculationOutput(
        getDefaultResultType(),
        ValueDataUtil.DateDiff(in.metaA, in.dataA, in.metaB, in.dataB, resultType));
  }

  public enum INTERVAL {
    DATE_DIFF("d", "CalculatorMetaFunction.CalcFunctions.DateDiff"),
    DATE_DIFF_HR("h", "CalculatorMetaFunction.CalcFunctions.DateDiffHr"),
    DATE_DIFF_MN("mn", "CalculatorMetaFunction.CalcFunctions.DateDiffMn"),
    DATE_DIFF_SEC("s", "CalculatorMetaFunction.CalcFunctions.DateDiffSec"),
    DATE_DIFF_MSEC("ms", "CalculatorMetaFunction.CalcFunctions.DateDiffMsec");
    final String resultType, messageKey;

    INTERVAL(String resultType, String messageKey) {
      this.resultType = resultType;
      this.messageKey = messageKey;
    }
  }
}
