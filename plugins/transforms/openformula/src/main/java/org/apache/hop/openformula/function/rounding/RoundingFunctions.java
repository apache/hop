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

package org.apache.hop.openformula.function.rounding;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import org.apache.hop.openformula.EvaluationException;
import org.apache.hop.openformula.FormulaContext;
import org.apache.hop.openformula.error.FormulaErrorValue;
import org.apache.hop.openformula.function.DefaultFunctionRegistry;
import org.apache.hop.openformula.function.Function;
import org.apache.hop.openformula.function.FunctionCategory;
import org.apache.hop.openformula.function.FunctionDescription;
import org.apache.hop.openformula.function.ParameterCallback;
import org.apache.hop.openformula.typing.DataType;
import org.apache.hop.openformula.typing.TypedValue;
import org.apache.hop.openformula.util.NumberUtil;

public final class RoundingFunctions {
  private RoundingFunctions() {}

  public static void registerAll(DefaultFunctionRegistry registry) {
    registry.registerFunction(
        new IntFunction(),
        new FunctionDescription(
            "INT",
            FunctionCategory.ROUNDING,
            "Rounds a number down to the nearest integer",
            DataType.NUMBER,
            1,
            false,
            List.of("number"),
            List.of("The real number to round down")));
  }

  public static class IntFunction implements Function {
    @Override
    public String getCanonicalName() {
      return "INT";
    }

    @Override
    public DataType getReturnType() {
      return DataType.NUMBER;
    }

    @Override
    public TypedValue evaluate(FormulaContext context, ParameterCallback params)
        throws EvaluationException {
      if (params.getParameterCount() != 1)
        throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
      BigDecimal num =
          context.getTypeRegistry().convertToNumber(params.getType(0), params.getValue(0));
      num = NumberUtil.performTuneRounding(num);
      return TypedValue.ofNumber(num.setScale(0, RoundingMode.FLOOR));
    }
  }
}
