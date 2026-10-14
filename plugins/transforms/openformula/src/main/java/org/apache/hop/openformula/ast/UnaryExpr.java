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

package org.apache.hop.openformula.ast;

import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.hop.openformula.EvaluationException;
import org.apache.hop.openformula.FormulaContext;
import org.apache.hop.openformula.typing.DataType;
import org.apache.hop.openformula.typing.TypedValue;
import org.apache.hop.openformula.util.NumberUtil;

public record UnaryExpr(Operator op, Expression operand) implements Expression {
  public enum Operator {
    PLUS,
    MINUS,
    PERCENT
  }

  @Override
  public TypedValue evaluate(FormulaContext context) throws EvaluationException {
    TypedValue child = operand.evaluate(context);
    if (child.isError()) {
      return child;
    }

    BigDecimal num = context.getTypeRegistry().convertToNumber(child.type(), child.value());
    return switch (op) {
      case PLUS -> TypedValue.ofNumber(num);
      case MINUS -> TypedValue.ofNumber(num.negate());
      case PERCENT ->
          TypedValue.ofNumber(
              NumberUtil.normalize(
                  num.divide(
                      BigDecimal.valueOf(100), NumberUtil.GLOBAL_SCALE, RoundingMode.HALF_UP)));
    };
  }

  @Override
  public DataType getType(FormulaContext context) {
    return DataType.NUMBER;
  }
}
