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

import java.util.List;
import org.apache.hop.openformula.EvaluationException;
import org.apache.hop.openformula.FormulaContext;
import org.apache.hop.openformula.error.FormulaErrorValue;
import org.apache.hop.openformula.function.Function;
import org.apache.hop.openformula.function.ParameterCallback;
import org.apache.hop.openformula.typing.DataType;
import org.apache.hop.openformula.typing.TypedValue;

public record FunctionCallExpr(String functionName, List<Expression> arguments)
    implements Expression {
  @Override
  public TypedValue evaluate(FormulaContext context) throws EvaluationException {
    Function fn = context.getFunctionRegistry().findFunction(functionName);
    if (fn == null) {
      throw new EvaluationException(FormulaErrorValue.ERROR_INVALID_FUNCTION_VALUE);
    }

    ParameterCallback callback =
        new ParameterCallback() {
          private final TypedValue[] cache = new TypedValue[arguments.size()];

          @Override
          public int getParameterCount() {
            return arguments.size();
          }

          private TypedValue eval(int pos) throws EvaluationException {
            if (pos < 0 || pos >= arguments.size()) {
              throw new EvaluationException(FormulaErrorValue.ERROR_ARGUMENTS_VALUE);
            }
            if (cache[pos] == null) {
              Expression expr = arguments.get(pos);
              if (expr == null) {
                cache[pos] = TypedValue.NULL;
              } else {
                try {
                  cache[pos] = expr.evaluate(context);
                } catch (EvaluationException e) {
                  cache[pos] = TypedValue.ofError(e.getErrorValue());
                }
              }
            }
            return cache[pos];
          }

          @Override
          public Object getValue(int position) throws EvaluationException {
            TypedValue tv = eval(position);
            return tv.value();
          }

          @Override
          public DataType getType(int position) throws EvaluationException {
            TypedValue tv = eval(position);
            return tv.type();
          }

          @Override
          public Expression getRaw(int position) {
            if (position < 0 || position >= arguments.size()) {
              return null;
            }
            return arguments.get(position);
          }
        };

    return fn.evaluate(context, callback);
  }

  @Override
  public DataType getType(FormulaContext context) throws EvaluationException {
    Function fn = context.getFunctionRegistry().findFunction(functionName);
    return fn != null ? fn.getReturnType() : DataType.ANY;
  }
}
