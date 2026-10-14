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
import java.util.Date;
import org.apache.hop.openformula.EvaluationException;
import org.apache.hop.openformula.FormulaContext;
import org.apache.hop.openformula.typing.ArrayCallback;
import org.apache.hop.openformula.typing.DataType;
import org.apache.hop.openformula.typing.ExtendedComparator;
import org.apache.hop.openformula.typing.TypeRegistry;
import org.apache.hop.openformula.typing.TypedValue;
import org.apache.hop.openformula.util.DateUtil;
import org.apache.hop.openformula.util.NumberUtil;

public record BinaryExpr(Operator op, Expression left, Expression right) implements Expression {
  public enum Operator {
    ADD,
    SUBTRACT,
    MULTIPLY,
    DIVIDE,
    POWER,
    CONCAT,
    EQUAL,
    NOT_EQUAL,
    LESS,
    LESS_EQUAL,
    GREATER,
    GREATER_EQUAL
  }

  @Override
  public TypedValue evaluate(FormulaContext context) throws EvaluationException {
    TypedValue l = left.evaluate(context);
    if (l.isError()) return l;
    TypedValue r = right.evaluate(context);
    if (r.isError()) return r;

    // Unwrap 1x1 arrays if present
    l = unwrapArray(l);
    r = unwrapArray(r);

    TypeRegistry registry = context.getTypeRegistry();

    return switch (op) {
      case ADD -> evaluateAdd(l, r, context);
      case SUBTRACT -> evaluateSubtract(l, r, context);
      case MULTIPLY -> evaluateMultiply(l, r, registry);
      case DIVIDE -> evaluateDivide(l, r, registry);
      case POWER -> evaluatePower(l, r, registry);
      case CONCAT -> evaluateConcat(l, r, registry);
      case EQUAL -> evaluateEqual(l, r, registry);
      case NOT_EQUAL -> evaluateNotEqual(l, r, registry);
      case LESS -> evaluateLess(l, r, registry);
      case LESS_EQUAL -> evaluateLessEqual(l, r, registry);
      case GREATER -> evaluateGreater(l, r, registry);
      case GREATER_EQUAL -> evaluateGreaterEqual(l, r, registry);
    };
  }

  private static TypedValue unwrapArray(TypedValue tv) throws EvaluationException {
    if (tv.value() instanceof ArrayCallback ac
        && ac.getRowCount() == 1
        && ac.getColumnCount() == 1) {
      return TypedValue.of(ac.getType(0, 0), ac.getValue(0, 0));
    }
    return tv;
  }

  private TypedValue evaluateAdd(TypedValue l, TypedValue r, FormulaContext context)
      throws EvaluationException {
    TypeRegistry registry = context.getTypeRegistry();
    if (l.value() instanceof Date ld && !(r.value() instanceof Date)) {
      BigDecimal days = registry.convertToNumber(r.type(), r.value());
      long millis =
          days.multiply(BigDecimal.valueOf(DateUtil.MILLIS_PER_DAY))
              .setScale(0, RoundingMode.HALF_UP)
              .longValue();
      return TypedValue.ofDate(new Date(ld.getTime() + millis));
    }
    if (r.value() instanceof Date rd && !(l.value() instanceof Date)) {
      BigDecimal days = registry.convertToNumber(l.type(), l.value());
      long millis =
          days.multiply(BigDecimal.valueOf(DateUtil.MILLIS_PER_DAY))
              .setScale(0, RoundingMode.HALF_UP)
              .longValue();
      return TypedValue.ofDate(new Date(rd.getTime() + millis));
    }
    BigDecimal n1 = registry.convertToNumber(l.type(), l.value());
    BigDecimal n2 = registry.convertToNumber(r.type(), r.value());
    return TypedValue.ofNumber(NumberUtil.add(n1, n2));
  }

  private TypedValue evaluateSubtract(TypedValue l, TypedValue r, FormulaContext context)
      throws EvaluationException {
    TypeRegistry registry = context.getTypeRegistry();
    if (l.value() instanceof Date ld && r.value() instanceof Date rd) {
      long diffMillis = ld.getTime() - rd.getTime();
      BigDecimal diffDays =
          BigDecimal.valueOf(diffMillis)
              .divide(
                  BigDecimal.valueOf(DateUtil.MILLIS_PER_DAY),
                  NumberUtil.GLOBAL_SCALE,
                  RoundingMode.HALF_UP);
      return TypedValue.ofNumber(NumberUtil.normalize(diffDays));
    }
    if (l.value() instanceof Date ld && !(r.value() instanceof Date)) {
      BigDecimal days = registry.convertToNumber(r.type(), r.value());
      long millis =
          days.multiply(BigDecimal.valueOf(DateUtil.MILLIS_PER_DAY))
              .setScale(0, RoundingMode.HALF_UP)
              .longValue();
      return TypedValue.ofDate(new Date(ld.getTime() - millis));
    }
    BigDecimal n1 = registry.convertToNumber(l.type(), l.value());
    BigDecimal n2 = registry.convertToNumber(r.type(), r.value());
    return TypedValue.ofNumber(NumberUtil.subtract(n1, n2));
  }

  private TypedValue evaluateMultiply(TypedValue l, TypedValue r, TypeRegistry registry)
      throws EvaluationException {
    BigDecimal n1 = registry.convertToNumber(l.type(), l.value());
    BigDecimal n2 = registry.convertToNumber(r.type(), r.value());
    return TypedValue.ofNumber(NumberUtil.multiply(n1, n2));
  }

  private TypedValue evaluateDivide(TypedValue l, TypedValue r, TypeRegistry registry)
      throws EvaluationException {
    BigDecimal n1 = registry.convertToNumber(l.type(), l.value());
    BigDecimal n2 = registry.convertToNumber(r.type(), r.value());
    return TypedValue.ofNumber(NumberUtil.divide(n1, n2));
  }

  private TypedValue evaluatePower(TypedValue l, TypedValue r, TypeRegistry registry)
      throws EvaluationException {
    BigDecimal n1 = registry.convertToNumber(l.type(), l.value());
    BigDecimal n2 = registry.convertToNumber(r.type(), r.value());
    return TypedValue.ofNumber(NumberUtil.power(n1, n2));
  }

  private TypedValue evaluateConcat(TypedValue l, TypedValue r, TypeRegistry registry)
      throws EvaluationException {
    String s1 = registry.convertToText(l.type(), l.value());
    String s2 = registry.convertToText(r.type(), r.value());
    return TypedValue.ofText(s1 + s2);
  }

  private TypedValue evaluateEqual(TypedValue l, TypedValue r, TypeRegistry registry)
      throws EvaluationException {
    ExtendedComparator cmp = registry.getComparator(l.type(), r.type());
    return TypedValue.ofLogical(cmp.isEqual(l.type(), l.value(), r.type(), r.value()));
  }

  private TypedValue evaluateNotEqual(TypedValue l, TypedValue r, TypeRegistry registry)
      throws EvaluationException {
    ExtendedComparator cmp = registry.getComparator(l.type(), r.type());
    return TypedValue.ofLogical(!cmp.isEqual(l.type(), l.value(), r.type(), r.value()));
  }

  private TypedValue evaluateLess(TypedValue l, TypedValue r, TypeRegistry registry)
      throws EvaluationException {
    ExtendedComparator cmp = registry.getComparator(l.type(), r.type());
    return TypedValue.ofLogical(cmp.compare(l.type(), l.value(), r.type(), r.value()) < 0);
  }

  private TypedValue evaluateLessEqual(TypedValue l, TypedValue r, TypeRegistry registry)
      throws EvaluationException {
    ExtendedComparator cmp = registry.getComparator(l.type(), r.type());
    return TypedValue.ofLogical(cmp.compare(l.type(), l.value(), r.type(), r.value()) <= 0);
  }

  private TypedValue evaluateGreater(TypedValue l, TypedValue r, TypeRegistry registry)
      throws EvaluationException {
    ExtendedComparator cmp = registry.getComparator(l.type(), r.type());
    return TypedValue.ofLogical(cmp.compare(l.type(), l.value(), r.type(), r.value()) > 0);
  }

  private TypedValue evaluateGreaterEqual(TypedValue l, TypedValue r, TypeRegistry registry)
      throws EvaluationException {
    ExtendedComparator cmp = registry.getComparator(l.type(), r.type());
    return TypedValue.ofLogical(cmp.compare(l.type(), l.value(), r.type(), r.value()) >= 0);
  }

  @Override
  public DataType getType(FormulaContext context) {
    return switch (op) {
      case ADD, SUBTRACT, MULTIPLY, DIVIDE, POWER -> DataType.NUMBER;
      case CONCAT -> DataType.TEXT;
      case EQUAL, NOT_EQUAL, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL -> DataType.LOGICAL;
    };
  }
}
