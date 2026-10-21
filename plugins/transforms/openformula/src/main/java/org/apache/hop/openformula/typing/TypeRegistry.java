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

package org.apache.hop.openformula.typing;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import org.apache.hop.openformula.EvaluationException;

/** Registry and conversion service for formula types. */
public interface TypeRegistry {
  ExtendedComparator getComparator(DataType type1, DataType type2);

  String convertToText(DataType type, Object value) throws EvaluationException;

  BigDecimal convertToNumber(DataType type, Object value) throws EvaluationException;

  Boolean convertToLogical(DataType type, Object value) throws EvaluationException;

  Date convertToDate(DataType type, Object value) throws EvaluationException;

  List<Object> convertToSequence(DataType type, Object value) throws EvaluationException;

  List<BigDecimal> convertToNumberSequence(
      DataType type, Object value, boolean includeLogicalAsNumbers) throws EvaluationException;

  DataType guessType(Object value);
}
