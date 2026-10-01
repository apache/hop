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
 *
 */

package org.apache.hop.pipeline.transforms.validator;

import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

@SuppressWarnings("java:S1104")
public class ValidatorData extends BaseTransformData implements ITransformData {

  public int[] fieldIndexes;

  public IValueMeta[] constantsMeta;

  /** The data type id of every validation, resolved once so we don't hit the registry per row. */
  public int[] dataTypes;

  public String[] minimumValueAsString;
  public String[] maximumValueAsString;
  public int[] fieldsMinimumLengthAsInt;
  public int[] fieldsMaximumLengthAsInt;
  public Object[][] listValues;

  /**
   * Hash based view on {@link #listValues}, used to look up allowed values in constant time. Only
   * filled in for data types where equality and comparison agree, and only used when the incoming
   * value meta compares with plain equality as well. Null means: fall back to the linear scan.
   */
  public Set<Object>[] listValuesSet;

  /** True when at least one validation rule of this field needs the value as a string. */
  public boolean[] needsStringValue;

  /** True when the field has any rule that is checked on a non-null value. */
  public boolean[] hasValueChecks;

  public Pattern[] patternExpected;

  public Pattern[] patternDisallowed;

  /**
   * Matchers reused for every row instead of allocating a fresh one per row. A matcher is stateful,
   * but this data object belongs to a single transform copy so it is never shared between threads.
   */
  public Matcher[] matcherExpected;

  public Matcher[] matcherDisallowed;

  /** Reused across rows so a clean row doesn't allocate a list to report nothing in. */
  public List<HopValidatorException> exceptions;

  public String[] errorCode;
  public String[] errorDescription;
  public String[] conversionMask;
  public String[] decimalSymbol;
  public String[] groupingSymbol;
  public String[] maximumLength;
  public String[] minimumLength;
  public Object[] maximumValue;
  public Object[] minimumValue;
  public String[] startString;
  public String[] endString;
  public String[] startStringNotAllowed;
  public String[] endStringNotAllowed;
  public String[] regularExpression;
  public String[] regularExpressionNotAllowed;
  public IRowMeta inputRowMeta;

  public ValidatorData() {
    super();
  }
}
