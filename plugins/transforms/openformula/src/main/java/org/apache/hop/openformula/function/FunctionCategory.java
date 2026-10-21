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

package org.apache.hop.openformula.function;

import java.io.Serializable;
import java.util.Locale;

public class FunctionCategory implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final FunctionCategory DATABASE =
      new FunctionCategory("Database", "Database and Pattern Matching Functions");
  public static final FunctionCategory DATETIME =
      new FunctionCategory("Date & Time", "Date and Time Manipulation Functions");
  public static final FunctionCategory FINANCIAL =
      new FunctionCategory("Financial", "Financial Functions");
  public static final FunctionCategory INFORMATION =
      new FunctionCategory("Information", "Information and Inspection Functions");
  public static final FunctionCategory LOGICAL =
      new FunctionCategory("Logical", "Logical and Boolean Functions");
  public static final FunctionCategory MATH =
      new FunctionCategory("Math", "Mathematical and Trigonometric Functions");
  public static final FunctionCategory ROUNDING =
      new FunctionCategory("Rounding", "Number Rounding Functions");
  public static final FunctionCategory TEXT =
      new FunctionCategory("Text", "String and Text Processing Functions");
  public static final FunctionCategory USER_DEFINED =
      new FunctionCategory("User Defined", "User-Defined and Sequence Functions");

  private final String name;
  private final String description;

  public FunctionCategory(String name, String description) {
    this.name = name;
    this.description = description;
  }

  public String getName() {
    return name;
  }

  public String getDisplayName(Locale locale) {
    return name;
  }

  public String getDescription(Locale locale) {
    return description;
  }

  @Override
  public String toString() {
    return name;
  }
}
