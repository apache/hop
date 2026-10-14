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
import java.util.List;
import java.util.Locale;
import org.apache.hop.openformula.typing.DataType;

public class FunctionDescription implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String canonicalName;
  private final FunctionCategory category;
  private final String description;
  private final DataType returnType;
  private final int parameterCount;
  private final boolean infiniteParameterCount;
  private final List<String> parameterNames;
  private final List<String> parameterDescriptions;

  public FunctionDescription(
      String canonicalName,
      FunctionCategory category,
      String description,
      DataType returnType,
      int parameterCount,
      boolean infiniteParameterCount,
      List<String> parameterNames,
      List<String> parameterDescriptions) {
    this.canonicalName = canonicalName;
    this.category = category;
    this.description = description;
    this.returnType = returnType;
    this.parameterCount = parameterCount;
    this.infiniteParameterCount = infiniteParameterCount;
    this.parameterNames = parameterNames != null ? parameterNames : List.of();
    this.parameterDescriptions = parameterDescriptions != null ? parameterDescriptions : List.of();
  }

  public String getCanonicalName() {
    return canonicalName;
  }

  public FunctionCategory getCategory() {
    return category;
  }

  public String getDisplayName(Locale locale) {
    return canonicalName;
  }

  public String getDescription(Locale locale) {
    return description;
  }

  public DataType getValueType() {
    return returnType;
  }

  public int getParameterCount() {
    return parameterCount;
  }

  public boolean isInfiniteParameterCount() {
    return infiniteParameterCount;
  }

  public String getParameterDisplayName(int position, Locale locale) {
    if (position >= 0 && position < parameterNames.size()) {
      return parameterNames.get(position);
    }
    return "arg" + (position + 1);
  }

  public String getParameterDescription(int position, Locale locale) {
    if (position >= 0 && position < parameterDescriptions.size()) {
      return parameterDescriptions.get(position);
    }
    return "";
  }
}
