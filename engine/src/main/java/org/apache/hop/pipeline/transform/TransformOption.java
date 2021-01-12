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
 */

package org.apache.hop.pipeline.transform;

import org.apache.commons.lang.BooleanUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;

import java.util.List;

public class TransformOption {
  private static final Class<?> PKG = TransformOption.class; // For Translator

  private final String key;
  private final String text;
  private String value;

  public TransformOption( String key, String text, String value ) {
    this.key = key;
    this.text = text;
    this.value = value;
  }

  public String getKey() {
    return key;
  }

  public String getText() {
    return text;
  }

  public String getValue() {
    return value;
  }

  public void setValue( String value ) {
    this.value = value;
  }

  public static void checkInteger( List<ICheckResult> remarks, TransformMeta transformMeta, IVariables variables,
                                   String identifier, String value ) {
    try {
      if ( !StringUtil.isEmpty( variables.resolve( value ) ) ) {
        Integer.parseInt( variables.resolve( value ) );
      }
    } catch ( NumberFormatException e ) {
      remarks.add( new CheckResult(
        ICheckResult.TYPE_RESULT_ERROR,
        BaseMessages.getString( PKG, "TransformOption.CheckResult.NotAInteger", identifier ),
        transformMeta ) );
    }
  }

  public static void checkLong( List<ICheckResult> remarks, TransformMeta transformMeta, IVariables variables,
                                String identifier, String value ) {
    try {
      if ( !StringUtil.isEmpty( variables.resolve( value ) ) ) {
        Long.parseLong( variables.resolve( value ) );
      }
    } catch ( NumberFormatException e ) {
      remarks.add( new CheckResult(
        ICheckResult.TYPE_RESULT_ERROR,
        BaseMessages.getString( PKG, "TransformOption.CheckResult.NotAInteger", identifier ),
        transformMeta ) );
    }
  }

  public static void checkBoolean( List<ICheckResult> remarks, TransformMeta transformMeta, IVariables variables,
                                   String identifier, String value ) {
    if ( !StringUtil.isEmpty( variables.resolve( value ) ) && null == BooleanUtils
      .toBooleanObject( variables.resolve( value ) ) ) {
      remarks.add( new CheckResult(
        ICheckResult.TYPE_RESULT_ERROR,
        BaseMessages.getString( PKG, "TransformOption.CheckResult.NotABoolean", identifier ),
        transformMeta ) );
    }
  }
}
