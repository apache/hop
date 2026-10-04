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

package org.apache.hop.pipeline.transforms.maskfields;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.i18n.BaseMessages;

/** Which field types a masking pattern is allowed to write. */
public final class MaskingRules {

  private static final Class<?> PKG = MaskingRules.class;

  private MaskingRules() {}

  /**
   * @return a message when the pattern cannot be applied to the field, or null when it can
   */
  public static String incompatibility(IValueMeta valueMeta, MaskingPattern pattern) {
    if (valueMeta == null || pattern == null || pattern.getValueSource() == null) {
      return BaseMessages.getString(PKG, "MaskingRules.Unsupported");
    }
    int type = valueMeta.getType();
    MaskingValueSource source = pattern.getValueSource();
    if (source == MaskingValueSource.SET_NULL) {
      return null;
    }
    if (source == MaskingValueSource.SET_EMPTY) {
      return type == IValueMeta.TYPE_STRING
          ? null
          : BaseMessages.getString(PKG, "MaskingRules.SetEmpty");
    }
    if (type == IValueMeta.TYPE_BOOLEAN || type == IValueMeta.TYPE_BINARY) {
      return BaseMessages.getString(PKG, "MaskingRules.OnlyNull");
    }
    if (type == IValueMeta.TYPE_DATE || type == IValueMeta.TYPE_TIMESTAMP) {
      return BaseMessages.getString(PKG, "MaskingRules.NoSyntheticDate");
    }
    if (type == IValueMeta.TYPE_STRING) {
      return null;
    }
    if (isNumber(type)) {
      if (pattern.getToken() == MaskingToken.UUID) {
        return BaseMessages.getString(PKG, "MaskingRules.Uuid");
      }
      if (StringUtils.isNotEmpty(pattern.getPrefix())
          || StringUtils.isNotEmpty(pattern.getSuffix())) {
        return BaseMessages.getString(PKG, "MaskingRules.Prefix");
      }
      return null;
    }
    return BaseMessages.getString(PKG, "MaskingRules.Unsupported");
  }

  public static boolean isNumber(int type) {
    return type == IValueMeta.TYPE_INTEGER
        || type == IValueMeta.TYPE_NUMBER
        || type == IValueMeta.TYPE_BIGNUMBER;
  }
}
