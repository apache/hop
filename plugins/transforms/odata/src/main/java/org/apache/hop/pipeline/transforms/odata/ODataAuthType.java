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

package org.apache.hop.pipeline.transforms.odata;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

public enum ODataAuthType implements IEnumHasCodeAndDescription {
  NONE("NONE", "ODataAuthType.None.Description"),
  BASIC("BASIC", "ODataAuthType.Basic.Description"),
  BEARER("BEARER", "ODataAuthType.Bearer.Description");

  private static final Class<?> PKG = ODataAuthType.class;

  private final String code;
  private final String descriptionKey;

  ODataAuthType(String code, String descriptionKey) {
    this.code = code;
    this.descriptionKey = descriptionKey;
  }

  @Override
  public String getCode() {
    return code;
  }

  @Override
  public String getDescription() {
    return BaseMessages.getString(PKG, descriptionKey);
  }

  public static String[] getDescriptions() {
    return IEnumHasCodeAndDescription.getDescriptions(ODataAuthType.class);
  }

  public static ODataAuthType lookupDescription(String description) {
    return IEnumHasCodeAndDescription.lookupDescription(
        ODataAuthType.class, description, ODataAuthType.NONE);
  }

  public static ODataAuthType lookupCode(String code) {
    for (ODataAuthType val : values()) {
      if (val.getCode().equalsIgnoreCase(code)) {
        return val;
      }
    }
    return NONE;
  }
}
