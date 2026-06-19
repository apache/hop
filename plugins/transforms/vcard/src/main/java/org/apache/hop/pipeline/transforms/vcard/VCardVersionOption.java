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
package org.apache.hop.pipeline.transforms.vcard;

import ezvcard.VCardVersion;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

public enum VCardVersionOption implements IEnumHasCodeAndDescription {
  V3_0("3.0", "VCardVersionOption.V30", VCardVersion.V3_0),
  V4_0("4.0", "VCardVersionOption.V40", VCardVersion.V4_0);

  private static final Class<?> PKG = VCardVersionOption.class;

  private final String code;
  private final String descriptionKey;
  private final VCardVersion vCardVersion;

  VCardVersionOption(String code, String descriptionKey, VCardVersion vCardVersion) {
    this.code = code;
    this.descriptionKey = descriptionKey;
    this.vCardVersion = vCardVersion;
  }

  @Override
  public String getCode() {
    return code;
  }

  @Override
  public String getDescription() {
    return BaseMessages.getString(PKG, descriptionKey);
  }

  public VCardVersion getVCardVersion() {
    return vCardVersion;
  }

  public static String[] getDescriptions() {
    return IEnumHasCodeAndDescription.getDescriptions(VCardVersionOption.class);
  }

  public static VCardVersionOption lookupDescription(String description) {
    return IEnumHasCodeAndDescription.lookupDescription(
        VCardVersionOption.class, description, VCardVersionOption.V3_0);
  }
}
