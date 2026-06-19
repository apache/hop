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

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/** vCard properties supported for stream field mapping. */
public enum VCardPropertyType implements IEnumHasCodeAndDescription {
  FN("fn", "VCardPropertyType.FN"),
  N_FAMILY("n_family", "VCardPropertyType.NFamily"),
  N_GIVEN("n_given", "VCardPropertyType.NGiven"),
  N_ADDITIONAL("n_additional", "VCardPropertyType.NAdditional"),
  N_PREFIX("n_prefix", "VCardPropertyType.NPrefix"),
  N_SUFFIX("n_suffix", "VCardPropertyType.NSuffix"),
  UID("uid", "VCardPropertyType.UID"),
  NICKNAME("nickname", "VCardPropertyType.Nickname"),
  ORG("org", "VCardPropertyType.Org"),
  TITLE("title", "VCardPropertyType.Title"),
  NOTE("note", "VCardPropertyType.Note"),
  URL("url", "VCardPropertyType.Url"),
  CATEGORIES("categories", "VCardPropertyType.Categories"),
  EMAIL("email", "VCardPropertyType.Email"),
  EMAIL_TYPE("email_type", "VCardPropertyType.EmailType"),
  TEL("tel", "VCardPropertyType.Tel"),
  TEL_TYPE("tel_type", "VCardPropertyType.TelType"),
  REV("rev", "VCardPropertyType.Rev"),
  PRODID("prodid", "VCardPropertyType.ProdId");

  private static final Class<?> PKG = VCardPropertyType.class;

  private final String code;
  private final String descriptionKey;

  VCardPropertyType(String code, String descriptionKey) {
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
    return IEnumHasCodeAndDescription.getDescriptions(VCardPropertyType.class);
  }

  public static VCardPropertyType lookupDescription(String description) {
    return IEnumHasCodeAndDescription.lookupDescription(
        VCardPropertyType.class, description, VCardPropertyType.FN);
  }

  public boolean supportsParameterTypes() {
    return this == EMAIL || this == EMAIL_TYPE || this == TEL || this == TEL_TYPE;
  }
}
