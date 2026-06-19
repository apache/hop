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

import ezvcard.VCard;
import ezvcard.property.Email;
import ezvcard.property.FormattedName;
import ezvcard.property.ProductId;
import ezvcard.property.StructuredName;
import ezvcard.property.Telephone;
import ezvcard.property.Uid;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;

/** Discovers {@link VCardFieldMapping} entries from parsed vCard files. */
public final class VCardFieldDiscovery {

  private static final int DEFAULT_MAX_FILES = 10;

  private VCardFieldDiscovery() {}

  public static List<VCardFieldMapping> discoverFromFiles(FileInputList files, String encoding)
      throws IOException {
    return discoverFromFiles(files, charset(encoding), DEFAULT_MAX_FILES);
  }

  public static List<VCardFieldMapping> discoverFromFiles(
      FileInputList files, Charset charset, int maxFiles) throws IOException {
    if (files == null || files.nrOfFiles() == 0) {
      return List.of();
    }
    List<VCard> cards = new ArrayList<>();
    int limit = maxFiles <= 0 ? files.nrOfFiles() : Math.min(files.nrOfFiles(), maxFiles);
    for (int i = 0; i < limit; i++) {
      FileObject file = files.getFile(i);
      try (InputStream inputStream = HopVfs.getInputStream(file)) {
        cards.addAll(VCardMapper.parseAll(IOUtils.toString(inputStream, charset)));
      }
    }
    return discoverFromCards(cards);
  }

  public static List<VCardFieldMapping> discoverFromCards(List<VCard> cards) {
    Map<String, VCardFieldMapping> unique = new LinkedHashMap<>();
    if (cards != null) {
      for (VCard card : cards) {
        for (VCardFieldMapping mapping : discoverFromCard(card)) {
          unique.putIfAbsent(mappingKey(mapping), mapping);
        }
      }
    }
    return new ArrayList<>(unique.values());
  }

  public static List<VCardFieldMapping> discoverFromCard(VCard card) {
    List<VCardFieldMapping> mappings = new ArrayList<>();
    if (card == null) {
      return mappings;
    }

    if (hasText(card.getFormattedName())) {
      mappings.add(mapping(VCardPropertyType.FN));
    }

    StructuredName name = card.getStructuredName();
    if (name != null) {
      if (!Utils.isEmpty(name.getFamily())) {
        mappings.add(mapping(VCardPropertyType.N_FAMILY));
      }
      if (!Utils.isEmpty(name.getGiven())) {
        mappings.add(mapping(VCardPropertyType.N_GIVEN));
      }
      if (!Utils.isEmpty(name.getAdditionalNames())) {
        mappings.add(mapping(VCardPropertyType.N_ADDITIONAL));
      }
      if (!Utils.isEmpty(name.getPrefixes())) {
        mappings.add(mapping(VCardPropertyType.N_PREFIX));
      }
      if (!Utils.isEmpty(name.getSuffixes())) {
        mappings.add(mapping(VCardPropertyType.N_SUFFIX));
      }
    }

    if (hasText(card.getUid())) {
      mappings.add(mapping(VCardPropertyType.UID));
    }
    if (card.getNickname() != null && !card.getNickname().getValues().isEmpty()) {
      mappings.add(mapping(VCardPropertyType.NICKNAME));
    }
    if (card.getOrganization() != null && !card.getOrganization().getValues().isEmpty()) {
      mappings.add(mapping(VCardPropertyType.ORG));
    }
    if (!card.getTitles().isEmpty()) {
      mappings.add(mapping(VCardPropertyType.TITLE));
    }
    if (!card.getNotes().isEmpty()) {
      mappings.add(mapping(VCardPropertyType.NOTE));
    }
    if (!card.getUrls().isEmpty()) {
      mappings.add(mapping(VCardPropertyType.URL));
    }
    if (card.getCategories() != null && !card.getCategories().getValues().isEmpty()) {
      mappings.add(mapping(VCardPropertyType.CATEGORIES));
    }
    if (card.getRevision() != null && card.getRevision().getValue() != null) {
      mappings.add(mapping(VCardPropertyType.REV));
    }
    if (hasText(card.getProductId())) {
      mappings.add(mapping(VCardPropertyType.PRODID));
    }

    for (Email email : card.getEmails()) {
      String parameterTypes = typeParameters(email.getTypes());
      mappings.add(mapping(VCardPropertyType.EMAIL, parameterTypes));
      mappings.add(mapping(VCardPropertyType.EMAIL_TYPE, parameterTypes));
    }
    for (Telephone tel : card.getTelephoneNumbers()) {
      String parameterTypes = typeParameters(tel.getTypes());
      mappings.add(mapping(VCardPropertyType.TEL, parameterTypes));
      mappings.add(mapping(VCardPropertyType.TEL_TYPE, parameterTypes));
    }

    return mappings;
  }

  private static VCardFieldMapping mapping(VCardPropertyType property) {
    return mapping(property, null);
  }

  private static VCardFieldMapping mapping(VCardPropertyType property, String parameterTypes) {
    return new VCardFieldMapping(
        property, defaultHopFieldName(property, parameterTypes), parameterTypes);
  }

  static String defaultHopFieldName(VCardPropertyType property, String parameterTypes) {
    VCardPropertyType valueProperty = property;
    if (property == VCardPropertyType.EMAIL_TYPE) {
      valueProperty = VCardPropertyType.EMAIL;
    } else if (property == VCardPropertyType.TEL_TYPE) {
      valueProperty = VCardPropertyType.TEL;
    }
    String valueFieldName = valueProperty.getCode();
    if (!Utils.isEmpty(parameterTypes)) {
      String suffix = parameterTypes.toLowerCase(Locale.ROOT).replace(',', '_').replace(' ', '_');
      valueFieldName = valueFieldName + "_" + suffix;
    }
    if (property == VCardPropertyType.EMAIL_TYPE || property == VCardPropertyType.TEL_TYPE) {
      return valueFieldName + "_type";
    }
    return valueFieldName;
  }

  private static String mappingKey(VCardFieldMapping mapping) {
    return mapping.getProperty().getCode()
        + '|'
        + (mapping.getParameterTypes() == null ? "" : mapping.getParameterTypes());
  }

  private static String typeParameters(List<?> types) {
    return VCardMapper.formatTypeParameters(types);
  }

  private static boolean hasText(FormattedName name) {
    return name != null && !Utils.isEmpty(name.getValue());
  }

  private static boolean hasText(Uid uid) {
    return uid != null && !Utils.isEmpty(uid.getValue());
  }

  private static boolean hasText(ProductId productId) {
    return productId != null && !Utils.isEmpty(productId.getValue());
  }

  private static Charset charset(String encoding) {
    if (Utils.isEmpty(encoding)) {
      return StandardCharsets.UTF_8;
    }
    return Charset.forName(encoding);
  }
}
