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

import ezvcard.Ezvcard;
import ezvcard.VCard;
import ezvcard.VCardVersion;
import ezvcard.parameter.EmailType;
import ezvcard.parameter.TelephoneType;
import ezvcard.property.Email;
import ezvcard.property.FormattedName;
import ezvcard.property.Nickname;
import ezvcard.property.Note;
import ezvcard.property.Organization;
import ezvcard.property.ProductId;
import ezvcard.property.Revision;
import ezvcard.property.StructuredName;
import ezvcard.property.Telephone;
import ezvcard.property.Title;
import ezvcard.property.Uid;
import ezvcard.property.Url;
import ezvcard.util.TelUri;
import java.io.IOException;
import java.io.StringWriter;
import java.time.Instant;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;

/** Converts between Hop rows and ez-vcard {@link VCard} objects. */
public final class VCardMapper {

  private VCardMapper() {}

  public static List<VCard> parseAll(String vcardText) throws IOException {
    if (StringUtils.isBlank(vcardText)) {
      return List.of();
    }
    return Ezvcard.parse(vcardText).all();
  }

  public static String write(VCard card, VCardVersion version) throws IOException {
    StringWriter writer = new StringWriter();
    Ezvcard.write(card).version(version).go(writer);
    return writer.toString();
  }

  public static String extractValue(VCard card, VCardFieldMapping mapping) {
    if (card == null || mapping == null || mapping.getProperty() == null) {
      return null;
    }
    return switch (mapping.getProperty()) {
      case FN -> text(card.getFormattedName());
      case N_FAMILY -> familyName(card);
      case N_GIVEN -> givenName(card);
      case N_ADDITIONAL -> joinList(structuredName(card).getAdditionalNames());
      case N_PREFIX -> joinList(structuredName(card).getPrefixes());
      case N_SUFFIX -> joinList(structuredName(card).getSuffixes());
      case UID -> text(card.getUid());
      case NICKNAME -> firstNickname(card);
      case ORG -> organization(card);
      case TITLE -> firstTitle(card);
      case NOTE -> firstNote(card);
      case URL -> firstUrl(card);
      case CATEGORIES -> joinList(card.getCategories().getValues());
      case EMAIL -> emailValue(card, mapping.getParameterTypes());
      case EMAIL_TYPE -> emailTypeValue(card, mapping.getParameterTypes());
      case TEL -> telephoneValue(card, mapping.getParameterTypes());
      case TEL_TYPE -> telephoneTypeValue(card, mapping.getParameterTypes());
      case REV -> revision(card);
      case PRODID -> text(card.getProductId());
    };
  }

  public static void applyMappings(
      VCard card,
      Object[] row,
      IRowMeta rowMeta,
      List<VCardFieldMapping> mappings,
      boolean addProdId,
      boolean addRevision) {
    StructuredName structuredName = card.getStructuredName();
    if (structuredName == null) {
      structuredName = new StructuredName();
      card.setStructuredName(structuredName);
    }

    for (VCardFieldMapping mapping : mappings) {
      if (mapping == null || mapping.getProperty() == null) {
        continue;
      }
      if (mapping.getProperty() == VCardPropertyType.EMAIL_TYPE
          || mapping.getProperty() == VCardPropertyType.TEL_TYPE) {
        continue;
      }
      String value = rowValue(row, rowMeta, mapping.getHopField());
      if (value == null) {
        continue;
      }
      applyValue(card, structuredName, mapping, value, mappings, row, rowMeta);
    }

    if (addProdId && card.getProductId() == null) {
      card.setProductId(new ProductId("-//Apache Hop//EN"));
    }
    if (addRevision) {
      card.setRevision(Revision.now());
    }
    if (card.getFormattedName() == null && card.getStructuredName() != null) {
      String given = card.getStructuredName().getGiven();
      String family = card.getStructuredName().getFamily();
      if (!Utils.isEmpty(given) || !Utils.isEmpty(family)) {
        card.setFormattedName(new FormattedName(joinSpace(given, family)));
      }
    }
  }

  private static void applyValue(
      VCard card,
      StructuredName structuredName,
      VCardFieldMapping mapping,
      String value,
      List<VCardFieldMapping> mappings,
      Object[] row,
      IRowMeta rowMeta) {
    switch (mapping.getProperty()) {
      case FN -> card.setFormattedName(new FormattedName(value));
      case N_FAMILY -> structuredName.setFamily(value);
      case N_GIVEN -> structuredName.setGiven(value);
      case N_ADDITIONAL -> replaceSingle(structuredName.getAdditionalNames(), value);
      case N_PREFIX -> replaceSingle(structuredName.getPrefixes(), value);
      case N_SUFFIX -> replaceSingle(structuredName.getSuffixes(), value);
      case UID -> card.setUid(new Uid(value));
      case NICKNAME -> {
        Nickname nickname = new Nickname();
        nickname.getValues().add(value);
        card.setNickname(nickname);
      }
      case ORG -> {
        Organization org = new Organization();
        org.getValues().add(value);
        card.setOrganization(org);
      }
      case TITLE -> card.addTitle(new Title(value));
      case NOTE -> card.addNote(new Note(value));
      case URL -> card.addUrl(new Url(value));
      case CATEGORIES -> card.getCategories().getValues().addAll(splitList(value));
      case EMAIL -> {
        Email email = new Email(value);
        String typeValue =
            companionTypeValue(
                mappings, row, rowMeta, VCardPropertyType.EMAIL_TYPE, mapping.getParameterTypes());
        for (EmailType type :
            parseEmailTypes(Utils.isEmpty(typeValue) ? mapping.getParameterTypes() : typeValue)) {
          email.getTypes().add(type);
        }
        card.addEmail(email);
      }
      case TEL -> {
        Telephone tel = new Telephone(value);
        String typeValue =
            companionTypeValue(
                mappings, row, rowMeta, VCardPropertyType.TEL_TYPE, mapping.getParameterTypes());
        for (TelephoneType type :
            parseTelephoneTypes(
                Utils.isEmpty(typeValue) ? mapping.getParameterTypes() : typeValue)) {
          tel.getTypes().add(type);
        }
        card.addTelephoneNumber(tel);
      }
      case REV -> card.setRevision(new Revision(parseRevisionInstant(value)));
      case PRODID -> card.setProductId(new ProductId(value));
      default -> {}
    }
  }

  private static String rowValue(Object[] row, IRowMeta rowMeta, String fieldName) {
    if (row == null || rowMeta == null || Utils.isEmpty(fieldName)) {
      return null;
    }
    int index = rowMeta.indexOfValue(fieldName);
    if (index < 0) {
      return null;
    }
    try {
      String value = rowMeta.getString(row, index);
      return Utils.isEmpty(value) ? null : value;
    } catch (HopValueException e) {
      return null;
    }
  }

  private static StructuredName structuredName(VCard card) {
    StructuredName name = card.getStructuredName();
    return name == null ? new StructuredName() : name;
  }

  private static String familyName(VCard card) {
    StructuredName name = card.getStructuredName();
    return name == null ? null : name.getFamily();
  }

  private static String givenName(VCard card) {
    StructuredName name = card.getStructuredName();
    return name == null ? null : name.getGiven();
  }

  private static String organization(VCard card) {
    Organization org = card.getOrganization();
    if (org == null || org.getValues().isEmpty()) {
      return null;
    }
    return org.getValues().get(0);
  }

  private static String firstNote(VCard card) {
    return card.getNotes().isEmpty() ? null : card.getNotes().get(0).getValue();
  }

  private static String firstUrl(VCard card) {
    return card.getUrls().isEmpty() ? null : card.getUrls().get(0).getValue();
  }

  private static String firstNickname(VCard card) {
    Nickname nickname = card.getNickname();
    return nickname == null ? null : joinList(nickname.getValues());
  }

  private static String firstTitle(VCard card) {
    return card.getTitles().isEmpty() ? null : card.getTitles().get(0).getValue();
  }

  private static String emailValue(VCard card, String parameterTypes) {
    Email match = findEmail(card, parameterTypes);
    return match == null ? null : match.getValue();
  }

  private static String emailTypeValue(VCard card, String parameterTypes) {
    Email match = findEmail(card, parameterTypes);
    return match == null ? null : formatTypeParameters(match.getTypes());
  }

  private static String telephoneValue(VCard card, String parameterTypes) {
    Telephone match = findTelephone(card, parameterTypes);
    if (match == null) {
      return null;
    }
    if (match.getText() != null) {
      return match.getText();
    }
    TelUri uri = match.getUri();
    return uri == null ? null : uri.toString();
  }

  private static String telephoneTypeValue(VCard card, String parameterTypes) {
    Telephone match = findTelephone(card, parameterTypes);
    return match == null ? null : formatTypeParameters(match.getTypes());
  }

  public static String formatTypeParameters(List<?> types) {
    if (types == null || types.isEmpty()) {
      return null;
    }
    return types.stream()
        .map(VCardMapper::typeParameterValue)
        .sorted()
        .collect(Collectors.joining(","));
  }

  private static String typeParameterValue(Object type) {
    if (type instanceof EmailType emailType) {
      return emailType.getValue().toUpperCase(Locale.ROOT);
    }
    if (type instanceof TelephoneType telephoneType) {
      return telephoneType.getValue().toUpperCase(Locale.ROOT);
    }
    return type.toString().toUpperCase(Locale.ROOT);
  }

  private static String companionTypeValue(
      List<VCardFieldMapping> mappings,
      Object[] row,
      IRowMeta rowMeta,
      VCardPropertyType typeProperty,
      String parameterTypes) {
    if (mappings == null) {
      return null;
    }
    for (VCardFieldMapping mapping : mappings) {
      if (mapping.getProperty() == typeProperty
          && parameterTypesEqual(mapping.getParameterTypes(), parameterTypes)) {
        return rowValue(row, rowMeta, mapping.getHopField());
      }
    }
    return null;
  }

  private static boolean parameterTypesEqual(String left, String right) {
    return typeTokens(left).equals(typeTokens(right));
  }

  private static Email findEmail(VCard card, String parameterTypes) {
    Set<String> wanted = typeTokens(parameterTypes);
    if (wanted.isEmpty()) {
      return card.getEmails().isEmpty() ? null : card.getEmails().get(0);
    }
    for (Email email : card.getEmails()) {
      if (typesMatch(email.getTypes(), wanted)) {
        return email;
      }
    }
    return null;
  }

  private static Telephone findTelephone(VCard card, String parameterTypes) {
    Set<String> wanted = typeTokens(parameterTypes);
    if (wanted.isEmpty()) {
      return card.getTelephoneNumbers().isEmpty() ? null : card.getTelephoneNumbers().get(0);
    }
    for (Telephone tel : card.getTelephoneNumbers()) {
      if (typesMatch(tel.getTypes(), wanted)) {
        return tel;
      }
    }
    return null;
  }

  private static boolean typesMatch(List<?> types, Set<String> wanted) {
    if (wanted.isEmpty()) {
      return true;
    }
    Set<String> actual = new LinkedHashSet<>();
    for (Object type : types) {
      if (type instanceof EmailType emailType) {
        actual.add(emailType.getValue().toUpperCase(Locale.ROOT));
      } else if (type instanceof TelephoneType telephoneType) {
        actual.add(telephoneType.getValue().toUpperCase(Locale.ROOT));
      }
    }
    return actual.containsAll(wanted);
  }

  public static List<EmailType> parseEmailTypes(String parameterTypes) {
    List<EmailType> types = new ArrayList<>();
    for (String token : splitList(parameterTypes)) {
      types.add(EmailType.get(token));
    }
    return types;
  }

  public static List<TelephoneType> parseTelephoneTypes(String parameterTypes) {
    List<TelephoneType> types = new ArrayList<>();
    for (String token : splitList(parameterTypes)) {
      types.add(TelephoneType.get(token));
    }
    return types;
  }

  public static String emailTypeOptions() {
    return EmailType.all().stream()
        .map(t -> t.getValue().toUpperCase(Locale.ROOT))
        .sorted()
        .collect(Collectors.joining(", "));
  }

  public static String telephoneTypeOptions() {
    return TelephoneType.all().stream()
        .map(t -> t.getValue().toUpperCase(Locale.ROOT))
        .sorted()
        .collect(Collectors.joining(", "));
  }

  private static Set<String> typeTokens(String parameterTypes) {
    return splitList(parameterTypes).stream()
        .map(s -> s.toUpperCase(Locale.ROOT))
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  private static List<String> splitList(String value) {
    if (Utils.isEmpty(value)) {
      return List.of();
    }
    return Arrays.stream(value.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
  }

  private static String joinList(List<String> values) {
    if (values == null || values.isEmpty()) {
      return null;
    }
    return values.stream().filter(s -> !Utils.isEmpty(s)).collect(Collectors.joining(", "));
  }

  private static void replaceSingle(List<String> target, String value) {
    target.clear();
    target.add(value);
  }

  private static String text(FormattedName name) {
    return name == null ? null : name.getValue();
  }

  private static String text(Uid uid) {
    return uid == null ? null : uid.getValue();
  }

  private static String text(Title title) {
    return title == null ? null : title.getValue();
  }

  private static String text(ProductId productId) {
    return productId == null ? null : productId.getValue();
  }

  private static String revision(VCard card) {
    Revision revision = card.getRevision();
    if (revision == null || revision.getValue() == null) {
      return null;
    }
    Temporal temporal = revision.getValue();
    if (temporal instanceof Instant instant) {
      return instant.toString();
    }
    return temporal.toString();
  }

  private static Instant parseRevisionInstant(String value) {
    try {
      return Instant.parse(value);
    } catch (Exception ignored) {
      return Instant.now();
    }
  }

  private static String joinSpace(String first, String second) {
    if (Utils.isEmpty(first)) {
      return second;
    }
    if (Utils.isEmpty(second)) {
      return first;
    }
    return first + " " + second;
  }

  public static void validateMappings(List<VCardFieldMapping> mappings) throws HopException {
    if (mappings == null || mappings.isEmpty()) {
      throw new HopException("At least one vCard field mapping is required");
    }
    for (VCardFieldMapping mapping : mappings) {
      if (mapping == null
          || mapping.getProperty() == null
          || Utils.isEmpty(mapping.getHopField())) {
        throw new HopException("Each vCard field mapping needs a Hop field and a vCard property");
      }
    }
  }
}
