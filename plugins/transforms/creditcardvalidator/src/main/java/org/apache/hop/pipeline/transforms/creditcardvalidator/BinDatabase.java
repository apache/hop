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

package org.apache.hop.pipeline.transforms.creditcardvalidator;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;

/** In-memory lookup table for Bank Identification Numbers (BIN) loaded from an external CSV. */
public class BinDatabase {

  private static final Class<?> PKG = CreditCardValidatorMeta.class;

  private static final int MIN_BIN_LENGTH = 1;
  private static final int MAX_BIN_LENGTH = 8;
  private static final int INITIAL_CAPACITY = 10_000;
  private static final Map<String, String> EMPTY_VALUES = Collections.emptyMap();

  private final Long2ObjectOpenHashMap<BinRecord> records;
  private final Map<String, String> dedup = new HashMap<>();

  /** Bitmask of the BIN lengths present in the dataset (bit {@code 1 << length}). */
  private int binLengths;

  /** Number of rows skipped because the BIN was empty, longer than 8 digits, or non-numeric. */
  private int skippedRows;

  public BinDatabase() {
    this.records = new Long2ObjectOpenHashMap<>(INITIAL_CAPACITY);
  }

  /** Load a user-supplied CSV file with an explicit column mapping and read configuration. */
  public void load(
      IVariables variables,
      String vfsFilename,
      String binCsvColumn,
      List<BinOutputField> outputFields,
      String delimiter,
      String enclosure,
      String encoding,
      boolean headerPresent)
      throws HopException {
    Charset charset;
    try {
      charset = Charset.forName(encoding == null ? "UTF-8" : encoding);
    } catch (Exception e) {
      charset = StandardCharsets.UTF_8;
    }
    try (InputStream in = HopVfs.getInputStream(vfsFilename, variables)) {
      List<BinOutputField> resolvedFields = resolveOutputFields(outputFields, variables);
      load(in, binCsvColumn, resolvedFields, delimiter, enclosure, charset, headerPresent);
    } catch (IOException e) {
      throw new HopException(
          BaseMessages.getString(PKG, "CreditCardValidator.Error.BinDatabaseRead"), e);
    }
  }

  private static List<BinOutputField> resolveOutputFields(
      List<BinOutputField> outputFields, IVariables variables) {
    List<BinOutputField> resolved = new java.util.ArrayList<>(outputFields.size());
    for (BinOutputField outputField : outputFields) {
      BinOutputField copy = outputField.clone();
      copy.setName(variables.resolve(outputField.getName()));
      resolved.add(copy);
    }
    return resolved;
  }

  private void load(
      InputStream in,
      String binCsvColumn,
      List<BinOutputField> outputFields,
      String delimiter,
      String enclosure,
      Charset charset,
      boolean headerPresent)
      throws HopException {
    try (CSVReader reader =
        new CSVReaderBuilder(new InputStreamReader(in, charset))
            .withCSVParser(parser(delimiter, enclosure))
            .build()) {
      int binIdx;
      int cardTypeIdx;
      Map<String, Integer> extraIdx = new HashMap<>();

      if (headerPresent) {
        String[] header = reader.readNext();
        if (header == null) {
          throw new HopException(
              BaseMessages.getString(PKG, "CreditCardValidator.Error.BinDatabaseEmpty"));
        }
        binIdx = resolveColumn(header, binCsvColumn, 0, "bin");
        if (binIdx < 0) {
          throw new HopException(
              BaseMessages.getString(
                  PKG, "CreditCardValidator.Error.BinColumnNotFound", binCsvColumn));
        }
        cardTypeIdx =
            resolveColumn(
                header, null, "cardtype", "card_type", "card type", "brand", "scheme", "type");
        for (BinOutputField outputField : outputFields) {
          if (Utils.isEmpty(outputField.getColumn())) {
            continue;
          }
          for (int i = 0; i < header.length; i++) {
            if (outputField.getColumn().equals(header[i])) {
              extraIdx.put(outputField.getName(), i);
              break;
            }
          }
        }
      } else {
        binIdx = 0;
        cardTypeIdx = 1;
        for (int k = 0; k < outputFields.size(); k++) {
          BinOutputField outputField = outputFields.get(k);
          if (!Utils.isEmpty(outputField.getName())) {
            extraIdx.put(outputField.getName(), 4 + k);
          }
        }
      }

      String[] fields;
      while ((fields = reader.readNext()) != null) {
        String rawBin = field(fields, binIdx).trim();
        if (rawBin.isEmpty() || rawBin.length() > MAX_BIN_LENGTH) {
          skippedRows++;
          continue;
        }
        int bin = parseDigits(rawBin);
        if (bin < 0) {
          skippedRows++;
          continue;
        }
        int length = rawBin.length();
        String cardType = dedup(field(fields, cardTypeIdx));
        Map<String, String> extraValues = EMPTY_VALUES;
        if (!extraIdx.isEmpty()) {
          extraValues = new HashMap<>();
          for (Map.Entry<String, Integer> entry : extraIdx.entrySet()) {
            extraValues.put(entry.getKey(), field(fields, entry.getValue()).trim());
          }
        }

        records.put(composeKey(length, bin), new BinRecord(cardType, extraValues));
        binLengths |= 1 << length;
      }
    } catch (IOException e) {
      throw new HopException(
          BaseMessages.getString(PKG, "CreditCardValidator.Error.BinDatabaseRead"), e);
    } catch (CsvValidationException e) {
      throw new HopException(
          BaseMessages.getString(PKG, "CreditCardValidator.Error.BinDatabaseRead"), e);
    }
  }

  private static CSVParser parser(String delimiter, String enclosure) {
    CSVParserBuilder builder = new CSVParserBuilder();
    if (!Utils.isEmpty(delimiter)) {
      builder.withSeparator(delimiter.charAt(0));
    }
    if (!Utils.isEmpty(enclosure)) {
      builder.withQuoteChar(enclosure.charAt(0));
    }
    return builder.build();
  }

  private static String field(String[] fields, int index) {
    if (index < 0 || index >= fields.length || fields[index] == null) {
      return "";
    }
    return fields[index];
  }

  private String dedup(String value) {
    String trimmed = value.trim();
    String existing = dedup.get(trimmed);
    if (existing != null) {
      return existing;
    }
    dedup.put(trimmed, trimmed);
    return trimmed;
  }

  /** Compose a unique long key from a BIN length and its numeric value. */
  private static long composeKey(int length, int value) {
    return ((long) length << 32) | (value & 0xFFFFFFFFL);
  }

  /** Parse a pure digit string into an int; returns -1 if any non-digit is found. */
  private static int parseDigits(String s) {
    int value = 0;
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c < '0' || c > '9') {
        return -1;
      }
      value = value * 10 + (c - '0');
    }
    return value;
  }

  /** Resolve the record matching the BIN using longest-prefix matching. */
  public BinRecord lookup(String bin) {
    if (Utils.isEmpty(bin)) {
      return null;
    }
    int len = bin.length();
    if (len > MAX_BIN_LENGTH) {
      len = MAX_BIN_LENGTH;
    }
    for (int l = len; l >= MIN_BIN_LENGTH; l--) {
      if ((binLengths & (1 << l)) == 0) {
        continue;
      }
      BinRecord record = records.get(composeKey(l, prefixValue(bin, l)));
      if (record != null) {
        return record;
      }
    }
    return null;
  }

  private static int prefixValue(String bin, int length) {
    int value = 0;
    for (int i = 0; i < length; i++) {
      char c = bin.charAt(i);
      if (c < '0' || c > '9') {
        return -1;
      }
      value = value * 10 + (c - '0');
    }
    return value;
  }

  public int size() {
    return records.size();
  }

  public int getSkippedRows() {
    return skippedRows;
  }

  /**
   * Resolve a column to its index. An explicit column name wins; otherwise the header is scanned
   * for known names. When nothing matches, the default index is returned.
   */
  private static int resolveColumn(
      String[] header, String columnName, int defaultIndex, String... knownNames) {
    int index = resolveColumn(header, columnName, knownNames);
    return index < 0 ? defaultIndex : index;
  }

  /** Resolve a column to its index; returns -1 when nothing matches. */
  private static int resolveColumn(String[] header, String columnName, String... knownNames) {
    if (!Utils.isEmpty(columnName)) {
      for (int i = 0; i < header.length; i++) {
        if (columnName.equals(header[i])) {
          return i;
        }
      }
      return -1;
    }
    for (String knownName : knownNames) {
      String normalized = normalize(knownName);
      for (int i = 0; i < header.length; i++) {
        if (normalized.equals(normalize(header[i]))) {
          return i;
        }
      }
    }
    return -1;
  }

  private static String normalize(String value) {
    if (value == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c == ' ' || c == '_' || c == '-') {
        continue;
      }
      sb.append(Character.toLowerCase(c));
    }
    return sb.toString();
  }

  /** A single BIN entry. */
  public static class BinRecord {
    private final String cardType;
    private final Map<String, String> extraValues;

    public BinRecord(String cardType, Map<String, String> extraValues) {
      this.cardType = cardType;
      this.extraValues = extraValues;
    }

    public String getCardType() {
      return cardType;
    }

    public Map<String, String> getExtraValues() {
      return extraValues;
    }

    public String getValue(String name) {
      return extraValues == null ? null : extraValues.get(name);
    }
  }
}
