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
package org.apache.hop.core.database.types;

import java.util.Locale;
import java.util.Set;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaString;

/**
 * What a column becomes when the database has no type of its own for the value.
 *
 * <p>Every dialect can spell a string, a number and a date. Beyond those, a Hop type only reaches a
 * column if some dialect said how — and most never do: a JSON value used to be written as the
 * column type {@code JSON} on all forty-odd dialects, including the many that have no such type,
 * which fails at CREATE TABLE. So a type nobody claimed is written the way the database can
 * actually hold it, which for these is text.
 *
 * <p>The same substitution answers the other half of the question, which is version rather than
 * vendor: SQL Server grew a JSON type in 2025 and Oracle in 21c, so the same dialect wants a
 * different column depending on what it is connected to. {@link #serverOffers} puts that question
 * to the dialect, which answers it from {@link ServerInfo}; a dialect that says nothing, or one
 * generating a definition with no connection at all, keeps the type it declared.
 */
public final class ColumnTypeFallback {

  /**
   * The Hop types a dialect's own getFieldDefinition knows how to spell. Everything else falls
   * through its switch to "UNKNOWN", which is not a column type anywhere.
   */
  private static final Set<Integer> WRITTEN_BY_EVERY_DIALECT =
      Set.of(
          IValueMeta.TYPE_STRING,
          IValueMeta.TYPE_INTEGER,
          IValueMeta.TYPE_NUMBER,
          IValueMeta.TYPE_BIGNUMBER,
          IValueMeta.TYPE_DATE,
          IValueMeta.TYPE_TIMESTAMP,
          IValueMeta.TYPE_BOOLEAN,
          IValueMeta.TYPE_BINARY);

  /** A UUID in its canonical text form. */
  private static final int UUID_TEXT_LENGTH = 36;

  /** An IPv6 address with an IPv4 tail, the longest an address gets. */
  private static final int ADDRESS_TEXT_LENGTH = 45;

  private ColumnTypeFallback() {
    // Utility class.
  }

  /**
   * Whether this value needs a substitute when no rule claimed it.
   *
   * @return true when the dialect's own mapping has no branch for the type
   */
  public static boolean needsSubstitute(IValueMeta valueMeta) {
    return valueMeta != null && !WRITTEN_BY_EVERY_DIALECT.contains(valueMeta.getType());
  }

  /**
   * The value to describe the column with instead.
   *
   * <p>Text of a length that fits, which is all that can be said without knowing the database. A
   * dialect that can do better says so with a write rule, and then this is never reached.
   *
   * @return a value the dialect's own mapping can spell, never null
   */
  public static IValueMeta substituteFor(IValueMeta valueMeta) {
    String name = valueMeta.getName();
    return switch (valueMeta.getType()) {
        // A date, not text: the dialects that lack a timestamp have a date, and it is the closer
        // thing. This is what the deprecated SUPPORTS_TIMESTAMP_DATA_TYPE attribute decides today.
      case IValueMeta.TYPE_TIMESTAMP -> new ValueMetaDate(name);
        // The single character the SUPPORTS_BOOLEAN_DATA_TYPE attribute falls back to today.
      case IValueMeta.TYPE_BOOLEAN -> new ValueMetaString(name, 1, 0);
      case IValueMeta.TYPE_UUID -> new ValueMetaString(name, UUID_TEXT_LENGTH, 0);
      case IValueMeta.TYPE_INET -> new ValueMetaString(name, ADDRESS_TEXT_LENGTH, 0);
        // A JSON document has no length worth guessing at, and neither does a type Hop has never
        // heard of, so both get the widest text the database has.
      default ->
          new ValueMetaString(
              name,
              valueMeta.getLength() > 0 ? valueMeta.getLength() : DatabaseMeta.CLOB_LENGTH,
              0);
    };
  }

  /**
   * Whether the database is known to have this column type.
   *
   * <p>Asked of the dialect, which is the only thing that knows which version of its database grew
   * which type. The default answer is yes, so this only ever overrules a dialect that said
   * otherwise about the server it is connected to.
   */
  public static boolean serverOffers(IDatabase database, String columnType) {
    if (database == null || columnType == null) {
      return true;
    }
    String baseName = baseTypeName(columnType);
    return baseName.isEmpty() || database.isColumnTypeAvailable(baseName);
  }

  /** The type name without its size or the column name around it: "NVARCHAR(MAX)" is NVARCHAR. */
  private static String baseTypeName(String columnType) {
    String name = columnType.trim();
    int end = name.length();
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      if (c == '(' || Character.isWhitespace(c)) {
        end = i;
        break;
      }
    }
    return name.substring(0, end).toUpperCase(Locale.ROOT);
  }
}
