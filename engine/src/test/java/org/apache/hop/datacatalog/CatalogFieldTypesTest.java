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

package org.apache.hop.datacatalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Set;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.datacatalog.ISchemaCapability.CatalogField;
import org.junit.jupiter.api.Test;

class CatalogFieldTypesTest {

  private static IValueMeta of(String nativeType, String type) {
    return CatalogFieldTypes.toValueMeta(
        new CatalogField("f", nativeType, type, null, Set.of(), Set.of()));
  }

  private static void assertField(
      IValueMeta valueMeta, int type, int length, int precision, String format) {
    String actual =
        valueMeta.getTypeDesc()
            + " "
            + valueMeta.getLength()
            + "/"
            + valueMeta.getPrecision()
            + " "
            + valueMeta.getConversionMask();
    assertEquals(type, valueMeta.getType(), actual);
    assertEquals(length, valueMeta.getLength(), actual);
    assertEquals(precision, valueMeta.getPrecision(), actual);
    assertEquals(format, valueMeta.getConversionMask(), actual);
  }

  @Test
  void databaseTypesKeepTheirLengthAndPrecision() {
    assertField(of("VARCHAR(255)", "STRING"), IValueMeta.TYPE_STRING, 255, -1, null);
    assertField(of("character varying(40)", "STRING"), IValueMeta.TYPE_STRING, 40, -1, null);
    assertField(of("DECIMAL(10, 2)", "NUMBER"), IValueMeta.TYPE_BIGNUMBER, 10, 2, null);
    assertField(of("NUMERIC(10)", "NUMBER"), IValueMeta.TYPE_INTEGER, 10, 0, null);
    assertField(of("NUMBER(38,0)", "NUMBER"), IValueMeta.TYPE_BIGNUMBER, 38, 0, null);
    assertField(of("decimal", "NUMBER"), IValueMeta.TYPE_BIGNUMBER, -1, -1, null);
    assertField(of("BIGINT", "NUMBER"), IValueMeta.TYPE_INTEGER, -1, 0, null);
    assertField(of("double precision", "NUMBER"), IValueMeta.TYPE_NUMBER, -1, -1, null);
    assertField(of("BOOLEAN", "BOOLEAN"), IValueMeta.TYPE_BOOLEAN, -1, -1, null);
    assertField(of("bytea", "BYTES"), IValueMeta.TYPE_BINARY, -1, 0, null);
  }

  @Test
  void datesAndTimesGetNoFormat() {
    assertField(of("DATE", "DATE"), IValueMeta.TYPE_DATE, -1, -1, null);
    assertField(of("DATETIME", "DATE"), IValueMeta.TYPE_DATE, -1, -1, null);
    assertField(of("TIME", "TIME"), IValueMeta.TYPE_DATE, -1, -1, null);
    assertField(of("TIMESTAMP(6) WITH TIME ZONE", "TIME"), IValueMeta.TYPE_TIMESTAMP, -1, -1, null);
  }

  @Test
  void aTimeIsADateWhetherItComesFromTheNativeOrTheCatalogType() {
    assertField(of("time", "TIME"), IValueMeta.TYPE_DATE, -1, -1, null);
    assertField(of(null, "TIME"), IValueMeta.TYPE_DATE, -1, -1, null);
    assertField(of("time with time zone", "TIME"), IValueMeta.TYPE_DATE, -1, -1, null);
  }

  @Test
  void aLengthTooLargeForAnIntIsNoLength() {
    assertField(of("varchar(4294967295)", "STRING"), IValueMeta.TYPE_STRING, -1, -1, null);
    assertField(of("decimal(99999999999, 2)", "NUMBER"), IValueMeta.TYPE_BIGNUMBER, -1, -1, null);
  }

  @Test
  void modifiersAndDialectAliasesKeepTheirType() {
    assertField(of("int unsigned", "NUMBER"), IValueMeta.TYPE_INTEGER, -1, 0, null);
    assertField(of("INT(11) UNSIGNED ZEROFILL", "NUMBER"), IValueMeta.TYPE_INTEGER, -1, 0, null);
    assertField(of("smallint signed", "NUMBER"), IValueMeta.TYPE_INTEGER, -1, 0, null);
    assertField(of("int64", "NUMBER"), IValueMeta.TYPE_INTEGER, -1, 0, null);
    assertField(of("float64", "NUMBER"), IValueMeta.TYPE_NUMBER, -1, -1, null);
    assertField(of("TIMESTAMP_NTZ(9)", "TIME"), IValueMeta.TYPE_TIMESTAMP, -1, -1, null);
    assertField(of("timestamp_ltz", "TIME"), IValueMeta.TYPE_TIMESTAMP, -1, -1, null);
    assertField(of("timestamp_tz", "TIME"), IValueMeta.TYPE_TIMESTAMP, -1, -1, null);
    assertField(of("datetimeoffset(7)", "TIME"), IValueMeta.TYPE_TIMESTAMP, -1, -1, null);
    assertField(
        of("TIMESTAMP WITH LOCAL TIME ZONE", "TIME"), IValueMeta.TYPE_TIMESTAMP, -1, -1, null);
  }

  @Test
  void anUnsignedBigintIsABigNumberAsWhenHopReadsItFromADatabase() {
    assertField(of("bigint unsigned", "NUMBER"), IValueMeta.TYPE_BIGNUMBER, -1, 0, null);
    assertField(of("bigint(20) unsigned", "NUMBER"), IValueMeta.TYPE_BIGNUMBER, -1, 0, null);
    assertField(of("bigint signed", "NUMBER"), IValueMeta.TYPE_INTEGER, -1, 0, null);
  }

  @Test
  void theFirstWordIsTheTypeOnlyWhenTheWholeNameIsUnknown() {
    assertField(of("time with time zone", "TIME"), IValueMeta.TYPE_DATE, -1, -1, null);
    assertField(of("varchar character set utf8", "STRING"), IValueMeta.TYPE_STRING, -1, -1, null);
    assertField(of("long raw", "BYTES"), IValueMeta.TYPE_BINARY, -1, 0, null);
    assertField(of("long varchar", "STRING"), IValueMeta.TYPE_STRING, -1, -1, null);
    assertField(of("bit varying(12)", "STRING"), IValueMeta.TYPE_STRING, 12, -1, null);
  }

  @Test
  void onlyASingleBitIsABoolean() {
    assertField(of("bit", "BOOLEAN"), IValueMeta.TYPE_BOOLEAN, -1, -1, null);
    assertField(of("BIT(1)", "BOOLEAN"), IValueMeta.TYPE_BOOLEAN, -1, -1, null);
    assertField(of("BIT(8)", "BYTES"), IValueMeta.TYPE_STRING, 8, -1, null);
    assertField(of("bit(64)", "STRING"), IValueMeta.TYPE_STRING, 64, -1, null);
  }

  @Test
  void theTypesAHopLineageSinkWritesComeBackAsTheHopTypes() {
    assertField(of("string", "STRING"), IValueMeta.TYPE_STRING, -1, -1, null);
    assertField(of("int", "NUMBER"), IValueMeta.TYPE_INTEGER, -1, 0, null);
    assertField(of("double", "NUMBER"), IValueMeta.TYPE_NUMBER, -1, -1, null);
    assertField(of("decimal", "NUMBER"), IValueMeta.TYPE_BIGNUMBER, -1, -1, null);
    assertField(of("date", "DATE"), IValueMeta.TYPE_DATE, -1, -1, null);
    assertField(of("timestamp", "TIME"), IValueMeta.TYPE_TIMESTAMP, -1, -1, null);
    assertField(of("boolean", "BOOLEAN"), IValueMeta.TYPE_BOOLEAN, -1, -1, null);
    assertField(of("binary", "BYTES"), IValueMeta.TYPE_BINARY, -1, 0, null);
  }

  @Test
  void anUnknownNativeTypeFallsBackToTheCatalogType() {
    assertField(of("money", "NUMBER"), IValueMeta.TYPE_NUMBER, -1, -1, null);
    assertField(of(null, "BOOLEAN"), IValueMeta.TYPE_BOOLEAN, -1, -1, null);
    assertField(of("", "DATE"), IValueMeta.TYPE_DATE, -1, -1, null);
    assertField(of("struct<a:int>", "STRUCT"), IValueMeta.TYPE_STRING, -1, -1, null);
    assertField(of(null, null), IValueMeta.TYPE_STRING, -1, -1, null);
  }

  @Test
  void theRowKeepsTheOrderAndNamesOfTheSchema() {
    IRowMeta rowMeta =
        CatalogFieldTypes.toRowMeta(
            List.of(
                new CatalogField("id", "INTEGER", "NUMBER", false, Set.of(), Set.of()),
                new CatalogField("email", "VARCHAR(255)", "STRING", true, Set.of(), Set.of())));

    assertEquals(List.of("id", "email"), List.of(rowMeta.getFieldNames()));
    assertNull(rowMeta.getValueMeta(0).getConversionMask());
  }
}
