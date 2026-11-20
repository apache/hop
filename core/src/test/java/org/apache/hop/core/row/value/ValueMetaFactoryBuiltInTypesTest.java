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

package org.apache.hop.core.row.value;

import static org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.BUILT_IN_NAMES_IN_ORDER;
import static org.apache.hop.core.row.value.ValueMetaFactoryTestPlugins.unknownTypeMessage;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.stream.Stream;
import org.apache.hop.core.exception.HopEofException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Pins what {@link ValueMetaFactory} answers for the built-in value types, with the real plugin
 * registry: the class, defaults and freshness of every instance, the names and ids, and the
 * exceptions for types that don't exist.
 */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class ValueMetaFactoryBuiltInTypesTest {

  /**
   * A built-in type as the factory hands it out.
   *
   * @param precisionWithLength what getPrecision() returns after createValueMeta(.., 10, 20)
   */
  record BuiltIn(
      int type,
      Class<? extends IValueMeta> valueMetaClass,
      String name,
      int defaultPrecision,
      int precisionWithLength,
      String conversionMask,
      String groupingSymbol) {
    @Override
    public String toString() {
      return name;
    }
  }

  static Stream<BuiltIn> builtIns() {
    return Stream.of(
        new BuiltIn(IValueMeta.TYPE_NONE, ValueMetaNone.class, "None", -1, 20, null, ","),
        new BuiltIn(
            IValueMeta.TYPE_NUMBER,
            ValueMetaNumber.class,
            "Number",
            -1,
            20,
            "####0.0#########;-####0.0#########",
            ","),
        new BuiltIn(IValueMeta.TYPE_STRING, ValueMetaString.class, "String", -1, -1, null, ","),
        new BuiltIn(IValueMeta.TYPE_DATE, ValueMetaDate.class, "Date", -1, 20, null, ","),
        new BuiltIn(IValueMeta.TYPE_BOOLEAN, ValueMetaBoolean.class, "Boolean", -1, -1, null, ","),
        new BuiltIn(
            IValueMeta.TYPE_INTEGER, ValueMetaInteger.class, "Integer", 0, 0, "####0;-####0", ","),
        new BuiltIn(
            IValueMeta.TYPE_BIGNUMBER,
            ValueMetaBigNumber.class,
            "BigNumber",
            -1,
            20,
            "######0.0###################;-######0.0###################",
            null),
        new BuiltIn(
            IValueMeta.TYPE_SERIALIZABLE,
            ValueMetaSerializable.class,
            "Serializable",
            -1,
            20,
            null,
            ","),
        new BuiltIn(IValueMeta.TYPE_BINARY, ValueMetaBinary.class, "Binary", 0, 0, null, ","),
        new BuiltIn(
            IValueMeta.TYPE_TIMESTAMP, ValueMetaTimestamp.class, "Timestamp", -1, 20, null, ","),
        new BuiltIn(
            IValueMeta.TYPE_INET,
            ValueMetaInternetAddress.class,
            "Internet Address",
            -1,
            20,
            null,
            ","),
        new BuiltIn(IValueMeta.TYPE_JSON, ValueMetaJson.class, "JSON", -1, 20, null, ","),
        new BuiltIn(
            IValueMeta.TYPE_AVRO, ValueMetaAvroRecord.class, "Avro Record", -1, 20, null, ","));
  }

  static Stream<Integer> unknownTypes() {
    return Stream.of(-1, -2, 12, 13, 19, 21, 100, Integer.MAX_VALUE, Integer.MIN_VALUE);
  }

  // createValueMeta, every overload
  //

  @ParameterizedTest
  @MethodSource("builtIns")
  void createValueMetaByTypeGivesTheExactClassWithDefaults(BuiltIn builtIn) throws Exception {
    IValueMeta valueMeta = ValueMetaFactory.createValueMeta(builtIn.type());

    assertSame(builtIn.valueMetaClass(), valueMeta.getClass());
    assertEquals(builtIn.type(), valueMeta.getType());
    assertNull(valueMeta.getName());
    assertEquals(-1, valueMeta.getLength());
    assertEquals(builtIn.defaultPrecision(), valueMeta.getPrecision());
    assertDefaultSettings(builtIn, valueMeta);
  }

  @ParameterizedTest
  @MethodSource("builtIns")
  void createValueMetaByNameAndTypeSetsTheName(BuiltIn builtIn) throws Exception {
    IValueMeta valueMeta = ValueMetaFactory.createValueMeta("field", builtIn.type());

    assertSame(builtIn.valueMetaClass(), valueMeta.getClass());
    assertEquals(builtIn.type(), valueMeta.getType());
    assertEquals("field", valueMeta.getName());
    assertEquals(-1, valueMeta.getLength());
    assertEquals(builtIn.defaultPrecision(), valueMeta.getPrecision());
    assertDefaultSettings(builtIn, valueMeta);
  }

  @ParameterizedTest
  @MethodSource("builtIns")
  void createValueMetaWithLengthAndPrecisionSetsBoth(BuiltIn builtIn) throws Exception {
    IValueMeta valueMeta = ValueMetaFactory.createValueMeta("field", builtIn.type(), 10, 20);

    assertSame(builtIn.valueMetaClass(), valueMeta.getClass());
    assertEquals(builtIn.type(), valueMeta.getType());
    assertEquals("field", valueMeta.getName());
    assertEquals(10, valueMeta.getLength());
    assertEquals(builtIn.precisionWithLength(), valueMeta.getPrecision());
    assertDefaultSettings(builtIn, valueMeta);
  }

  @ParameterizedTest
  @MethodSource("builtIns")
  void createValueMetaWithNullNameAndNegativeLength(BuiltIn builtIn) throws Exception {
    IValueMeta valueMeta = ValueMetaFactory.createValueMeta(null, builtIn.type(), -5, -7);

    assertSame(builtIn.valueMetaClass(), valueMeta.getClass());
    assertNull(valueMeta.getName());
    assertEquals(-5, valueMeta.getLength());
  }

  private static void assertDefaultSettings(BuiltIn builtIn, IValueMeta valueMeta)
      throws Exception {
    assertEquals(IValueMeta.STORAGE_TYPE_NORMAL, valueMeta.getStorageType());
    assertNull(valueMeta.getStorageMetadata());
    assertNull(valueMeta.getIndex());
    assertEquals(IValueMeta.TRIM_TYPE_NONE, valueMeta.getTrimType());
    assertEquals(builtIn.conversionMask(), valueMeta.getConversionMask());
    assertEquals(".", valueMeta.getDecimalSymbol());
    assertEquals(builtIn.groupingSymbol(), valueMeta.getGroupingSymbol());
    assertEquals("$", valueMeta.getCurrencySymbol());
    assertNull(valueMeta.getStringEncoding());
    assertNull(valueMeta.getOrigin());
    assertNull(valueMeta.getComments());
    assertNull(valueMeta.getRoundingType());
    assertFalse(valueMeta.isCaseInsensitive());
    assertTrue(valueMeta.isCollatorDisabled());
    assertEquals(0, valueMeta.getCollatorStrength());
    assertFalse(valueMeta.isSortedDescending());
    assertFalse(valueMeta.isOutputPaddingEnabled());
    assertFalse(valueMeta.isLargeTextField());
    assertFalse(valueMeta.isDateFormatLenient());
    assertFalse(valueMeta.isLenientStringToNumber());
    assertFalse(valueMeta.isIgnoreWhitespace());
    assertEquals(Locale.getDefault(Locale.Category.FORMAT), valueMeta.getDateFormatLocale());
    assertEquals(TimeZone.getDefault(), valueMeta.getDateFormatTimeZone());
    assertEquals(builtIn.name(), valueMeta.getTypeDesc());
  }

  @ParameterizedTest
  @MethodSource("builtIns")
  void everyOverloadReturnsANewInstanceOnEveryCall(BuiltIn builtIn) throws Exception {
    List<IValueMeta> created = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      created.add(ValueMetaFactory.createValueMeta(builtIn.type()));
      created.add(ValueMetaFactory.createValueMeta("field", builtIn.type()));
      created.add(ValueMetaFactory.createValueMeta("field", builtIn.type(), 10, 2));
    }
    for (int i = 0; i < created.size(); i++) {
      for (int j = i + 1; j < created.size(); j++) {
        assertNotSame(created.get(i), created.get(j), "instances " + i + " and " + j);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("builtIns")
  void instancesDoNotShareMutableState(BuiltIn builtIn) throws Exception {
    IValueMeta first = ValueMetaFactory.createValueMeta("first", builtIn.type(), 10, 2);
    IValueMeta second = ValueMetaFactory.createValueMeta("second", builtIn.type(), 10, 2);

    // Formatters are created per instance.
    assertNotSame(first.getDateFormat(), second.getDateFormat());
    assertNotSame(first.getDecimalFormat(), second.getDecimalFormat());

    first.setName("changed");
    first.setLength(99, 9);
    first.setConversionMask("0000");
    first.setDecimalSymbol(",");
    first.setGroupingSymbol(".");
    first.setCurrencySymbol("EUR");
    first.setTrimType(IValueMeta.TRIM_TYPE_BOTH);
    first.setStringEncoding("UTF-16");
    first.setComments("comments");
    first.setOrigin("origin");
    first.setCaseInsensitive(true);
    first.setSortedDescending(true);
    first.setLargeTextField(true);
    first.setDateFormatLenient(true);
    first.setLenientStringToNumber(true);
    first.setDateFormatLocale(Locale.JAPAN);
    first.setDateFormatTimeZone(TimeZone.getTimeZone("Asia/Tokyo"));
    first.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    first.setStorageMetadata(new ValueMetaString("storage"));
    first.setIndex(new Object[] {"a"});
    first.getDateFormat().applyPattern("yyyy");

    IValueMeta third = ValueMetaFactory.createValueMeta("third", builtIn.type(), 10, 2);
    for (IValueMeta other : List.of(second, third)) {
      assertEquals(10, other.getLength());
      assertDefaultSettings(builtIn, other);
      assertNotSame(first.getDateFormat(), other.getDateFormat());
    }
    assertEquals("second", second.getName());
    assertEquals("third", third.getName());
  }

  // Unknown ids
  //

  @ParameterizedTest
  @MethodSource("unknownTypes")
  void createValueMetaByTypeFailsForAnUnknownId(int type) {
    HopPluginException e =
        assertThrows(HopPluginException.class, () -> ValueMetaFactory.createValueMeta(type));
    assertSame(HopPluginException.class, e.getClass());
    assertEquals(unknownTypeMessage(type), e.getMessage());
    assertNull(e.getCause());
  }

  @ParameterizedTest
  @MethodSource("unknownTypes")
  void createValueMetaByNameAndTypeFailsForAnUnknownId(int type) {
    HopPluginException e =
        assertThrows(
            HopPluginException.class, () -> ValueMetaFactory.createValueMeta("field", type));
    assertSame(HopPluginException.class, e.getClass());
    assertEquals(unknownTypeMessage(type), e.getMessage());
  }

  @ParameterizedTest
  @MethodSource("unknownTypes")
  void createValueMetaWithLengthFailsForAnUnknownId(int type) {
    HopPluginException e =
        assertThrows(
            HopPluginException.class, () -> ValueMetaFactory.createValueMeta("field", type, 1, 2));
    assertSame(HopPluginException.class, e.getClass());
    assertEquals(unknownTypeMessage(type), e.getMessage());
  }

  @Test
  void anUnknownIdKeepsFailingAndDoesNotDisturbKnownIds() throws Exception {
    for (int i = 0; i < 3; i++) {
      assertThrows(HopPluginException.class, () -> ValueMetaFactory.createValueMeta(12));
      assertInstanceOf(
          ValueMetaString.class, ValueMetaFactory.createValueMeta(IValueMeta.TYPE_STRING));
    }
  }

  // cloneValueMeta
  //

  @ParameterizedTest
  @MethodSource("builtIns")
  void cloneValueMetaToTheSameTypeCopiesEverything(BuiltIn builtIn) throws Exception {
    IValueMeta source = ValueMetaFactory.createValueMeta("source", builtIn.type(), 12, 3);
    customize(source);

    IValueMeta cloned = ValueMetaFactory.cloneValueMeta(source);
    IValueMeta clonedExplicitly = ValueMetaFactory.cloneValueMeta(source, builtIn.type());

    // ValueMetaAvroRecord.clone() builds a new instance from the name and schema only: the length
    // and precision of the source are lost (cloneInfo() doesn't copy them either).
    boolean avro = builtIn.type() == IValueMeta.TYPE_AVRO;
    for (IValueMeta target : List.of(cloned, clonedExplicitly)) {
      assertNotSame(source, target);
      assertSame(builtIn.valueMetaClass(), target.getClass());
      assertEquals("source", target.getName());
      assertEquals(avro ? -1 : 12, target.getLength());
      assertEquals(avro ? -1 : source.getPrecision(), target.getPrecision());
      assertCustomized(source, target);
    }
    assertNotSame(cloned, clonedExplicitly);
  }

  @Test
  void cloneValueMetaToEveryOtherBuiltInType() throws Exception {
    for (BuiltIn from : builtIns().toList()) {
      for (BuiltIn to : builtIns().toList()) {
        if (from.type() == to.type()) {
          continue;
        }
        IValueMeta source = ValueMetaFactory.createValueMeta("source", from.type(), 12, 3);
        customize(source);

        IValueMeta target = ValueMetaFactory.cloneValueMeta(source, to.type());

        String what = from + " -> " + to;
        assertSame(to.valueMetaClass(), target.getClass(), what);
        assertEquals(to.type(), target.getType(), what);
        assertEquals("source", target.getName(), what);
        assertEquals(12, target.getLength(), what);
        // The source's own precision rule applies first (String and Boolean say -1, Integer and
        // Binary say 0), then the target's.
        int expectedPrecision =
            to.precisionWithLength() == 20 ? source.getPrecision() : to.precisionWithLength();
        assertEquals(expectedPrecision, target.getPrecision(), what);
        assertCustomized(source, target);
      }
    }
  }

  @Test
  void cloneValueMetaClonesTheStorageMetadata() throws Exception {
    IValueMeta source = ValueMetaFactory.createValueMeta("lazy", IValueMeta.TYPE_INTEGER);
    IValueMeta storage = ValueMetaFactory.createValueMeta("lazy", IValueMeta.TYPE_STRING);
    storage.setConversionMask("#");
    source.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    source.setStorageMetadata(storage);

    for (int targetType : new int[] {IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_NUMBER}) {
      IValueMeta target = ValueMetaFactory.cloneValueMeta(source, targetType);
      assertEquals(IValueMeta.STORAGE_TYPE_BINARY_STRING, target.getStorageType());
      assertNotSame(storage, target.getStorageMetadata());
      assertSame(ValueMetaString.class, target.getStorageMetadata().getClass());
      assertEquals("lazy", target.getStorageMetadata().getName());
      assertEquals("#", target.getStorageMetadata().getConversionMask());
    }
  }

  @ParameterizedTest
  @MethodSource("unknownTypes")
  void cloneValueMetaToAnUnknownTypeFails(int type) {
    IValueMeta source = new ValueMetaString("source");
    HopPluginException e =
        assertThrows(HopPluginException.class, () -> ValueMetaFactory.cloneValueMeta(source, type));
    assertEquals(unknownTypeMessage(type), e.getMessage());
  }

  private static void customize(IValueMeta source) {
    source.setConversionMask("#.##");
    source.setDecimalSymbol(",");
    source.setGroupingSymbol(".");
    source.setTrimType(IValueMeta.TRIM_TYPE_LEFT);
    source.setStringEncoding("UTF-8");
    source.setComments("comments");
    source.setOrigin("origin");
    source.setCaseInsensitive(true);
    source.setCollatorDisabled(false);
    source.setCollatorStrength(2);
    source.setDateFormatLenient(true);
    source.setDateFormatLocale(Locale.GERMANY);
    source.setDateFormatTimeZone(TimeZone.getTimeZone("Europe/Brussels"));
    source.setLenientStringToNumber(true);
    source.setLargeTextField(true);
    source.setRoundingType("HALF_UP");
    source.setOriginalColumnType(4);
    source.setOriginalColumnTypeName("INT4");
    source.setOriginalPrecision(10);
    source.setOriginalScale(2);
    source.setOriginalAutoIncrement(true);
    source.setOriginalNullable(1);
    source.setOriginalSigned(true);
  }

  private static void assertCustomized(IValueMeta source, IValueMeta target) {
    assertEquals("#.##", target.getConversionMask());
    assertEquals(",", target.getDecimalSymbol());
    assertEquals(".", target.getGroupingSymbol());
    assertEquals(IValueMeta.TRIM_TYPE_LEFT, target.getTrimType());
    assertEquals("UTF-8", target.getStringEncoding());
    assertEquals("comments", target.getComments());
    assertEquals("origin", target.getOrigin());
    assertTrue(target.isCaseInsensitive());
    assertFalse(target.isCollatorDisabled());
    assertEquals(2, target.getCollatorStrength());
    assertTrue(target.isDateFormatLenient());
    assertEquals(Locale.GERMANY, target.getDateFormatLocale());
    assertEquals("Europe/Brussels", target.getDateFormatTimeZone().getID());
    assertTrue(target.isLenientStringToNumber());
    assertTrue(target.isLargeTextField());
    assertEquals("HALF_UP", target.getRoundingType());
    assertEquals(4, target.getOriginalColumnType());
    assertEquals("INT4", target.getOriginalColumnTypeName());
    assertEquals(10, target.getOriginalPrecision());
    assertEquals(2, target.getOriginalScale());
    assertTrue(target.isOriginalAutoIncrement());
    assertEquals(1, target.isOriginalNullable());
    assertTrue(target.isOriginalSigned());
    assertEquals(source.getStorageType(), target.getStorageType());
  }

  // Names and ids
  //

  @ParameterizedTest
  @MethodSource("builtIns")
  void getValueMetaNameGivesThePluginName(BuiltIn builtIn) {
    assertEquals(builtIn.name(), ValueMetaFactory.getValueMetaName(builtIn.type()));
  }

  @ParameterizedTest
  @MethodSource("unknownTypes")
  void getValueMetaNameGivesADashForAnUnknownId(int type) {
    assertEquals("-", ValueMetaFactory.getValueMetaName(type));
  }

  @ParameterizedTest
  @MethodSource("builtIns")
  void getIdForValueMetaMatchesTheNameIgnoringCase(BuiltIn builtIn) {
    String name = builtIn.name();
    assertEquals(builtIn.type(), ValueMetaFactory.getIdForValueMeta(name));
    assertEquals(builtIn.type(), ValueMetaFactory.getIdForValueMeta(name.toUpperCase(Locale.ROOT)));
    assertEquals(builtIn.type(), ValueMetaFactory.getIdForValueMeta(name.toLowerCase(Locale.ROOT)));
    StringBuilder mixed = new StringBuilder();
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      mixed.append(i % 2 == 0 ? Character.toLowerCase(c) : Character.toUpperCase(c));
    }
    assertEquals(builtIn.type(), ValueMetaFactory.getIdForValueMeta(mixed.toString()));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        " ",
        "Strin",
        "Strings",
        " String",
        "String ",
        "InternetAddress",
        "Internet  Address",
        "Avro",
        "Json Object",
        "-",
        "2",
        "Unknown"
      })
  void getIdForValueMetaGivesNoneForAnUnknownName(String name) {
    assertEquals(IValueMeta.TYPE_NONE, ValueMetaFactory.getIdForValueMeta(name));
  }

  @Test
  void getIdForValueMetaGivesNoneForNull() {
    assertEquals(IValueMeta.TYPE_NONE, ValueMetaFactory.getIdForValueMeta(null));
  }

  @Test
  void getValueMetaNamesListsTheSelectableTypesInRegistryOrder() {
    assertArrayEquals(
        new String[] {
          "Avro Record",
          "BigNumber",
          "Binary",
          "Boolean",
          "Date",
          "Integer",
          "Internet Address",
          "JSON",
          "Number",
          "String",
          "Timestamp"
        },
        ValueMetaFactory.getValueMetaNames());
  }

  @Test
  void getAllValueMetaNamesAlsoListsSerializableButNotNone() {
    assertArrayEquals(
        new String[] {
          "Avro Record",
          "BigNumber",
          "Binary",
          "Boolean",
          "Date",
          "Integer",
          "Internet Address",
          "JSON",
          "Number",
          "Serializable",
          "String",
          "Timestamp"
        },
        ValueMetaFactory.getAllValueMetaNames());
  }

  @Test
  void nameArraysAreNewOnEveryCall() {
    String[] names = ValueMetaFactory.getValueMetaNames();
    String[] all = ValueMetaFactory.getAllValueMetaNames();
    String[] expectedNames = names.clone();
    String[] expectedAll = all.clone();

    names[0] = "changed";
    all[0] = "changed";

    assertNotSame(names, ValueMetaFactory.getValueMetaNames());
    assertArrayEquals(expectedNames, ValueMetaFactory.getValueMetaNames());
    assertArrayEquals(expectedAll, ValueMetaFactory.getAllValueMetaNames());
  }

  @Test
  void getValueMetaPluginClassesGivesOneNewInstancePerPluginInRegistryOrder() throws Exception {
    List<IValueMeta> first = ValueMetaFactory.getValueMetaPluginClasses();
    List<IValueMeta> second = ValueMetaFactory.getValueMetaPluginClasses();

    List<Class<?>> expectedClasses =
        List.of(
            ValueMetaAvroRecord.class,
            ValueMetaBigNumber.class,
            ValueMetaBinary.class,
            ValueMetaBoolean.class,
            ValueMetaDate.class,
            ValueMetaInteger.class,
            ValueMetaInternetAddress.class,
            ValueMetaJson.class,
            ValueMetaNone.class,
            ValueMetaNumber.class,
            ValueMetaSerializable.class,
            ValueMetaString.class,
            ValueMetaTimestamp.class);
    assertEquals(expectedClasses, first.stream().map(Object::getClass).toList());
    assertEquals(expectedClasses, second.stream().map(Object::getClass).toList());
    for (int i = 0; i < first.size(); i++) {
      assertNotSame(first.get(i), second.get(i));
      assertNull(first.get(i).getName());
      assertEquals(-1, first.get(i).getLength());
      assertEquals(BUILT_IN_NAMES_IN_ORDER.get(i), first.get(i).getTypeDesc());
    }
  }

  @Test
  void namesAndIdsRoundTripForEverySelectableType() throws Exception {
    for (String name : ValueMetaFactory.getAllValueMetaNames()) {
      int id = ValueMetaFactory.getIdForValueMeta(name);
      assertEquals(name, ValueMetaFactory.getValueMetaName(id));
      assertEquals(name, ValueMetaFactory.createValueMeta(id).getTypeDesc());
    }
  }

  // guessValueMetaInterface
  //

  @Test
  void guessValueMetaInterfaceForCommonJavaObjects() {
    assertSame(
        ValueMetaBigNumber.class,
        ValueMetaFactory.guessValueMetaInterface(BigDecimal.TEN).getClass());
    assertSame(ValueMetaNumber.class, ValueMetaFactory.guessValueMetaInterface(1.5d).getClass());
    assertSame(ValueMetaInteger.class, ValueMetaFactory.guessValueMetaInterface(1L).getClass());
    assertSame(ValueMetaString.class, ValueMetaFactory.guessValueMetaInterface("x").getClass());
    assertSame(
        ValueMetaDate.class, ValueMetaFactory.guessValueMetaInterface(new Date()).getClass());
    // Subclasses of Date are guessed as Date, not as Timestamp
    assertSame(
        ValueMetaDate.class,
        ValueMetaFactory.guessValueMetaInterface(new Timestamp(0L)).getClass());
    assertSame(
        ValueMetaDate.class,
        ValueMetaFactory.guessValueMetaInterface(new java.sql.Date(0L)).getClass());
    assertSame(ValueMetaBoolean.class, ValueMetaFactory.guessValueMetaInterface(true).getClass());
    assertSame(
        ValueMetaBinary.class, ValueMetaFactory.guessValueMetaInterface(new byte[0]).getClass());
    assertSame(
        ValueMetaJson.class,
        ValueMetaFactory.guessValueMetaInterface(JsonNodeFactory.instance.objectNode()).getClass());
    assertSame(
        ValueMetaJson.class,
        ValueMetaFactory.guessValueMetaInterface(JsonNodeFactory.instance.textNode("x"))
            .getClass());

    // Not guessed: no widening of other numbers, characters or arbitrary objects
    assertNull(ValueMetaFactory.guessValueMetaInterface(1));
    assertNull(ValueMetaFactory.guessValueMetaInterface(BigInteger.ONE));
    assertNull(ValueMetaFactory.guessValueMetaInterface(1.5f));
    assertNull(ValueMetaFactory.guessValueMetaInterface('c'));
    assertNull(ValueMetaFactory.guessValueMetaInterface(new Object()));
    assertNull(ValueMetaFactory.guessValueMetaInterface(new int[0]));
    assertNull(ValueMetaFactory.guessValueMetaInterface(null));
  }

  @Test
  void guessValueMetaInterfaceGivesANewUnnamedInstanceEveryTime() {
    IValueMeta first = ValueMetaFactory.guessValueMetaInterface("x");
    IValueMeta second = ValueMetaFactory.guessValueMetaInterface("x");
    assertNotSame(first, second);
    assertNull(first.getName());
    assertEquals(-1, first.getLength());
  }

  // loadValueMeta / loadValueMetaFromJson create through the factory too
  //

  @ParameterizedTest
  @MethodSource("builtIns")
  void loadValueMetaReadsWhatWriteMetaWrote(BuiltIn builtIn) throws Exception {
    IValueMeta source = ValueMetaFactory.createValueMeta("field", builtIn.type(), 10, 2);
    source.setConversionMask("mask");
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      source.writeMeta(out);
    }

    IValueMeta loaded;
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      loaded = ValueMetaFactory.loadValueMeta(in);
    }

    assertSame(builtIn.valueMetaClass(), loaded.getClass());
    assertEquals("field", loaded.getName());
    assertEquals(10, loaded.getLength());
    assertEquals(source.getPrecision(), loaded.getPrecision());
    assertEquals("mask", loaded.getConversionMask());
  }

  @Test
  void loadValueMetaWrapsAnUnknownTypeInAFileException() throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeInt(12);
    }
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      HopFileException e =
          assertThrows(HopFileException.class, () -> ValueMetaFactory.loadValueMeta(in));
      assertSame(HopFileException.class, e.getClass());
      assertInstanceOf(HopPluginException.class, e.getCause());
      assertEquals(unknownTypeMessage(12), e.getCause().getMessage());
      assertTrue(e.getMessage().contains("Unable to read value metadata from input stream"));
    }
  }

  @Test
  void loadValueMetaGivesAnEofExceptionOnAnEmptyStream() {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(new byte[0]));
    assertThrows(HopEofException.class, () -> ValueMetaFactory.loadValueMeta(in));
  }

  @ParameterizedTest
  @MethodSource("builtIns")
  @SuppressWarnings("unchecked")
  void loadValueMetaFromJsonCreatesTheType(BuiltIn builtIn) throws Exception {
    JSONObject json = new JSONObject();
    json.put("name", "field");
    json.put("type", (long) builtIn.type());
    json.put("length", 10L);
    json.put("precision", 2L);
    json.put("conversionMask", "mask");

    IValueMeta loaded = ValueMetaFactory.loadValueMetaFromJson(json);

    assertSame(builtIn.valueMetaClass(), loaded.getClass());
    assertEquals("field", loaded.getName());
    assertEquals(10, loaded.getLength());
    assertEquals("mask", loaded.getConversionMask());
  }

  @Test
  @SuppressWarnings("unchecked")
  void loadValueMetaFromJsonFailsForAnUnknownType() {
    JSONObject json = new JSONObject();
    json.put("name", "field");
    json.put("type", 12L);
    HopPluginException e =
        assertThrows(HopPluginException.class, () -> ValueMetaFactory.loadValueMetaFromJson(json));
    assertEquals(unknownTypeMessage(12), e.getMessage());
  }
}
