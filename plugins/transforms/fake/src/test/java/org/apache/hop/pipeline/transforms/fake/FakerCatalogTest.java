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

package org.apache.hop.pipeline.transforms.fake;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import net.datafaker.Faker;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;

class FakerCatalogTest {

  @Test
  void catalogIsDiscoveredAndLarge() {
    List<FakerCatalog.Generator> generators = FakerCatalog.getGenerators();
    assertFalse(generators.isEmpty());
    // DataFaker exposes hundreds of providers; the catalog should be well into the thousands.
    assertTrue(generators.size() > 1000, "Unexpectedly small catalog: " + generators.size());
  }

  @Test
  void featuredCategoriesAreACommonSubsetThatExcludesNovelty() {
    // Featured = the everyday providers the browser surfaces by default.
    assertTrue(FakerCatalog.isFeaturedCategory("name"));
    assertTrue(FakerCatalog.isFeaturedCategory("address"));
    assertTrue(FakerCatalog.isFeaturedCategory("internet"));
    // Novelty/fandom providers are hidden until "Show all".
    assertFalse(FakerCatalog.isFeaturedCategory("harryPotter"));
    assertFalse(FakerCatalog.isFeaturedCategory("zelda"));
    // It is a strict subset: fewer featured generators than the full catalog, but not empty.
    long featured =
        FakerCatalog.getGenerators().stream()
            .filter(g -> FakerCatalog.isFeaturedCategory(g.category()))
            .count();
    assertTrue(featured > 0, "Featured set should not be empty");
    assertTrue(featured < FakerCatalog.getGenerators().size(), "Featured set should be a subset");
  }

  @Test
  void catalogExposesParameterizedGenerators() {
    boolean hasParameterized =
        FakerCatalog.getGenerators().stream().anyMatch(FakerCatalog.Generator::hasParameters);
    assertTrue(hasParameterized, "No parameterized generators were discovered");
  }

  @Test
  void simpleZeroArgGeneratorProducesValue() throws HopException {
    FakeField field = new FakeField("firstName", "name", "firstName");
    Object value = FakerCatalog.bind(new Faker(), field, new Variables()).produce();
    assertInstanceOf(String.class, value);
    assertFalse(((String) value).isEmpty());
  }

  @Test
  void parameterizedGeneratorResolvesOverloadAndStaysInRange() throws HopException {
    FakeField field =
        new FakeField(
            "age",
            "number",
            "numberBetween",
            List.of(new FakeArgument("int", "18"), new FakeArgument("int", "65")));
    Object value = FakerCatalog.bind(new Faker(), field, new Variables()).produce();
    assertInstanceOf(Long.class, value);
    long age = (Long) value;
    assertTrue(age >= 18 && age < 65, "Generated value out of range: " + age);
  }

  @Test
  void argumentValuesResolvePipelineVariables() throws HopException {
    Variables variables = new Variables();
    variables.setVariable("MIN_AGE", "40");
    variables.setVariable("MAX_AGE", "41");
    FakeField field =
        new FakeField(
            "age",
            "number",
            "numberBetween",
            List.of(new FakeArgument("int", "${MIN_AGE}"), new FakeArgument("int", "${MAX_AGE}")));
    assertEquals(40L, FakerCatalog.bind(new Faker(), field, variables).produce());
  }

  @Test
  void legacyCapitalizedTypeNameStillResolves() throws HopException {
    // A pipeline saved before the DataFaker migration stored "Name" rather than "name".
    FakeField field = new FakeField("firstName", "Name", "firstName");
    assertNotNull(FakerCatalog.bind(new Faker(), field, new Variables()).produce());
  }

  @Test
  void valueMetaMappingIsBroadened() {
    assertEquals(IValueMeta.TYPE_STRING, FakerCatalog.valueMetaType(String.class));
    assertEquals(IValueMeta.TYPE_INTEGER, FakerCatalog.valueMetaType(int.class));
    assertEquals(IValueMeta.TYPE_INTEGER, FakerCatalog.valueMetaType(long.class));
    assertEquals(IValueMeta.TYPE_NUMBER, FakerCatalog.valueMetaType(double.class));
    assertEquals(IValueMeta.TYPE_BOOLEAN, FakerCatalog.valueMetaType(boolean.class));
    assertEquals(IValueMeta.TYPE_DATE, FakerCatalog.valueMetaType(java.util.Date.class));
    assertEquals(IValueMeta.TYPE_DATE, FakerCatalog.valueMetaType(java.sql.Timestamp.class));
    assertEquals(-1, FakerCatalog.valueMetaType(void.class));
    assertEquals(-1, FakerCatalog.valueMetaType(int[].class));
  }

  @Test
  void parameterTypeNamesRoundTrip() throws ClassNotFoundException {
    assertEquals(int.class, FakerCatalog.parameterClass(FakerCatalog.parameterTypeName(int.class)));
    assertEquals(
        String.class, FakerCatalog.parameterClass(FakerCatalog.parameterTypeName(String.class)));
    assertEquals(
        boolean.class, FakerCatalog.parameterClass(FakerCatalog.parameterTypeName(boolean.class)));
  }

  @Test
  void resolveGeneratorAppliesLegacyAliases() {
    // Generators DataFaker renamed, moved or dropped redirect to their equivalent.
    assertArrayEquals(
        new String[] {"hashing", "md5"}, FakerCatalog.resolveGenerator("Crypto", "md5"));
    assertArrayEquals(
        new String[] {"bloodtype", "bloodGroup"},
        FakerCatalog.resolveGenerator("Name", "bloodGroup"));
    assertArrayEquals(
        new String[] {"name", "firstName"}, FakerCatalog.resolveGenerator("Address", "firstName"));
    // A generator that was not touched passes straight through.
    assertArrayEquals(
        new String[] {"name", "firstName"}, FakerCatalog.resolveGenerator("name", "firstName"));
  }

  @Test
  void everyRemovedJavaFakerGeneratorStillResolves() throws HopException {
    // The 12 zero-arg generators DataFaker dropped/renamed - all must still produce a value
    // so that pipelines built on the old JavaFaker library keep working unchanged.
    Faker faker = new Faker();
    String[][] legacy = {
      {"Crypto", "md5"},
      {"Crypto", "sha1"},
      {"Crypto", "sha256"},
      {"Crypto", "sha512"},
      {"Name", "bloodGroup"},
      {"Internet", "avatar"},
      {"Internet", "userAgentAny"},
      {"Address", "firstName"},
      {"Address", "lastName"},
      {"Commerce", "color"},
      {"HitchhikersGuideToTheGalaxy", "specie"},
      {"StarTrek", "specie"},
    };
    for (String[] generator : legacy) {
      FakeField field = new FakeField("f", generator[0], generator[1]);
      Object value = FakerCatalog.bind(faker, field, new Variables()).produce();
      assertNotNull(
          value, generator[0] + "." + generator[1] + " must still resolve via the alias table");
    }
  }

  @Test
  void descriptorOverlayUsesCuratedTextWhenAvailable() {
    FakerCatalog.Generator numberBetween = findGenerator("number", "numberBetween", 2);
    assertEquals("Number between", numberBetween.displayTitle());
    assertFalse(numberBetween.description().isEmpty());
    assertTrue(
        numberBetween.parameterLabel(0).startsWith("Minimum"),
        "Expected a curated parameter label, got: " + numberBetween.parameterLabel(0));
  }

  @Test
  void shortParameterLabelsUseCuratedNamesTrimmedForInlineDisplay() {
    // Curated labels ("Minimum (inclusive)", "Maximum (exclusive)") are shortened for the tree.
    FakerCatalog.Generator numberBetween = findGenerator("number", "numberBetween", 2);
    assertEquals("Minimum", numberBetween.shortParameterLabel(0));
    assertEquals("Maximum", numberBetween.shortParameterLabel(1));
    assertEquals("Minimum, Maximum", numberBetween.parameterDisplaySignature());
  }

  @Test
  void shortParameterLabelFallsBackToTheTypeWhenUncurated() {
    // internet.url(boolean x6) has no curated parameter label: show the raw type as a last resort.
    FakerCatalog.Generator url = findGenerator("internet", "url", 6);
    assertEquals(url.parameterTypes()[0].getSimpleName(), url.shortParameterLabel(0));
  }

  @Test
  void curatedParameterLabelsGuardsTheOverlayFile() {
    // A handful of representative generators to catch accidental generators.properties breakage.
    assertEquals(
        "Width, Height", findGenerator("internet", "image", 2).parameterDisplaySignature());
    assertEquals("Prefix", findGenerator("internet", "macAddress", 1).parameterDisplaySignature());
    assertEquals(
        "Minimum port, Maximum port",
        findGenerator("internet", "port", 2).parameterDisplaySignature());
    assertEquals(
        "Minimum, Maximum",
        findGenerator("weather", "temperatureCelsius", 2).parameterDisplaySignature());
    assertEquals(
        "Directory, Base name, Extension, Separator",
        findGenerator("file", "fileName", 4).parameterDisplaySignature());
  }

  @Test
  void signatureKeysDisambiguateSameArityOverloads() {
    // date.future(int, int, TimeUnit) and date.future(int, TimeUnit, String) both have arity 3.
    FakerCatalog.Generator range =
        findOverload("date", "future", int.class, int.class, TimeUnit.class);
    assertEquals("Maximum amount, Minimum amount, Time unit", range.parameterDisplaySignature());
    FakerCatalog.Generator pattern =
        findOverload("date", "future", int.class, TimeUnit.class, String.class);
    assertEquals(
        "Maximum amount, Time unit, Date format pattern", pattern.parameterDisplaySignature());

    // lorem.characters(boolean) must read "Include uppercase", NOT "Number of characters".
    assertEquals(
        "Include uppercase",
        findOverload("lorem", "characters", boolean.class).shortParameterLabel(0));
    assertEquals(
        "Number of characters",
        findOverload("lorem", "characters", int.class).shortParameterLabel(0));
  }

  @Test
  void loremCharactersLabelsMatchTheDataFakerSource() {
    // (int, boolean, boolean) = fixedCount, includeUppercase, includeDigit - the 3rd flag is DIGIT,
    // not "special" (special only appears once there are four flags).
    assertEquals(
        "Number of characters, Include uppercase, Include digits",
        findOverload("lorem", "characters", int.class, boolean.class, boolean.class)
            .parameterDisplaySignature());
    // (int, boolean, boolean, boolean) = fixedCount, uppercase, special, digit.
    assertEquals(
        "Number of characters, Include uppercase, Include special characters, Include digits",
        findOverload("lorem", "characters", int.class, boolean.class, boolean.class, boolean.class)
            .parameterDisplaySignature());
    // (int, int, boolean, boolean) = min, max, uppercase, digit.
    assertEquals(
        "Minimum length, Maximum length, Include uppercase, Include digits",
        findOverload("lorem", "characters", int.class, int.class, boolean.class, boolean.class)
            .parameterDisplaySignature());
  }

  private static FakerCatalog.Generator findOverload(
      String category, String function, Class<?>... types) {
    return FakerCatalog.getGenerators().stream()
        .filter(
            g ->
                g.category().equals(category)
                    && g.functionName().equals(function)
                    && java.util.Arrays.equals(g.parameterTypes(), types))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Overload not found: " + category + "." + function));
  }

  @Test
  void defaultArgumentValuesUseIncreasingNumericsSoRangesArePreviewable() throws HopException {
    // Numeric params get strictly increasing values so (min, max) ranges are valid.
    String[] values =
        FakerCatalog.defaultArgumentValues(new Class<?>[] {int.class, int.class, boolean.class});
    assertEquals("8", values[0]);
    assertEquals("16", values[1]);
    assertEquals("true", values[2]);

    // A generator that threw with a naive (1, 1, ...) now produces a value with these defaults -
    // this is what keeps the browser's live preview from erroring.
    FakerCatalog.Generator g =
        findOverload(
            "lorem",
            "characters",
            int.class,
            int.class,
            boolean.class,
            boolean.class,
            boolean.class);
    Class<?>[] types = g.parameterTypes();
    String[] defaults = FakerCatalog.defaultArgumentValues(types);
    List<FakeArgument> args = new java.util.ArrayList<>();
    for (int i = 0; i < types.length; i++) {
      args.add(new FakeArgument(FakerCatalog.parameterTypeName(types[i]), defaults[i]));
    }
    FakeField field = new FakeField("f", g.category(), g.functionName(), args);
    assertNotNull(FakerCatalog.bind(new Faker(), field, new Variables()).produce());
  }

  @Test
  void descriptorOverlayFallsBackToReflection() {
    // name.lastName has no descriptor entry: the title is the prettified function name and the
    // description is empty.
    FakerCatalog.Generator lastName = findGenerator("name", "lastName", 0);
    assertEquals("Last Name", lastName.displayTitle());
    assertEquals("", lastName.description());
    // A parameterized generator with no curated label falls back to "Parameter N (type)".
    // internet.url(boolean x6) is deliberately left uncurated (ambiguous flags).
    FakerCatalog.Generator url = findGenerator("internet", "url", 6);
    assertTrue(
        url.parameterLabel(0).startsWith("Parameter 1"),
        "Expected a reflected parameter label, got: " + url.parameterLabel(0));
  }

  // -------------------------------------------------------------------------------------------
  // Argument conversion
  // -------------------------------------------------------------------------------------------

  @Test
  void convertArgumentHandsBackEveryPrimitiveType() throws HopException {
    assertEquals(5, FakerCatalog.convertArgument("int", "5"));
    assertEquals(5L, FakerCatalog.convertArgument("long", "5"));
    assertEquals(1.5d, FakerCatalog.convertArgument("double", "1.5"));
    assertEquals(1.5f, FakerCatalog.convertArgument("float", "1.5"));
    assertEquals((short) 3, FakerCatalog.convertArgument("short", "3"));
    assertEquals((byte) 2, FakerCatalog.convertArgument("byte", "2"));
    assertEquals(true, FakerCatalog.convertArgument("boolean", "true"));
  }

  @Test
  void convertArgumentTakesFirstCharForCharAndSpaceWhenBlank() throws HopException {
    assertEquals('a', FakerCatalog.convertArgument("char", "abc"));
    assertEquals(' ', FakerCatalog.convertArgument("char", ""));
  }

  @Test
  void convertArgumentKeepsRawStringUntrimmed() throws HopException {
    assertEquals("  spaced  ", FakerCatalog.convertArgument("string", "  spaced  "));
  }

  @Test
  void convertArgumentResolvesEnumConstant() throws HopException {
    assertEquals(DayOfWeek.MONDAY, FakerCatalog.convertArgument("java.time.DayOfWeek", "MONDAY"));
  }

  @Test
  void convertArgumentWrapsFailuresInHopException() {
    assertThrows(HopException.class, () -> FakerCatalog.convertArgument("int", "not-a-number"));
    assertThrows(
        HopException.class, () -> FakerCatalog.convertArgument("java.time.DayOfWeek", "FUNDAY"));
  }

  @Test
  void defaultArgumentValueIsSensiblePerType() {
    assertEquals("1.0", FakerCatalog.defaultArgumentValue(double.class));
    assertEquals("1.0", FakerCatalog.defaultArgumentValue(float.class));
    assertEquals("true", FakerCatalog.defaultArgumentValue(boolean.class));
    assertEquals("a", FakerCatalog.defaultArgumentValue(char.class));
    assertEquals("1", FakerCatalog.defaultArgumentValue(int.class));
    assertEquals("1", FakerCatalog.defaultArgumentValue(Integer.class));
    assertEquals("", FakerCatalog.defaultArgumentValue(String.class));
    // The first constant of an enum is a safe pre-fill.
    assertEquals("MONDAY", FakerCatalog.defaultArgumentValue(DayOfWeek.class));
  }

  // -------------------------------------------------------------------------------------------
  // Parameter type plumbing
  // -------------------------------------------------------------------------------------------

  @Test
  void parameterClassResolvesPrimitivesStringAndEnums() throws ClassNotFoundException {
    assertEquals(int.class, FakerCatalog.parameterClass("int"));
    assertEquals(long.class, FakerCatalog.parameterClass("long"));
    assertEquals(double.class, FakerCatalog.parameterClass("double"));
    assertEquals(float.class, FakerCatalog.parameterClass("float"));
    assertEquals(short.class, FakerCatalog.parameterClass("short"));
    assertEquals(byte.class, FakerCatalog.parameterClass("byte"));
    assertEquals(boolean.class, FakerCatalog.parameterClass("boolean"));
    assertEquals(char.class, FakerCatalog.parameterClass("char"));
    assertEquals(String.class, FakerCatalog.parameterClass("string"));
    assertEquals(DayOfWeek.class, FakerCatalog.parameterClass("java.time.DayOfWeek"));
  }

  @Test
  void parameterClassRejectsUnknownType() {
    assertThrows(
        ClassNotFoundException.class, () -> FakerCatalog.parameterClass("com.example.Nope"));
  }

  @Test
  void parameterTypeNameIsPersistableAndInverse() {
    assertEquals("int", FakerCatalog.parameterTypeName(int.class));
    assertEquals("string", FakerCatalog.parameterTypeName(String.class));
    assertEquals("boolean", FakerCatalog.parameterTypeName(boolean.class));
    assertEquals("java.time.DayOfWeek", FakerCatalog.parameterTypeName(DayOfWeek.class));
  }

  @Test
  void supportedParameterTypesAreEditableOnesOnly() {
    assertTrue(FakerCatalog.isSupportedParameterType(int.class));
    assertTrue(FakerCatalog.isSupportedParameterType(String.class));
    assertTrue(FakerCatalog.isSupportedParameterType(CharSequence.class));
    assertTrue(FakerCatalog.isSupportedParameterType(DayOfWeek.class));
    assertFalse(FakerCatalog.isSupportedParameterType(void.class));
    assertFalse(FakerCatalog.isSupportedParameterType(Object.class));
    assertFalse(FakerCatalog.isSupportedParameterType(int[].class));
  }

  // -------------------------------------------------------------------------------------------
  // Return types / value metadata
  // -------------------------------------------------------------------------------------------

  @Test
  void valueMetaTypeCoversTheFullReturnTypeMatrix() {
    assertEquals(IValueMeta.TYPE_STRING, FakerCatalog.valueMetaType(char.class));
    assertEquals(IValueMeta.TYPE_STRING, FakerCatalog.valueMetaType(DayOfWeek.class));
    assertEquals(IValueMeta.TYPE_INTEGER, FakerCatalog.valueMetaType(java.math.BigInteger.class));
    assertEquals(IValueMeta.TYPE_NUMBER, FakerCatalog.valueMetaType(float.class));
    assertEquals(IValueMeta.TYPE_NUMBER, FakerCatalog.valueMetaType(java.math.BigDecimal.class));
    assertEquals(IValueMeta.TYPE_DATE, FakerCatalog.valueMetaType(LocalDate.class));
    assertEquals(IValueMeta.TYPE_DATE, FakerCatalog.valueMetaType(LocalDateTime.class));
    assertEquals(IValueMeta.TYPE_DATE, FakerCatalog.valueMetaType(Instant.class));
    assertEquals(-1, FakerCatalog.valueMetaType(Void.class));
    assertEquals(-1, FakerCatalog.valueMetaType(Object.class));
  }

  @Test
  void createValueMetaMapsEachTypeToItsConcreteClass() {
    assertInstanceOf(
        ValueMetaInteger.class, FakerCatalog.createValueMeta("i", IValueMeta.TYPE_INTEGER));
    assertInstanceOf(
        ValueMetaNumber.class, FakerCatalog.createValueMeta("n", IValueMeta.TYPE_NUMBER));
    assertInstanceOf(
        ValueMetaBoolean.class, FakerCatalog.createValueMeta("b", IValueMeta.TYPE_BOOLEAN));
    assertInstanceOf(ValueMetaDate.class, FakerCatalog.createValueMeta("d", IValueMeta.TYPE_DATE));
    ValueMetaString string =
        assertInstanceOf(
            ValueMetaString.class, FakerCatalog.createValueMeta("s", IValueMeta.TYPE_STRING));
    assertEquals("s", string.getName());
  }

  @Test
  void resolveValueMetaTypeResolvesFieldsWithoutAFaker() throws HopException {
    assertEquals(
        IValueMeta.TYPE_STRING,
        FakerCatalog.resolveValueMetaType(new FakeField("f", "name", "firstName")));
    assertEquals(
        IValueMeta.TYPE_INTEGER,
        FakerCatalog.resolveValueMetaType(
            new FakeField(
                "age",
                "number",
                "numberBetween",
                List.of(new FakeArgument("int", "1"), new FakeArgument("int", "9")))));
  }

  @Test
  void resolveValueMetaTypeThrowsForUnknownGenerator() {
    assertThrows(
        HopException.class,
        () -> FakerCatalog.resolveValueMetaType(new FakeField("f", "nope", "nope")));
  }

  // -------------------------------------------------------------------------------------------
  // Value coercion
  // -------------------------------------------------------------------------------------------

  @Test
  void coerceReturnsNullForNullRegardlessOfType() {
    assertNull(FakerCatalog.coerce(null, IValueMeta.TYPE_STRING));
    assertNull(FakerCatalog.coerce(null, IValueMeta.TYPE_INTEGER));
    assertNull(FakerCatalog.coerce(null, IValueMeta.TYPE_DATE));
  }

  @Test
  void coerceToIntegerAcceptsNumbersCharsAndStrings() {
    assertEquals(5L, FakerCatalog.coerce(5, IValueMeta.TYPE_INTEGER));
    assertEquals((long) 'A', FakerCatalog.coerce('A', IValueMeta.TYPE_INTEGER));
    assertEquals(7L, FakerCatalog.coerce("7", IValueMeta.TYPE_INTEGER));
  }

  @Test
  void coerceToNumberAcceptsNumbersAndStrings() {
    assertEquals(3.0d, FakerCatalog.coerce(3, IValueMeta.TYPE_NUMBER));
    assertEquals(2.5d, FakerCatalog.coerce("2.5", IValueMeta.TYPE_NUMBER));
  }

  @Test
  void coerceToBooleanAcceptsBooleansAndStrings() {
    assertEquals(Boolean.TRUE, FakerCatalog.coerce(true, IValueMeta.TYPE_BOOLEAN));
    assertEquals(Boolean.TRUE, FakerCatalog.coerce("true", IValueMeta.TYPE_BOOLEAN));
    assertEquals(Boolean.FALSE, FakerCatalog.coerce("nope", IValueMeta.TYPE_BOOLEAN));
  }

  @Test
  void coerceToDateNormalisesEveryTemporalType() {
    Date date = new Date(0L);
    assertEquals(date, FakerCatalog.coerce(date, IValueMeta.TYPE_DATE));
    assertInstanceOf(
        Date.class, FakerCatalog.coerce(LocalDate.of(2020, 1, 1), IValueMeta.TYPE_DATE));
    assertInstanceOf(Date.class, FakerCatalog.coerce(LocalDateTime.now(), IValueMeta.TYPE_DATE));
    assertInstanceOf(Date.class, FakerCatalog.coerce(Instant.now(), IValueMeta.TYPE_DATE));
  }

  @Test
  void coerceToStringStringifiesCharArraysAndOtherValues() {
    assertEquals("hi", FakerCatalog.coerce(new char[] {'h', 'i'}, IValueMeta.TYPE_STRING));
    assertEquals("123", FakerCatalog.coerce(123, IValueMeta.TYPE_STRING));
  }

  // -------------------------------------------------------------------------------------------
  // Faker construction & labels
  // -------------------------------------------------------------------------------------------

  @Test
  void createFakerFallsBackToDefaultForBlankLocale() throws HopException {
    assertNotNull(
        FakerCatalog.bind(
                FakerCatalog.createFaker(""),
                new FakeField("f", "name", "firstName"),
                new Variables())
            .produce());
    assertNotNull(
        FakerCatalog.bind(
                FakerCatalog.createFaker(null),
                new FakeField("f", "name", "firstName"),
                new Variables())
            .produce());
  }

  @Test
  void createFakerAcceptsUnderscoreStyleLocaleTags() throws HopException {
    // "de_DE" must be normalised to the "de-DE" language tag DataFaker expects.
    Object value =
        FakerCatalog.bind(
                FakerCatalog.createFaker("de_DE"),
                new FakeField("f", "name", "firstName"),
                new Variables())
            .produce();
    assertInstanceOf(String.class, value);
    assertFalse(((String) value).isEmpty());
  }

  @Test
  void prettyLabelSplitsCamelCaseAndCapitalises() {
    assertEquals("Phone Number", FakerCatalog.prettyLabel("phoneNumber"));
    assertEquals("Name", FakerCatalog.prettyLabel("name"));
    assertEquals("A", FakerCatalog.prettyLabel("a"));
    assertEquals("", FakerCatalog.prettyLabel(""));
  }

  // -------------------------------------------------------------------------------------------
  // Generator record surface
  // -------------------------------------------------------------------------------------------

  @Test
  void zeroArgGeneratorReportsItsReflectionDerivedShape() {
    FakerCatalog.Generator firstName = findGenerator("name", "firstName", 0);
    assertEquals("firstName", firstName.functionName());
    assertEquals(String.class, firstName.returnType());
    assertFalse(firstName.hasParameters());
    assertEquals("", firstName.parameterSignature());
    assertEquals("firstName()", firstName.displayName());
  }

  @Test
  void parameterizedGeneratorRendersSignatureAndTypedLabels() {
    // numberBetween is overloaded (e.g. int,int and long,long); derive the expected rendering from
    // whichever arity-2 overload the catalog surfaces rather than hard-coding the primitive type.
    FakerCatalog.Generator numberBetween = findGenerator("number", "numberBetween", 2);
    assertTrue(numberBetween.hasParameters());

    Class<?>[] types = numberBetween.parameterTypes();
    assertEquals(2, types.length);
    String signature = types[0].getSimpleName() + ", " + types[1].getSimpleName();
    assertEquals(signature, numberBetween.parameterSignature());
    assertEquals("numberBetween(" + signature + ")", numberBetween.displayName());
    // The parameter type is always appended, whether or not a curated label exists.
    assertTrue(numberBetween.parameterLabel(0).endsWith("(" + types[0].getSimpleName() + ")"));
  }

  @Test
  void bindWrapsUnknownGeneratorInHopException() {
    assertThrows(
        HopException.class,
        () -> FakerCatalog.bind(new Faker(), new FakeField("f", "nope", "nope"), new Variables()));
  }

  private static FakerCatalog.Generator findGenerator(String category, String function, int arity) {
    return FakerCatalog.getGenerators().stream()
        .filter(
            g ->
                g.category().equals(category)
                    && g.functionName().equals(function)
                    && g.parameterTypes().length == arity)
        .findFirst()
        .orElseThrow(() -> new AssertionError("Generator not found: " + category + "." + function));
  }
}
