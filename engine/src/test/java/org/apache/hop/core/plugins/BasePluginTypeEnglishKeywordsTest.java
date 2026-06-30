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

package org.apache.hop.core.plugins;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.i18n.LanguageChoice;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Unit tests for the English-locale search aliases added by {@link BasePluginType} (issue #2633).
 * When the UI runs in a non-English language, a transform/action must still be findable in the
 * context dialog by its original English name, category and keywords.
 *
 * <p>The tests drive the real {@link TransformPluginType} (a {@link BasePluginType} subclass) with
 * a synthetic {@code @Transform} fixture whose i18n keys point at the test bundle {@code
 * org/apache/hop/core/plugins/englishsearch/messages/messages*.properties} (English base + {@code
 * _fr_FR}).
 */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class BasePluginTypeEnglishKeywordsTest {

  private static final String NAME_KEY =
      "i18n:org.apache.hop.core.plugins.englishsearch:EnglishSearch.Name";
  private static final String CATEGORY_KEY =
      "i18n:org.apache.hop.core.plugins.englishsearch:EnglishSearch.Category";
  private static final String KEYWORDS_KEY =
      "i18n:org.apache.hop.core.plugins.englishsearch:EnglishSearch.Keywords";

  private Locale originalLocale;

  @BeforeEach
  void rememberLocale() {
    originalLocale = LanguageChoice.getInstance().getDefaultLocale();
  }

  @AfterEach
  void restoreLocale() {
    LanguageChoice.getInstance().setDefaultLocale(originalLocale);
  }

  @Test
  void frenchLocaleAddsEnglishAliasesForNameCategoryAndKeywords() {
    LanguageChoice.getInstance().setDefaultLocale(Locale.FRENCH);

    // Localized terms differ from English, so all three English variants must be added as aliases.
    List<String> aliases =
        Arrays.asList(extractAliases("Génération de lignes", "Entrée", new String[] {"ligne"}));

    assertTrue(aliases.contains("Generate rows"), "English name missing: " + aliases);
    assertTrue(aliases.contains("Input"), "English category missing: " + aliases);
    assertTrue(aliases.contains("row,generator"), "English keywords missing: " + aliases);
  }

  @Test
  void dedupSkipsTermsAlreadyMatchedInTheActiveLocale() {
    LanguageChoice.getInstance().setDefaultLocale(Locale.FRENCH);

    // The active-locale terms already equal their English counterparts: nothing new to add.
    String[] aliases = extractAliases("Generate rows", "Input", new String[] {"row,generator"});

    assertEquals(0, aliases.length, "Expected no aliases, got: " + Arrays.toString(aliases));
  }

  @Test
  void englishLocaleAddsNoAliases() {
    LanguageChoice.getInstance().setDefaultLocale(Locale.US);

    String[] aliases = extractAliases("anything", "anything", new String[] {"anything"});

    assertEquals(0, aliases.length, "Expected no aliases, got: " + Arrays.toString(aliases));
  }

  @Test
  void getEnglishTranslationResolvesEnglishRegardlessOfActiveLocale() {
    LanguageChoice.getInstance().setDefaultLocale(Locale.FRENCH);

    assertEquals(
        "Generate rows",
        BasePluginType.getEnglishTranslation(NAME_KEY, packageName(), EnglishSearchFixture.class));
    assertEquals(
        "Input",
        BasePluginType.getEnglishTranslation(
            CATEGORY_KEY, packageName(), EnglishSearchFixture.class));
  }

  @Test
  void getEnglishTranslationReturnsNullForUnresolvableI18nCode() {
    // An i18n-coded key that has no entry must not leak the raw "i18n:..." code into the search
    // set.
    assertNull(
        BasePluginType.getEnglishTranslation(
            "i18n:org.apache.hop.core.plugins.englishsearch:No.Such.Key",
            packageName(),
            EnglishSearchFixture.class));
  }

  @Test
  void getEnglishTranslationKeepsPlainStringWhenTranslationsUnsupported() {
    assertEquals(
        "PlainEnglish",
        BasePluginType.getEnglishTranslation("PlainEnglish", "", EnglishSearchFixture.class));
    assertNull(
        BasePluginType.getEnglishTranslation(null, packageName(), EnglishSearchFixture.class));
  }

  @Test
  void unresolvableI18nKeywordIsNotAddedAsAlias() {
    LanguageChoice.getInstance().setDefaultLocale(Locale.FRENCH);

    Transform annotation = UnresolvableKeywordFixture.class.getAnnotation(Transform.class);
    String[] aliases =
        TransformPluginType.getInstance()
            .extractEnglishSearchKeywords(
                annotation,
                packageName(),
                UnresolvableKeywordFixture.class,
                "Nom",
                "Catégorie",
                new String[] {"motclef"});

    assertFalse(
        Arrays.stream(aliases).anyMatch(a -> a.startsWith("i18n:")),
        "Raw i18n code leaked into search aliases: " + Arrays.toString(aliases));
  }

  // ---- helpers --------------------------------------------------------------------------------

  private static String packageName() {
    return EnglishSearchFixture.class.getPackage().getName();
  }

  private static String[] extractAliases(
      String localizedName, String localizedCategory, String[] localizedKeywords) {
    Transform annotation = EnglishSearchFixture.class.getAnnotation(Transform.class);
    return TransformPluginType.getInstance()
        .extractEnglishSearchKeywords(
            annotation,
            packageName(),
            EnglishSearchFixture.class,
            localizedName,
            localizedCategory,
            localizedKeywords);
  }

  // ---- fixture annotations --------------------------------------------------------------------

  @Transform(
      id = "EnglishSearchFixture",
      name = NAME_KEY,
      categoryDescription = CATEGORY_KEY,
      keywords = {KEYWORDS_KEY})
  private static final class EnglishSearchFixture {}

  @Transform(
      id = "UnresolvableKeywordFixture",
      name = NAME_KEY,
      categoryDescription = CATEGORY_KEY,
      keywords = {"i18n:org.apache.hop.core.plugins.englishsearch:No.Such.Key"})
  private static final class UnresolvableKeywordFixture {}
}
