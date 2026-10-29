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

package org.apache.hop.i18n;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Locale;
import java.util.ResourceBundle;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Unit test for {@link GlobalMessageUtil} */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class GlobalMessageUtilTest {

  @Test
  void testGetLocaleString() {
    assertEquals("", GlobalMessageUtil.getLocaleString(null));
    assertEquals("", GlobalMessageUtil.getLocaleString(Locale.of("")));
    assertEquals("en", GlobalMessageUtil.getLocaleString(Locale.ENGLISH));
    assertEquals("en_US", GlobalMessageUtil.getLocaleString(Locale.US));
    assertEquals("en", GlobalMessageUtil.getLocaleString(Locale.of("EN")));
    assertEquals("en_US", GlobalMessageUtil.getLocaleString(Locale.of("EN", "us")));
  }

  @Test
  void isMissingKey() {
    assertTrue(GlobalMessageUtil.isMissingKey(null));
    assertFalse(GlobalMessageUtil.isMissingKey(""));
    assertFalse(GlobalMessageUtil.isMissingKey(" "));
    assertTrue(GlobalMessageUtil.isMissingKey("!foo!"));
    assertTrue(GlobalMessageUtil.isMissingKey("!foo! "));
    assertTrue(GlobalMessageUtil.isMissingKey(" !foo!"));
    assertFalse(GlobalMessageUtil.isMissingKey("!foo"));
    assertFalse(GlobalMessageUtil.isMissingKey("foo!"));
    assertFalse(GlobalMessageUtil.isMissingKey("foo"));
    assertFalse(GlobalMessageUtil.isMissingKey("!"));
    assertFalse(GlobalMessageUtil.isMissingKey(" !"));
  }

  @Test
  void bundlesAreOnlyLookedUpAsPropertiesFiles() {
    assertEquals(
        ResourceBundle.Control.FORMAT_PROPERTIES,
        new GlobalMessageUtil.GlobalMessageControl(true).getFormats("any.base.name"));
  }

  @Test
  void bundleIsFoundThroughALoaderThatDoesNotDelegateToTheSystemLoader() throws Exception {
    // A loader without the system class loader as an ancestor: the bundle comes from the retry
    // on the system class loader
    ClassLoader isolated = new URLClassLoader(new URL[0], ClassLoader.getPlatformClassLoader());
    ResourceBundle bundle =
        new GlobalMessageUtil.GlobalMessageControl(true)
            .newBundle(
                GlobalMessages.SYSTEM_BUNDLE_PACKAGE + "." + GlobalMessages.BUNDLE_NAME,
                Locale.FRANCE,
                "java.properties",
                isolated,
                false);
    assertEquals("Une certaine valeur {0}", bundle.getString("someKey"));
  }

  @Test
  void noRetryOnTheSystemLoaderForALoaderThatDelegatesToIt() {
    ClassLoader system = ClassLoader.getSystemClassLoader();
    assertTrue(GlobalMessageUtil.GlobalMessageControl.delegatesToSystemClassLoader(system));
    assertTrue(
        GlobalMessageUtil.GlobalMessageControl.delegatesToSystemClassLoader(
            new URLClassLoader(new URL[0], system)));
    assertTrue(
        GlobalMessageUtil.GlobalMessageControl.delegatesToSystemClassLoader(
            new URLClassLoader(new URL[0], new URLClassLoader(new URL[0], system))));
    assertFalse(
        GlobalMessageUtil.GlobalMessageControl.delegatesToSystemClassLoader(
            new URLClassLoader(new URL[0], ClassLoader.getPlatformClassLoader())));
  }

  @Test
  void bundleIsFoundThroughALoaderThatDelegatesToTheSystemLoader() throws Exception {
    // No retry for this loader: the bundle comes from its own parent chain
    ClassLoader child = new URLClassLoader(new URL[0], ClassLoader.getSystemClassLoader());
    ResourceBundle bundle =
        new GlobalMessageUtil.GlobalMessageControl(true)
            .newBundle(
                GlobalMessages.SYSTEM_BUNDLE_PACKAGE + "." + GlobalMessages.BUNDLE_NAME,
                Locale.FRANCE,
                "java.properties",
                child,
                false);
    assertEquals("Une certaine valeur {0}", bundle.getString("someKey"));
  }

  @Test
  void calculateString() {

    // "fr", "FR"
    assertEquals(
        "Une certaine valeur foo",
        GlobalMessageUtil.calculateString(
            GlobalMessages.SYSTEM_BUNDLE_PACKAGE,
            Locale.FRANCE,
            "someKey",
            new String[] {"foo"},
            GlobalMessages.PKG,
            GlobalMessages.BUNDLE_NAME));

    // "fr" - should fall back on default bundle
    String str =
        GlobalMessageUtil.calculateString(
            GlobalMessages.SYSTEM_BUNDLE_PACKAGE,
            Locale.FRENCH,
            "someKey",
            new String[] {"foo"},
            GlobalMessages.PKG,
            GlobalMessages.BUNDLE_NAME);
    assertEquals("Some Value foo", str);

    // "jp"
    assertEquals(
        "何らかの値 foo",
        GlobalMessageUtil.calculateString(
            GlobalMessages.SYSTEM_BUNDLE_PACKAGE,
            Locale.JAPANESE,
            "someKey",
            new String[] {"foo"},
            GlobalMessages.PKG,
            GlobalMessages.BUNDLE_NAME));

    // "jp", "JP" - should fall back on "jp"
    str =
        GlobalMessageUtil.calculateString(
            GlobalMessages.SYSTEM_BUNDLE_PACKAGE,
            Locale.JAPAN,
            "someKey",
            new String[] {"foo"},
            GlobalMessages.PKG,
            GlobalMessages.BUNDLE_NAME);
    assertEquals("何らかの値 foo", str);

    // try with multiple packages
    // make sure the selected language is used correctly
    LanguageChoice.getInstance().setDefaultLocale(Locale.FRANCE); // "fr", "FR"
    assertEquals(
        "Une certaine valeur foo",
        GlobalMessageUtil.calculateString(
            new String[] {GlobalMessages.SYSTEM_BUNDLE_PACKAGE},
            "someKey",
            new String[] {"foo"},
            GlobalMessages.PKG,
            GlobalMessages.BUNDLE_NAME));

    LanguageChoice.getInstance()
        .setDefaultLocale(Locale.FRENCH); // "fr" - fall back on "default" messages.properties
    assertEquals(
        "Some Value foo",
        GlobalMessageUtil.calculateString(
            new String[] {GlobalMessages.SYSTEM_BUNDLE_PACKAGE},
            "someKey",
            new String[] {"foo"},
            GlobalMessages.PKG,
            GlobalMessages.BUNDLE_NAME));

    LanguageChoice.getInstance()
        .setDefaultLocale(Locale.FRENCH); // "fr" - fall back on foo/messages_fr.properties
    assertEquals(
        "Une certaine valeur foo",
        GlobalMessageUtil.calculateString(
            new String[] {GlobalMessages.SYSTEM_BUNDLE_PACKAGE, "org.apache.hop.foo"},
            "someKey",
            new String[] {"foo"},
            GlobalMessages.PKG,
            GlobalMessages.BUNDLE_NAME));

    LanguageChoice.getInstance().setDefaultLocale(Locale.JAPANESE); // "jp"
    assertEquals(
        "何らかの値 foo",
        GlobalMessageUtil.calculateString(
            new String[] {GlobalMessages.SYSTEM_BUNDLE_PACKAGE},
            "someKey",
            new String[] {"foo"},
            GlobalMessages.PKG,
            GlobalMessages.BUNDLE_NAME));

    LanguageChoice.getInstance().setDefaultLocale(Locale.JAPAN); // "jp", "JP" - fall back on "jp"
    assertEquals(
        "何らかの値 foo",
        GlobalMessageUtil.calculateString(
            new String[] {GlobalMessages.SYSTEM_BUNDLE_PACKAGE},
            "someKey",
            new String[] {"foo"},
            GlobalMessages.PKG,
            GlobalMessages.BUNDLE_NAME));
  }
}
