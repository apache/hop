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
package org.apache.hop.lint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.hop.i18n.BaseMessages;
import org.junit.jupiter.api.Test;

/**
 * Guards the i18n wiring. A missing bundle does not fail the build — {@link BaseMessages} returns a
 * {@code !Key!} placeholder instead — so without a test the UI silently degrades to raw keys.
 */
public class MessagesBundleTest {

  private static final String BUNDLE = "org/apache/hop/lint/messages/messages_en_US.properties";

  @Test
  public void bundleIsOnTheClasspath() throws IOException {
    try (InputStream stream = getClass().getClassLoader().getResourceAsStream(BUNDLE)) {
      assertNotNull(stream, "Missing resource bundle: " + BUNDLE);
    }
  }

  @Test
  public void keysResolveInsteadOfFallingBackToPlaceholders() {
    assertEquals(
        "Lint", BaseMessages.getString(PkgAnchor.class, "LinterGuiPlugin.Menu.Lint.Label"));
    assertEquals(
        "Enable Linter",
        BaseMessages.getString(PkgAnchor.class, "LinterConfigPlugin.Option.Enabled.Label"));
    assertEquals(
        "Confirm Delete",
        BaseMessages.getString(PkgAnchor.class, "RuleManagerDialog.Dialog.ConfirmDelete.Title"));
  }

  /** Parameterised messages must actually substitute, not print the {0} placeholder. */
  @Test
  public void parameterisedKeysSubstitute() {
    String message =
        BaseMessages.getString(
            PkgAnchor.class, "RuleManagerDialog.Dialog.ConfirmDelete.Message", "My Rule");
    assertTrue(message.contains("My Rule"), "Parameter not substituted: " + message);
    assertFalse(message.contains("{0}"), "Placeholder left in place: " + message);
  }

  /** Every key in the bundle must resolve, catching typos and stale entries. */
  @Test
  public void everyKeyInTheBundleResolves() throws IOException {
    Properties properties = new Properties();
    try (InputStream stream = getClass().getClassLoader().getResourceAsStream(BUNDLE)) {
      assertNotNull(stream);
      properties.load(stream);
    }
    assertFalse(properties.isEmpty());

    List<String> unresolved = new ArrayList<>();
    for (String key : properties.stringPropertyNames()) {
      String value = BaseMessages.getString(PkgAnchor.class, key);
      if (value == null || value.startsWith("!")) {
        unresolved.add(key);
      }
    }
    assertTrue(unresolved.isEmpty(), "Unresolved message keys: " + unresolved);
  }

  /**
   * Every {@code i18n::Key} referenced from a GUI annotation must exist in the bundle.
   *
   * <p>Annotations cannot call a method, so these keys are plain strings that nothing checks at
   * compile time. A typo shows up only as a raw key in the UI, on someone else's screen.
   */
  @Test
  public void everyAnnotationKeyExistsInTheBundle() throws IOException {
    Properties properties = new Properties();
    try (InputStream stream = getClass().getClassLoader().getResourceAsStream(BUNDLE)) {
      assertNotNull(stream);
      properties.load(stream);
    }

    Path sourceRoot = Paths.get("src", "main", "java", "org", "apache", "hop", "lint");
    if (!Files.isDirectory(sourceRoot)) {
      return; // Not running from the module directory; the bundle checks above still apply.
    }

    Pattern reference = Pattern.compile("i18n::([A-Za-z0-9._]+)");
    List<String> missing = new ArrayList<>();
    try (Stream<Path> sources = Files.walk(sourceRoot)) {
      for (Path source : sources.filter(p -> p.toString().endsWith(".java")).toList()) {
        Matcher matcher = reference.matcher(Files.readString(source));
        while (matcher.find()) {
          String key = matcher.group(1);
          if (!properties.containsKey(key)) {
            missing.add(key + " (" + source.getFileName() + ")");
          }
        }
      }
    }

    assertTrue(missing.isEmpty(), "Annotation keys missing from the bundle: " + missing);
  }

  /** Resolves the bundle relative to the org.apache.hop.lint package. */
  private static final class PkgAnchor {}
}
