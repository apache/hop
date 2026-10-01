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

package org.apache.hop.marketplace.gui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * Hop renders every message through {@link MessageFormat}, so a literal brace in a message value is
 * read as an argument placeholder and throws at display time rather than at build time. Braces have
 * to be quoted as {@code '{'} / {@code '}'}.
 */
class MessagesFormatTest {

  private static final String BUNDLE =
      "/org/apache/hop/marketplace/gui/messages/messages_en_US.properties";

  private static Properties messages() throws Exception {
    Properties properties = new Properties();
    try (InputStream in = MessagesFormatTest.class.getResourceAsStream(BUNDLE)) {
      assertNotNull(in, "missing bundle " + BUNDLE);
      properties.load(new InputStreamReader(in, StandardCharsets.UTF_8));
    }
    return properties;
  }

  @Test
  void everyMessageIsAValidMessageFormatPattern() throws Exception {
    Properties messages = messages();
    for (String key : messages.stringPropertyNames()) {
      String value = messages.getProperty(key);
      try {
        new MessageFormat(value);
      } catch (IllegalArgumentException e) {
        fail(
            "Message '"
                + key
                + "' is not a valid MessageFormat pattern ("
                + e.getMessage()
                + "). Quote literal braces as '{' and '}':\n  "
                + value);
      }
    }
  }

  @Test
  void placeholderDocumentationSurvivesFormatting() throws Exception {
    // The urlTemplate tooltip documents ${...} placeholders, which collide with MessageFormat.
    String tooltip = messages().getProperty("ManageRepositoriesDialog.Edit.UrlTemplate.Tooltip");
    assertNotNull(tooltip);
    String rendered = new MessageFormat(tooltip).format(new Object[0]);
    assertEquals(
        true,
        rendered.contains("${artifactId}") && rendered.contains("${version}"),
        "placeholders must reach the user with their braces intact, got:\n  " + rendered);
  }
}
