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

package org.apache.hop.setup.gui;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class MessagesFormatTest {

  private static final String BUNDLE =
      "/org/apache/hop/setup/gui/messages/messages_en_US.properties";

  @Test
  void everyMessageFormats() throws Exception {
    Properties properties = new Properties();
    try (InputStream in = MessagesFormatTest.class.getResourceAsStream(BUNDLE)) {
      assertNotNull(in, BUNDLE);
      properties.load(new InputStreamReader(in, StandardCharsets.UTF_8));
    }
    for (String key : properties.stringPropertyNames()) {
      String value = properties.getProperty(key);
      try {
        MessageFormat.format(value, "0", "1", "2", "3", "4");
      } catch (IllegalArgumentException e) {
        fail("MessageFormat failed for " + key + ": " + e.getMessage());
      }
    }
  }
}
