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
package org.apache.hop.ai.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.junit.jupiter.api.Test;

class AiNodeSettingsTest {

  @Getter
  @Setter
  public static class Field {
    @HopMetadataProperty private String name;
    @HopMetadataProperty private String type;

    public Field() {}

    Field(String name, String type) {
      this.name = name;
      this.type = type;
    }
  }

  @Getter
  @Setter
  public static class ReadTableMeta {
    @HopMetadataProperty private String connection = "sales";
    @HopMetadataProperty private String sql = "SELECT id, amount FROM orders WHERE status = 'OPEN'";

    @HopMetadataProperty(password = true)
    private String password = "Encrypted 2be98afc86aa7f2e4cb79ce10be9b9d83";

    @HopMetadataProperty private String emptyText = "";
    @HopMetadataProperty private boolean lazy;
    @HopMetadataProperty private boolean cached = true;
    @HopMetadataProperty private int limit = 100;
    @HopMetadataProperty private String encoding = "UTF-8";

    @HopMetadataProperty(key = "field")
    private List<Field> fields = new ArrayList<>(List.of(new Field("id", "Integer")));

    private String notAProperty = "hidden";
  }

  /** A transform whose settings all differ from the defaults, except for the encoding. */
  private static ReadTableMeta changed() {
    ReadTableMeta meta = new ReadTableMeta();
    meta.setConnection("warehouse");
    meta.setSql("SELECT id, amount FROM orders WHERE status = 'OPEN' AND amount > 0");
    meta.setLazy(true);
    meta.setCached(false);
    meta.setLimit(500);
    meta.setFields(new ArrayList<>(List.of(new Field("amount", "Number"))));
    return meta;
  }

  @Test
  void readsPropertiesAndSkipsPasswordsAndEmptyValues() {
    String json = AiNodeSettings.toJson(changed());
    assertTrue(json.contains("\"connection\":\"warehouse\""), json);
    assertTrue(json.contains("SELECT id, amount FROM orders"), json);
    assertTrue(json.contains("\"limit\":500"), json);
    assertTrue(json.contains("\"field\":[{\"name\":\"amount\",\"type\":\"Number\"}]"), json);
    assertFalse(json.contains("password"), json);
    assertFalse(json.contains("2be98afc"), json);
    assertFalse(json.contains("emptyText"), json);
    assertTrue(json.contains("\"lazy\":true"), json);
    assertTrue(json.contains("\"cached\":false"), "an option switched off is kept: " + json);
    assertFalse(json.contains("hidden"), json);
  }

  @Test
  void leavesOutTheDefaults() {
    String json = AiNodeSettings.toJson(changed());
    assertFalse(json.contains("encoding"), json);
    assertNull(AiNodeSettings.toJson(new ReadTableMeta()), "only defaults: nothing to say");
  }

  @Test
  void longListsAndTextsAreShortened() {
    ReadTableMeta meta = new ReadTableMeta();
    meta.setSql("x".repeat(AiNodeSettings.MAX_TEXT_CHARS + 100));
    List<Field> fields = new ArrayList<>();
    for (int i = 0; i < AiNodeSettings.MAX_LIST_ITEMS + 5; i++) {
      fields.add(new Field("f" + i, "String"));
    }
    meta.setFields(fields);
    String json = AiNodeSettings.toJson(meta);
    assertTrue(json.contains("… 5 more"), json);
    assertFalse(json.contains("x".repeat(AiNodeSettings.MAX_TEXT_CHARS + 1)), json);
  }

  @Test
  void nodesWithoutPropertiesHaveNoSettings() {
    assertNull(AiNodeSettings.toJson(new Object()));
    assertNull(AiNodeSettings.toJson(null));
  }

  @Test
  void oversizedSettingsAreMarkedTruncated() {
    ReadTableMeta meta = new ReadTableMeta();
    List<Field> fields = new ArrayList<>();
    for (int i = 0; i < AiNodeSettings.MAX_LIST_ITEMS; i++) {
      fields.add(new Field("field_" + "y".repeat(200) + i, "String"));
    }
    meta.setFields(fields);
    assertEquals("{\"truncated\":true}", AiNodeSettings.toJson(meta));
  }
}
