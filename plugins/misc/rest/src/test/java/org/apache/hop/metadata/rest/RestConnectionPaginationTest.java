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

package org.apache.hop.metadata.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPlugin;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.metadata.serializer.json.JsonMetadataParser;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The paging options added for APIs that count pages from zero and wrap their items in an envelope
 * (issues #9548 and #9549) are new keys only: a connection saved before them loads without them.
 */
class RestConnectionPaginationTest {

  @BeforeAll
  static void initPasswordEncoder() throws Exception {
    PluginRegistry.getInstance()
        .registerPluginClass(
            HopTwoWayPasswordEncoder.class.getName(),
            TwoWayPasswordEncoderPluginType.class,
            TwoWayPasswordEncoderPlugin.class);
    Encr.init("Hop");
  }

  @Test
  void aConnectionSavedBeforeTheseOptionsExistedLoadsWithoutThem() throws Exception {
    RestConnection connection =
        fromJson(
            "{\"name\":\"old\",\"pagination_type\":\"PAGE_NUMBER\",\"page_param_name\":\"page\","
                + "\"base_url\":\"http://paging\"}");

    assertEquals(RestPaginationType.PAGE_NUMBER, connection.getPaginationType());
    // Empty first page number means page one, as before.
    assertNull(connection.getFirstPageNumber());
    assertNull(connection.getItemsJsonPath());
    assertNull(connection.getItemsXPath());
    assertNull(connection.getTotalCountJsonPath());
    assertNull(connection.getTotalCountXPath());
    assertEquals(0, connection.getDefaultLimit());
  }

  @Test
  void theNewOptionsSurviveASaveAndLoad() throws Exception {
    RestConnection connection = new RestConnection();
    connection.setName("ironclad");
    connection.setPaginationType(RestPaginationType.PAGE_NUMBER);
    connection.setFirstPageNumber("0");
    connection.setLimitParamName("pageSize");
    connection.setDefaultLimit(100);
    connection.setItemsJsonPath("$.list");
    connection.setItemsXPath("/page/list/item");
    connection.setTotalCountJsonPath("$.count");
    connection.setTotalCountXPath("/page/count");

    JSONObject json = parser().getJsonObject(connection);
    assertEquals("0", json.get("first_page_number"));
    assertEquals("$.list", json.get("items_json_path"));
    assertEquals("$.count", json.get("total_count_json_path"));

    RestConnection loaded = fromJson(json.toJSONString());

    assertEquals("0", loaded.getFirstPageNumber());
    assertEquals("pageSize", loaded.getLimitParamName());
    assertEquals(100, loaded.getDefaultLimit());
    assertEquals("$.list", loaded.getItemsJsonPath());
    assertEquals("/page/list/item", loaded.getItemsXPath());
    assertEquals("$.count", loaded.getTotalCountJsonPath());
    assertEquals("/page/count", loaded.getTotalCountXPath());
  }

  private static JsonMetadataParser<RestConnection> parser() {
    return new JsonMetadataParser<>(RestConnection.class, new MemoryMetadataProvider());
  }

  private static RestConnection fromJson(String json) throws Exception {
    JsonParser jsonParser = new JsonFactory().createParser(json);
    jsonParser.nextToken();
    return parser().loadJsonObject(RestConnection.class, jsonParser);
  }
}
