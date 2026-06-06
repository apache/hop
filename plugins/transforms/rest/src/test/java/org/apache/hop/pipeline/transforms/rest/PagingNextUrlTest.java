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

package org.apache.hop.pipeline.transforms.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import org.junit.jupiter.api.Test;

class PagingNextUrlTest {

  private static final Configuration JSON_PATH_CONFIGURATION =
      Configuration.builder()
          .options(Option.DEFAULT_PATH_LEAF_TO_NULL, Option.SUPPRESS_EXCEPTIONS)
          .build();

  @Test
  void jsonPathReadsNextPageUrlFromBody() {
    String body = "{\"data\":[{\"id\":1}],\"next\":\"https://api.example.com/items?page=2\"}";
    Object raw = JsonPath.using(JSON_PATH_CONFIGURATION).parse(body).read("$.next");
    assertEquals("https://api.example.com/items?page=2", raw.toString());
  }

  @Test
  void jsonPathReturnsNullWhenNextUrlMissing() {
    String body = "{\"data\":[]}";
    Object raw = JsonPath.using(JSON_PATH_CONFIGURATION).parse(body).read("$.next");
    assertTrue(raw == null);
  }
}
