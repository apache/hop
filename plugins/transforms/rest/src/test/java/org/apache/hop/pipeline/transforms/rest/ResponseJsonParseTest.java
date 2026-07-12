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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Response body parsing for pagination/split must not be locked to the request Content-Type.
 * Slack-style FORM posts still return JSON responses addressed by JsonPath.
 */
class ResponseJsonParseTest {

  private TransformMockHelper<RestMeta, RestData> mockHelper;
  private Rest rest;

  @BeforeEach
  void setUp() {
    mockHelper = new TransformMockHelper<>("REST form+jsonpath", RestMeta.class, RestData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);

    RestMeta meta = new RestMeta();
    meta.setApplicationType(RestMeta.APPLICATION_TYPE_FORM_URLENCODED);
    RestData data = new RestData();
    rest =
        new Rest(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void shouldParseResponseAsJsonWhenAppTypeIsJson() {
    assertTrue(Rest.shouldParseResponseAsJson(RestMeta.APPLICATION_TYPE_JSON, "not-a-path"));
  }

  @Test
  void shouldParseResponseAsJsonWhenPathIsJsonPath() {
    assertTrue(
        Rest.shouldParseResponseAsJson(RestMeta.APPLICATION_TYPE_FORM_URLENCODED, "$.channels[*]"));
  }

  @Test
  void shouldNotParseAsJsonWithoutJsonAppTypeOrJsonPath() {
    assertFalse(Rest.shouldParseResponseAsJson(RestMeta.APPLICATION_TYPE_FORM_URLENCODED, "/item"));
    assertFalse(Rest.shouldParseResponseAsJson(RestMeta.APPLICATION_TYPE_TEXT_PLAIN, null));
  }

  @Test
  void formUrlEncodedWithJsonPathAllowsEmptyPagedSplitCheck() {
    assertTrue(
        rest.isLegitimateEmptyPagedSplit("{\"channels\":[]}", "$.channels[*]"),
        "FORM request + JsonPath split on empty JSON array must be treated as legitimate empty");
  }
}
