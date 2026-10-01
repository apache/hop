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

import org.junit.jupiter.api.Test;

/**
 * A REST connection's base URL and the transform's own URL used to be concatenated as raw strings,
 * which produced a doubled slash, a missing one, or a mangled absolute URL depending on how the two
 * happened to be written.
 */
class RestUrlResolutionTest {

  private static final String BASE = "https://api.example.com";
  private static final String BASE_SLASH = "https://api.example.com/";

  @Test
  void aMissingSeparatorIsAdded() {
    assertEquals(BASE + "/v1/users", Rest.resolveAgainstBase(BASE, "v1/users"));
  }

  @Test
  void aDoubledSeparatorIsCollapsed() {
    assertEquals(BASE + "/v1/users", Rest.resolveAgainstBase(BASE_SLASH, "/v1/users"));
  }

  @Test
  void aSingleSeparatorOnEitherSideIsKept() {
    assertEquals(BASE + "/v1/users", Rest.resolveAgainstBase(BASE, "/v1/users"));
    assertEquals(BASE + "/v1/users", Rest.resolveAgainstBase(BASE_SLASH, "v1/users"));
  }

  @Test
  void aQueryStringSurvivesTheJoin() {
    assertEquals(
        BASE + "/v1/users?active=true", Rest.resolveAgainstBase(BASE, "v1/users?active=true"));
  }

  @Test
  void anAbsoluteUrlIgnoresTheBase() {
    // A URL-in-field row pointing somewhere else used to be glued onto the base, producing
    // "https://api.example.comhttps://other.example.com/x".
    assertEquals(
        "https://other.example.com/x",
        Rest.resolveAgainstBase(BASE, "https://other.example.com/x"));
    assertEquals(
        "http://other.example.com/x", Rest.resolveAgainstBase(BASE, "http://other.example.com/x"));
  }

  @Test
  void aHostAndPortIsNotMistakenForAScheme() {
    // Requiring "://" rather than just a colon keeps this a relative path.
    assertEquals(BASE + "/localhost:8080/x", Rest.resolveAgainstBase(BASE, "localhost:8080/x"));
  }

  @Test
  void withoutABaseTheValueIsUsedAsIs() {
    assertEquals("/v1/users", Rest.resolveAgainstBase(null, "/v1/users"));
    assertEquals(
        "https://other.example.com/x", Rest.resolveAgainstBase("", "https://other.example.com/x"));
  }

  @Test
  void anEmptyValueLeavesTheBaseAlone() {
    assertEquals(BASE, Rest.resolveAgainstBase(BASE, ""));
    assertEquals(BASE, Rest.resolveAgainstBase(BASE, null));
  }

  @Test
  void withNeitherSideSetTheResultIsEmpty() {
    assertEquals("", Rest.resolveAgainstBase(null, null));
  }
}
