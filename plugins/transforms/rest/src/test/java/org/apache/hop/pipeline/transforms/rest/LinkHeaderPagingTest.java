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
import static org.junit.jupiter.api.Assertions.assertNull;

import jakarta.ws.rs.core.MultivaluedHashMap;
import org.junit.jupiter.api.Test;

class LinkHeaderPagingTest {

  @Test
  void prefersNextWhenPrevAndNextShareOneHeaderValue() {
    MultivaluedHashMap<String, Object> h = new MultivaluedHashMap<>();
    h.add(
        "Link",
        "<https://api.github.com/repos/apache/hop/issues?page=1>; rel=\"prev\", "
            + "<https://api.github.com/repos/apache/hop/issues?page=3>; rel=\"next\"");
    assertEquals(
        "https://api.github.com/repos/apache/hop/issues?page=3",
        LinkHeaderPaging.findFirstUriWithRelNext(h));
  }

  @Test
  void mergesMultipleLinkHeaderFields() {
    MultivaluedHashMap<String, Object> h = new MultivaluedHashMap<>();
    h.add("Link", "<https://api.example.com/a>; rel=\"first\"");
    h.add("link", "<https://api.example.com/b>; rel=\"next\"");
    assertEquals("https://api.example.com/b", LinkHeaderPaging.findFirstUriWithRelNext(h));
  }

  @Test
  void compoundRelTokenList() {
    assertEquals(
        "https://x.test/page2",
        LinkHeaderPaging.findFirstUriWithRelNext(
            header("<https://x.test/page2>; type=\"application/json\"; rel=\"section next\"")));
  }

  @Test
  void unquotedRelNext() {
    assertEquals(
        "https://x.test/n",
        LinkHeaderPaging.findFirstUriWithRelNext(header("<https://x.test/n>; rel=next")));
  }

  @Test
  void commaInsideBracketsDoesNotSplitSegment() {
    assertEquals(
        "https://x.test/items?tags=a,b,c",
        LinkHeaderPaging.findFirstUriWithRelNext(
            header("<https://x.test/items?tags=a,b,c>; rel=\"next\"")));
  }

  @Test
  void prevOnlyYieldsNull() {
    assertNull(
        LinkHeaderPaging.findFirstUriWithRelNext(header("<https://x.test/prev>; rel=\"prev\"")));
  }

  static MultivaluedHashMap<String, Object> header(String linkValue) {
    MultivaluedHashMap<String, Object> h = new MultivaluedHashMap<>();
    h.add("Link", linkValue);
    return h;
  }
}
