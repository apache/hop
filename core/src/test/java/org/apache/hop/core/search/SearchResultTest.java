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

package org.apache.hop.core.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class SearchResultTest {

  private static final class DummySearchable implements ISearchable<String> {
    @Override
    public String getLocation() {
      return "loc";
    }

    @Override
    public String getName() {
      return "n";
    }

    @Override
    public String getType() {
      return "t";
    }

    @Override
    public String getFilename() {
      return "f.hpl";
    }

    @Override
    public String getSearchableObject() {
      return "obj";
    }

    @Override
    public ISearchableCallback getSearchCallback() {
      return null;
    }
  }

  @Test
  void accessorsAndMutators() {
    DummySearchable searchable = new DummySearchable();
    SearchResult r = new SearchResult(searchable, "match", "desc", "transform", "value");

    assertEquals(searchable, r.getMatchingSearchable());
    assertEquals("match", r.getMatchingString());
    assertEquals("desc", r.getDescription());
    assertEquals("transform", r.getComponent());
    assertEquals("value", r.getValue());

    r.setMatchingString("m2");
    r.setDescription("d2");
    r.setComponent("c2");
    r.setValue("v2");
    r.setMatchingSearchable(null);

    assertEquals("m2", r.getMatchingString());
    assertEquals("d2", r.getDescription());
    assertEquals("c2", r.getComponent());
    assertEquals("v2", r.getValue());
    assertNull(r.getMatchingSearchable());
  }
}
