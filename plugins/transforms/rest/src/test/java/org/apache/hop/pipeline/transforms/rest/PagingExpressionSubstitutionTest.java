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
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.Collections;
import java.util.Map;
import org.apache.hop.core.util.StringUtil;
import org.junit.jupiter.api.Test;

class PagingExpressionSubstitutionTest {

  /**
   * {@link StringUtil#environmentSubstitute} applies hex decoding and mangles JsonPath $ + bracket.
   */
  @Test
  void environmentSubstituteDecodesBracketAfterDollarLikeHexNotation() {
    String mangled =
        StringUtil.environmentSubstitute("$[*]", Collections.singletonMap("__none__", "x"));
    assertNotEquals("$[*]", mangled);
  }

  @Test
  void windowsUnixOnlyPreservesJsonPathBracketStar() {
    Map<String, String> empty = Collections.emptyMap();
    String preserved =
        StringUtil.substituteUnix(StringUtil.substituteWindows("$[*]", empty), empty);
    assertEquals("$[*]", preserved);
  }

  @Test
  void substitutesVariablesTogetherWithBracketStar() {
    Map<String, String> m = Collections.singletonMap("SUFFIX", "&ok=1");
    String combined =
        StringUtil.substituteUnix(StringUtil.substituteWindows("$[*] ${SUFFIX}", m), m);
    assertEquals("$[*] &ok=1", combined);
  }
}
