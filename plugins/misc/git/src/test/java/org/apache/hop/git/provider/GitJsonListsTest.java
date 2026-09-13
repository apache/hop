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

package org.apache.hop.git.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.jupiter.api.Test;

class GitJsonListsTest {

  private static JSONObject parse(String json) throws Exception {
    return (JSONObject) new JSONParser().parse(json);
  }

  @Test
  void joinsGithubStyleObjectArrays() throws Exception {
    JSONObject json = parse("{\"labels\":[{\"name\":\"bug\"},{\"name\":\"needs review\"}]}");
    assertEquals("bug, needs review", GitJsonLists.names(json, "labels", "name"));
  }

  @Test
  void joinsGitlabStylePlainStringArrays() throws Exception {
    JSONObject json = parse("{\"labels\":[\"bug\",\"needs review\"]}");
    assertEquals("bug, needs review", GitJsonLists.names(json, "labels", "name"));
  }

  @Test
  void readsASingleValuedFieldThatIsNotAnArray() throws Exception {
    // Bitbucket carries one assignee as an object rather than a list of them.
    JSONObject json = parse("{\"assignee\":{\"display_name\":\"Ada Lovelace\"}}");
    assertEquals("Ada Lovelace", GitJsonLists.names(json, "assignee", "display_name"));
  }

  @Test
  void skipsBlankAndUnusableEntriesRatherThanFailingTheRow() throws Exception {
    JSONObject json = parse("{\"labels\":[{\"name\":\"bug\"},{\"other\":\"x\"},null,42,\"\"]}");
    assertEquals("bug", GitJsonLists.names(json, "labels", "name"));
  }

  @Test
  void anAbsentOrEmptyListIsAnEmptyValue() throws Exception {
    assertEquals("", GitJsonLists.names(parse("{}"), "labels", "name"));
    assertEquals("", GitJsonLists.names(parse("{\"labels\":[]}"), "labels", "name"));
    assertEquals("", GitJsonLists.names(null, "labels", "name"));
  }

  @Test
  void moreThanOneParentIsAMerge() throws Exception {
    assertTrue(
        GitJsonLists.mergeFlag(
            parse("{\"parents\":[{\"sha\":\"a\"},{\"sha\":\"b\"}]}"), "parents"));
    assertFalse(GitJsonLists.mergeFlag(parse("{\"parents\":[{\"sha\":\"a\"}]}"), "parents"));
    assertFalse(GitJsonLists.mergeFlag(parse("{\"parents\":[]}"), "parents"));
  }

  @Test
  void anAbsentParentListIsNotReportedAsAMerge() throws Exception {
    // The payload does not say; guessing "merge" here would silently corrupt commit counts.
    assertFalse(GitJsonLists.mergeFlag(parse("{}"), "parents"));
    assertFalse(GitJsonLists.mergeFlag(parse("{\"parent_ids\":[\"a\",\"b\"]}"), "parents"));
    assertTrue(GitJsonLists.mergeFlag(parse("{\"parent_ids\":[\"a\",\"b\"]}"), "parent_ids"));
  }
}
