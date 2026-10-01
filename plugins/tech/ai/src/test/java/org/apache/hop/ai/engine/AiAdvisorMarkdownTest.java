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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class AiAdvisorMarkdownTest {

  @Test
  void stripsMarkersAndKeepsContent() {
    AiAdvisorMarkdown.Document document =
        AiAdvisorMarkdown.render(
            """
            ## Cause
            The errors are **not from the workflow graph**. `start` is valid.

            1. Create the folder
            2. Re-run
            """);
    String text = document.text();
    assertFalse(text.contains("##"));
    assertFalse(text.contains("**"));
    assertTrue(text.contains("Cause"));
    assertTrue(text.contains("not from the workflow graph"));
    assertTrue(text.contains("start"));
    assertTrue(text.contains("1. Create the folder"));
    assertTrue(
        document.spans().stream().anyMatch(span -> span.kind() == AiAdvisorMarkdown.Kind.HEADING));
    assertTrue(
        document.spans().stream().anyMatch(span -> span.kind() == AiAdvisorMarkdown.Kind.BOLD));
    assertTrue(
        document.spans().stream().anyMatch(span -> span.kind() == AiAdvisorMarkdown.Kind.CODE));
  }
}
