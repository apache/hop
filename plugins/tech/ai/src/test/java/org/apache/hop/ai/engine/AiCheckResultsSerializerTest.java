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

import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.junit.jupiter.api.Test;

class AiCheckResultsSerializerTest {

  @Test
  void serializesAndRedactsSecrets() {
    ICheckResult result =
        new CheckResult(
            ICheckResult.TYPE_RESULT_ERROR, "password=\"s3cret\" on transform Input", null);
    String json = AiCheckResultsSerializer.serialize(List.of(result));
    assertTrue(json.contains("\"type\":\"ERROR\""));
    assertFalse(json.contains("s3cret"));
    assertTrue(json.contains("password=\\\"***\\\"") || json.contains("password=\"***\""));
  }

  @Test
  void emptyListIsValidJson() {
    assertTrue(AiCheckResultsSerializer.serialize(List.of()).contains("\"results\":[]"));
  }
}
