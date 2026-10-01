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

package org.apache.hop.ai.ui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ServiceConfigurationError;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.Test;

class AiAdvisorErrorReportingTest {

  @Test
  void serviceConfigurationErrorIsShown() {
    ServiceConfigurationError error =
        new ServiceConfigurationError(
            "dev.langchain4j.http.client.jdk.JdkHttpClientBuilderFactory not a subtype");
    assertTrue(AiAdvisorSessionPane.userVisibleError(error).contains("not a subtype"));
  }

  @Test
  void hopExceptionUsesItsOwnMessage() {
    HopException wrapped =
        new HopException(
            "AI request failed: not a subtype", new ServiceConfigurationError("not a subtype"));
    assertEquals(
        "AI request failed: not a subtype", AiAdvisorSessionPane.userVisibleError(wrapped));
  }

  @Test
  void namelessThrowableUsesClassName() {
    assertEquals(
        "NullPointerException", AiAdvisorSessionPane.userVisibleError(new NullPointerException()));
  }
}
