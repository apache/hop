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

package org.apache.hop.ai.providers;

import org.apache.hop.ai.provider.AiProviderPlugin;

@AiProviderPlugin(
    id = "grok",
    name = "Grok (xAI)",
    description = "xAI Grok via the OpenAI-compatible API",
    classLoaderGroup = "hop-ai")
public class GrokProvider extends OpenAiCompatibleProvider {

  public GrokProvider() {
    setDefaultBaseUrl("https://api.x.ai/v1");
    setDefaultModelName("grok-4");
  }
}
