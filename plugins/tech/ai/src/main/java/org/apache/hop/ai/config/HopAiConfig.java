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

package org.apache.hop.ai.config;

import lombok.Getter;
import lombok.Setter;

/** Global AI advisory options stored in hop-config.json. Credentials live on {@code AiProvider}. */
@Getter
@Setter
public class HopAiConfig {

  public static final String HOP_CONFIG_KEY = "hopAiConfig";

  /** Master switch. Default false so privacy-sensitive installs stay dark. */
  private boolean aiEnabled;

  /** Name of the default {@code AiProvider} metadata object for GUI advisors. */
  private String defaultProviderName = "";

  /** When true, advisors may send full pipeline/workflow XML on the first turn. */
  private boolean allowSendFullXml;

  public HopAiConfig() {}

  public HopAiConfig(HopAiConfig other) {
    if (other == null) {
      return;
    }
    this.aiEnabled = other.aiEnabled;
    this.defaultProviderName = other.defaultProviderName;
    this.allowSendFullXml = other.allowSendFullXml;
  }
}
