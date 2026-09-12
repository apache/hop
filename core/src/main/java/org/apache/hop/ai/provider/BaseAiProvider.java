/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ai.provider;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.ai.AiAuthKind;
import org.apache.hop.core.exception.HopRuntimeException;

/** Identity and defaults for an {@link IAiProvider} plugin. */
@Getter
@Setter
public abstract class BaseAiProvider implements IAiProvider {

  private String pluginId;
  private String pluginName;
  private AiAuthKind authKind = AiAuthKind.API_KEY;
  private String defaultBaseUrl = "";
  private String defaultModelName = "";
  private String hopModelType = "OPEN_AI";

  @Getter(AccessLevel.NONE)
  private boolean requiresApiKey = true;

  @Override
  public boolean requiresApiKey() {
    return requiresApiKey;
  }

  public void setRequiresApiKey(boolean requiresApiKey) {
    this.requiresApiKey = requiresApiKey;
  }

  @Override
  public BaseAiProvider clone() {
    try {
      return (BaseAiProvider) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new HopRuntimeException("Unable to clone AI provider " + pluginId, e);
    }
  }
}
