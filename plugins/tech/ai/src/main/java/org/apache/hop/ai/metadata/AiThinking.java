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

package org.apache.hop.ai.metadata;

import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/**
 * Whether a model reasons before it answers. Thinking models (qwen3, deepseek-r1, gpt-oss, ...)
 * reason by default, which for a call per row dominates the run time.
 *
 * <p>Only Ollama applies it for now. The setting is generic so that the reasoning options of other
 * provider types can map onto it later.
 */
public enum AiThinking implements IEnumHasCodeAndDescription {
  /** Leave it to the model. The value for a provider saved before the option existed. */
  DEFAULT(null),

  /** Answer without reasoning first. */
  OFF(Boolean.FALSE),

  /** Reason before answering. */
  ON(Boolean.TRUE);

  private static final Class<?> PKG = AiThinking.class;

  private final Boolean think;

  AiThinking(Boolean think) {
    this.think = think;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getDescription() {
    return BaseMessages.getString(PKG, "AiThinking." + name());
  }

  /** The value for the model's {@code think} option, or null to leave it unset. */
  public Boolean think() {
    return think;
  }

  /**
   * The setting a stored value stands for. A code or a label is accepted, both ignoring case: a
   * provider saved before codes were stored holds the label.
   *
   * @param value the resolved field value
   * @return the setting, {@link #DEFAULT} for an empty value, or null when the value is not one of
   *     the settings
   */
  public static AiThinking lookup(String value) {
    if (Utils.isEmpty(value) || value.isBlank()) {
      return DEFAULT;
    }
    String text = value.trim();
    for (AiThinking thinking : values()) {
      if (thinking.name().equalsIgnoreCase(text)
          || thinking.getDescription().equalsIgnoreCase(text)) {
        return thinking;
      }
    }
    return null;
  }

  /**
   * The code to store for a value: the code of the setting it names, or the value as it is when it
   * is empty, a variable or not a setting.
   */
  public static String toCode(String value) {
    if (value == null || value.isBlank()) {
      return value;
    }
    AiThinking thinking = lookup(value);
    return thinking == null ? value : thinking.getCode();
  }

  /** The label to show for a stored value, or the value as it is when it names no setting. */
  public static String toDescription(String value) {
    if (value == null || value.isBlank()) {
      return value;
    }
    AiThinking thinking = lookup(value);
    return thinking == null ? value : thinking.getDescription();
  }

  /** The labels the editor offers, in order. */
  public static String[] descriptions() {
    return IEnumHasCodeAndDescription.getDescriptions(AiThinking.class);
  }
}
