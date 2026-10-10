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

package org.apache.hop.pipeline.transforms.languagemodelchat.internals;

import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.i18nUtil.i18n;

import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/**
 * Whether an Ollama thinking model (qwen3, deepseek-r1, gpt-oss, ...) reasons before it answers.
 *
 * <p>Three states rather than a Boolean: a transform saved before the option existed has no value,
 * and that has to stay "leave it to the model", not turn thinking off.
 */
public enum OllamaThink implements IEnumHasCodeAndDescription {
  /** Leave it to the model: {@code think} is not sent. */
  DEFAULT(null),

  /** {@code think: false}. */
  OFF(Boolean.FALSE),

  /** {@code think: true}. */
  ON(Boolean.TRUE);

  private final Boolean think;

  OllamaThink(Boolean think) {
    this.think = think;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getDescription() {
    return i18n("LanguageModelChatDialog.OLLAMA.Think." + name());
  }

  /** The value for the model's {@code think} option, or null to leave it unset. */
  public Boolean think() {
    return think;
  }

  /** The setting for a {@code think} value; null is {@link #DEFAULT}. */
  public static OllamaThink of(Boolean think) {
    if (think == null) {
      return DEFAULT;
    }
    return think ? ON : OFF;
  }

  /** The {@code think} value of a setting, where null, as an older transform loads, is unset. */
  public static Boolean think(OllamaThink setting) {
    return setting == null ? null : setting.think();
  }
}
