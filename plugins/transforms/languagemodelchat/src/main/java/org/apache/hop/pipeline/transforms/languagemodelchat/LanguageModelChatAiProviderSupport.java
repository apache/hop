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

package org.apache.hop.pipeline.transforms.languagemodelchat;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/**
 * Applies a named AI Provider metadata object onto {@link LanguageModelChatMeta} without a compile
 * dependency on hop-tech-ai. Inline transform fields remain as fallbacks for empty provider values.
 */
public final class LanguageModelChatAiProviderSupport {

  public static final String METADATA_KEY = "ai-provider";
  private static final String FACTORY_CLASS = "org.apache.hop.ai.engine.AiChatFactory";
  private static final String OVERLAY_METHOD = "overlayNamedProvider";

  private LanguageModelChatAiProviderSupport() {}

  public static LanguageModelChatMeta resolve(
      LanguageModelChatMeta meta, IVariables variables, IHopMetadataProvider metadataProvider)
      throws HopException {
    if (meta == null || isBlank(meta.getAiProviderName())) {
      return meta;
    }
    try {
      Class<?> factory = Class.forName(FACTORY_CLASS);
      Method overlay =
          factory.getMethod(
              OVERLAY_METHOD,
              LanguageModelChatMeta.class,
              String.class,
              IVariables.class,
              IHopMetadataProvider.class);
      return (LanguageModelChatMeta)
          overlay.invoke(null, meta, meta.getAiProviderName(), variables, metadataProvider);
    } catch (ClassNotFoundException e) {
      throw new HopException(
          "Named AI Provider '" + meta.getAiProviderName() + "' requires the hop-tech-ai plugin.",
          e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      if (cause instanceof HopException hopException) {
        throw hopException;
      }
      throw new HopException(
          "Unable to apply AI Provider '" + meta.getAiProviderName() + "'", cause);
    } catch (Exception e) {
      throw new HopException("Unable to apply AI Provider '" + meta.getAiProviderName() + "'", e);
    }
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }
}
