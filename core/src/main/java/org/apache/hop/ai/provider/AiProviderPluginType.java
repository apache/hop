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

import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;

/** Plugin type for {@link IAiProvider} implementations. */
@PluginMainClassType(IAiProvider.class)
@PluginAnnotationType(AiProviderPlugin.class)
public class AiProviderPluginType extends BasePluginType<AiProviderPlugin> {

  private static AiProviderPluginType pluginType;

  private AiProviderPluginType() {
    super(AiProviderPlugin.class, "AI_PROVIDERS", "AI providers");
  }

  public static AiProviderPluginType getInstance() {
    if (pluginType == null) {
      pluginType = new AiProviderPluginType();
    }
    return pluginType;
  }

  @Override
  protected String extractID(AiProviderPlugin annotation) {
    return annotation.id();
  }

  @Override
  protected String extractName(AiProviderPlugin annotation) {
    return annotation.name();
  }

  @Override
  protected String extractDesc(AiProviderPlugin annotation) {
    return annotation.description();
  }

  @Override
  protected String extractImageFile(AiProviderPlugin annotation) {
    return annotation.image();
  }

  @Override
  protected String extractDocumentationUrl(AiProviderPlugin annotation) {
    return annotation.documentationUrl();
  }

  @Override
  protected String extractClassLoaderGroup(AiProviderPlugin annotation) {
    return annotation.classLoaderGroup();
  }
}
