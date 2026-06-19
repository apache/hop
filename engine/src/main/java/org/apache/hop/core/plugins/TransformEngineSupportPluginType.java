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

package org.apache.hop.core.plugins;

import org.apache.hop.core.annotations.TransformEngineSupport;
import org.apache.hop.pipeline.transform.ITransformMeta;

/**
 * Plugin type for {@link TransformEngineSupport} fragments. Scanned at startup; each annotated
 * class becomes an {@link IPlugin} whose {@code supportedEngines} / {@code excludedEngines} arrays
 * are merged into the host {@link TransformPluginType} plugin with matching {@code id()} via {@link
 * IPlugin#merge(IPlugin)}.
 *
 * <p>This lets third-party jars declare engine compatibility for transforms they do not own — the
 * fragment lives in its own jar, ships its own {@code @TransformEngineSupport(id="HostId", ...)},
 * and {@link BaseFragmentType}'s listeners take care of the merge regardless of registration order
 * (host-first or fragment-first).
 */
@PluginMainClassType(ITransformMeta.class)
@PluginAnnotationType(TransformEngineSupport.class)
public class TransformEngineSupportPluginType extends BaseFragmentType<TransformEngineSupport> {

  private static TransformEngineSupportPluginType instance;

  private TransformEngineSupportPluginType() {
    super(
        TransformEngineSupport.class,
        "TRANSFORM_ENGINE_SUPPORT",
        "Transform Engine Support",
        TransformPluginType.class);
  }

  public static synchronized TransformEngineSupportPluginType getInstance() {
    if (instance == null) {
      instance = new TransformEngineSupportPluginType();
    }
    return instance;
  }

  @Override
  protected String extractID(TransformEngineSupport annotation) {
    return annotation.id();
  }

  @Override
  protected String[] extractSupportedEngines(TransformEngineSupport annotation) {
    return annotation.supportedEngines();
  }

  @Override
  protected String[] extractExcludedEngines(TransformEngineSupport annotation) {
    return annotation.excludedEngines();
  }
}
