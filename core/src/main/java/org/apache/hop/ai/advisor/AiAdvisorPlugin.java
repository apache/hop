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

package org.apache.hop.ai.advisor;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks an {@link IAiAdvisor} implementation so the plugin registry finds it.
 *
 * <pre>
 * {@literal @}AiAdvisorPlugin(
 *     id = "pipeline-advisor",
 *     name = "Pipeline AI Help",
 *     locations = {AiAdvisorLocations.PIPELINE_GRAPH})
 * public class PipelineAiAdvisor implements IAiAdvisor { ... }
 * </pre>
 *
 * {@link #locations()} are free-form ids. Hop ships {@link AiAdvisorLocations} for pipeline and
 * workflow graphs; hopper-edw (and any other plugin) adds its own, for example {@code
 * data-vault-graph}, {@code business-vault-graph}, {@code dimensional-graph}, {@code lineage-view}.
 * GUI entry points are separate {@code @GuiToolbarElement} / {@code @GuiContextAction} plugins that
 * call {@code HopGui.openAiAdvisorSession}.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface AiAdvisorPlugin {

  String id();

  String name();

  String description() default "";

  String image() default "";

  String documentationUrl() default "";

  /**
   * Location ids this advisor is intended for. Empty means the workbench may offer it anywhere. Not
   * a closed set — plugins invent new ids as they add file types and editors.
   */
  String[] locations() default {};

  String classLoaderGroup() default "";
}
