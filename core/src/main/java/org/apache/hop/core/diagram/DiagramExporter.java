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

package org.apache.hop.core.diagram;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Annotation used to define a diagram exporter plugin. */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface DiagramExporter {
  /** The unique ID of the diagram exporter plugin. */
  String id();

  /** The user-friendly name of the exporter. */
  String name();

  /** A short description of the exporter. */
  String description() default "";

  /** The format identifier (e.g. SVG, MERMAID, PDF, PLANTUML, DRAWIO). */
  String format();

  /** The default file extension (without dot), e.g. "svg", "mmd". */
  String fileExtension();

  /** The file filter descriptions for file dialogs. */
  String[] fileFilterNames() default {};

  /** Supported domain subject classes (e.g. PipelineMeta, WorkflowMeta, or custom models). */
  Class<?>[] supportedSubjectTypes() default {};
}
