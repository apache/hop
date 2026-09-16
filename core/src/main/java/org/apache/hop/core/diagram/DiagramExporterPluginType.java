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

import java.util.Map;
import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;

/** Plugin type managing diagram exporter plugins. */
@PluginMainClassType(IDiagramExporter.class)
@PluginAnnotationType(DiagramExporter.class)
public class DiagramExporterPluginType extends BasePluginType<DiagramExporter> {

  private static DiagramExporterPluginType pluginType;

  private DiagramExporterPluginType() {
    super(DiagramExporter.class, "DIAGRAM_EXPORTER", "Diagram Exporter");
  }

  public static DiagramExporterPluginType getInstance() {
    if (pluginType == null) {
      pluginType = new DiagramExporterPluginType();
    }
    return pluginType;
  }

  @Override
  protected String extractCategory(DiagramExporter annotation) {
    return annotation.format();
  }

  @Override
  protected String extractDesc(DiagramExporter annotation) {
    return annotation.description();
  }

  @Override
  protected String extractID(DiagramExporter annotation) {
    return annotation.id();
  }

  @Override
  protected String extractName(DiagramExporter annotation) {
    return annotation.name();
  }

  @Override
  protected String extractImageFile(DiagramExporter annotation) {
    return null;
  }

  @Override
  protected boolean extractSeparateClassLoader(DiagramExporter annotation) {
    return false;
  }

  @Override
  protected void addExtraClasses(
      Map<Class<?>, String> classMap, Class<?> clazz, DiagramExporter annotation) {
    // No extra classes needed
  }

  @Override
  protected String extractDocumentationUrl(DiagramExporter annotation) {
    return null;
  }

  @Override
  protected String extractCasesUrl(DiagramExporter annotation) {
    return null;
  }

  @Override
  protected String extractForumUrl(DiagramExporter annotation) {
    return null;
  }

  @Override
  protected String extractSuggestion(DiagramExporter annotation) {
    return null;
  }

  @Override
  protected String extractClassLoaderGroup(DiagramExporter annotation) {
    return null;
  }
}
