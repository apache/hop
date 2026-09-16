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

import java.util.Objects;
import lombok.Getter;

/** Encapsulates the target diagram export format and its standard properties. */
@Getter
public class DiagramExportFormat {
  public static final DiagramExportFormat SVG =
      new DiagramExportFormat("SVG", "Scalable Vector Graphics (SVG)", "svg", "image/svg+xml");
  public static final DiagramExportFormat MERMAID =
      new DiagramExportFormat("MERMAID", "Mermaid Diagram (.mmd)", "mmd", "text/vnd.mermaid");
  public static final DiagramExportFormat PDF =
      new DiagramExportFormat("PDF", "Portable Document Format (PDF)", "pdf", "application/pdf");
  public static final DiagramExportFormat PLANTUML =
      new DiagramExportFormat("PLANTUML", "PlantUML (.puml)", "puml", "text/plain");
  public static final DiagramExportFormat DRAWIO =
      new DiagramExportFormat("DRAWIO", "Draw.io Diagram (.drawio)", "drawio", "application/xml");

  private final String id;
  private final String name;
  private final String defaultExtension;
  private final String mimeType;

  public DiagramExportFormat(String id, String name, String defaultExtension, String mimeType) {
    this.id = id;
    this.name = name;
    this.defaultExtension = defaultExtension;
    this.mimeType = mimeType;
  }

  public static DiagramExportFormat of(
      String id, String name, String defaultExtension, String mimeType) {
    return new DiagramExportFormat(id, name, defaultExtension, mimeType);
  }

  public static DiagramExportFormat parse(String formatId) {
    if (formatId == null) {
      return null;
    }
    String upper = formatId.trim().toUpperCase();
    if (upper.equals(SVG.getId())) {
      return SVG;
    }
    if (upper.equals(MERMAID.getId())) {
      return MERMAID;
    }
    if (upper.equals(PDF.getId())) {
      return PDF;
    }
    if (upper.equals(PLANTUML.getId())) {
      return PLANTUML;
    }
    if (upper.equals(DRAWIO.getId())) {
      return DRAWIO;
    }
    return new DiagramExportFormat(upper, upper, upper.toLowerCase(), "application/octet-stream");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DiagramExportFormat that)) {
      return false;
    }
    return Objects.equals(id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public String toString() {
    return name != null ? name : id;
  }
}
