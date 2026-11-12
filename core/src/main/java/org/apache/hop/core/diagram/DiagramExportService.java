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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;

/** Service facade for discovering diagram exporters and orchestrating diagram export operations. */
public class DiagramExportService {
  private static final ILogChannel log = new LogChannel("DiagramExportService");
  private static DiagramExportService instance;

  private final List<IDiagramSubjectLoader> subjectLoaders = new CopyOnWriteArrayList<>();
  private final List<IDiagramExporter<?>> staticExporters = new CopyOnWriteArrayList<>();

  private DiagramExportService() {}

  public static synchronized DiagramExportService getInstance() {
    if (instance == null) {
      instance = new DiagramExportService();
    }
    return instance;
  }

  public void registerSubjectLoader(IDiagramSubjectLoader loader) {
    if (loader != null && !subjectLoaders.contains(loader)) {
      subjectLoaders.add(loader);
    }
  }

  public void unregisterSubjectLoader(IDiagramSubjectLoader loader) {
    subjectLoaders.remove(loader);
  }

  public void registerStaticExporter(IDiagramExporter<?> exporter) {
    if (exporter != null && !staticExporters.contains(exporter)) {
      staticExporters.add(exporter);
    }
  }

  public void unregisterStaticExporter(IDiagramExporter<?> exporter) {
    staticExporters.remove(exporter);
  }

  public List<IDiagramExporter<?>> getAllExporters() {
    List<IDiagramExporter<?>> exporters = new ArrayList<>(staticExporters);
    PluginRegistry registry = PluginRegistry.getInstance();
    List<IPlugin> plugins = registry.getPlugins(DiagramExporterPluginType.class);
    for (IPlugin plugin : plugins) {
      try {
        IDiagramExporter<?> exporter = (IDiagramExporter<?>) registry.loadClass(plugin);
        if (exporter != null) {
          exporters.add(exporter);
        }
      } catch (Exception e) {
        log.logError("Failed to load diagram exporter plugin " + plugin.getName(), e);
      }
    }
    return exporters;
  }

  public List<IDiagramExporter<?>> findExportersForSubject(Object subject) {
    List<IDiagramExporter<?>> result = new ArrayList<>();
    if (subject == null) {
      return result;
    }
    for (IDiagramExporter<?> exporter : getAllExporters()) {
      if (exporter.supportsSubject(subject)) {
        result.add(exporter);
      }
    }
    return result;
  }

  public IDiagramExporter<?> findExporter(Object subject, String formatId) {
    if (formatId == null) {
      return null;
    }
    return findExporterForFormat(findExportersForSubject(subject), formatId);
  }

  /**
   * Finds the first of the given exporters that writes a format, matched in upper or lower case on
   * the format id ({@code MERMAID}), the file extension ({@code mmd}) or the exporter id ({@code
   * pipeline-mermaid}). The exporter id is what the GUI sends for an exporter without a format.
   *
   * @param exporters the exporters to look in, for example those for one subject
   * @param format the format id, file extension or exporter id
   * @return the matching exporter, or {@code null} when none matches
   */
  public static IDiagramExporter<?> findExporterForFormat(
      List<IDiagramExporter<?>> exporters, String format) {
    if (exporters == null || StringUtils.isBlank(format)) {
      return null;
    }
    String wanted = format.trim();
    for (IDiagramExporter<?> exporter : exporters) {
      if ((exporter.getFormat() != null
              && StringUtils.equalsIgnoreCase(wanted, exporter.getFormat().getId()))
          || StringUtils.equalsIgnoreCase(wanted, exporter.getFileExtension())
          || StringUtils.equalsIgnoreCase(wanted, exporter.getId())) {
        return exporter;
      }
    }
    return null;
  }

  public IDiagramExporter<?> getExporter(String id) {
    if (id == null) {
      return null;
    }
    for (IDiagramExporter<?> exporter : getAllExporters()) {
      if (id.equalsIgnoreCase(exporter.getId())) {
        return exporter;
      }
    }
    return null;
  }

  public IDiagramSubjectLoader findSubjectLoader(String filename) {
    if (filename == null) {
      return null;
    }
    for (IDiagramSubjectLoader loader : subjectLoaders) {
      if (loader.supportsFile(filename)) {
        return loader;
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  public DiagramExportResult export(
      Object subject, DiagramExportOptions options, IExportContext context) throws HopException {
    if (subject == null) {
      throw new HopException("No subject provided to export");
    }
    if (options == null) {
      throw new HopException("No export options provided");
    }
    List<IDiagramExporter<?>> available = findExportersForSubject(subject);
    IDiagramExporter exporter;
    if (StringUtils.isNotBlank(options.getFormat())) {
      // A format that was asked for must be honoured: never write another format under its name.
      exporter = findExporterForFormat(available, options.getFormat());
      if (exporter == null) {
        throw new HopException(
            "No diagram exporter for format '"
                + options.getFormat().trim()
                + "' supports "
                + subject.getClass().getName()
                + ". Available formats: "
                + describeFormats(available));
      }
    } else if (!available.isEmpty()) {
      exporter = available.get(0);
    } else {
      throw new HopException(
          "No diagram exporter found supporting subject " + subject.getClass().getName());
    }
    return exporter.export(subject, options, context);
  }

  /**
   * Describes the formats of the given exporters, for example {@code MERMAID (.mmd), SVG (.svg)}.
   *
   * @param exporters the exporters to describe
   * @return the format ids with their file extension, sorted, or {@code none} for an empty list
   */
  public static String describeFormats(List<IDiagramExporter<?>> exporters) {
    Set<String> formats = new TreeSet<>();
    for (IDiagramExporter<?> exporter : exporters) {
      if (exporter.getFormat() == null) {
        continue;
      }
      String extension = exporter.getFileExtension();
      formats.add(
          exporter.getFormat().getId()
              + (StringUtils.isNotBlank(extension) ? " (." + extension + ")" : ""));
    }
    return formats.isEmpty() ? "none" : String.join(", ", formats);
  }

  public DiagramExportResult exportFile(
      String filename, DiagramExportOptions options, IExportContext context) throws HopException {
    return export(loadSubject(filename, context), options, context);
  }

  /**
   * Loads the subject (pipeline, workflow, ...) of a file with the subject loader for that file.
   *
   * @param filename the file to load
   * @param context the context with the metadata provider and variables to load it with
   * @return the subject, never {@code null}
   * @throws HopException when no loader supports the file or the file can't be loaded
   */
  public Object loadSubject(String filename, IExportContext context) throws HopException {
    if (StringUtils.isBlank(filename)) {
      throw new HopException("No filename provided to export");
    }
    IDiagramSubjectLoader loader = findSubjectLoader(filename);
    if (loader == null) {
      throw new HopException("No subject loader available for file: " + filename);
    }
    Object subject =
        loader.loadSubject(
            filename,
            context != null ? context.getMetadataProvider() : null,
            context != null ? context.getVariables() : null);
    if (subject == null) {
      throw new HopException("Failed to load diagram subject from file: " + filename);
    }
    return subject;
  }
}
