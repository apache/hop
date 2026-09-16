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
    for (IDiagramExporter<?> exporter : findExportersForSubject(subject)) {
      if (exporter.getFormat() != null
          && exporter.getFormat().getId().equalsIgnoreCase(formatId.trim())) {
        return exporter;
      }
      if (exporter.getFileExtension() != null
          && exporter.getFileExtension().equalsIgnoreCase(formatId.trim())) {
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
    IDiagramExporter exporter = null;
    if (StringUtils.isNotEmpty(options.getFormat())) {
      exporter = findExporter(subject, options.getFormat());
    }
    if (exporter == null) {
      List<IDiagramExporter<?>> available = findExportersForSubject(subject);
      if (!available.isEmpty()) {
        exporter = available.get(0);
      }
    }
    if (exporter == null) {
      throw new HopException(
          "No diagram exporter found supporting subject "
              + subject.getClass().getName()
              + (options.getFormat() != null ? " with format " + options.getFormat() : ""));
    }
    return exporter.export(subject, options, context);
  }

  public DiagramExportResult exportFile(
      String filename, DiagramExportOptions options, IExportContext context) throws HopException {
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
    return export(subject, options, context);
  }
}
