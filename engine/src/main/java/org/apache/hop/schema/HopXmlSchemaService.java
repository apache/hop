/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.schema;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.serializer.xml.XmlMetadataSchema;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;

/**
 * Service to generate XML Schema (XSD) documents for pipelines, workflows, transforms, and actions.
 */
public class HopXmlSchemaService {

  private static final ILogChannel log = LogChannel.GENERAL;

  private static HopXmlSchemaService instance;

  public static synchronized HopXmlSchemaService getInstance() {
    if (instance == null) {
      instance = new HopXmlSchemaService();
    }
    return instance;
  }

  /** Generate XML schema for PipelineMeta. */
  public String generatePipelineSchema(HopXmlSchemaExportOptions options) {
    XmlMetadataSchema schema = new XmlMetadataSchema(PipelineMeta.class, PipelineMeta.XML_TAG);
    schema.setFlexibleElementOrder(options.isFlexibleElementOrder());
    schema.setIncludeLaxAny(options.isIncludeLaxAny());
    return schema.generate();
  }

  /** Generate XML schema for WorkflowMeta. */
  public String generateWorkflowSchema(HopXmlSchemaExportOptions options) {
    XmlMetadataSchema schema = new XmlMetadataSchema(WorkflowMeta.class, WorkflowMeta.XML_TAG);
    schema.setFlexibleElementOrder(options.isFlexibleElementOrder());
    schema.setIncludeLaxAny(options.isIncludeLaxAny());
    return schema.generate();
  }

  /** Generate XML schema for an individual transform plugin. */
  public String generateTransformSchema(IPlugin plugin, HopXmlSchemaExportOptions options)
      throws HopException {
    PluginRegistry registry = PluginRegistry.getInstance();
    Class<? extends ITransformMeta> pluginClass;
    try {
      ITransformMeta meta = registry.loadClass(plugin, ITransformMeta.class);
      pluginClass = meta.getClass();
    } catch (Exception e) {
      throw new HopException("Unable to load transform plugin class for " + plugin.getIds()[0], e);
    }

    String pluginId = plugin.getIds()[0];
    XmlMetadataSchema schema = new XmlMetadataSchema(TransformMeta.class, "transform");
    schema.setFlexibleElementOrder(options.isFlexibleElementOrder());
    schema.setIncludeLaxAny(false);
    schema.substituteType(ITransformMeta.class, pluginClass);
    schema.setFixedFieldValue("type", pluginId);

    // Also register the standalone plugin type element
    String pluginTypeName = schema.getOrCreateComplexTypeName(pluginClass);
    schema.generateComplexType(pluginClass, pluginTypeName);
    schema.addExtraRootElement(pluginId, pluginTypeName);

    return schema.generate();
  }

  /** Generate XML schema for a transform plugin by ID. */
  public String generateTransformSchema(String pluginId, HopXmlSchemaExportOptions options)
      throws HopException {
    IPlugin plugin =
        PluginRegistry.getInstance().findPluginWithId(TransformPluginType.class, pluginId);
    if (plugin == null) {
      throw new HopException("Transform plugin not found with ID: " + pluginId);
    }
    return generateTransformSchema(plugin, options);
  }

  /** Generate XML schema for an individual action plugin. */
  public String generateActionSchema(IPlugin plugin, HopXmlSchemaExportOptions options)
      throws HopException {
    PluginRegistry registry = PluginRegistry.getInstance();
    Class<? extends IAction> pluginClass;
    try {
      IAction action = registry.loadClass(plugin, IAction.class);
      pluginClass = action.getClass();
    } catch (Exception e) {
      throw new HopException("Unable to load action plugin class for " + plugin.getIds()[0], e);
    }

    String pluginId = plugin.getIds()[0];
    XmlMetadataSchema schema = new XmlMetadataSchema(ActionMeta.class, "action");
    schema.setFlexibleElementOrder(options.isFlexibleElementOrder());
    schema.setIncludeLaxAny(false);
    schema.substituteType(IAction.class, pluginClass);
    schema.setFixedFieldValue("type", pluginId);

    // Also register the standalone plugin type element
    String pluginTypeName = schema.getOrCreateComplexTypeName(pluginClass);
    schema.generateComplexType(pluginClass, pluginTypeName);
    schema.addExtraRootElement(pluginId, pluginTypeName);

    return schema.generate();
  }

  /** Generate XML schema for an action plugin by ID. */
  public String generateActionSchema(String pluginId, HopXmlSchemaExportOptions options)
      throws HopException {
    IPlugin plugin =
        PluginRegistry.getInstance().findPluginWithId(ActionPluginType.class, pluginId);
    if (plugin == null) {
      throw new HopException("Action plugin not found with ID: " + pluginId);
    }
    return generateActionSchema(plugin, options);
  }

  /** Export all selected schemas into the target folder using Hop Commons VFS. */
  public HopXmlSchemaExportResult exportAllSchemas(
      FileObject targetFolder, HopXmlSchemaExportOptions options) throws HopException {
    return exportAllSchemas(targetFolder, options, null);
  }

  /** Export all selected schemas into the target folder using Hop Commons VFS. */
  public HopXmlSchemaExportResult exportAllSchemas(
      FileObject targetFolder, HopXmlSchemaExportOptions options, IProgressMonitor monitor)
      throws HopException {

    HopXmlSchemaExportResult result = new HopXmlSchemaExportResult();

    try {
      if (!targetFolder.exists()) {
        targetFolder.createFolder();
      }

      int totalWork = 0;
      if (options.isExportPipeline()) totalWork++;
      if (options.isExportWorkflow()) totalWork++;

      PluginRegistry registry = PluginRegistry.getInstance();
      List<IPlugin> transformPlugins = registry.getPlugins(TransformPluginType.class);
      List<IPlugin> actionPlugins = registry.getPlugins(ActionPluginType.class);

      if (options.isExportTransforms()) {
        totalWork +=
            StringUtils.isNotEmpty(options.getTransformPluginId()) ? 1 : transformPlugins.size();
      }
      if (options.isExportActions()) {
        totalWork += StringUtils.isNotEmpty(options.getActionPluginId()) ? 1 : actionPlugins.size();
      }

      if (monitor != null) {
        monitor.beginTask("Exporting XML Schemas...", totalWork);
      }

      // 1. Pipeline Schema
      if (options.isExportPipeline()) {
        if (monitor != null) {
          monitor.subTask("Generating pipeline.xsd...");
        }
        try {
          String pipelineXsd = generatePipelineSchema(options);
          FileObject pipelineFile = targetFolder.resolveFile("pipeline.xsd");
          writeFile(pipelineFile, pipelineXsd);
          result.setPipelineSchemaGenerated(true);
          result.getGeneratedFiles().add(pipelineFile.getName().getURI());
        } catch (Exception e) {
          result.getErrors().add("Error generating pipeline.xsd: " + e.getMessage());
          log.logError("Error generating pipeline.xsd", e);
        }
        if (monitor != null) {
          monitor.worked(1);
        }
      }

      // 2. Workflow Schema
      if (options.isExportWorkflow()) {
        if (monitor != null) {
          monitor.subTask("Generating workflow.xsd...");
        }
        try {
          String workflowXsd = generateWorkflowSchema(options);
          FileObject workflowFile = targetFolder.resolveFile("workflow.xsd");
          writeFile(workflowFile, workflowXsd);
          result.setWorkflowSchemaGenerated(true);
          result.getGeneratedFiles().add(workflowFile.getName().getURI());
        } catch (Exception e) {
          result.getErrors().add("Error generating workflow.xsd: " + e.getMessage());
          log.logError("Error generating workflow.xsd", e);
        }
        if (monitor != null) {
          monitor.worked(1);
        }
      }

      // 3. Transform Schemas
      if (options.isExportTransforms()) {
        FileObject transformsFolder = targetFolder.resolveFile("transforms");
        if (!transformsFolder.exists()) {
          transformsFolder.createFolder();
        }

        for (IPlugin plugin : transformPlugins) {
          String pluginId = plugin.getIds()[0];
          if (!matchesFilter(
              pluginId, options.getTransformPluginId(), options.getPluginFilterPattern())) {
            continue;
          }

          if (monitor != null) {
            monitor.subTask("Generating transform schema: " + pluginId + "...");
          }

          try {
            String transformXsd = generateTransformSchema(plugin, options);
            FileObject transformFile = transformsFolder.resolveFile(pluginId + ".xsd");
            writeFile(transformFile, transformXsd);
            result.setTransformSchemasCount(result.getTransformSchemasCount() + 1);
            result.getGeneratedFiles().add(transformFile.getName().getURI());
          } catch (Exception e) {
            String msg =
                "Could not generate schema for transform plugin "
                    + pluginId
                    + ": "
                    + e.getMessage();
            result.getWarnings().add(msg);
            log.logBasic(msg);
          }

          if (monitor != null) {
            monitor.worked(1);
          }
        }
      }

      // 4. Action Schemas
      if (options.isExportActions()) {
        FileObject actionsFolder = targetFolder.resolveFile("actions");
        if (!actionsFolder.exists()) {
          actionsFolder.createFolder();
        }

        for (IPlugin plugin : actionPlugins) {
          String pluginId = plugin.getIds()[0];
          if (!matchesFilter(
              pluginId, options.getActionPluginId(), options.getPluginFilterPattern())) {
            continue;
          }

          if (monitor != null) {
            monitor.subTask("Generating action schema: " + pluginId + "...");
          }

          try {
            String actionXsd = generateActionSchema(plugin, options);
            FileObject actionFile = actionsFolder.resolveFile(pluginId + ".xsd");
            writeFile(actionFile, actionXsd);
            result.setActionSchemasCount(result.getActionSchemasCount() + 1);
            result.getGeneratedFiles().add(actionFile.getName().getURI());
          } catch (Exception e) {
            String msg =
                "Could not generate schema for action plugin " + pluginId + ": " + e.getMessage();
            result.getWarnings().add(msg);
            log.logBasic(msg);
          }

          if (monitor != null) {
            monitor.worked(1);
          }
        }
      }

    } catch (Exception e) {
      throw new HopException("Error exporting XML schemas", e);
    } finally {
      if (monitor != null) {
        monitor.done();
      }
    }

    return result;
  }

  private boolean matchesFilter(String pluginId, String specificId, String filterPattern) {
    if (StringUtils.isNotEmpty(specificId) && !pluginId.equalsIgnoreCase(specificId)) {
      return false;
    }
    if (StringUtils.isNotEmpty(filterPattern)) {
      try {
        if (!pluginId.matches(filterPattern)
            && !pluginId.toLowerCase().contains(filterPattern.toLowerCase())) {
          return false;
        }
      } catch (Exception e) {
        if (!pluginId.toLowerCase().contains(filterPattern.toLowerCase())) {
          return false;
        }
      }
    }
    return true;
  }

  private void writeFile(FileObject file, String content) throws HopException {
    try (OutputStream out = HopVfs.getOutputStream(file, false)) {
      out.write(content.getBytes(StandardCharsets.UTF_8));
      out.flush();
    } catch (Exception e) {
      throw new HopException("Error writing XML schema file " + file.getName().getURI(), e);
    }
  }
}
