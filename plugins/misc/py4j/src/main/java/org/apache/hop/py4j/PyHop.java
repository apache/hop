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
 *
 */

package org.apache.hop.py4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingBuffer;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.plugin.MetadataPluginType;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engine.WorkflowEngineFactory;
import org.jspecify.annotations.NonNull;

/** This is the starting point for the Hop Python scripting methods. In here we'll have */
@Getter
@Setter
public class PyHop {
  private IVariables variables;
  private IHopMetadataProvider metadataProvider;
  private ILogChannel log;
  private String stopPassword;
  private ArrayBlockingQueue<Object> blockingQueue;

  public PyHop() {
    this.blockingQueue = new ArrayBlockingQueue<>(10);
  }

  public void initialize(
      IVariables variables, IHopMetadataProvider metadataProvider, ILogChannel log) {
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.log = log;
  }

  public ILogChannel getLogChannel() {
    return log;
  }

  //
  // The Pipeline API
  //

  /**
   * Load a pipeline from a file. The filename can contain variable expressions.
   *
   * @param filename The filename
   * @return The pipeline metadata
   * @throws HopException In case there was an error loading the metadata from the specified file.
   */
  public PipelineMeta loadPipelineMeta(String filename) throws HopException {
    return new PipelineMeta(variables.resolve(filename), metadataProvider, variables);
  }

  /**
   * Create a new pipeline metadata object
   *
   * @return A new empty pipeline metadata object.
   */
  public PipelineMeta newPipelineMeta() {
    return new PipelineMeta();
  }

  /**
   * Describe the available transform plugins
   *
   * @return A description of the available transform plugins IDs and names
   */
  public String describeAvailableTransformPlugins() {
    return describeAvailablePlugins(TransformPluginType.class);
  }

  /**
   * Create a new transform metadata object
   *
   * @param name The name of the object to create
   * @param pluginId The ID of the plugin
   * @return a new transform metadata object
   * @throws HopPluginException In case there was an error creating the new transform metadata
   */
  public TransformMeta newTransformMeta(String name, String pluginId) throws HopPluginException {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName(name);

    PluginRegistry registry = PluginRegistry.getInstance();
    ITransformMeta transform =
        registry.loadClass(TransformPluginType.class, pluginId, ITransformMeta.class);
    if (transform == null) {
      throw new HopPluginException(
          "Unable to find transform plugin ID: "
              + pluginId
              + ".  These are available:"
              + Const.CR
              + describeAvailableActionPlugins()
              + ". These transform plugins are available:"
              + Const.CR
              + describeAvailableActionPlugins());
    }
    transformMeta.setTransform(transform);
    return transformMeta;
  }

  /**
   * Create a new pipeline hop between two transforms
   *
   * @param from The originating transform
   * @param to The destination transform
   * @return A new pipeline hop metadata object
   */
  public PipelineHopMeta newPipelineHopMeta(TransformMeta from, TransformMeta to) {
    return new PipelineHopMeta(from, to);
  }

  /**
   * Create a new pipeline engine. You can execute your pipeline with it.
   *
   * @param pipelineMeta The pipeline metadata
   * @param runConfiguration The name of the pipeline run configuration
   * @param logLevelDescription The logging level description
   * @return The new pipeline engine
   * @throws HopException In case there was a problem creating the new pipeline engine.
   */
  public IPipelineEngine<PipelineMeta> newPipelineEngine(
      PipelineMeta pipelineMeta, String runConfiguration, String logLevelDescription)
      throws HopException {
    LogLevel logLevel = LogLevel.lookupDescription(logLevelDescription);
    IPipelineEngine<PipelineMeta> pipelineEngine =
        PipelineEngineFactory.createPipelineEngine(
            variables, runConfiguration, metadataProvider, pipelineMeta);
    pipelineEngine.setLogLevel(logLevel);
    return pipelineEngine;
  }

  //
  // The Workflow API
  //

  /**
   * Load workflow metadata from a file. The filename can contain variable expressions.
   *
   * @param filename The filename
   * @return The workflow metadata
   * @throws HopException In case there was an error loading the metadata from the specified file.
   */
  public WorkflowMeta loadWorkflowMeta(String filename) throws HopException {
    return new WorkflowMeta(variables, variables.resolve(filename), metadataProvider);
  }

  /**
   * Create a new workflow metadata object. It does not contain a START action.
   *
   * @return A new empty workflow metadata object.
   */
  public WorkflowMeta newWorkflowMeta() {
    return new WorkflowMeta();
  }

  /**
   * Describe the available action plugins
   *
   * @return A description of the available action plugins IDs and names
   */
  public String describeAvailableActionPlugins() {
    return describeAvailablePlugins(ActionPluginType.class);
  }

  /**
   * Create a new action metadata object
   *
   * @param name The name of the object to create
   * @param pluginId The ID of the plugin
   * @return a new action metadata object
   * @throws HopPluginException In case there was an error creating the new action metadata
   */
  public ActionMeta newActionMeta(String name, String pluginId) throws HopPluginException {
    ActionMeta actionMeta = new ActionMeta();
    actionMeta.setName(name);

    PluginRegistry registry = PluginRegistry.getInstance();
    IAction action = registry.loadClass(ActionPluginType.class, pluginId, IAction.class);
    if (action == null) {
      throw new HopPluginException(
          "Unable to find action plugin ID: "
              + pluginId
              + ".  These action plugins are available:"
              + Const.CR
              + describeAvailableActionPlugins());
    }
    actionMeta.setAction(action);
    return actionMeta;
  }

  /**
   * Create a new workflow hop between two actions
   *
   * @param from The originating action
   * @param to The destination action
   * @return A new workflow hop metadata object
   */
  public WorkflowHopMeta newWorkflowHopMeta(ActionMeta from, ActionMeta to) {
    return new WorkflowHopMeta(from, to);
  }

  /**
   * Create a new workflow engine. You can execute your workflow with it.
   *
   * @param workflowMeta The workflow metadata
   * @param runConfiguration The name of the workflow run configuration
   * @param logLevelDescription The logging level description
   * @return The new workflow engine
   * @throws HopException In case there was a problem creating the new workflow engine.
   */
  public IWorkflowEngine<WorkflowMeta> newWorkflowEngine(
      WorkflowMeta workflowMeta, String runConfiguration, String logLevelDescription)
      throws HopException {
    LogLevel logLevel = LogLevel.lookupDescription(logLevelDescription);
    IWorkflowEngine<WorkflowMeta> workflowEngine =
        WorkflowEngineFactory.createWorkflowEngine(
            variables, runConfiguration, metadataProvider, workflowMeta, null);
    workflowEngine.setLogLevel(logLevel);
    return workflowEngine;
  }

  //
  // The Metadata API
  //

  /**
   * Describe the available metadata plugins
   *
   * @return A description of the available metadata plugin IDs and names
   */
  public @NonNull String describeAvailableMetadataPlugins() {
    return describeAvailablePlugins(MetadataPluginType.class);
  }

  public IHopMetadataSerializer<IHopMetadata> getMetadataSerializer(String key)
      throws HopException {
    Class<IHopMetadata> metadataClass = getMetadataClass(key);
    return metadataProvider.getSerializer(metadataClass);
  }

  /**
   * Create a new metadata element for the given type key.
   *
   * @param key The metadata plugin ID or key. It's the same as the folder name for the JSON
   *     serialized elements.
   * @return The new metadata object.
   * @throws HopException In case the object couldn't be created.
   */
  public IHopMetadata newMetadataElement(String key) throws HopException {
    Class<IHopMetadata> metadataClass = getMetadataClass(key);
    try {
      return metadataClass.getConstructor().newInstance();
    } catch (Exception e) {
      throw new HopException("Unable to instantiate metadata class: " + metadataClass.getName(), e);
    }
  }

  public IHopMetadata loadMetadataElement(String key, String name) throws HopException {
    validateMetadataKey(key);
    if (StringUtils.isEmpty(name)) {
      throw new HopException("Please provide a metadata name to load");
    }
    Class<IHopMetadata> metadataClass = getMetadataClass(key);
    IHopMetadataSerializer<IHopMetadata> serializer = metadataProvider.getSerializer(metadataClass);
    return serializer.load(name);
  }

  public void saveMetadataElement(IHopMetadata metadata) throws HopException {
    if (metadata == null) {
      throw new HopException("Please provide a non-null metadata element to save");
    }
    IHopMetadataSerializer<IHopMetadata> serializer =
        (IHopMetadataSerializer<IHopMetadata>) metadataProvider.getSerializer(metadata.getClass());
    serializer.save(metadata);
  }

  public String[] listMetadataKeys() {
    List<String> keys = new ArrayList<>();
    for (Class<IHopMetadata> metadataClass : metadataProvider.getMetadataClasses()) {
      HopMetadata annotation = metadataClass.getAnnotation(HopMetadata.class);
      keys.add(annotation.key());
    }
    return keys.toArray(new String[0]);
  }

  public String[] listMetadataElements(String key) throws HopException {
    validateMetadataKey(key);
    Class<IHopMetadata> metadataClass = getMetadataClass(key);
    IHopMetadataSerializer<IHopMetadata> serializer = metadataProvider.getSerializer(metadataClass);
    return serializer.listObjectNames().toArray(new String[0]);
  }

  /**
   * Create a new object in the classpath of the given transform plugin class. For example, we would
   * have a CsvInputMeta object and want to create a new CsvInputField field.
   *
   * @param pluginObject The plugin object to reference
   * @param className The class of the object to instantiate
   * @return A new object of the given class
   * @throws HopException In case the class can't be found
   */
  public Object newTransformObject(Object pluginObject, String className) throws HopException {
    if (pluginObject == null) {
      throw new HopPluginException("Please specify a plugin object to reference.");
    }
    if (className == null) {
      throw new HopPluginException("Please specify the name of the class to instantiate.");
    }

    PluginRegistry registry = PluginRegistry.getInstance();
    String pluginClass = pluginObject.getClass().getName();

    // We'll find the plugin ID
    //
    String pluginId =
        registry.findPluginIdWithMainClassName(TransformPluginType.class, pluginClass);
    if (pluginId == null) {
      throw new HopPluginException(
          "Unable to find transform plugin ID for class: " + pluginObject.getClass().getName());
    }
    IPlugin plugin = registry.getPlugin(TransformPluginType.class, pluginId);
    ClassLoader classLoader = registry.getClassLoader(plugin);
    if (plugin == null || classLoader == null) {
      throw new HopException(
          "The plugin or classloader for plugin ID "
              + pluginId
              + " and class "
              + pluginClass
              + " could not be found.");
    }

    try {
      Class<?> clazz = classLoader.loadClass(className);
      return clazz.getConstructor().newInstance();
    } catch (ClassNotFoundException e) {
      throw new HopException(
          "Unable to load class '" + className + "' in the class loader of plugin " + pluginId, e);
    } catch (Exception e) {
      throw new HopException(
          "Unable to instantiate class '"
              + className
              + "'.  Make sure there is a public empty constructor available.",
          e);
    }
  }

  private Class<IHopMetadata> getMetadataClass(String key) throws HopException {
    try {
      Class<IHopMetadata> metadataClass = metadataProvider.getMetadataClassForKey(key);
      validateMetadataKey(key, metadataClass);
      return metadataClass;
    } catch (Exception e) {
      throw new HopException(
          "Unable to find metadata class for key: "
              + key
              + ". The available keys are: "
              + Const.CR
              + describeAvailableMetadataPlugins(),
          e);
    }
  }

  private void validateMetadataKey(String key, Class<IHopMetadata> metadataClass)
      throws HopException {
    validateMetadataKey(key);
    if (metadataClass == null) {
      throw new HopPluginException(
          "Unable to find the metadata type class for "
              + key
              + ". The available metadata types are:"
              + Const.CR
              + describeAvailableMetadataPlugins());
    }
  }

  private void validateMetadataKey(String key) throws HopException {
    if (StringUtils.isEmpty(key)) {
      throw new HopException(
          "Please provide a non-null metadata type key, one of: "
              + Const.CR
              + describeAvailableMetadataPlugins());
    }
  }

  //
  // Logging API
  //

  public String getLogging(String logChannelId) {
    LoggingBuffer loggingBuffer = HopLogStore.getAppender();
    return loggingBuffer.getBuffer(logChannelId, false).toString();
  }

  //
  // The PyHop server
  //

  public void stopServer(String passwordToVerify) throws HopException {
    // Only with a stop password can the server be stopped.
    if (!StringUtil.isEmpty(this.stopPassword) && this.stopPassword.equals(passwordToVerify)) {
      blockingQueue.add(new Object());
    } else {
      throw new HopException("Specify a stop password, here and during server startup.");
    }
  }

  public void waitUntilStopped() {
    try {
      blockingQueue.take();
    } catch (InterruptedException e) {
      log.logError("Waiting interrupted on the PyHop gateway server", e);
    }
  }

  //
  // Utility
  //

  public static @NonNull String describeAvailablePlugins(
      Class<? extends IPluginType<?>> pluginTypeClass) {
    List<IPlugin> plugins = PluginRegistry.getInstance().getPlugins(pluginTypeClass);
    StringBuilder available = new StringBuilder();
    for (IPlugin plugin : plugins) {
      available.append(plugin.getIds()[0]).append(" : ").append(plugin.getName());
      available.append(Const.CR);
    }
    return available.toString();
  }
}
