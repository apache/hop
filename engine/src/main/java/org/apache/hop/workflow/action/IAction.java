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

package org.apache.hop.workflow.action;

import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.logging.IHasLogChannel;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.w3c.dom.Node;

import java.util.List;
import java.util.Map;

/**
 * IAction is the main Java interface that a plugin implements. The responsibilities of the implementing class
 * are listed below:
 * <p>
 * <ul>
 * <li><b>Maintain action settings</b><br/>
 * The implementing class typically keeps track of action settings using private fields with corresponding getters
 * and setters. The dialog class implementing IActionDialog is using the getters and setters to copy the user
 * supplied configuration in and out of the dialog.<br/>
 * <br/>
 * The following interface method also falls into the area of maintaining settings:<br/>
 * <br/>
 * <a href="#clone()"><code>Object clone()</code></a> <br/>
 * This method is called when a action is duplicated in HopGui. It needs to return a deep copy of this action
 * object. It is essential that the implementing class creates proper deep copies if the action configuration is
 * stored in modifiable objects, such as lists or custom helper objects. This interface does not extend Cloneable, but
 * the implementing class will provide a similar method due to this interface.</li><br/>
 * <li><b>Serialize action settings</b><br>
 * The plugin needs to be able to serialize its settings to XML. The interface methods are as
 * follows:<br/>
 * <br/>
 * <a href="#getXml()"><code>String getXml()</code></a><br/>
 * This method is called by PDI whenever a action needs to serialize its settings to XML. It is called when saving a
 * workflow in HopGui. The method returns an XML string, containing the serialized settings. The string contains a series of
 * XML tags, typically one tag per setting. The helper class org.apache.hop.core.xml.XmlHandler is typically used to
 * construct the XML string.<br/>
 * <br/>
 * <a href="#loadXml(org.w3c.dom.Node)">
 * <code>void loadXml(...)</code></a></br> This method is called by Hop whenever a action needs to read its
 * settings from XML. The XML node containing the action's settings is passed in as an argument. Again, the helper
 * class org.apache.hop.core.xml.XmlHandler is typically used to conveniently read the settings from the XML node.<br/>
 * <br/>
 * <br/>
 * <quote>Hint: When developing plugins, make sure the serialization code is in sync with the settings available from
 * the action dialog. When testing a plugin in HopGui, Hop will internally first save and load a copy of the
 * workflow.</quote></li><br/>
 * <li><b>Provide access to dialog class</b><br/>
 * Hop needs to know which class will take care of the settings dialog for the action. The interface method
 * getDialogClassName() must return the name of the class implementing the IActionDialog.</li></br>
 * <li><b>Provide information about possible outcomes</b><br/>
 * A action may support up to three types of outgoing hops: true, false, and unconditional. Sometimes it does not
 * make sense to support all three possibilities. For instance, if the action performs a task that does not produce a
 * boolean outcome, like the dummy action, it may make sense to suppress the true and false outgoing hops. There are
 * other actions, which carry an inherent boolean outcome, like the "file exists" action for instance. It may
 * make sense in such cases to suppress the unconditional outgoing hop.<br/>
 * <br/>
 * The action plugin class must implement two methods to indicate to PDI which outgoing hops it supports:<br/>
 * <br/>
 * <a href="#isEvaluation()"><code>boolean isEvaluation()</code></a><br/>
 * This method must return true if the action supports the true/false outgoing hops. If the action does not
 * support distinct outcomes, it must return false.<br/>
 * <br/>
 * <a href="#isUnconditional()"><code>boolean isUnconditional()</code></a><br/>
 * This method must return true if the action supports the unconditional outgoing hop. If the action does not
 * support the unconditional hop, it must return false.</li><br/>
 * <li><b>Execute a action task</b><br/>
 * The class implementing IAction executes the actual action task by implementing the following method:
 * <br/>
 * <br/>
 * <a href="#execute(org.apache.hop.core.Result, int)"><code>Result execute(..)</code></a><br/>
 * The execute() method is going to be called by Hop when it is time for the action to execute its logic. The
 * arguments are a result object, which is passed in from the previously executed action and an integer number
 * indicating the distance of the action from the start entry of the workflow.<br/>
 * <br/>
 * The action should execute its configured task, and report back on the outcome. A action does that by calling
 * certain methods on the passed in <a href="../../core/Result.html">Result</a> object:<br/>
 * <br/>
 * <code>prevResult.setNrErrors(..)</code><br/>
 * The action needs to indicate whether it has encountered any errors during execution. If there are errors,
 * setNrErrors must be called with the number of errors encountered (typically this is 1). If there are no errors,
 * setNrErrors must be called with an argument of 0.<br/>
 * <br/>
 * <code>prevResult.setResult(..)</code><br/>
 * The action must indicate the outcome of the task. This value determines which output hops can be followed next. If
 * a action does not support evaluation, it need not call prevResult.setResult().<br/>
 * <br/>
 * Finally, the passed in prevResult object must be returned.</li>
 * </ul>
 * </p>
 *
 * @author Matt Casters
 * @since 18-06-04
 */

public interface IAction extends IVariables, IHasLogChannel {

  /**
   * Execute the action. The previous result and number of rows are provided to the method for the purpose of
   * chaining actions, pipelines, etc.
   *
   * @param prevResult the previous result
   * @param nr          the number of rows
   * @return the Result object from execution of this action
   * @throws HopException if any Hop exceptions occur
   */
  Result execute( Result prevResult, int nr ) throws HopException;

  /**
   * Sets the parent workflow.
   *
   * @param workflow the parent workflow
   */
  void setParentWorkflow( IWorkflowEngine<WorkflowMeta> workflow );

  /**
   * Gets the parent workflow.
   *
   * @return the parent workflow
   */
  IWorkflowEngine<WorkflowMeta> getParentWorkflow();

  /**
   * Gets the log channel.
   *
   * @return the log channel
   */
  ILogChannel getLogChannel();

  /**
   * Sets the MetaStore
   *
   * @param metadataProvider The new MetaStore to use
   */
  void setMetadataProvider( IHopMetadataProvider metadataProvider );

  /**
   * This method should clear out any variables, objects, etc. used by the action.
   */
  void clear();

  /**
   * Gets the name of this action.
   *
   * @return the name
   */
  String getName();

  /**
   * Sets the name for this action.
   *
   * @param name the new name
   */
  void setName( String name );

  /**
   * Gets the plugin id.
   *
   * @return the plugin id
   */
  String getPluginId();

  /**
   * Sets the plugin id.
   *
   * @param pluginId the new plugin id
   */
  void setPluginId( String pluginId );

  /**
   * Gets the description of this action
   *
   * @return the description
   */
  String getDescription();

  /**
   * Sets the description of this action
   *
   * @param description the new description
   */
  void setDescription( String description );

  /**
   * Sets whether the action has changed
   */
  void setChanged();

  /**
   * Sets whether the action has changed
   *
   * @param ch true if the action has changed, false otherwise
   */
  void setChanged( boolean ch );

  /**
   * Checks whether the action has changed
   *
   * @return true if whether the action has changed
   */
  boolean hasChanged();

  /**
   * This method is called by PDI whenever a action needs to read its settings from XML. The XML node containing the
   * action's settings is passed in as an argument. Again, the helper class org.apache.hop.core.xml.XmlHandler is
   * typically used to conveniently read the settings from the XML node.
   *
   * @param entrynode the top-level XML node
   * @param metadataProvider The metadataProvider to optionally load from.
   * @param variables
   * @throws HopXmlException if any errors occur during the loading of the XML
   */
  void loadXml( Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables ) throws HopXmlException;

  /**
   * This method is called by PDI whenever a action needs to serialize its settings to XML. It is called when saving
   * a workflow in HopGui. The method returns an XML string, containing the serialized settings. The string contains a series
   * of XML tags, typically one tag per setting. The helper class org.apache.hop.core.xml.XmlHandler is typically used
   * to construct the XML string.
   *
   * @return the xml representation of the action
   */
  String getXml();

  /**
   * Checks if the action is a starting point
   *
   * @return true if starting point, false otherwise
   */
  boolean isStart();

  /**
   * This method is called when a action is duplicated in HopGui. It needs to return a deep copy of this action
   * object. It is essential that the implementing class creates proper deep copies if the action configuration is
   * stored in modifiable objects, such as lists or custom helper objects.
   *
   * @return a clone of the object
   */
  Object clone();

  /**
   * Checks whether a reset of the number of errors is required before execution.
   *
   * @return true if a reset of the number of errors is required before execution, false otherwise
   */
  boolean resetErrorsBeforeExecution();

  /**
   * This method must return true if the action supports the true/false outgoing hops. If the action does not
   * support distinct outcomes, it must return false.
   *
   * @return true if the action supports the true/false outgoing hops, false otherwise
   */
  boolean isEvaluation();

  /**
   * This method must return true if the action supports the unconditional outgoing hop. If the action does not
   * support the unconditional hop, it must return false.
   *
   * @return true if the action supports the unconditional outgoing hop, false otherwise
   */
  boolean isUnconditional();


  /**
   * Checks if this action executes a pipeline
   *
   * @return true if this action executes a pipeline, false otherwise
   */
  boolean isPipeline();

  /**
   * Checks if the action executes a workflow
   *
   * @return true if the action executes a workflow, false otherwise
   */
  boolean isWorkflow();

  /**
   * Gets the SQL statements needed by this action to execute successfully, given a set of variables.
   *
   * @param metadataProvider the MetaStore to use
   * @param variables a variable variables object containing variable bindings
   * @return a list of SQL statements
   * @throws HopException if any errors occur during the generation of SQL statements
   */
  List<SqlStatement> getSqlStatements( IHopMetadataProvider metadataProvider, IVariables variables ) throws HopException;

  /**
   * Get the name of the class that implements the dialog for the action. ActionBase provides a default
   *
   * @return the name of the class implementing the dialog for the action
   */
  String getDialogClassName();

  /**
   * Gets the filename of the action. This method is used by actions and pipelines that call or refer to
   * other actions.
   *
   * @return the filename
   */
  String getFilename();

  /**
   * Gets the real filename of the action, by substituting any environment variables present in the filename.
   *
   * @return the real (resolved) filename for the action
   */
  String getRealFilename();

  /**
   * Allows Action objects to check themselves for consistency
   *
   * @param remarks      List of CheckResult objects indicating consistency status
   * @param workflowMeta the metadata object for the action
   * @param variables    the variable variables to resolve string expressions with variables with
   * @param metadataProvider    the MetaStore to load common elements from
   */
  void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables, IHopMetadataProvider metadataProvider );

  /**
   * Get a list of all the resource dependencies that the transform is depending on.
   *
   * @return a list of all the resource dependencies that the transform is depending on
   */
  List<ResourceReference> getResourceDependencies( IVariables variables, WorkflowMeta workflowMeta );

  /**
   * Exports the object to a flat-file system, adding content with filename keys to a set of definitions. The supplied
   * resource naming interface allows the object to name appropriately without worrying about those parts of the
   * implementation specific details.
   *
   * @param variables       The variable variables to resolve (environment) variables with.
   * @param definitions     The map containing the filenames and content
   * @param namingInterface The resource naming interface allows the object to be named appropriately
   * @param metadataProvider       the metadataProvider to load external metadata from
   * @return The filename for this object. (also contained in the definitions map)
   * @throws HopException in case something goes wrong during the export
   */
  String exportResources( IVariables variables, Map<String, ResourceDefinition> definitions,
                          IResourceNaming namingInterface, IHopMetadataProvider metadataProvider ) throws HopException;

  /**
   * @return The objects referenced in the transform, like a a pipeline, a workflow, a mapper, a reducer, a combiner, ...
   */
  String[] getReferencedObjectDescriptions();

  /**
   * @return true for each referenced object that is enabled or has a valid reference definition.
   */
  boolean[] isReferencedObjectEnabled();

  /**
   * Load the referenced object
   *
   * @param index     the referenced object index to load (in case there are multiple references)
   * @param metadataProvider the metadataProvider
   * @param variables the variable variables to use
   * @return the referenced object once loaded
   * @throws HopException
   */
  IHasFilename loadReferencedObject( int index, IHopMetadataProvider metadataProvider, IVariables variables ) throws HopException;

  /**
   * At save and run time, the system will attempt to set the workflowMeta so that it can be accessed by the actions
   * if necessary.  This default method will be applied to all actions that do not need to be aware of the parent
   * WorkflowMeta
   *
   * @param workflowMeta the WorkflowMeta to which this IAction belongs
   */
  default void setParentWorkflowMeta( WorkflowMeta workflowMeta ) {
  }

  /**
   * Return Gets the parent workflowMeta.  This default method will throw an exception if a action attempts to call the
   * getter when not implemented.
   */
  default WorkflowMeta getParentWorkflowMeta() {
    throw new UnsupportedOperationException( "Attempted access of parent workflow metadata is not supported by the default IAction implementation" );
  }

}
