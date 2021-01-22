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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.IAttributes;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.ICheckResultSource;
import org.apache.hop.core.IExtensionData;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.attributes.AttributesUtil;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.logging.DefaultLogLevel;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.IResourceHolder;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Base class for the different types of workflow actions. Workflow actions can extend this base class to get access to common
 * member variables and default method behavior. However, ActionBase does not implement IAction (although it
 * implements most of the same methods), so individual action classes must implement IAction and
 * specifically the <code>execute()</code> method.
 *
 * @author Matt Created on 18-jun-04
 */
public abstract class ActionBase implements IAction, Cloneable, ILoggingObject,
  IAttributes, IExtensionData, ICheckResultSource, IResourceHolder {

  /**
   * The name of the action
   */
  private String name;

  /**
   * The description of the action
   */
  private String description;

  /**
   * ID as defined in the xml or annotation.
   */
  private String pluginId;

  /**
   * Whether the action has changed.
   */
  private boolean changed;

  /**
   * The variable bindings for the action
   */
  private IVariables variables = new Variables();

  /**
   * The map for transform variable bindings for the action
   */
  protected Map<String, String> entryTransformSetVariablesMap = new ConcurrentHashMap<>();

  /**
   * The parent workflow
   */
  protected IWorkflowEngine<WorkflowMeta> parentWorkflow;

  /**
   * The log channel interface object, used for logging
   */
  protected ILogChannel log;

  /**
   * The log level
   */
  private LogLevel logLevel = DefaultLogLevel.getLogLevel();

  /**
   * The container object id
   */
  protected String containerObjectId;

  private IHopMetadataProvider metadataProvider;

  protected Map<String, Map<String, String>> attributesMap;

  protected Map<String, Object> extensionDataMap;

  protected WorkflowMeta parentWorkflowMeta;

  /**
   * Instantiates a new action base object.
   */
  protected ActionBase() {
    name = null;
    description = null;
    log = new LogChannel( this );
    attributesMap = new HashMap<>();
    extensionDataMap = new HashMap<>();
  }

  /**
   * Instantiates a new action base object with the given name and description.
   *
   * @param name        the name of the action
   * @param description the description of the action
   */
  protected ActionBase( String name, String description ) {
    setName( name );
    setDescription( description );
    log = new LogChannel( this );
    attributesMap = new HashMap<>();
    extensionDataMap = new HashMap<>();
  }

  /**
   * Checks if the Action object is equal to the specified object
   *
   * @return true if the two objects are equal, false otherwise
   * @see Object#equals(Object)
   */
  @Override
  public boolean equals( Object obj ) {
    if ( !( obj instanceof ActionBase ) ) {
      return false;
    }
    if ( this == obj ) {
      return true;
    }

    return name.equalsIgnoreCase( ( (ActionBase) obj ).getName() );
  }

  @Override
  public int hashCode() {
    return name.toLowerCase().hashCode();
  }

  /**
   * Clears all variable values
   */
  public void clear() {
    name = null;
    description = null;
    changed = false;
  }

  /**
   * Gets the plug-in type description
   *
   * @return the plug-in type description
   */
  public String getTypeDesc() {
    IPlugin plugin = PluginRegistry.getInstance().findPluginWithId( ActionPluginType.class, pluginId );
    return plugin.getDescription();
  }

  /**
   * Sets the name of the action
   *
   * @param name the new name
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * Gets the name of the action
   *
   * @return the name of the action
   * @see ICheckResultSource#getName()
   */
  public String getName() {
    return name;
  }

  /**
   * Sets the description for the action.
   *
   * @param Description the new description
   */
  public void setDescription( String description ) {
    this.description = description;
  }

  /**
   * Gets the description of the action
   *
   * @return the description of the action
   * @see ICheckResultSource#getDescription()
   */
  public String getDescription() {
    return description;
  }

  @Override public String getTypeId() {
    return ActionPluginType.ID;
  }

  /**
   * Sets that the action has changed (i.e. a call to setChanged(true))
   *
   * @see ActionBase#setChanged(boolean)
   */
  public void setChanged() {
    setChanged( true );
  }

  /**
   * Sets whether the action has changed
   *
   * @param ch true if the action has changed, false otherwise
   */
  public void setChanged( boolean ch ) {
    changed = ch;
  }

  /**
   * Checks whether the action has changed
   *
   * @return true if the action has changed, false otherwise
   */
  public boolean hasChanged() {
    return changed;
  }

  /**
   * Checks if the action has started
   *
   * @return true if the action has started, false otherwise
   */
  public boolean isStart() {
    return false;
  }

  /**
   * Checks if the action executes a workflow
   *
   * @return true if the action executes a workflow, false otherwise
   */
  public boolean isWorkflow() {
    return "WORKFLOW".equals( pluginId );
  }

  /**
   * Checks if this action executes a pipeline
   *
   * @return true if this action executes a pipeline, false otherwise
   */
  public boolean isPipeline() {
    return "PIPELINE".equals( pluginId );
  }

  /**
   * This method is called by Hop whenever a action needs to serialize its settings to XML. It is called when saving
   * a workflow in HopGui. The method returns an XML string, containing the serialized settings. The string contains a series
   * of XML tags, typically one tag per setting. The helper class org.apache.hop.core.xml.XmlHandler is typically used
   * to construct the XML string.
   *
   * @return the xml representation of the action
   */
  public String getXml() {
    StringBuilder retval = new StringBuilder();
    retval.append( "      " ).append( XmlHandler.addTagValue( "name", getName() ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "description", getDescription() ) );
    retval.append( "      " ).append( XmlHandler.addTagValue( "type", pluginId ) );

    retval.append( AttributesUtil.getAttributesXml( attributesMap ) );

    return retval.toString();
  }

  /**
   * This method is called by Hop whenever a action needs to read its settings from XML. The XML node containing the
   * action's settings is passed in as an argument. Again, the helper class org.apache.hop.core.xml.XmlHandler is
   * typically used to conveniently read the settings from the XML node.
   *
   * @param node the top-level XML node
   * @throws HopXmlException if any errors occur during the loading of the XML
   */
  public void loadXml( Node node ) throws HopXmlException {
    try {
      setName( XmlHandler.getTagValue( node, "name" ) );
      setDescription( XmlHandler.getTagValue( node, "description" ) );

      // Load the attribute groups map
      //
      attributesMap = AttributesUtil.loadAttributes( XmlHandler.getSubNode( node, AttributesUtil.XML_TAG ) );

    } catch ( Exception e ) {
      throw new HopXmlException( "Unable to load base info for action", e );
    }
  }

  /**
   * This method is called when a action is duplicated in HopGui. It needs to return a deep copy of this action
   * object. It is essential that the implementing class creates proper deep copies if the action configuration is
   * stored in modifiable objects, such as lists or custom helper objects.
   *
   * @return a clone of the object
   */
  @Override
  public Object clone() {
    ActionBase je;
    try {
      je = (ActionBase) super.clone();
    } catch ( CloneNotSupportedException cnse ) {
      return null;
    }
    return je;
  }

  /**
   * Returns a string representation of the object. For ActionBase, this method returns the name
   *
   * @see Object#toString()
   */
  @Override
  public String toString() {
    return name;
  }

  /**
   * Checks whether a reset of the number of errors is required before execution.
   *
   * @return true if a reset of the number of errors is required before execution, false otherwise
   */
  public boolean resetErrorsBeforeExecution() {
    return true;
  }

  /**
   * This method must return true if the action supports the true/false outgoing hops. For ActionBase, this method
   * always returns false
   *
   * @return false
   */
  public boolean isEvaluation() {
    return false;
  }

  /**
   * This method must return true if the action supports the unconditional outgoing hop. For ActionBase, this
   * method always returns true
   *
   * @return true
   */
  public boolean isUnconditional() {
    return true;
  }

  /**
   * Gets the SQL statements needed by this action to execute successfully, given a set of variables. For
   * ActionBase, this method returns an empty list.
   *
   * @param variables a variable variables object containing variable bindings
   * @return an empty list
   * @throws HopException if any errors occur during the generation of SQL statements
   */
  public List<SqlStatement> getSqlStatements( IHopMetadataProvider metadataProvider, IVariables variables ) throws HopException {
    return new ArrayList<>();
  }

  /**
   * Gets the filename of the action. For ActionBase, this method always returns null
   *
   * @return null
   * @see ILoggingObject#getFilename()
   */
  @Override
  public String getFilename() {
    return null;
  }

  /**
   * Gets the real filename of the action, by substituting any environment variables present in the filename. For
   * ActionBase, this method always returns null
   *
   * @return null
   */
  public String getRealFilename() {
    return null;
  }

  /**
   * Gets all the database connections that are used by the action. For ActionBase, this method returns an empty
   * (non-null) array
   *
   * @return an empty (non-null) array
   */
  public DatabaseMeta[] getUsedDatabaseConnections() {
    return new DatabaseMeta[] {};
  }

  /**
   * Copies variables from a given variable variables to this action
   *
   * @see IVariables#copyFrom(IVariables)
   */
  @Override
  public void copyFrom( IVariables variables ) {
    this.variables.copyFrom( variables );
  }

  /**
   * Substitutes any variable values into the given string, and returns the resolved string
   *
   * @return the string with any environment variables resolved and substituted
   * @see IVariables#resolve(String)
   */
  @Override
  public String resolve( String aString ) {
    return variables.resolve( aString );
  }

  /**
   * Substitutes any variable values into each of the given strings, and returns an array containing the resolved
   * string(s)
   *
   * @see IVariables#resolve(String[])
   */
  @Override
  public String[] resolve( String[] aString ) {
    return variables.resolve( aString );
  }

  @Override
  public String resolve( String aString, IRowMeta rowMeta, Object[] rowData ) throws HopValueException {
    return variables.resolve( aString, rowMeta, rowData );
  }

  /**
   * Gets the parent variable variables
   *
   * @return the parent variable variables
   * @see IVariables#getParentVariables()
   */
  @Override
  public IVariables getParentVariables() {
    return variables.getParentVariables();
  }

  /**
   * Sets the parent variable variables
   *
   * @see IVariables#setParentVariables(
   *IVariables)
   */
  @Override
  public void setParentVariables( IVariables parent ) {
    variables.setParentVariables( parent );
  }

  /**
   * Gets the value of the specified variable, or returns a default value if no such variable exists
   *
   * @return the value of the specified variable, or returns a default value if no such variable exists
   * @see IVariables#getVariable(String, String)
   */
  @Override
  public String getVariable( String variableName, String defaultValue ) {
    return variables.getVariable( variableName, defaultValue );
  }

  /**
   * Gets the value of the specified variable, or returns a default value if no such variable exists
   *
   * @return the value of the specified variable, or returns a default value if no such variable exists
   * @see IVariables#getVariable(String)
   */
  @Override
  public String getVariable( String variableName ) {
    return variables.getVariable( variableName );
  }

  /**
   * Returns a boolean representation of the specified variable after performing any necessary substitution. Truth
   * values include case-insensitive versions of "Y", "YES", "TRUE" or "1".
   *
   * @param variableName the name of the variable to interrogate
   * @return a boolean representation of the specified variable after performing any necessary substitution
   * @boolean defaultValue the value to use if the specified variable is unassigned.
   * @see IVariables#getVariableBoolean(String, boolean)
   */
  @Override
  public boolean getVariableBoolean( String variableName, boolean defaultValue ) {
    if ( !Utils.isEmpty( variableName ) ) {
      String value = resolve( variableName );
      if ( !Utils.isEmpty( value ) ) {
        return ValueMetaString.convertStringToBoolean( value );
      }
    }
    return defaultValue;
  }

  /**
   * Sets the values of the action's variables to the values from the parent variables
   *
   * @see IVariables#initializeFrom(
   *IVariables)
   */
  @Override
  public void initializeFrom( IVariables parent ) {
    variables.initializeFrom( parent );
  }

  /**
   * Gets a list of variable names for the action
   *
   * @return a list of variable names
   * @see IVariables#getVariableNames()
   */
  @Override
  public String[] getVariableNames() {
    return variables.getVariableNames();
  }

  /**
   * Sets the value of the specified variable to the specified value
   *
   * @see IVariables#setVariable(String, String)
   */
  @Override
  public void setVariable( String variableName, String variableValue ) {
    variables.setVariable( variableName, variableValue );
  }

  /**
   * Shares a variable variables from another variable variables. This means that the object should take over the variables used as
   * argument.
   *
   * @see IVariables#shareWith(IVariables)
   */
  @Override
  public void shareWith( IVariables variables ) {
    this.variables = variables;
  }

  /**
   * Injects variables using the given Map. The behavior should be that the properties object will be stored and at the
   * time the IVariables is initialized (or upon calling this method if the variables is already initialized). After
   * injecting the link of the properties object should be removed.
   *
   * @see IVariables#setVariables(Map)
   */
  @Override
  public void setVariables( Map<String, String> map ) {
    variables.setVariables( map );
  }

  /**
   * Allows Action objects to check themselves for consistency
   *
   * @param remarks   List of CheckResult objects indicating consistency status
   * @param workflowMeta   the metadata object for the action
   * @param variables     the variable variables to resolve string expressions with variables with
   * @param metadataProvider the MetaStore to load common elements from
   */
  public void check( List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables, IHopMetadataProvider metadataProvider ) {

  }

  /**
   * Gets a list of all the resource dependencies that the transform is depending on. In ActionBase, this method returns an
   * empty resource dependency list.
   *
   * @return an empty list of ResourceReferences
   * @see ResourceReference
   */
  public List<ResourceReference> getResourceDependencies( IVariables variables, WorkflowMeta workflowMeta ) {
    return new ArrayList<>( 5 ); // default: return an empty resource dependency list. Lower the
    // initial capacity
  }

  /**
   * Exports the object to a flat-file system, adding content with filename keys to a set of definitions. The supplied
   * resource naming interface allows the object to name appropriately without worrying about those parts of the
   * implementation specific details.
   *
   * @param variables           The variable variables to resolve (environment) variables with.
   * @param definitions     The map containing the filenames and content
   * @param namingInterface The resource naming interface allows the object to be named appropriately
   * @param metadataProvider       the metadataProvider to load external metadata from
   * @return The filename for this object. (also contained in the definitions map)
   * @throws HopException in case something goes wrong during the export
   */
  public String exportResources( IVariables variables, Map<String, ResourceDefinition> definitions,
                                 IResourceNaming namingInterface, IHopMetadataProvider metadataProvider ) throws HopException {
    return null;
  }

  /**
   * Gets the plugin id.
   *
   * @return the plugin id
   */
  public String getPluginId() {
    return pluginId;
  }

  /**
   * Sets the plugin id.
   *
   * @param pluginId the new plugin id
   */
  public void setPluginId( String pluginId ) {
    this.pluginId = pluginId;
  }

  /**
   * You can use this to point to an alternate class for the Dialog.
   * By default we return null.  This means we simply add Dialog to the Action plugin class name.
   *
   * @return full class name of the action dialog class (null by default)
   */
  public String getDialogClassName() {
    return null;
  }

  /**
   * Gets the variable bindings for the action.
   *
   * @return the variable bindings for the action.
   */
  protected IVariables getVariables() {
    return variables;
  }

  /**
   * Sets the parent workflow.
   *
   * @param parentWorkflow the new parent workflow
   */
  public void setParentWorkflow( IWorkflowEngine<WorkflowMeta> parentWorkflow ) {
    this.parentWorkflow = parentWorkflow;
    this.logLevel = parentWorkflow.getLogLevel();
    this.log = new LogChannel( this, parentWorkflow );
    this.containerObjectId = parentWorkflow.getContainerId();
  }

  /**
   * Gets the parent workflow.
   *
   * @return the parent workflow
   */
  public IWorkflowEngine<WorkflowMeta> getParentWorkflow() {
    return parentWorkflow;
  }

  /**
   * Checks if the logging level is basic.
   *
   * @return true if the logging level is basic, false otherwise
   */
  public boolean isBasic() {
    return log.isBasic();
  }

  /**
   * Checks if the logging level is detailed.
   *
   * @return true if the logging level is detailed, false otherwise
   */
  public boolean isDetailed() {
    return log.isDetailed();
  }

  /**
   * Checks if the logging level is debug.
   *
   * @return true if the logging level is debug, false otherwise
   */
  public boolean isDebug() {
    return log.isDebug();
  }

  /**
   * Checks if the logging level is rowlevel.
   *
   * @return true if the logging level is rowlevel, false otherwise
   */
  public boolean isRowlevel() {
    return log.isRowLevel();
  }

  /**
   * Logs the specified string at the minimal level.
   *
   * @param message the message
   */
  public void logMinimal( String message ) {
    log.logMinimal( message );
  }

  /**
   * Logs the specified string and arguments at the minimal level.
   *
   * @param message   the message
   * @param arguments the arguments
   */
  public void logMinimal( String message, Object... arguments ) {
    log.logMinimal( message, arguments );
  }

  /**
   * Logs the specified string at the basic level.
   *
   * @param message the message
   */
  public void logBasic( String message ) {
    log.logBasic( message );
  }

  /**
   * Logs the specified string and arguments at the basic level.
   *
   * @param message   the message
   * @param arguments the arguments
   */
  public void logBasic( String message, Object... arguments ) {
    log.logBasic( message, arguments );
  }

  /**
   * Logs the specified string at the detailed level.
   *
   * @param message the message
   */
  public void logDetailed( String message ) {
    log.logDetailed( message );
  }

  /**
   * Logs the specified string and arguments at the detailed level.
   *
   * @param message   the message
   * @param arguments the arguments
   */
  public void logDetailed( String message, Object... arguments ) {
    log.logDetailed( message, arguments );
  }

  /**
   * Logs the specified string at the debug level.
   *
   * @param message the message
   */
  public void logDebug( String message ) {
    log.logDebug( message );
  }

  /**
   * Logs the specified string and arguments at the debug level.
   *
   * @param message   the message
   * @param arguments the arguments
   */
  public void logDebug( String message, Object... arguments ) {
    log.logDebug( message, arguments );
  }

  /**
   * Logs the specified string at the row level.
   *
   * @param message the message
   */
  public void logRowlevel( String message ) {
    log.logRowlevel( message );
  }

  /**
   * Logs the specified string and arguments at the row level.
   *
   * @param message   the message
   * @param arguments the arguments
   */
  public void logRowlevel( String message, Object... arguments ) {
    log.logRowlevel( message, arguments );
  }

  /**
   * Logs the specified string at the error level.
   *
   * @param message the message
   */
  public void logError( String message ) {
    log.logError( message );
  }

  /**
   * Logs the specified string and Throwable object at the error level.
   *
   * @param message the message
   * @param e       the e
   */
  public void logError( String message, Throwable e ) {
    log.logError( message, e );
  }

  /**
   * Logs the specified string and arguments at the error level.
   *
   * @param message   the message
   * @param arguments the arguments
   */
  public void logError( String message, Object... arguments ) {
    log.logError( message, arguments );
  }

  /**
   * Gets the log channel.
   *
   * @return the log channel
   */
  public ILogChannel getLogChannel() {
    return log;
  }

  /**
   * Gets the logging channel id
   *
   * @return the log channel id
   * @see ILoggingObject#getLogChannelId()
   */
  @Override
  public String getLogChannelId() {
    return log.getLogChannelId();
  }

  /**
   * Gets the object name
   *
   * @return the object name
   * @see ILoggingObject#getObjectName()
   */
  @Override
  public String getObjectName() {
    return getName();
  }

  /**
   * Gets a string identifying a copy in a series of transforms
   *
   * @return a string identifying a copy in a series of transforms
   * @see ILoggingObject#getObjectCopy()
   */
  @Override
  public String getObjectCopy() {
    return null;
  }


  /**
   * Gets the logging object type
   *
   * @return the logging object type
   * @see ILoggingObject#getObjectType()
   */
  @Override
  public LoggingObjectType getObjectType() {
    return LoggingObjectType.ACTION;
  }

  /**
   * Gets the logging object interface's parent
   *
   * @return the logging object interface's parent
   * @see ILoggingObject#getParent()
   */
  @Override
  public ILoggingObject getParent() {
    return parentWorkflow;
  }

  /**
   * Gets the logging level for the action
   *
   * @see ILoggingObject#getLogLevel()
   */
  @Override
  public LogLevel getLogLevel() {
    return logLevel;
  }

  /**
   * Sets the logging level for the action
   *
   * @param logLevel the new log level
   */
  public void setLogLevel( LogLevel logLevel ) {
    this.logLevel = logLevel;
    log.setLogLevel( logLevel );
  }

  /**
   * Gets the container object id
   *
   * @return the container object id
   */
  @Override
  public String getContainerId() {
    return containerObjectId;
  }

  /**
   * Sets the container object id
   *
   * @param containerObjectId the container object id to set
   */
  public void setContainerObjectId( String containerObjectId ) {
    this.containerObjectId = containerObjectId;
  }

  /**
   * Returns the registration date for the action. For ActionBase, this method always returns null
   *
   * @return null
   */
  @Override
  public Date getRegistrationDate() {
    return null;
  }

  /**
   * @return The objects referenced in the transform, like a a pipeline, a workflow, a mapper, a reducer, a combiner, ...
   */
  public String[] getReferencedObjectDescriptions() {
    return null;
  }

  /**
   * @return true for each referenced object that is enabled or has a valid reference definition.
   */
  public boolean[] isReferencedObjectEnabled() {
    return null;
  }

  /**
   * Load the referenced object
   *
   * @param index     the referenced object index to load (in case there are multiple references)
   * @param metadataProvider the metadataProvider to load from
   * @param variables     the variable variables to use
   * @return the referenced object once loaded
   * @throws HopException
   */
  public IHasFilename loadReferencedObject( int index, IHopMetadataProvider metadataProvider, IVariables variables ) throws HopException {
    return null;
  }

  @Override
  public boolean isGatheringMetrics() {
    return log != null && log.isGatheringMetrics();
  }

  @Override
  public void setGatheringMetrics( boolean gatheringMetrics ) {
    if ( log != null ) {
      log.setGatheringMetrics( gatheringMetrics );
    }
  }

  @Override
  public boolean isForcingSeparateLogging() {
    return log != null && log.isForcingSeparateLogging();
  }

  @Override
  public void setForcingSeparateLogging( boolean forcingSeparateLogging ) {
    if ( log != null ) {
      log.setForcingSeparateLogging( forcingSeparateLogging );
    }
  }

  public IHopMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  public void setMetadataProvider( IHopMetadataProvider metadataProvider ) {
    this.metadataProvider = metadataProvider;
  }

  @Override
  public void setAttributesMap( Map<String, Map<String, String>> attributesMap ) {
    this.attributesMap = attributesMap;
  }

  @Override
  public Map<String, Map<String, String>> getAttributesMap() {
    return attributesMap;
  }

  @Override
  public void setAttribute( String groupName, String key, String value ) {
    Map<String, String> attributes = getAttributes( groupName );
    if ( attributes == null ) {
      attributes = new HashMap<>();
      attributesMap.put( groupName, attributes );
    }
    attributes.put( key, value );
  }

  @Override
  public void setAttributes( String groupName, Map<String, String> attributes ) {
    attributesMap.put( groupName, attributes );
  }

  @Override
  public Map<String, String> getAttributes( String groupName ) {
    return attributesMap.get( groupName );
  }

  @Override
  public String getAttribute( String groupName, String key ) {
    Map<String, String> attributes = attributesMap.get( groupName );
    if ( attributes == null ) {
      return null;
    }
    return attributes.get( key );
  }

  @Override
  public Map<String, Object> getExtensionDataMap() {
    return extensionDataMap;
  }

  /**
   * @return The parent workflowMeta at save and during execution.
   */
  public WorkflowMeta getParentWorkflowMeta() {
    return parentWorkflowMeta;
  }

  /**
   * At save and run time, the system will attempt to set the workflowMeta so that it can be accessed by the actions
   * if necessary.
   *
   * @param parentWorkflowMeta the WorkflowMeta to which this IAction belongs
   */
  public void setParentWorkflowMeta( WorkflowMeta parentWorkflowMeta ) {
    this.parentWorkflowMeta = parentWorkflowMeta;
  }

  /**
   * Gets a Map of variables set in EntryTransformSetVariables
   *
   * @return a map of variable names and values
   */
  protected Map<String, String> getEntryTransformSetVariablesMap() {
    return entryTransformSetVariablesMap;
  }

  /**
   * Sets the value of the specified EntryTransformSetVariable
   */
  public void setEntryTransformSetVariable( String variableName, String variableValue ) {
    // ConcurrentHashMap does not allow null keys and null values.
    if ( variableName != null ) {
      if ( variableValue != null ) {
        entryTransformSetVariablesMap.put( variableName, variableValue );
      } else {
        entryTransformSetVariablesMap.put( variableName, StringUtils.EMPTY );
      }
    }
  }

  /**
   * Gets the value of the specified EntryTransformSetVariable
   */
  public String getEntryTransformSetVariable( String variableName ) {
    return entryTransformSetVariablesMap.get( variableName );
  }
}
