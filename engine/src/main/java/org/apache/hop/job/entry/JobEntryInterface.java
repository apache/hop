/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.job.entry;

import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Result;
import org.apache.hop.core.SQLStatement;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.job.Job;
import org.apache.hop.job.JobMeta;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceNamingInterface;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.util.List;
import java.util.Map;

/**
 * JobEntryInterface is the main Java interface that a plugin implements. The responsibilities of the implementing class
 * are listed below:
 * <p>
 * <ul>
 * <li><b>Maintain job entry settings</b><br/>
 * The implementing class typically keeps track of job entry settings using private fields with corresponding getters
 * and setters. The dialog class implementing JobEntryDialogInterface is using the getters and setters to copy the user
 * supplied configuration in and out of the dialog.<br/>
 * <br/>
 * The following interface method also falls into the area of maintaining settings:<br/>
 * <br/>
 * <a href="#clone()"><code>Object clone()</code></a> <br/>
 * This method is called when a job entry is duplicated in Spoon. It needs to return a deep copy of this job entry
 * object. It is essential that the implementing class creates proper deep copies if the job entry configuration is
 * stored in modifiable objects, such as lists or custom helper objects. This interface does not extend Cloneable, but
 * the implementing class will provide a similar method due to this interface.</li><br/>
 * <li><b>Serialize job entry settings</b><br>
 * The plugin needs to be able to serialize its settings to XML. The interface methods are as
 * follows:<br/>
 * <br/>
 * <a href="#getXML()"><code>String getXML()</code></a><br/>
 * This method is called by PDI whenever a job entry needs to serialize its settings to XML. It is called when saving a
 * job in Spoon. The method returns an XML string, containing the serialized settings. The string contains a series of
 * XML tags, typically one tag per setting. The helper class org.apache.hop.core.xml.XMLHandler is typically used to
 * construct the XML string.<br/>
 * <br/>
 * <a href="#loadXML(org.w3c.dom.Node)">
 * <code>void loadXML(...)</code></a></br> This method is called by PDI whenever a job entry needs to read its
 * settings from XML. The XML node containing the job entry's settings is passed in as an argument. Again, the helper
 * class org.apache.hop.core.xml.XMLHandler is typically used to conveniently read the settings from the XML node.<br/>
 * <br/>
 * <br/>
 * <quote>Hint: When developing plugins, make sure the serialization code is in sync with the settings available from
 * the job entry dialog. When testing a plugin in Spoon, PDI will internally first save and load a copy of the
 * job.</quote></li><br/>
 * <li><b>Provide access to dialog class</b><br/>
 * PDI needs to know which class will take care of the settings dialog for the job entry. The interface method
 * getDialogClassName() must return the name of the class implementing the JobEntryDialogInterface.</li></br>
 * <li><b>Provide information about possible outcomes</b><br/>
 * A job entry may support up to three types of outgoing hops: true, false, and unconditional. Sometimes it does not
 * make sense to support all three possibilities. For instance, if the job entry performs a task that does not produce a
 * boolean outcome, like the dummy job entry, it may make sense to suppress the true and false outgoing hops. There are
 * other job entries, which carry an inherent boolean outcome, like the "file exists" job entry for instance. It may
 * make sense in such cases to suppress the unconditional outgoing hop.<br/>
 * <br/>
 * The job entry plugin class must implement two methods to indicate to PDI which outgoing hops it supports:<br/>
 * <br/>
 * <a href="#evaluates()"><code>boolean evaluates()</code></a><br/>
 * This method must return true if the job entry supports the true/false outgoing hops. If the job entry does not
 * support distinct outcomes, it must return false.<br/>
 * <br/>
 * <a href="#isUnconditional()"><code>boolean isUnconditional()</code></a><br/>
 * This method must return true if the job entry supports the unconditional outgoing hop. If the job entry does not
 * support the unconditional hop, it must return false.</li><br/>
 * <li><b>Execute a job entry task</b><br/>
 * The class implementing JobEntryInterface executes the actual job entry task by implementing the following method:
 * <br/>
 * <br/>
 * <a href="#execute(org.apache.hop.core.Result, int)"><code>Result execute(..)</code></a><br/>
 * The execute() method is going to be called by PDI when it is time for the job entry to execute its logic. The
 * arguments are a result object, which is passed in from the previously executed job entry and an integer number
 * indicating the distance of the job entry from the start entry of the job.<br/>
 * <br/>
 * The job entry should execute its configured task, and report back on the outcome. A job entry does that by calling
 * certain methods on the passed in <a href="../../core/Result.html">Result</a> object:<br/>
 * <br/>
 * <code>prev_result.setNrErrors(..)</code><br/>
 * The job entry needs to indicate whether it has encountered any errors during execution. If there are errors,
 * setNrErrors must be called with the number of errors encountered (typically this is 1). If there are no errors,
 * setNrErrors must be called with an argument of 0.<br/>
 * <br/>
 * <code>prev_result.setResult(..)</code><br/>
 * The job entry must indicate the outcome of the task. This value determines which output hops can be followed next. If
 * a job entry does not support evaluation, it need not call prev_result.setResult().<br/>
 * <br/>
 * Finally, the passed in prev_result object must be returned.</li>
 * </ul>
 * </p>
 *
 * @author Matt Casters
 * @since 18-06-04
 */

public interface JobEntryInterface {

  /**
   * Execute the job entry. The previous result and number of rows are provided to the method for the purpose of
   * chaining job entries, transformations, etc.
   *
   * @param prev_result the previous result
   * @param nr          the number of rows
   * @return the Result object from execution of this job entry
   * @throws HopException if any Hop exceptions occur
   */
  Result execute( Result prev_result, int nr ) throws HopException;

  /**
   * Sets the parent job.
   *
   * @param job the parent job
   */
  void setParentJob( Job job );

  /**
   * Gets the parent job.
   *
   * @return the parent job
   */
  Job getParentJob();

  /**
   * Gets the log channel.
   *
   * @return the log channel
   */
  LogChannelInterface getLogChannel();

  /**
   * Sets the MetaStore
   *
   * @param metaStore The new MetaStore to use
   */
  void setMetaStore( IMetaStore metaStore );

  /**
   * This method should clear out any variables, objects, etc. used by the job entry.
   */
  void clear();

  /**
   * Gets the name of this job entry.
   *
   * @return the name
   */
  String getName();

  /**
   * Sets the name for this job entry.
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
   * Gets the description of this job entry
   *
   * @return the description
   */
  String getDescription();

  /**
   * Sets the description of this job entry
   *
   * @param description the new description
   */
  void setDescription( String description );

  /**
   * Sets whether the job entry has changed
   */
  void setChanged();

  /**
   * Sets whether the job entry has changed
   *
   * @param ch true if the job entry has changed, false otherwise
   */
  void setChanged( boolean ch );

  /**
   * Checks whether the job entry has changed
   *
   * @return true if whether the job entry has changed
   */
  boolean hasChanged();

  /**
   * This method is called by PDI whenever a job entry needs to read its settings from XML. The XML node containing the
   * job entry's settings is passed in as an argument. Again, the helper class org.apache.hop.core.xml.XMLHandler is
   * typically used to conveniently read the settings from the XML node.
   *
   * @param entrynode the top-level XML node
   * @param metaStore The metaStore to optionally load from.
   * @throws HopXMLException if any errors occur during the loading of the XML
   */
  void loadXML( Node entrynode, IMetaStore metaStore ) throws HopXMLException;

  /**
   * This method is called by PDI whenever a job entry needs to serialize its settings to XML. It is called when saving
   * a job in Spoon. The method returns an XML string, containing the serialized settings. The string contains a series
   * of XML tags, typically one tag per setting. The helper class org.apache.hop.core.xml.XMLHandler is typically used
   * to construct the XML string.
   *
   * @return the xml representation of the job entry
   */
  String getXML();

  /**
   * Checks if the job entry has started
   *
   * @return true if started, false otherwise
   */
  boolean isStart();

  /**
   * Checks if this job entry is a dummy entry
   *
   * @return true if this job entry is a dummy entry, false otherwise
   */
  boolean isDummy();

  /**
   * This method is called when a job entry is duplicated in Spoon. It needs to return a deep copy of this job entry
   * object. It is essential that the implementing class creates proper deep copies if the job entry configuration is
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
   * This method must return true if the job entry supports the true/false outgoing hops. If the job entry does not
   * support distinct outcomes, it must return false.
   *
   * @return true if the job entry supports the true/false outgoing hops, false otherwise
   */
  boolean evaluates();

  /**
   * This method must return true if the job entry supports the unconditional outgoing hop. If the job entry does not
   * support the unconditional hop, it must return false.
   *
   * @return true if the job entry supports the unconditional outgoing hop, false otherwise
   */
  boolean isUnconditional();

  /**
   * Checks if the job entry is an evaluation
   *
   * @return true if the job entry is an evaluation, false otherwise
   */
  boolean isEvaluation();

  /**
   * Checks if this job entry executes a transformation
   *
   * @return true if this job entry executes a transformation, false otherwise
   */
  boolean isTransformation();

  /**
   * Checks if the job entry executes a job
   *
   * @return true if the job entry executes a job, false otherwise
   */
  boolean isJob();

  /**
   * Checks if the job entry executes a shell program
   *
   * @return true if the job entry executes a shell program, false otherwise
   */
  boolean isShell();

  /**
   * Checks if the job entry sends email
   *
   * @return true if the job entry sends email, false otherwise
   */
  boolean isMail();

  /**
   * Checks if the job entry is of a special type (Start, Dummy, etc.)
   *
   * @return true if the job entry is of a special type, false otherwise
   */
  boolean isSpecial();

  /**
   * Gets the SQL statements needed by this job entry to execute successfully, given a set of variables.
   *
   * @param metaStore the MetaStore to use
   * @param space     a variable space object containing variable bindings
   * @return a list of SQL statements
   * @throws HopException if any errors occur during the generation of SQL statements
   */
  List<SQLStatement> getSQLStatements( IMetaStore metaStore, VariableSpace space ) throws HopException;

  /**
   * Get the name of the class that implements the dialog for the job entry JobEntryBase provides a default
   *
   * @return the name of the class implementing the dialog for the job entry
   * @deprecated As of release 8.1, use annotated-based dialog instead {@see org.apache.hop.core.annotations.PluginDialog}
   */
  @Deprecated
  String getDialogClassName();

  /**
   * Gets the filename of the job entry. This method is used by job entries and transformations that call or refer to
   * other job entries.
   *
   * @return the filename
   */
  String getFilename();

  /**
   * Gets the real filename of the job entry, by substituting any environment variables present in the filename.
   *
   * @return the real (resolved) filename for the job entry
   */
  String getRealFilename();

  /**
   * Allows JobEntry objects to check themselves for consistency
   *
   * @param remarks   List of CheckResult objects indicating consistency status
   * @param jobMeta   the metadata object for the job entry
   * @param space     the variable space to resolve string expressions with variables with
   * @param metaStore the MetaStore to load common elements from
   */
  void check( List<CheckResultInterface> remarks, JobMeta jobMeta, VariableSpace space, IMetaStore metaStore );

  /**
   * Get a list of all the resource dependencies that the step is depending on.
   *
   * @return a list of all the resource dependencies that the step is depending on
   */
  List<ResourceReference> getResourceDependencies( JobMeta jobMeta );

  /**
   * Exports the object to a flat-file system, adding content with filename keys to a set of definitions. The supplied
   * resource naming interface allows the object to name appropriately without worrying about those parts of the
   * implementation specific details.
   *
   * @param space           The variable space to resolve (environment) variables with.
   * @param definitions     The map containing the filenames and content
   * @param namingInterface The resource naming interface allows the object to be named appropriately
   * @param metaStore       the metaStore to load external metadata from
   * @return The filename for this object. (also contained in the definitions map)
   * @throws HopException in case something goes wrong during the export
   */
  String exportResources( VariableSpace space, Map<String, ResourceDefinition> definitions,
                          ResourceNamingInterface namingInterface, IMetaStore metaStore ) throws HopException;

  /**
   * @return The objects referenced in the step, like a a transformation, a job, a mapper, a reducer, a combiner, ...
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
   * @param metaStore the metaStore
   * @param space     the variable space to use
   * @return the referenced object once loaded
   * @throws HopException
   */
  Object loadReferencedObject( int index, IMetaStore metaStore, VariableSpace space ) throws HopException;

  /**
   * At save and run time, the system will attempt to set the jobMeta so that it can be accessed by the jobEntries
   * if necessary.  This default method will be applied to all JobEntries that do not need to be aware of the parent
   * JobMeta
   *
   * @param jobMeta the JobMeta to which this JobEntryInterface belongs
   */
  default void setParentJobMeta( JobMeta jobMeta ) {
  }

  /**
   * Return Gets the parent jobMeta.  This default method will throw an exception if a job entry attempts to call the
   * getter when not implemented.
   */
  default JobMeta getParentJobMeta() {
    throw new UnsupportedOperationException( "Attemped access of getJobMet not supported by JobEntryInterface implementation" );
  }

}
