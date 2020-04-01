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

package org.apache.hop.pipeline.step;

import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.SQLStatement;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceNamingInterface;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.step.errorhandling.StreamInterface;
import org.w3c.dom.Node;

import java.util.List;
import java.util.Map;

/**
 * This interface allows custom steps to talk to Hop. The StepMetaInterface is the main Java interface that a plugin
 * implements. The responsibilities of the implementing class are listed below:
 * <p>
 * <ul>
 * <li><b>Keep track of the step settings</b></br> The implementing class typically keeps track of step settings using
 * private fields with corresponding getters and setters. The dialog class implementing StepDialogInterface is using the
 * getters and setters to copy the user supplied configuration in and out of the dialog.
 * <p>
 * The following interface methods also fall into the area of maintaining settings:
 * <p>
 * <i><a href="#setDefault()">void setDefault()</a></i>
 * <p>
 * This method is called every time a new step is created and should allocate or set the step configuration to sensible
 * defaults. The values set here will be used by HopGui when a new step is created. This is often a good place to ensure
 * that the step&#8217;s settings are initialized to non-null values. Null values can be cumbersome to deal with in
 * serialization and dialog population, so most PDI step implementations stick to non-null values for all step settings.
 * <p>
 * <i><a href="#clone()">public Object clone()</a></i>
 * <p>
 * This method is called when a step is duplicated in HopGui. It needs to return a deep copy of this step meta object. It
 * is essential that the implementing class creates proper deep copies if the step configuration is stored in modifiable
 * objects, such as lists or custom helper objects. See org.apache.hop.pipeline.steps.rowgenerator.RowGeneratorMeta.clone()
 * for an example on creating a deep copy.
 * <p></li>
 * <li>
 * <b>Serialize step settings</b><br/>
 * The plugin needs to be able to serialize its settings to XML . The interface methods are as
 * follows.
 * <p>
 * <i><a href="#getXML()">public String getXML()</a></i>
 * <p>
 * This method is called by PDI whenever a step needs to serialize its settings to XML. It is called when saving a
 * pipeline in HopGui. The method returns an XML string, containing the serialized step settings. The string
 * contains a series of XML tags, typically one tag per setting. The helper class org.apache.hop.core.xml.XMLHandler is
 * typically used to construct the XML string.
 * <p>
 * <i><a href="#loadXML(org.w3c.dom.Node)">public void loadXML(...)</a></i>
 * <p>
 * This method is called by PDI whenever a step needs to read its settings from XML. The XML node containing the step's
 * settings is passed in as an argument. Again, the helper class org.apache.hop.core.xml.XMLHandler is typically used to
 * conveniently read the step settings from the XML node.
 * <p>
 * <li>
 * <b>Provide instances of other plugin classes</b><br/>
 * The StepMetaInterface plugin class is the main class tying in with the rest of PDI architecture. It is responsible
 * for supplying instances of the other plugin classes implementing StepDialogInterface, StepInterface and
 * StepDataInterface. The following methods cover these responsibilities. Each of the method&#8217;s implementation is
 * typically constructing a new instance of the corresponding class forwarding the passed in arguments to the
 * constructor. The methods are as follows.
 * <p>
 * public StepDialogInterface getDialog(...)<br/>
 * public StepInterface getStep(...)<br/>
 * public StepDataInterface getStepData()<br/>
 * <p>
 * Each of the above methods returns a new instance of the plugin class implementing StepDialogInterface, StepInterface
 * and StepDataInterface.</li>
 * <li>
 * <b>Report the step&#8217;s changes to the row stream</b> PDI needs to know how a step affects the row structure. A
 * step may be adding or removing fields, as well as modifying the metadata of a field. The method implementing this
 * aspect of a step plugin is getFields().
 * <p>
 * <i><a href= "#getFields(org.apache.hop.core.row.RowMetaInterface, java.lang.String,
 * org.apache.hop.core.row.RowMetaInterface[], org.apache.hop.pipeline.step.StepMeta,
 * org.apache.hop.core.variables.VariableSpace)" >public void getFields(...)</a></i>
 * <p>
 * Given a description of the input rows, the plugin needs to modify it to match the structure for its output fields.
 * The implementation modifies the passed in RowMetaInterface object to reflect any changes to the row stream. Typically
 * a step adds fields to the row structure, which is done by creating ValueMeta objects (PDI&#8217;s default
 * implementation of ValueMetaInterface), and appending them to the RowMetaInterface object. The section Working with
 * Fields goes into deeper detail on ValueMetaInterface.</li>
 * <li>
 * <b>Validate step settings</b><br>
 * HopGui supports a &#8220;validate pipeline&#8221; feature, which triggers a self-check of all steps. PDI invokes
 * the check() method of each step on the canvas allowing each step to validate its settings.
 * <p>
 * <i><a href= "#check(java.util.List, org.apache.hop.pipeline.PipelineMeta, org.apache.hop.pipeline.step.StepMeta,
 * org.apache.hop.core.row.RowMetaInterface, java.lang.String[], java.lang.String[],
 * org.apache.hop.core.row.RowMetaInterface)" >public void check()</a></i>
 * <p>
 * Each step has the opportunity to validate its settings and verify that the configuration given by the user is
 * reasonable. In addition to that a step typically checks if it is connected to preceding or following steps, if the
 * nature of the step requires that kind of connection. An input step may expect to not have a preceding step for
 * example. The check method passes in a list of check remarks that the method should append its validation results to.
 * HopGui then displays the list of remarks collected from the steps, allowing the user to take corrective action in case
 * of validation warnings or errors.</li>
 * Given a description of the input rows, the plugin needs to modify it to match the structure for its output fields.
 * The implementation modifies the passed in RowMetaInterface object to reflect any changes to the row stream. Typically
 * a step adds fields to the row structure, which is done by creating ValueMeta objects (PDI&#8217;s default
 * implementation of ValueMetaInterface), and appending them to the RowMetaInterface object. The section Working with
 * Fields goes into deeper detail on ValueMetaInterface.
 * </ul>
 *
 * @author Matt
 * @since 4-aug-2004
 */

public interface StepMetaInterface {
  /**
   * Set default values
   */
  void setDefault();

  /**
   * Gets the fields.
   *
   * @param inputRowMeta the input row meta that is modified in this method to reflect the output row metadata of the step
   * @param name         Name of the step to use as input for the origin field in the values
   * @param info         Fields used as extra lookup information
   * @param nextStep     the next step that is targeted
   * @param space        the space The variable space to use to replace variables
   * @param metaStore    the MetaStore to use to load additional external data or metadata impacting the output fields
   * @throws HopStepException the kettle step exception
   */
  void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep,
                  VariableSpace space, IMetaStore metaStore ) throws HopStepException;

  /**
   * Get the XML that represents the values in this step
   *
   * @return the XML that represents the metadata in this step
   * @throws HopException in case there is a conversion or XML encoding error
   */
  String getXML() throws HopException;

  /**
   * Load the values for this step from an XML Node
   *
   * @param stepnode  the Node to get the info from
   * @param metaStore the metastore to optionally load external reference metadata from
   * @throws HopXMLException When an unexpected XML error occurred. (malformed etc.)
   */
  void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException;


  /**
   * Checks the settings of this step and puts the findings in a remarks List.
   *
   * @param remarks   The list to put the remarks in @see org.apache.hop.core.CheckResult
   * @param stepMeta  The stepMeta to help checking
   * @param prev      The fields coming from the previous step
   * @param input     The input step names
   * @param output    The output step names
   * @param info      The fields that are used as information by the step
   * @param space     the variable space to resolve variable expressions with
   * @param metaStore the MetaStore to use to load additional external data or metadata impacting the output fields
   */
  void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, StepMeta stepMeta,
              RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
              IMetaStore metaStore );

  /**
   * Make an exact copy of this step, make sure to explicitly copy Collections etc.
   *
   * @return an exact copy of this step
   */
  Object clone();

  /**
   * @return The fields used by this step, this is being used for the Impact analyses.
   */
  RowMetaInterface getTableFields();

  /**
   * This method is added to exclude certain steps from layout checking.
   *
   * @since 2.5.0
   */
  boolean excludeFromRowLayoutVerification();

  /**
   * This method is added to exclude certain steps from copy/distribute checking.
   *
   * @since 4.0.0
   */
  boolean excludeFromCopyDistributeVerification();

  /**
   * Get the executing step, needed by Pipeline to launch a step.
   *
   * @param stepMeta          The step info
   * @param stepDataInterface the step data interface linked to this step. Here the step can store temporary data, database connections,
   *                          etc.
   * @param copyNr            The copy nr to get
   * @param pipelineMeta         The pipeline info
   * @param pipeline             The launching pipeline
   */
  StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
                         PipelineMeta pipelineMeta, Pipeline pipeline );

  /**
   * Get a new instance of the appropriate data class. This data class implements the StepDataInterface. It basically
   * contains the persisting data that needs to live on, even if a worker thread is terminated.
   *
   * @return The appropriate StepDataInterface class.
   */
  StepDataInterface getStepData();

  /**
   * Each step must be able to report on the impact it has on a database, table field, etc.
   *
   * @param impact    The list of impacts @see org.apache.hop.pipelineMeta.DatabaseImpact
   * @param pipelineMeta The pipeline information
   * @param stepMeta  The step information
   * @param prev      The fields entering this step
   * @param input     The previous step names
   * @param output    The output step names
   * @param info      The fields used as information by this step
   * @param metaStore the MetaStore to use to load additional external data or metadata impacting the output fields
   */
  void analyseImpact( List<DatabaseImpact> impact, PipelineMeta pipelineMeta, StepMeta stepMeta,
                      RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, IMetaStore metaStore ) throws HopStepException;

  /**
   * Standard method to return an SQLStatement object with SQL statements that the step needs in order to work
   * correctly. This can mean "create table", "create index" statements but also "alter table ... add/drop/modify"
   * statements.
   *
   * @param pipelineMeta PipelineMeta object containing the complete pipeline
   * @param stepMeta  StepMeta object containing the complete step
   * @param prev      Row containing meta-data for the input fields (no data)
   * @param metaStore the MetaStore to use to load additional external data or metadata impacting the output fields
   * @return The SQL Statements for this step. If nothing has to be done, the SQLStatement.getSQL() == null. @see
   * SQLStatement
   */
  SQLStatement getSQLStatements( PipelineMeta pipelineMeta, StepMeta stepMeta, RowMetaInterface prev,
                                 IMetaStore metaStore ) throws HopStepException;

  /**
   * Call this to cancel trailing database queries (too long running, etc)
   */
  void cancelQueries() throws HopDatabaseException;


  /**
   * The natural way of data flow in a pipeline is source-to-target. However, this makes mapping to target tables
   * difficult to do. To help out here, we supply information to the pipeline meta-data model about which fields
   * are required for a step. This allows us to automate certain tasks like the mapping to pre-defined tables. The Table
   * Output step in this case will output the fields in the target table using this method.
   *
   * @param space the variable space to reference
   * @return the required fields for this steps metadata.
   * @throws HopException in case the required fields can't be determined.
   */
  RowMetaInterface getRequiredFields( VariableSpace space ) throws HopException;

  /**
   * @return true if this step supports error "reporting" on rows: the ability to send rows to a certain target step.
   */
  boolean supportsErrorHandling();

  /**
   * Get a list of all the resource dependencies that the step is depending on.
   *
   * @param pipelineMeta
   * @param stepMeta
   * @return a list of all the resource dependencies that the step is depending on
   */
  List<ResourceReference> getResourceDependencies( PipelineMeta pipelineMeta, StepMeta stepMeta );

  /**
   * @param space                   the variable space to use
   * @param definitions
   * @param resourceNamingInterface
   * @param metaStore               the metaStore in which non-kettle metadata could reside.
   * @return the filename of the exported resource
   */
  String exportResources( VariableSpace space, Map<String, ResourceDefinition> definitions,
                          ResourceNamingInterface resourceNamingInterface, IMetaStore metaStore ) throws HopException;

  /**
   * @return The StepMeta object to which this metadata class belongs. With this, we can see to which pipeline
   * metadata (etc) this metadata pertains to. (hierarchy)
   */
  StepMeta getParentStepMeta();

  /**
   * Provide original lineage for this metadata object
   *
   * @param parentStepMeta the parent step metadata container object
   */
  void setParentStepMeta( StepMeta parentStepMeta );

  /**
   * Returns the Input/Output metadata for this step.
   */
  StepIOMetaInterface getStepIOMeta();

  /**
   * @return The list of optional input streams. It allows the user to select f rom a list of possible actions like
   * "New target step"
   */
  List<StreamInterface> getOptionalStreams();

  /**
   * When an optional stream is selected, this method is called to handled the ETL metadata implications of that.
   *
   * @param stream The optional stream to handle.
   */
  void handleStreamSelection( StreamInterface stream );

  /**
   * For steps that handle dynamic input (info) or output (target) streams, it is useful to be able to force the
   * recreate the StepIoMeta definition. By default this definition is cached.
   */
  void resetStepIoMeta();

  /**
   * Change step names into step objects to allow them to be name-changed etc.
   *
   * @param steps the steps to reference
   */
  void searchInfoAndTargetSteps( List<StepMeta> steps );

  /**
   * @return Optional interface that allows an external program to inject step metadata in a standardized fasion. This
   * method will return null if the interface is not available for this step.
   * @deprecated Use annotation-based injection instead
   */
  @Deprecated
  StepMetaInjectionInterface getStepMetaInjectionInterface();

  /**
   * @return The step metadata itself, not the metadata description.
   * For lists it will have 0 entries in case there are no entries.
   * @throws HopException
   */
  List<StepInjectionMetaEntry> extractStepMetadataEntries() throws HopException;

  void setChanged();

  boolean hasChanged();

  /**
   * @return The objects referenced in the step, like a mapping, a pipeline, a job, ...
   */
  String[] getReferencedObjectDescriptions();

  /**
   * @return true for each referenced object that is enabled or has a valid reference definition.
   */
  boolean[] isReferencedObjectEnabled();

  /**
   * @return A description of the active referenced object in a pipeline.
   * Null if nothing special needs to be done or if the active metadata isn't different from design.
   */
  String getActiveReferencedObjectDescription();

  /**
   * Load the referenced object
   *
   * @param index     the referenced object index to load (in case there are multiple references)
   * @param metaStore the MetaStore to use
   * @param space     the variable space to use
   * @return the referenced object once loaded
   * @throws HopException
   */
  Object loadReferencedObject( int index, IMetaStore metaStore, VariableSpace space ) throws HopException;

  /**
   * Action remove hop exiting this step
   *
   * @return step was changed
   */
  default boolean cleanAfterHopFromRemove() {
    return false;
  }

  /**
   * <p>Action remove hop exiting this step</p>
   *
   * @param toStep the to-step of the hop being removed
   * @return step was changed
   */
  default boolean cleanAfterHopFromRemove( StepMeta toStep ) {
    return false;
  }

  /**
   * <p>Action remove hop entering this step.</p>
   * <p>Sometimes, in addition to the hops themselves, a Step has internal
   * configuration referencing some of the steps that connect to it (to identify the main step or the one to be used for
   * lookup, for instance). If the hop being deleted comes from one of those steps, the references to them should be
   * removed.</p>
   *
   * @param fromStep the from-step of the hop being removed
   * @return step was changed
   */
  default boolean cleanAfterHopToRemove( StepMeta fromStep ) {
    return false;
  }

  /**
   * True if the step passes it's result data straight to the servlet output. See exposing Hop data over a web service
   * <a href="http://wiki.pentaho.com/display/EAI/PDI+data+over+web+services">http://wiki.pentaho.com/display/EAI/PDI+data+over+web+services</a>
   *
   * @return True if the step passes it's result data straight to the servlet output, false otherwise
   */
  default boolean passDataToServletOutput() {
    return false;
  }

  /**
   * Allows for someone to fetch the related PipelineMeta object. Returns null if not found (or not implemented)
   *
   * @param stepMeta  StepMetaInterface object
   * @param metastore metastore object
   * @param space     VariableSpace
   * @return the associated PipelineMeta object, or null if not found (or not implemented)
   * @throws HopException should something go wrong
   */
  default PipelineMeta fetchPipelineMeta( StepMetaInterface stepMeta, IMetaStore metastore, VariableSpace space ) throws HopException {
    return null; // default
  }

  /**
   * This returns the expected name for the dialog that edits a job entry. The expected name is in the org.apache.hop.ui
   * tree and has a class name that is the name of the job entry with 'Dialog' added to the end.
   * <p>
   * e.g. if the job entry is org.apache.hop.job.entries.zipfile.JobEntryZipFile the dialog would be
   * org.apache.hop.ui.job.entries.zipfile.JobEntryZipFileDialog
   * <p>
   * If the dialog class for a job entry does not match this pattern it should override this method and return the
   * appropriate class name
   *
   * @return full class name of the dialog
   */
  public String getDialogClassName();
}
