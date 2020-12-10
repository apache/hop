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

package org.apache.hop.pipeline.transform;

import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.w3c.dom.Node;

import java.util.List;
import java.util.Map;

/**
 * This interface allows custom transforms to talk to Hop. The ITransformMeta is the main Java interface that a plugin
 * implements. The responsibilities of the implementing class are listed below:
 * <p>
 * <ul>
 * <li><b>Keep track of the transform settings</b></br> The implementing class typically keeps track of transform settings using
 * private fields with corresponding getters and setters. The dialog class implementing ITransformDialog is using the
 * getters and setters to copy the user supplied configuration in and out of the dialog.
 * <p>
 * The following interface methods also fall into the area of maintaining settings:
 * <p>
 * <i><a href="#setDefault()">void setDefault()</a></i>
 * <p>
 * This method is called every time a new transform is created and should allocate or set the transform configuration to sensible
 * defaults. The values set here will be used by HopGui when a new transform is created. This is often a good place to ensure
 * that the transform&#8217;s settings are initialized to non-null values. Null values can be cumbersome to deal with in
 * serialization and dialog population, so most PDI transform implementations stick to non-null values for all transform settings.
 * <p>
 * <i><a href="#clone()">public Object clone()</a></i>
 * <p>
 * This method is called when a transform is duplicated in HopGui. It needs to return a deep copy of this transform meta object. It
 * is essential that the implementing class creates proper deep copies if the transform configuration is stored in modifiable
 * objects, such as lists or custom helper objects. See org.apache.hop.pipeline.transforms.rowgenerator.RowGeneratorMeta.clone()
 * for an example on creating a deep copy.
 * <p></li>
 * <li>
 * <b>Serialize transform settings</b><br/>
 * The plugin needs to be able to serialize its settings to XML . The interface methods are as
 * follows.
 * <p>
 * <i><a href="#getXml()">public String getXml()</a></i>
 * <p>
 * This method is called by PDI whenever a transform needs to serialize its settings to XML. It is called when saving a
 * pipeline in HopGui. The method returns an XML string, containing the serialized transform settings. The string
 * contains a series of XML tags, typically one tag per setting. The helper class org.apache.hop.core.xml.XmlHandler is
 * typically used to construct the XML string.
 * <p>
 * <i><a href="#loadXml(org.w3c.dom.Node)">public void loadXml(...)</a></i>
 * <p>
 * This method is called by PDI whenever a transform needs to read its settings from XML. The XML node containing the transform's
 * settings is passed in as an argument. Again, the helper class org.apache.hop.core.xml.XmlHandler is typically used to
 * conveniently read the transform settings from the XML node.
 * <p>
 * <li>
 * <b>Provide instances of other plugin classes</b><br/>
 * The ITransformMeta plugin class is the main class tying in with the rest of PDI architecture. It is responsible
 * for supplying instances of the other plugin classes implementing ITransformDialog, ITransform and
 * ITransformData. The following methods cover these responsibilities. Each of the method&#8217;s implementation is
 * typically constructing a new instance of the corresponding class forwarding the passed in arguments to the
 * constructor. The methods are as follows.
 * <p>
 * public ITransformDialog getDialog(...)<br/>
 * public ITransform getTransform(...)<br/>
 * public ITransformData getTransformData()<br/>
 * <p>
 * Each of the above methods returns a new instance of the plugin class implementing ITransformDialog, ITransform
 * and ITransformData.</li>
 * <li>
 * <b>Report the transform&#8217;s changes to the row stream</b> PDI needs to know how a transform affects the row structure. A
 * transform may be adding or removing fields, as well as modifying the metadata of a field. The method implementing this
 * aspect of a transform plugin is getFields().
 * <p>
 * <i><a href= "#getFields(org.apache.hop.core.row.IRowMeta, java.lang.String,
 * org.apache.hop.core.row.IRowMeta[], org.apache.hop.pipeline.transform.TransformMeta,
 * org.apache.hop.core.variables.IVariables)" >public void getFields(...)</a></i>
 * <p>
 * Given a description of the input rows, the plugin needs to modify it to match the structure for its output fields.
 * The implementation modifies the passed in IRowMeta object to reflect any changes to the row stream. Typically
 * a transform adds fields to the row structure, which is done by creating ValueMeta objects (PDI&#8217;s default
 * implementation of IValueMeta), and appending them to the IRowMeta object. The section Working with
 * Fields goes into deeper detail on IValueMeta.</li>
 * <li>
 * <b>Validate transform settings</b><br>
 * HopGui supports a &#8220;validate pipeline&#8221; feature, which triggers a self-check of all transforms. PDI invokes
 * the check() method of each transform on the canvas allowing each transform to validate its settings.
 * <p>
 * <i><a href= "#check(java.util.List, org.apache.hop.pipeline.PipelineMeta, org.apache.hop.pipeline.transform.TransformMeta,
 * org.apache.hop.core.row.IRowMeta, java.lang.String[], java.lang.String[],
 * org.apache.hop.core.row.IRowMeta)" >public void check()</a></i>
 * <p>
 * Each transform has the opportunity to validate its settings and verify that the configuration given by the user is
 * reasonable. In addition to that a transform typically checks if it is connected to preceding or following transforms, if the
 * nature of the transform requires that kind of connection. An input transform may expect to not have a preceding transform for
 * example. The check method passes in a list of check remarks that the method should append its validation results to.
 * HopGui then displays the list of remarks collected from the transforms, allowing the user to take corrective action in case
 * of validation warnings or errors.</li>
 * Given a description of the input rows, the plugin needs to modify it to match the structure for its output fields.
 * The implementation modifies the passed in IRowMeta object to reflect any changes to the row stream. Typically
 * a transform adds fields to the row structure, which is done by creating ValueMeta objects (PDI&#8217;s default
 * implementation of IValueMeta), and appending them to the IRowMeta object. The section Working with
 * Fields goes into deeper detail on IValueMeta.
 * </ul>
 *
 * @author Matt
 * @since 4-aug-2004
 */

public interface ITransformMeta<Main extends ITransform, Data extends ITransformData> {
  /**
   * Set default values
   */
  void setDefault();

  /**
   * Gets the fields.
   *
   * @param inputRowMeta the input row meta that is modified in this method to reflect the output row metadata of the transform
   * @param name         Name of the transform to use as input for the origin field in the values
   * @param info         Fields used as extra lookup information
   * @param nextTransform     the next transform that is targeted
   * @param variables        the variables The variable variables to use to replace variables
   * @param metadataProvider    the MetaStore to use to load additional external data or metadata impacting the output fields
   * @throws HopTransformException the hop transform exception
   */
  void getFields( IRowMeta inputRowMeta, String name, IRowMeta[] info, TransformMeta nextTransform,
                  IVariables variables, IHopMetadataProvider metadataProvider ) throws HopTransformException;

  /**
   * Get the XML that represents the values in this transform
   *
   * @return the XML that represents the metadata in this transform
   * @throws HopException in case there is a conversion or XML encoding error
   */
  String getXml() throws HopException;

  /**
   * Load the values for this transform from an XML Node
   *
   * @param transformNode  the Node to get the info from
   * @param metadataProvider the metadata to optionally load external reference metadata from
   * @throws HopXmlException When an unexpected XML error occurred. (malformed etc.)
   */
  void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException;


  /**
   * Checks the settings of this transform and puts the findings in a remarks List.
   *
   * @param remarks   The list to put the remarks in @see org.apache.hop.core.CheckResult
   * @param transformMeta  The transformMeta to help checking
   * @param prev      The fields coming from the previous transform
   * @param input     The input transform names
   * @param output    The output transform names
   * @param info      The fields that are used as information by the transform
   * @param variables     the variable variables to resolve variable expressions with
   * @param metadataProvider the MetaStore to use to load additional external data or metadata impacting the output fields
   */
  void check( List<ICheckResult> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
              IRowMeta prev, String[] input, String[] output, IRowMeta info, IVariables variables,
              IHopMetadataProvider metadataProvider );

  /**
   * Make an exact copy of this transform, make sure to explicitly copy Collections etc.
   *
   * @return an exact copy of this transform
   */
  Object clone();

  /**
   * @return The fields used by this transform, this is being used for the Impact analyses.
   * @param variables
   */
  IRowMeta getTableFields( IVariables variables );

  /**
   * This method is added to exclude certain transforms from layout checking.
   *
   * @since 2.5.0
   */
  boolean excludeFromRowLayoutVerification();

  /**
   * This method is added to exclude certain transforms from copy/distribute checking.
   *
   * @since 4.0.0
   */
  boolean excludeFromCopyDistributeVerification();

  ITransform createTransform( TransformMeta transformMeta, Data data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline );

  /**
   * Get a new instance of the appropriate data class. This data class implements the ITransformData. It basically
   * contains the persisting data that needs to live on, even if a worker thread is terminated.
   *
   * @return The appropriate ITransformData class.
   */
  Data getTransformData();

  /**
   * Each transform must be able to report on the impact it has on a database, table field, etc.
   *
   * @param variables the variables to resolve expression with
   * @param impact    The list of impacts @see org.apache.hop.pipelineMeta.DatabaseImpact
   * @param pipelineMeta The pipeline information
   * @param transformMeta  The transform information
   * @param prev      The fields entering this transform
   * @param input     The previous transform names
   * @param output    The output transform names
   * @param info      The fields used as information by this transform
   * @param metadataProvider the MetaStore to use to load additional external data or metadata impacting the output fields
   */
  void analyseImpact( IVariables variables, List<DatabaseImpact> impact, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                      IRowMeta prev, String[] input, String[] output, IRowMeta info, IHopMetadataProvider metadataProvider ) throws HopTransformException;

  /**
   * Standard method to return an SqlStatement object with SQL statements that the transform needs in order to work
   * correctly. This can mean "create table", "create index" statements but also "alter table ... add/drop/modify"
   * statements.
   *
   * @param variables the variables to resolve expressions with
   * @param pipelineMeta PipelineMeta object containing the complete pipeline
   * @param transformMeta  TransformMeta object containing the complete transform
   * @param prev      Row containing meta-data for the input fields (no data)
   * @param metadataProvider the MetaStore to use to load additional external data or metadata impacting the output fields
   * @return The SQL Statements for this transform. If nothing has to be done, the SqlStatement.getSql() == null. @see
   * SqlStatement
   */
  SqlStatement getSqlStatements( IVariables variables, PipelineMeta pipelineMeta, TransformMeta transformMeta, IRowMeta prev,
                                 IHopMetadataProvider metadataProvider ) throws HopTransformException;

  /**
   * Call this to cancel trailing database queries (too long running, etc)
   */
  void cancelQueries() throws HopDatabaseException;


  /**
   * The natural way of data flow in a pipeline is source-to-target. However, this makes mapping to target tables
   * difficult to do. To help out here, we supply information to the pipeline meta-data model about which fields
   * are required for a transform. This allows us to automate certain tasks like the mapping to pre-defined tables. The Table
   * Output transform in this case will output the fields in the target table using this method.
   *
   * @param variables the variable variables to reference
   * @return the required fields for this transforms metadata.
   * @throws HopException in case the required fields can't be determined.
   */
  IRowMeta getRequiredFields( IVariables variables ) throws HopException;

  /**
   * @return true if this transform supports error "reporting" on rows: the ability to send rows to a certain target transform.
   */
  boolean supportsErrorHandling();

  /**
   * Get a list of all the resource dependencies that the transform is depending on.
   *
   * @param transformMeta
   * @return a list of all the resource dependencies that the transform is depending on
   */
  List<ResourceReference> getResourceDependencies( IVariables variables, TransformMeta transformMeta );

  /**
   * @param variables                   the variable variables to use
   * @param definitions
   * @param iResourceNaming
   * @param metadataProvider               the metadataProvider in which non-hop metadata could reside.
   * @return the filename of the exported resource
   */
  String exportResources( IVariables variables, Map<String, ResourceDefinition> definitions,
                          IResourceNaming iResourceNaming, IHopMetadataProvider metadataProvider ) throws HopException;

  /**
   * @return The TransformMeta object to which this metadata class belongs. With this, we can see to which pipeline
   * metadata (etc) this metadata pertains to. (hierarchy)
   */
  TransformMeta getParentTransformMeta();

  /**
   * Provide original lineage for this metadata object
   *
   * @param parentTransformMeta the parent transform metadata container object
   */
  void setParentTransformMeta( TransformMeta parentTransformMeta );

  /**
   * Returns the Input/Output metadata for this transform.
   */
  ITransformIOMeta getTransformIOMeta();

  /**
   * @return The list of optional input streams. It allows the user to select f rom a list of possible actions like
   * "New target transform"
   */
  List<IStream> getOptionalStreams();

  /**
   * When an optional stream is selected, this method is called to handled the ETL metadata implications of that.
   *
   * @param stream The optional stream to handle.
   */
  void handleStreamSelection( IStream stream );

  /**
   * For transforms that handle dynamic input (info) or output (target) streams, it is useful to be able to force the
   * recreate the TransformIoMeta definition. By default this definition is cached.
   */
  void resetTransformIoMeta();

  /**
   * Change transform names into transform objects to allow them to be name-changed etc.
   *
   * @param transforms the transforms to reference
   */
  void searchInfoAndTargetTransforms( List<TransformMeta> transforms );


  void setChanged();

  boolean hasChanged();

  /**
   * @return The objects referenced in the transform, like a mapping, a pipeline, a workflow, ...
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
   * @param metadataProvider the MetaStore to use
   * @param variables     the variable variables to use
   * @return the referenced object once loaded
   * @throws HopException
   */
  IHasFilename loadReferencedObject( int index, IHopMetadataProvider metadataProvider, IVariables variables ) throws HopException;

  /**
   * Action remove hop exiting this transform
   *
   * @return transform was changed
   */
  default boolean cleanAfterHopFromRemove() {
    return false;
  }

  /**
   * <p>Action remove hop exiting this transform</p>
   *
   * @param toTransform the to-transform of the hop being removed
   * @return transform was changed
   */
  default boolean cleanAfterHopFromRemove( TransformMeta toTransform ) {
    return false;
  }

  /**
   * <p>Action remove hop entering this transform.</p>
   * <p>Sometimes, in addition to the hops themselves, a Transform has internal
   * configuration referencing some of the transforms that connect to it (to identify the main transform or the one to be used for
   * lookup, for instance). If the hop being deleted comes from one of those transforms, the references to them should be
   * removed.</p>
   *
   * @param fromTransform the from-transform of the hop being removed
   * @return transform was changed
   */
  default boolean cleanAfterHopToRemove( TransformMeta fromTransform ) {
    return false;
  }

  /**
   * True if the transform passes it's result data straight to the servlet output. See exposing Hop data over a web service
   *
   * @return True if the transform passes it's result data straight to the servlet output, false otherwise
   */
  default boolean passDataToServletOutput() {
    return false;
  }

  /**
￼   * This returns the expected name for the dialog that edits a action. The expected name is in the org.apache.hop.ui
￼   * tree and has a class name that is the name of the action with 'Dialog' added to the end.
￼   * <p>
￼   * e.g. if the action is org.apache.hop.workflow.actions.zipfile.JobEntryZipFile the dialog would be
￼   * org.apache.hop.ui.workflow.actions.zipfile.JobEntryZipFileDialog
￼   * <p>
￼   * If the dialog class for a action does not match this pattern it should override this method and return the
￼   * appropriate class name
￼   *
￼   * @return full class name of the dialog
￼   */
  String getDialogClassName();


}
