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
import org.apache.hop.core.IHopAttribute;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class is responsible for implementing common functionality regarding transform meta, such as logging. All Hop
 * transforms have an extension of this where private fields have been added with public accessors.
 * <p>
 * For example, the "Text File Output" transform's TextFileOutputMeta class extends BaseTransformMeta by adding fields for the
 * output file name, compression, file format, etc...
 * <p>
 *
 * @created 19-June-2003
 */
public class BaseTransformMeta implements Cloneable {
  public static final ILoggingObject loggingObject = new SimpleLoggingObject(
    "Transform metadata", LoggingObjectType.TRANSFORM_META, null );

  private boolean changed;

  /**
   * database connection object to use for searching fields & checking transforms
   */
  protected Database[] databases;

  protected TransformMeta parentTransformMeta;

  private volatile ITransformIOMeta ioMetaVar;
  ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public BaseTransformMeta() {
    changed = false;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#clone()
   */
  @Override
  public Object clone() {
    try {
      BaseTransformMeta retval = (BaseTransformMeta) super.clone();

      // PDI-15799: Makes a copy of the TransformMeta. This copy can be used within the same Pipeline.
      // That means than inner transform references are copied rather then cloned.
      // If the copy is acquired for another Pipeline (e.g. this method is called from Pipeline.clone() )
      // then the transform references must be corrected.
      lock.readLock().lock();
      try {
        if ( ioMetaVar != null ) {
          ITransformIOMeta transformIOMeta =
            new TransformIOMeta( ioMetaVar.isInputAcceptor(), ioMetaVar.isOutputProducer(), ioMetaVar.isInputOptional(), ioMetaVar.isSortedDataRequired(), ioMetaVar.isInputDynamic(),
              ioMetaVar.isOutputDynamic() );

          List<IStream> infoStreams = ioMetaVar.getInfoStreams();
          for ( IStream infoStream : infoStreams ) {
            transformIOMeta.addStream( new Stream( infoStream ) );
          }

          List<IStream> targetStreams = ioMetaVar.getTargetStreams();
          for ( IStream targetStream : targetStreams ) {
            transformIOMeta.addStream( new Stream( targetStream ) );
          }
          lock.readLock().unlock(); // the setter acquires the write lock which would deadlock unless we release
          retval.setTransformIOMeta( transformIOMeta );
          lock.readLock().lock(); // reacquire read lock
        }
      } finally {
        lock.readLock().unlock();
      }
      return retval;
    } catch ( CloneNotSupportedException e ) {
      return null;
    }
  }

  /**
   * Sets the changed.
   *
   * @param ch the new changed
   */
  public void setChanged( boolean ch ) {
    changed = ch;
  }

  /**
   * Sets the changed.
   */
  public void setChanged() {
    changed = true;
  }

  /**
   * Checks for changed.
   *
   * @return true, if successful
   */
  public boolean hasChanged() {
    return changed;
  }

  /**
   * Gets the table fields.
   *
   * @return the table fields
   * @param variables
   */
  public IRowMeta getTableFields( IVariables variables ) {
    return null;
  }

  /**
   * Produces the XML string that describes this transform's information.
   *
   * @return String containing the XML describing this transform.
   * @throws HopException in case there is an XML conversion or encoding error
   */
  public String getXml() throws HopException {
    return "";
  }

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
  public void getFields( IRowMeta inputRowMeta, String name, IRowMeta[] info, TransformMeta nextTransform,
                         IVariables variables, IHopMetadataProvider metadataProvider ) throws HopTransformException {
    // Default: no values are added to the row in the transform
  }

  /**
   * Each transform must be able to report on the impact it has on a database, table field, etc.
   *
   * @param variables The variables to use to resolve expressions
   * @param impact    The list of impacts @see org.apache.hop.pipelineMeta.DatabaseImpact
   * @param pipelineMeta The pipeline information
   * @param transformMeta  The transform information
   * @param prev      The fields entering this transform
   * @param input     The previous transform names
   * @param output    The output transform names
   * @param info      The fields used as information by this transform
   * @param metadataProvider the MetaStore to use to load additional external data or metadata impacting the output fields
   */
  public void analyseImpact( IVariables variables, List<DatabaseImpact> impact, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                             IRowMeta prev, String[] input, String[] output,
                             IRowMeta info, IHopMetadataProvider metadataProvider ) throws HopTransformException {

  }


  /**
   * Standard method to return an SqlStatement object with SQL statements that the transform needs in order to work
   * correctly. This can mean "create table", "create index" statements but also "alter table ... add/drop/modify"
   * statements.
   *
   *
   * @param variables
   * @param pipelineMeta PipelineMeta object containing the complete pipeline
   * @param transformMeta  TransformMeta object containing the complete transform
   * @param prev      Row containing meta-data for the input fields (no data)
   * @param metadataProvider the MetaStore to use to load additional external data or metadata impacting the output fields
   * @return The SQL Statements for this transform. If nothing has to be done, the SqlStatement.getSql() == null. @see
   * SqlStatement
   */
  public SqlStatement getSqlStatements( IVariables variables, PipelineMeta pipelineMeta, TransformMeta transformMeta, IRowMeta prev,
                                        IHopMetadataProvider metadataProvider ) throws HopTransformException {
    // default: this doesn't require any SQL statements to be executed!
    return new SqlStatement( transformMeta.getName(), null, null );
  }

  /**
   * Call this to cancel trailing database queries (too long running, etc)
   */
  public void cancelQueries() throws HopDatabaseException {
    //
    // Cancel all defined queries...
    //
    if ( databases != null ) {
      for ( int i = 0; i < databases.length; i++ ) {
        if ( databases[ i ] != null ) {
          databases[ i ].cancelQuery();
        }
      }
    }
  }

  /**
   * The natural way of data flow in a pipeline is source-to-target. However, this makes mapping to target tables
   * difficult to do. To help out here, we supply information to the pipeline meta-data model about which fields
   * are required for a transform. This allows us to automate certain tasks like the mapping to pre-defined tables. The Table
   * Output transform in this case will output the fields in the target table using this method.
   * <p>
   * This default implementation returns an empty row meaning that no fields are required for this transform to operate.
   *
   * @param variables the variable variables to use to do variable substitution.
   * @return the required fields for this transforms meta data.
   * @throws HopException in case the required fields can't be determined
   */
  public IRowMeta getRequiredFields( IVariables variables ) throws HopException {
    return new RowMeta();
  }

  /**
   * This method returns all the database connections that are used by the transform.
   *
   * @return an array of database connections meta-data. Return an empty array if no connections are used.
   */
  public DatabaseMeta[] getUsedDatabaseConnections() {
    return new DatabaseMeta[] {};
  }

  /**
   * @return true if this transform supports error "reporting" on rows: the ability to send rows to a certain target transform.
   */
  public boolean supportsErrorHandling() {
    return false;
  }

  /**
   * This method is added to exclude certain transforms from layout checking.
   *
   * @since 2.5.0
   */
  public boolean excludeFromRowLayoutVerification() {
    return false;
  }

  /**
   * This method is added to exclude certain transforms from copy/distribute checking.
   *
   * @since 4.0.0
   */
  public boolean excludeFromCopyDistributeVerification() {
    return false;
  }

  /**
   * Get a list of all the resource dependencies that the transform is depending on.
   *
   * @return a list of all the resource dependencies that the transform is depending on
   */
  public List<ResourceReference> getResourceDependencies( IVariables variables, TransformMeta transformMeta ) {
    return Arrays.asList( new ResourceReference( transformMeta ) );
  }

  /**
   * Export resources.
   *
   * @param variables                   the variables
   * @param definitions             the definitions
   * @param iResourceNaming the resource naming interface
   * @param metadataProvider               The place to load additional information
   * @return the string
   * @throws HopException the hop exception
   */
  public String exportResources( IVariables variables, Map<String, ResourceDefinition> definitions,
                                 IResourceNaming iResourceNaming, IHopMetadataProvider metadataProvider ) throws HopException {
    return null;
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
  public String getDialogClassName() {
    String className = getClass().getCanonicalName();

    if ( className.endsWith( "Meta" ) ) {
      className = className.substring( 0, className.length() - 4 );
    }

    className += "Dialog";
    try {
      Class clazz = Class.forName(className.replaceFirst( "\\.hop\\.", ".hop.ui." ));
      className = clazz.getName();
    }catch (ClassNotFoundException e){
      //do nothing and return plugin classname
    }

    return className;
  }


  /**
   * Gets the parent transform meta.
   *
   * @return the parent transform meta
   */
  public TransformMeta getParentTransformMeta() {
    return parentTransformMeta;
  }

  /**
   * Sets the parent transform meta.
   *
   * @param parentTransformMeta the new parent transform meta
   */
  public void setParentTransformMeta( TransformMeta parentTransformMeta ) {
    this.parentTransformMeta = parentTransformMeta;
  }

  // TODO find a way to factor out these methods...
  //

  protected ILogChannel log;

  protected ArrayList<IHopAttribute> attributes;

  // Late init to prevent us from logging blank transform names, etc.

  /**
   * Gets the log.
   *
   * @return the log
   */
  public ILogChannel getLog() {
    if ( log == null ) {
      log = new LogChannel( this );
    }
    return log;
  }

  /**
   * Checks if is basic.
   *
   * @return true, if is basic
   */
  public boolean isBasic() {
    return getLog().isBasic();
  }

  /**
   * Checks if is detailed.
   *
   * @return true, if is detailed
   */
  public boolean isDetailed() {
    return getLog().isDetailed();
  }

  /**
   * Checks if is debug.
   *
   * @return true, if is debug
   */
  public boolean isDebug() {
    return getLog().isDebug();
  }

  /**
   * Checks if is row level.
   *
   * @return true, if is row level
   */
  public boolean isRowLevel() {
    return getLog().isRowLevel();
  }

  /**
   * Log minimal.
   *
   * @param message the message
   */
  public void logMinimal( String message ) {
    getLog().logMinimal( message );
  }

  /**
   * Log minimal.
   *
   * @param message   the message
   * @param arguments the arguments
   */
  public void logMinimal( String message, Object... arguments ) {
    getLog().logMinimal( message, arguments );
  }

  /**
   * Log basic.
   *
   * @param message the message
   */
  public void logBasic( String message ) {
    getLog().logBasic( message );
  }

  /**
   * Log basic.
   *
   * @param message   the message
   * @param arguments the arguments
   */
  public void logBasic( String message, Object... arguments ) {
    getLog().logBasic( message, arguments );
  }

  /**
   * Log detailed.
   *
   * @param message the message
   */
  public void logDetailed( String message ) {
    getLog().logDetailed( message );
  }

  /**
   * Log detailed.
   *
   * @param message   the message
   * @param arguments the arguments
   */
  public void logDetailed( String message, Object... arguments ) {
    getLog().logDetailed( message, arguments );
  }

  /**
   * Log debug.
   *
   * @param message the message
   */
  public void logDebug( String message ) {
    getLog().logDebug( message );
  }

  /**
   * Log debug.
   *
   * @param message   the message
   * @param arguments the arguments
   */
  public void logDebug( String message, Object... arguments ) {
    getLog().logDebug( message, arguments );
  }

  /**
   * Log rowlevel.
   *
   * @param message the message
   */
  public void logRowlevel( String message ) {
    getLog().logRowlevel( message );
  }

  /**
   * Log rowlevel.
   *
   * @param message   the message
   * @param arguments the arguments
   */
  public void logRowlevel( String message, Object... arguments ) {
    getLog().logRowlevel( message, arguments );
  }

  /**
   * Log error.
   *
   * @param message the message
   */
  public void logError( String message ) {
    getLog().logError( message );
  }

  /**
   * Log error.
   *
   * @param message the message
   * @param e       the e
   */
  public void logError( String message, Throwable e ) {
    getLog().logError( message, e );
  }

  /**
   * Log error.
   *
   * @param message   the message
   * @param arguments the arguments
   */
  public void logError( String message, Object... arguments ) {
    getLog().logError( message, arguments );
  }

  /**
   * Gets the log channel id.
   *
   * @return the log channel id
   */
  public String getLogChannelId() {
    return null;
  }

  /**
   * Gets the name.
   *
   * @return the name
   */
  public String getName() {
    return null;
  }

  /**
   * Gets the object copy.
   *
   * @return the object copy
   */
  public String getObjectCopy() {
    return null;
  }

  /**
   * Gets the object type.
   *
   * @return the object type
   */
  public LoggingObjectType getObjectType() {
    return null;
  }

  /**
   * Gets the parent.
   *
   * @return the parent
   */
  public ILoggingObject getParent() {
    return null;
  }

  public ITransformIOMeta getTransformIOMeta() {
    return getTransformIOMeta( true ); // Default to creating transform IO Meta
  }

  /**
   * Returns the Input/Output metadata for this transform. By default, each transform produces and accepts optional input.
   */
  public ITransformIOMeta getTransformIOMeta( boolean createIfAbsent ) {
    ITransformIOMeta ioMeta = null;
    lock.readLock().lock();
    try {
      if ( ( ioMetaVar == null ) && ( createIfAbsent ) ) {
        ioMeta = new TransformIOMeta( true, true, true, false, false, false );
        lock.readLock().unlock();
        lock.writeLock().lock();
        try {
          ioMetaVar = ioMeta;
          lock.readLock().lock(); // downgrade to read lock before releasing write lock
        } finally {
          lock.writeLock().unlock();
        }
      } else {
        ioMeta = ioMetaVar;
      }
      return ioMeta;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Sets the Input/Output metadata for this transform. By default, each transform produces and accepts optional input.
   *
   * @param value the ITransformIOMeta to set for this transform.
   */
  public void setTransformIOMeta( ITransformIOMeta value ) {
    lock.writeLock().lock();
    try {
      ioMetaVar = value;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * @return The list of optional input streams. It allows the user to select from a list of possible actions like
   * "New target transform"
   */
  public List<IStream> getOptionalStreams() {
    List<IStream> list = new ArrayList<>();
    return list;
  }

  /**
   * When an optional stream is selected, this method is called to handled the ETL metadata implications of that.
   *
   * @param stream The optional stream to handle.
   */
  public void handleStreamSelection( IStream stream ) {
  }

  /**
   * Reset transform io meta.
   */
  public void resetTransformIoMeta() {
    lock.writeLock().lock();
    try {
      ioMetaVar = null;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Change transform names into transform objects to allow them to be name-changed etc.
   *
   * @param transforms the transforms to reference
   */
  public void searchInfoAndTargetTransforms( List<TransformMeta> transforms ) {
  }

  /**
   * @return The supported pipeline types that this transform supports.
   */
  public PipelineType[] getSupportedPipelineTypes() {
    return new PipelineType[] { PipelineType.Normal, PipelineType.SingleThreaded, };
  }

  /**
   * @return The objects referenced in the transform, like a mapping, a pipeline, a workflow, ...
   */
  public String[] getReferencedObjectDescriptions() {
    return null;
  }

  public boolean[] isReferencedObjectEnabled() {
    return null;
  }

  /**
   * @return A description of the active referenced object in a pipeline.
   * Null if nothing special needs to be done or if the active metadata isn't different from design.
   */
  public String getActiveReferencedObjectDescription() {
    return null;
  }

  public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    // provided for API (compile & runtime) compatibility with v4
  }


  /**
   * @param remarks
   * @param pipelineMeta
   * @param transformMeta
   * @param prev
   * @param input
   * @param output
   * @param info
   * @param metadataProvider
   */
  public void check( List<ICheckResult> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
  }

  /**
   * Load the referenced object
   *
   * @param index     the referenced object index to load (in case there are multiple references)
   * @param metadataProvider the MetaStore to use
   * @param variables     the variable variables to use
   * @return the referenced object once loaded
   * @throws HopException
   */
  public IHasFilename loadReferencedObject( int index, IHopMetadataProvider metadataProvider, IVariables variables ) throws HopException {
    return null;
  }

}
