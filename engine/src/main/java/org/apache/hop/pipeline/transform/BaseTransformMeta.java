/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.pipeline.transform;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.HopAttribute;
import org.apache.hop.core.IHopAttribute;
import org.apache.hop.core.SQLStatement;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.InputStream;
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
public class BaseTransformMeta implements Cloneable, ITransformAttributes {
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

  protected String transformAttributesFile;

  public BaseTransformMeta() {
    this( null );
  }

  /**
   * Instantiates a new base transform meta.
   */
  public BaseTransformMeta( String transformAttributesFile ) {
    this.transformAttributesFile = transformAttributesFile;
    changed = false;

    if ( StringUtils.isNotEmpty( transformAttributesFile ) ) {
      try {
        loadTransformAttributes();
      } catch ( Exception e ) {
        throw new RuntimeException( "Unable to create/load base metadata information for class " + getClass().getName(), e );
      }
    }
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
   */
  public IRowMeta getTableFields() {
    return null;
  }

  /**
   * Produces the XML string that describes this transform's information.
   *
   * @return String containing the XML describing this transform.
   * @throws HopException in case there is an XML conversion or encoding error
   */
  public String getXML() throws HopException {
    return "";
  }

  /**
   * Gets the fields.
   *
   * @param inputRowMeta the input row meta that is modified in this method to reflect the output row metadata of the transform
   * @param name         Name of the transform to use as input for the origin field in the values
   * @param info         Fields used as extra lookup information
   * @param nextTransform     the next transform that is targeted
   * @param variables        the space The variable space to use to replace variables
   * @param metaStore    the MetaStore to use to load additional external data or metadata impacting the output fields
   * @throws HopTransformException the kettle transform exception
   */
  public void getFields( IRowMeta inputRowMeta, String name, IRowMeta[] info, TransformMeta nextTransform,
                         IVariables variables, IMetaStore metaStore ) throws HopTransformException {
    // Default: no values are added to the row in the transform
  }

  /**
   * Each transform must be able to report on the impact it has on a database, table field, etc.
   *
   * @param impact    The list of impacts @see org.apache.hop.pipelineMeta.DatabaseImpact
   * @param pipelineMeta The pipeline information
   * @param transformMeta  The transform information
   * @param prev      The fields entering this transform
   * @param input     The previous transform names
   * @param output    The output transform names
   * @param info      The fields used as information by this transform
   * @param metaStore the MetaStore to use to load additional external data or metadata impacting the output fields
   */
  public void analyseImpact( List<DatabaseImpact> impact, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                             IRowMeta prev, String[] input, String[] output,
                             IRowMeta info, IMetaStore metaStore ) throws HopTransformException {

  }


  /**
   * Standard method to return an SQLStatement object with SQL statements that the transform needs in order to work
   * correctly. This can mean "create table", "create index" statements but also "alter table ... add/drop/modify"
   * statements.
   *
   * @param pipelineMeta PipelineMeta object containing the complete pipeline
   * @param transformMeta  TransformMeta object containing the complete transform
   * @param prev      Row containing meta-data for the input fields (no data)
   * @param metaStore the MetaStore to use to load additional external data or metadata impacting the output fields
   * @return The SQL Statements for this transform. If nothing has to be done, the SQLStatement.getSQL() == null. @see
   * SQLStatement
   */
  public SQLStatement getSQLStatements( PipelineMeta pipelineMeta, TransformMeta transformMeta, IRowMeta prev,
                                        IMetaStore metaStore ) throws HopTransformException {
    // default: this doesn't require any SQL statements to be executed!
    return new SQLStatement( transformMeta.getName(), null, null );
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
   * @param variables the variable space to use to do variable substitution.
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
  public List<ResourceReference> getResourceDependencies( PipelineMeta pipelineMeta, TransformMeta transformInfo ) {
    return Arrays.asList( new ResourceReference( transformInfo ) );
  }

  /**
   * Export resources.
   *
   * @param variables                   the space
   * @param definitions             the definitions
   * @param iResourceNaming the resource naming interface
   * @param metaStore               The place to load additional information
   * @return the string
   * @throws HopException the kettle exception
   */
  public String exportResources( IVariables variables, Map<String, ResourceDefinition> definitions,
                                 IResourceNaming iResourceNaming, IMetaStore metaStore ) throws HopException {
    return null;
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
  public String getDialogClassName() {
    String className = getClass().getCanonicalName();
    className = className.replaceFirst( "\\.hop\\.", ".hop.ui." );
    if ( className.endsWith( "Meta" ) ) {
      className = className.substring( 0, className.length() - 4 );
    }
    className += "Dialog";
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
    List<IStream> list = new ArrayList<IStream>();
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
   * Load transform attributes.
   *
   * @throws HopException the kettle exception
   */
  protected void loadTransformAttributes() throws HopException {
    InputStream inputStream = null;
    try {
      inputStream = this.getClass().getClassLoader().getResourceAsStream( transformAttributesFile );
      if ( inputStream == null ) {
        inputStream = getClass().getResourceAsStream( transformAttributesFile );
      }
      if ( inputStream != null ) {
        Document document = XMLHandler.loadXMLFile( inputStream );
        Node attrsNode = XMLHandler.getSubNode( document, "attributes" );
        List<Node> nodes = XMLHandler.getNodes( attrsNode, "attribute" );
        attributes = new ArrayList<IHopAttribute>();
        for ( Node node : nodes ) {
          String key = XMLHandler.getTagAttribute( node, "id" );
          String xmlCode = XMLHandler.getTagValue( node, "xmlcode" );
          String description = XMLHandler.getTagValue( node, "description" );
          String tooltip = XMLHandler.getTagValue( node, "tooltip" );
          int valueType = ValueMetaFactory.getIdForValueMeta( XMLHandler.getTagValue( node, "valuetype" ) );
          String parentId = XMLHandler.getTagValue( node, "parentid" );

          HopAttribute attribute = new HopAttribute( key, xmlCode, description, tooltip, valueType, findParent( attributes, parentId ) );
          attributes.add( attribute );
        }
      } else {
        throw new HopException( "Unable to find transform attributes file '" + transformAttributesFile + "' for transform class '" + getClass().getName() + "'" );
      }
    } catch ( Exception e ) {
      throw new HopException( "Unable to load file " + transformAttributesFile, e );
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransformAttributes#findParent(java.util.List, java.lang.String)
   */
  @Override
  public IHopAttribute findParent( List<IHopAttribute> attributes, String parentId ) {
    if ( Utils.isEmpty( parentId ) ) {
      return null;
    }
    for ( IHopAttribute attribute : attributes ) {
      if ( attribute.getKey().equals( parentId ) ) {
        return attribute;
      }
    }
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransformAttributes#findAttribute(java.lang.String)
   */
  @Override
  public IHopAttribute findAttribute( String key ) {
    for ( IHopAttribute attribute : attributes ) {
      if ( attribute.getKey().equals( key ) ) {
        return attribute;
      }
    }
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransformAttributes#getXmlCode(java.lang.String)
   */
  @Override
  public String getXmlCode( String attributeKey ) {
    return findAttribute( attributeKey ).getXmlCode();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransformAttributes#getDescription(java.lang.String)
   */
  @Override
  public String getDescription( String attributeKey ) {
    return findAttribute( attributeKey ).getDescription();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransformAttributes#getTooltip(java.lang.String)
   */
  @Override
  public String getTooltip( String attributeKey ) {
    return findAttribute( attributeKey ).getTooltip();
  }

  /**
   * @return The supported pipeline types that this transform supports.
   */
  public PipelineType[] getSupportedPipelineTypes() {
    return new PipelineType[] { PipelineType.Normal, PipelineType.SingleThreaded, };
  }

  /**
   * @return The objects referenced in the transform, like a mapping, a pipeline, a job, ...
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

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
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
   * @param metaStore
   */
  public void check( List<ICheckResult> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, IVariables variables,
                     IMetaStore metaStore ) {
  }

  /**
   * Load the referenced object
   *
   * @param index     the referenced object index to load (in case there are multiple references)
   * @param metaStore the MetaStore to use
   * @param variables     the variable space to use
   * @return the referenced object once loaded
   * @throws HopException
   */
  public Object loadReferencedObject( int index, IMetaStore metaStore, IVariables variables ) throws HopException {
    return null;
  }

  /**
   * Gets transformAttributesFile
   *
   * @return value of transformAttributesFile
   */
  public String getTransformAttributesFile() {
    return transformAttributesFile;
  }

  /**
   * @param transformAttributesFile The transformAttributesFile to set
   */
  public void setTransformAttributesFile( String transformAttributesFile ) {
    this.transformAttributesFile = transformAttributesFile;
  }
}
