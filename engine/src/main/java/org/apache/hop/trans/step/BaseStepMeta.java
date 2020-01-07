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

package org.apache.hop.trans.step;

import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Counter;
import org.apache.hop.core.HopAttribute;
import org.apache.hop.core.HopAttributeInterface;
import org.apache.hop.core.SQLStatement;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;

import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceNamingInterface;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.trans.DatabaseImpact;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.TransMeta.TransformationType;
import org.apache.hop.trans.step.errorhandling.Stream;
import org.apache.hop.trans.step.errorhandling.StreamInterface;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class is responsible for implementing common functionality regarding step meta, such as logging. All Hop
 * steps have an extension of this where private fields have been added with public accessors.
 * <p>
 * For example, the "Text File Output" step's TextFileOutputMeta class extends BaseStepMeta by adding fields for the
 * output file name, compression, file format, etc...
 * <p>
 *
 * @created 19-June-2003
 */
public class BaseStepMeta implements Cloneable, StepAttributesInterface {
  public static final LoggingObjectInterface loggingObject = new SimpleLoggingObject(
    "Step metadata", LoggingObjectType.STEPMETA, null );

  public static final String STEP_ATTRIBUTES_FILE = "step-attributes.xml";

  private boolean changed;

  /** database connection object to use for searching fields & checking steps */
  protected Database[] databases;

  protected StepMeta parentStepMeta;

  private volatile StepIOMetaInterface ioMetaVar;
  ReentrantReadWriteLock lock = new ReentrantReadWriteLock(  );

  /**
   * Instantiates a new base step meta.
   */
  public BaseStepMeta() {
    changed = false;

    try {
      loadStepAttributes();
    } catch ( Exception e ) {
      e.printStackTrace();
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
      BaseStepMeta retval = (BaseStepMeta) super.clone();

      // PDI-15799: Makes a copy of the StepMeta. This copy can be used within the same Transformation.
      // That means than inner step references are copied rather then cloned.
      // If the copy is acquired for another Transformation (e.g. this method is called from Transformation.clone() )
      // then the step references must be corrected.
      lock.readLock().lock();
      try {
        if ( ioMetaVar != null ) {
          StepIOMetaInterface stepIOMeta = new StepIOMeta( ioMetaVar.isInputAcceptor(), ioMetaVar.isOutputProducer(), ioMetaVar.isInputOptional(), ioMetaVar.isSortedDataRequired(), ioMetaVar.isInputDynamic(), ioMetaVar.isOutputDynamic() );

          List<StreamInterface> infoStreams = ioMetaVar.getInfoStreams();
          for ( StreamInterface infoStream : infoStreams ) {
            stepIOMeta.addStream( new Stream( infoStream ) );
          }

          List<StreamInterface> targetStreams = ioMetaVar.getTargetStreams();
          for ( StreamInterface targetStream : targetStreams ) {
            stepIOMeta.addStream( new Stream( targetStream ) );
          }
          lock.readLock().unlock(); // the setter acquires the write lock which would deadlock unless we release
          retval.setStepIOMeta( stepIOMeta );
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
   * @param ch
   *          the new changed
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
  public RowMetaInterface getTableFields() {
    return null;
  }

  /**
   * Produces the XML string that describes this step's information.
   *
   * @return String containing the XML describing this step.
   * @throws HopException
   *           in case there is an XML conversion or encoding error
   */
  public String getXML() throws HopException {
    return "";
  }

  /**
   * Gets the fields.
   *
   * @param inputRowMeta
   *          the input row meta that is modified in this method to reflect the output row metadata of the step
   * @param name
   *          Name of the step to use as input for the origin field in the values
   * @param info
   *          Fields used as extra lookup information
   * @param nextStep
   *          the next step that is targeted
   * @param space
   *          the space The variable space to use to replace variables
   * @param metaStore
   *          the MetaStore to use to load additional external data or metadata impacting the output fields
   * @throws HopStepException
   *           the kettle step exception
   */
  public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep,
    VariableSpace space, IMetaStore metaStore ) throws HopStepException {
    // Default: no values are added to the row in the step
  }

  /**
   * Each step must be able to report on the impact it has on a database, table field, etc.
   *
   * @param impact
   *          The list of impacts @see org.apache.hop.transMeta.DatabaseImpact
   * @param transMeta
   *          The transformation information
   * @param stepMeta
   *          The step information
   * @param prev
   *          The fields entering this step
   * @param input
   *          The previous step names
   * @param output
   *          The output step names
   * @param info
   *          The fields used as information by this step
   * @param metaStore
   *          the MetaStore to use to load additional external data or metadata impacting the output fields
   */
  public void analyseImpact( List<DatabaseImpact> impact, TransMeta transMeta, StepMeta stepMeta,
                             RowMetaInterface prev, String[] input, String[] output,
                             RowMetaInterface info, IMetaStore metaStore ) throws HopStepException {

  }


  /**
   * Standard method to return an SQLStatement object with SQL statements that the step needs in order to work
   * correctly. This can mean "create table", "create index" statements but also "alter table ... add/drop/modify"
   * statements.
   *
   * @return The SQL Statements for this step. If nothing has to be done, the SQLStatement.getSQL() == null. @see
   *         SQLStatement
   * @param transMeta
   *          TransInfo object containing the complete transformation
   * @param stepMeta
   *          StepMeta object containing the complete step
   * @param prev
   *          Row containing meta-data for the input fields (no data)
   * @param metaStore
   *          the MetaStore to use to load additional external data or metadata impacting the output fields
   */
  public SQLStatement getSQLStatements( TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
    IMetaStore metaStore ) throws HopStepException {
    // default: this doesn't require any SQL statements to be executed!
    return new SQLStatement( stepMeta.getName(), null, null );
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
        if ( databases[i] != null ) {
          databases[i].cancelQuery();
        }
      }
    }
  }

  /**
   * Default a step doesn't use any arguments. Implement this to notify the GUI that a window has to be displayed BEFORE
   * launching a transformation.
   *
   * @return A row of argument values. (name and optionally a default value)
   */
  public Map<String, String> getUsedArguments() {
    return null;
  }

  /**
   * The natural way of data flow in a transformation is source-to-target. However, this makes mapping to target tables
   * difficult to do. To help out here, we supply information to the transformation meta-data model about which fields
   * are required for a step. This allows us to automate certain tasks like the mapping to pre-defined tables. The Table
   * Output step in this case will output the fields in the target table using this method.
   *
   * This default implementation returns an empty row meaning that no fields are required for this step to operate.
   *
   * @param space
   *          the variable space to use to do variable substitution.
   * @return the required fields for this steps meta data.
   * @throws HopException
   *           in case the required fields can't be determined
   */
  public RowMetaInterface getRequiredFields( VariableSpace space ) throws HopException {
    return new RowMeta();
  }

  /**
   * This method returns all the database connections that are used by the step.
   *
   * @return an array of database connections meta-data. Return an empty array if no connections are used.
   */
  public DatabaseMeta[] getUsedDatabaseConnections() {
    return new DatabaseMeta[] {};
  }

  /**
   * @return true if this step supports error "reporting" on rows: the ability to send rows to a certain target step.
   */
  public boolean supportsErrorHandling() {
    return false;
  }

  /**
   * This method is added to exclude certain steps from layout checking.
   *
   * @since 2.5.0
   */
  public boolean excludeFromRowLayoutVerification() {
    return false;
  }

  /**
   * This method is added to exclude certain steps from copy/distribute checking.
   *
   * @since 4.0.0
   */
  public boolean excludeFromCopyDistributeVerification() {
    return false;
  }

  /**
   * Get a list of all the resource dependencies that the step is depending on.
   *
   * @return a list of all the resource dependencies that the step is depending on
   */
  public List<ResourceReference> getResourceDependencies( TransMeta transMeta, StepMeta stepInfo ) {
    return Arrays.asList( new ResourceReference( stepInfo ) );
  }

  /**
   * Export resources.
   *
   * @param space
   *          the space
   * @param definitions
   *          the definitions
   * @param resourceNamingInterface
   *          the resource naming interface
   * @param metaStore
   *          The place to load additional information
   * @return the string
   * @throws HopException
   *           the kettle exception
   */
  public String exportResources( VariableSpace space, Map<String, ResourceDefinition> definitions,
    ResourceNamingInterface resourceNamingInterface, IMetaStore metaStore ) throws HopException {
    return null;
  }

  /**
   * This returns the expected name for the dialog that edits a job entry. The expected name is in the org.apache.hop.ui
   * tree and has a class name that is the name of the job entry with 'Dialog' added to the end.
   *
   * e.g. if the job entry is org.apache.hop.job.entries.zipfile.JobEntryZipFile the dialog would be
   * org.apache.hop.ui.job.entries.zipfile.JobEntryZipFileDialog
   *
   * If the dialog class for a job entry does not match this pattern it should override this method and return the
   * appropriate class name
   *
   * @return full class name of the dialog
   *
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
   * Gets the parent step meta.
   *
   * @return the parent step meta
   */
  public StepMeta getParentStepMeta() {
    return parentStepMeta;
  }

  /**
   * Sets the parent step meta.
   *
   * @param parentStepMeta
   *          the new parent step meta
   */
  public void setParentStepMeta( StepMeta parentStepMeta ) {
    this.parentStepMeta = parentStepMeta;
  }

  // TODO find a way to factor out these methods...
  //

  protected LogChannelInterface log;

  protected ArrayList<HopAttributeInterface> attributes;

  // Late init to prevent us from logging blank step names, etc.
  /**
   * Gets the log.
   *
   * @return the log
   */
  public LogChannelInterface getLog() {
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
   * @param message
   *          the message
   */
  public void logMinimal( String message ) {
    getLog().logMinimal( message );
  }

  /**
   * Log minimal.
   *
   * @param message
   *          the message
   * @param arguments
   *          the arguments
   */
  public void logMinimal( String message, Object... arguments ) {
    getLog().logMinimal( message, arguments );
  }

  /**
   * Log basic.
   *
   * @param message
   *          the message
   */
  public void logBasic( String message ) {
    getLog().logBasic( message );
  }

  /**
   * Log basic.
   *
   * @param message
   *          the message
   * @param arguments
   *          the arguments
   */
  public void logBasic( String message, Object... arguments ) {
    getLog().logBasic( message, arguments );
  }

  /**
   * Log detailed.
   *
   * @param message
   *          the message
   */
  public void logDetailed( String message ) {
    getLog().logDetailed( message );
  }

  /**
   * Log detailed.
   *
   * @param message
   *          the message
   * @param arguments
   *          the arguments
   */
  public void logDetailed( String message, Object... arguments ) {
    getLog().logDetailed( message, arguments );
  }

  /**
   * Log debug.
   *
   * @param message
   *          the message
   */
  public void logDebug( String message ) {
    getLog().logDebug( message );
  }

  /**
   * Log debug.
   *
   * @param message
   *          the message
   * @param arguments
   *          the arguments
   */
  public void logDebug( String message, Object... arguments ) {
    getLog().logDebug( message, arguments );
  }

  /**
   * Log rowlevel.
   *
   * @param message
   *          the message
   */
  public void logRowlevel( String message ) {
    getLog().logRowlevel( message );
  }

  /**
   * Log rowlevel.
   *
   * @param message
   *          the message
   * @param arguments
   *          the arguments
   */
  public void logRowlevel( String message, Object... arguments ) {
    getLog().logRowlevel( message, arguments );
  }

  /**
   * Log error.
   *
   * @param message
   *          the message
   */
  public void logError( String message ) {
    getLog().logError( message );
  }

  /**
   * Log error.
   *
   * @param message
   *          the message
   * @param e
   *          the e
   */
  public void logError( String message, Throwable e ) {
    getLog().logError( message, e );
  }

  /**
   * Log error.
   *
   * @param message
   *          the message
   * @param arguments
   *          the arguments
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
  public LoggingObjectInterface getParent() {
    return null;
  }

  public StepIOMetaInterface getStepIOMeta(  ) {
    return getStepIOMeta( true ); // Default to creating step IO Meta
  }

  /**
   * Returns the Input/Output metadata for this step. By default, each step produces and accepts optional input.
   */
  public StepIOMetaInterface getStepIOMeta( boolean createIfAbsent ) {
    StepIOMetaInterface ioMeta = null;
    lock.readLock().lock();
    try {
      if ( ( ioMetaVar == null ) && ( createIfAbsent ) ) {
        ioMeta = new StepIOMeta( true, true, true, false, false, false );
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
   * Sets the Input/Output metadata for this step. By default, each step produces and accepts optional input.
   * @param value the StepIOMetaInterface to set for this step.
   */
  public void setStepIOMeta( StepIOMetaInterface value ) {
    lock.writeLock().lock();
    try {
      ioMetaVar = value;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * @return The list of optional input streams. It allows the user to select from a list of possible actions like
   *         "New target step"
   */
  public List<StreamInterface> getOptionalStreams() {
    List<StreamInterface> list = new ArrayList<StreamInterface>();
    return list;
  }

  /**
   * When an optional stream is selected, this method is called to handled the ETL metadata implications of that.
   *
   * @param stream
   *          The optional stream to handle.
   */
  public void handleStreamSelection( StreamInterface stream ) {
  }

  /**
   * Reset step io meta.
   */
  public void resetStepIoMeta() {
    lock.writeLock().lock();
    try {
      ioMetaVar = null;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Change step names into step objects to allow them to be name-changed etc.
   *
   * @param steps
   *          the steps to reference
   */
  public void searchInfoAndTargetSteps( List<StepMeta> steps ) {
  }

  /**
   * @return Optional interface that allows an external program to inject step metadata in a standardized fasion. This
   *         method will return null if the interface is not available for this step.
   */
  public StepMetaInjectionInterface getStepMetaInjectionInterface() {
    return null;
  }

  /**
   * @return The step metadata itself, not the metadata description.
   * For lists it will have 0 entries in case there are no entries.
   * @throws HopException
   */
  public List<StepInjectionMetaEntry> extractStepMetadataEntries() throws HopException {
    return null;
  }

  /**
   * Find parent entry.
   *
   * @param entries
   *          the entries
   * @param key
   *          the key
   * @return the step injection meta entry
   */
  protected StepInjectionMetaEntry findParentEntry( List<StepInjectionMetaEntry> entries, String key ) {
    for ( StepInjectionMetaEntry look : entries ) {
      if ( look.getKey().equals( key ) ) {
        return look;
      }
      StepInjectionMetaEntry check = findParentEntry( look.getDetails(), key );
      if ( check != null ) {
        return check;
      }
    }
    return null;
  }

  /**
   * Creates the entry.
   *
   * @param attr
   *          the attr
   * @param PKG
   *          the pkg
   * @return the step injection meta entry
   */
  protected StepInjectionMetaEntry createEntry( HopAttributeInterface attr, Class<?> PKG ) {
    return new StepInjectionMetaEntry( attr.getKey(), attr.getType(), BaseMessages.getString( PKG, attr
      .getDescription() ) );
  }

  /**
   * Describe the metadata attributes that can be injected into this step metadata object.
   */
  public List<StepInjectionMetaEntry> getStepInjectionMetadataEntries( Class<?> PKG ) {
    List<StepInjectionMetaEntry> entries = new ArrayList<StepInjectionMetaEntry>();

    for ( HopAttributeInterface attr : attributes ) {
      if ( attr.getParent() == null ) {
        entries.add( createEntry( attr, PKG ) );
      } else {
        StepInjectionMetaEntry entry = createEntry( attr, PKG );
        StepInjectionMetaEntry parentEntry = findParentEntry( entries, attr.getParent().getKey() );
        if ( parentEntry == null ) {
          throw new RuntimeException(
            "An error was detected in the step attributes' definition: the parent was not found for attribute "
              + attr );
        }
        parentEntry.getDetails().add( entry );
      }
    }

    return entries;
  }

  /**
   * Load step attributes.
   *
   * @throws HopException
   *           the kettle exception
   */
  protected void loadStepAttributes() throws HopException {
    try ( InputStream inputStream = getClass().getResourceAsStream( STEP_ATTRIBUTES_FILE ) ) {
      if ( inputStream != null ) {
        Document document = XMLHandler.loadXMLFile( inputStream );
        Node attrsNode = XMLHandler.getSubNode( document, "attributes" );
        List<Node> nodes = XMLHandler.getNodes( attrsNode, "attribute" );
        attributes = new ArrayList<HopAttributeInterface>();
        for ( Node node : nodes ) {
          String key = XMLHandler.getTagAttribute( node, "id" );
          String xmlCode = XMLHandler.getTagValue( node, "xmlcode" );
          String repCode = XMLHandler.getTagValue( node, "repcode" );
          String description = XMLHandler.getTagValue( node, "description" );
          String tooltip = XMLHandler.getTagValue( node, "tooltip" );
          int valueType = ValueMetaFactory.getIdForValueMeta( XMLHandler.getTagValue( node, "valuetype" ) );
          String parentId = XMLHandler.getTagValue( node, "parentid" );

          HopAttribute attribute = new HopAttribute( key, xmlCode, repCode, description, tooltip, valueType, findParent( attributes, parentId ) );
          attributes.add( attribute );
        }
      }
    } catch ( Exception e ) {
      throw new HopException( "Unable to load file " + STEP_ATTRIBUTES_FILE, e );
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.trans.step.StepAttributesInterface#findParent(java.util.List, java.lang.String)
   */
  @Override
  public HopAttributeInterface findParent( List<HopAttributeInterface> attributes, String parentId ) {
    if ( Utils.isEmpty( parentId ) ) {
      return null;
    }
    for ( HopAttributeInterface attribute : attributes ) {
      if ( attribute.getKey().equals( parentId ) ) {
        return attribute;
      }
    }
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.trans.step.StepAttributesInterface#findAttribute(java.lang.String)
   */
  @Override
  public HopAttributeInterface findAttribute( String key ) {
    for ( HopAttributeInterface attribute : attributes ) {
      if ( attribute.getKey().equals( key ) ) {
        return attribute;
      }
    }
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.trans.step.StepAttributesInterface#getXmlCode(java.lang.String)
   */
  @Override
  public String getXmlCode( String attributeKey ) {
    return findAttribute( attributeKey ).getXmlCode();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.trans.step.StepAttributesInterface#getRepCode(java.lang.String)
   */
  @Override
  public String getRepCode( String attributeKey ) {
    HopAttributeInterface attr = findAttribute( attributeKey );
    return Utils.isEmpty( attr.getRepCode() ) ? attr.getXmlCode() : attr.getRepCode();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.trans.step.StepAttributesInterface#getDescription(java.lang.String)
   */
  @Override
  public String getDescription( String attributeKey ) {
    return findAttribute( attributeKey ).getDescription();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.trans.step.StepAttributesInterface#getTooltip(java.lang.String)
   */
  @Override
  public String getTooltip( String attributeKey ) {
    return findAttribute( attributeKey ).getTooltip();
  }

  /**
   * @return The supported transformation types that this step supports.
   */
  public TransformationType[] getSupportedTransformationTypes() {
    return new TransformationType[] { TransformationType.Normal, TransformationType.SingleThreaded, };
  }

  /**
   * @return The objects referenced in the step, like a mapping, a transformation, a job, ...
   */
  public String[] getReferencedObjectDescriptions() {
    return null;
  }

  public boolean[] isReferencedObjectEnabled() {
    return null;
  }

  /**
   * @return A description of the active referenced object in a transformation.
   * Null if nothing special needs to be done or if the active metadata isn't different from design.
   */
  public String getActiveReferencedObjectDescription() {
    return null;
  }

  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    // provided for API (compile & runtime) compatibility with v4
  }


  /**
   * @param remarks
   * @param transMeta
   * @param stepMeta
   * @param prev
   * @param input
   * @param output
   * @param info
   * @param metaStore
   */
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
    RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
    IMetaStore metaStore ) {
  }

  /**
   * Load the referenced object
   *
   * @param index
   *          the referenced object index to load (in case there are multiple references)
   * @param metaStore
   *          the MetaStore to use
   * @param space
   *          the variable space to use
   * @return the referenced object once loaded
   * @throws HopException
   */
  public Object loadReferencedObject( int index, IMetaStore metaStore, VariableSpace space ) throws HopException {
    return null;
  }
}
