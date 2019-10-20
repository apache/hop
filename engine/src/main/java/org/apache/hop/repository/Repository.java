/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.repository;

import java.util.Calendar;
import java.util.List;

import org.apache.hop.cluster.ClusterSchema;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.Condition;
import org.apache.hop.core.ProgressMonitorListener;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopSecurityException;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.job.JobMeta;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.shared.SharedObjects;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.metastore.api.IMetaStore;

public interface Repository {

  /**
   * @return The name of the repository
   */
  public String getName();

  /**
   * Get the repository version.
   *
   * @return The repository version as a string
   */
  public String getVersion();

  /**
   * @return the metadata describing this repository.
   */
  public RepositoryMeta getRepositoryMeta();

  /**
   * @return the currently logged on user. (also available through the repository security provider)
   */
  public IUser getUserInfo();

  /**
   * @return The security provider for this repository.
   */
  public RepositorySecurityProvider getSecurityProvider();

  /**
   * @return The security manager for this repository.
   */
  public RepositorySecurityManager getSecurityManager();

  /**
   * @return the logging channel of this repository
   */
  public LogChannelInterface getLog();

  /**
   * Connect to the repository. Make sure you don't connect more than once to the same repository with this repository
   * object.
   *
   * @param username
   *          the username of the user connecting to the repository.
   * @param password
   *          the password of the user connecting to the repository.
   * @throws HopSecurityException
   *           in case the supplied user or password is incorrect.
   * @throws HopException
   *           in case there is a general unexpected error OR if we're already connected to the repository.
   */
  public void connect( String username, String password ) throws HopException, HopSecurityException;

  /**
   * Disconnect from the repository.
   */
  public void disconnect();

  public boolean isConnected();

  /**
   * Initialize the repository with the repository metadata and user information.
   * */
  public void init( RepositoryMeta repositoryMeta );

  // Common methods...

  /**
   * See if a repository object exists in the repository
   *
   * @param name
   *          the name of the object
   * @param repositoryDirectory
   *          the directory in which it should reside
   * @param objectType
   *          the type of repository object
   * @return true if the job exists
   * @throws HopException
   *           in case something goes wrong.
   */
  public boolean exists( String name, RepositoryDirectoryInterface repositoryDirectory,
    RepositoryObjectType objectType ) throws HopException;

  public ObjectId getTransformationID( String name, RepositoryDirectoryInterface repositoryDirectory ) throws HopException;

  public ObjectId getJobId( String name, RepositoryDirectoryInterface repositoryDirectory ) throws HopException;

  public void save( RepositoryElementInterface repositoryElement, String versionComment,
    ProgressMonitorListener monitor ) throws HopException;

  /**
   * Save an object to the repository optionally overwriting any associated objects.
   *
   * @param repositoryElement
   *          Object to save
   * @param versionComment
   *          Version comment for update
   * @param monitor
   *          Progress Monitor to report feedback to
   * @param overwrite
   *          Overwrite any existing objects involved in saving {@code repositoryElement}, e.g. repositoryElement,
   *          database connections, slave servers
   * @throws HopException
   *           Error saving the object to the repository
   */
  public void save( RepositoryElementInterface repositoryElement, String versionComment,
    ProgressMonitorListener monitor, boolean overwrite ) throws HopException;

  /**
   * Save the object to the repository with version comments as well as version dates. This form exists largely to
   * support the importing of revisions, preserving their revision date.
   *
   * @param repositoryElement
   * @param versionComment
   * @param versionDate
   * @param monitor
   * @param overwrite
   * @throws HopException
   */
  public void save( RepositoryElementInterface repositoryElement, String versionComment, Calendar versionDate,
    ProgressMonitorListener monitor, boolean overwrite ) throws HopException;

  public RepositoryDirectoryInterface getDefaultSaveDirectory( RepositoryElementInterface repositoryElement ) throws HopException;

  public RepositoryDirectoryInterface getUserHomeDirectory() throws HopException;

  /**
   * Clear the shared object cache, if applicable.
   */
  public void clearSharedObjectCache();

  // Transformations : Loading & saving objects...

  /**
   * Load a transformation with a name from a folder in the repository
   *
   * @param transname
   *          the name of the transformation to load
   * @param The
   *          folder to load it from
   * @param monitor
   *          the progress monitor to use (UI feedback)
   * @param setInternalVariables
   *          set to true if you want to automatically set the internal variables of the loaded transformation. (true is
   *          the default with very few exceptions!)
   * @param revision
   *          the revision to load. Specify null to load the last version.
   */
  public TransMeta loadTransformation( String transname, RepositoryDirectoryInterface repdir,
    ProgressMonitorListener monitor, boolean setInternalVariables, String revision ) throws HopException;

  /**
   * Load a transformation by id
   *
   * @param id_transformation
   *          the id of the transformation to load
   * @param versionLabel
   *          version to load. Specify null to load the last version.
   */
  public TransMeta loadTransformation( ObjectId id_transformation, String versionLabel ) throws HopException;

  public SharedObjects readTransSharedObjects( TransMeta transMeta ) throws HopException;

  /**
   * Move / rename a transformation
   *
   * @param id_transformation
   *          The ObjectId of the transformation to move
   * @param newDirectory
   *          The RepositoryDirectoryInterface that will be the new parent of the transformation (May be null if a move
   *          is not desired)
   * @param newName
   *          The new name of the transformation (May be null if a rename is not desired)
   * @return The ObjectId of the transformation that was moved
   * @throws HopException
   */
  public ObjectId renameTransformation( ObjectId id_transformation, RepositoryDirectoryInterface newDirectory,
    String newName ) throws HopException;

  /**
   * Move / rename a transformation
   *
   * @param id_transformation
   *          The ObjectId of the transformation to move
   * @param versionComment
   *          Version comment for rename
   * @param newDirectory
   *          The RepositoryDirectoryInterface that will be the new parent of the transformation (May be null if a move
   *          is not desired)
   * @param newName
   *          The new name of the transformation (May be null if a rename is not desired)
   * @return The ObjectId of the transformation that was moved
   * @throws HopException
   */
  public ObjectId renameTransformation( ObjectId id_transformation, String versionComment,
    RepositoryDirectoryInterface newDirectory, String newName ) throws HopException;

  /**
   * Delete everything related to a transformation from the repository. This does not included shared objects :
   * databases, slave servers, cluster and partition schema.
   *
   * @param id_transformation
   *          the transformation id to delete
   * @throws HopException
   */
  public void deleteTransformation( ObjectId id_transformation ) throws HopException;

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  // Jobs: Loading & saving objects...
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Load a job from the repository
   *
   * @param jobname
   *          the name
   * @param repdir
   *          the directory
   * @param monitor
   *          the progress monitor or null
   * @param revision
   *          the revision to load. Specify null to load the last version.
   */
  public JobMeta loadJob( String jobname, RepositoryDirectoryInterface repdir, ProgressMonitorListener monitor,
    String revision ) throws HopException;

  /**
   * Load a job from the repository by id
   *
   * @param id_job
   *          the id of the job
   * @param versionLabel
   *          version to load. Specify null to load the last version.
   */
  public JobMeta loadJob( ObjectId id_job, String versionLabel ) throws HopException;

  public SharedObjects readJobMetaSharedObjects( JobMeta jobMeta ) throws HopException;

  /**
   * Move / rename a job
   *
   * @param id_job
   *          The ObjectId of the job to move
   * @param versionComment
   *          Version comment for rename
   * @param newDirectory
   *          The RepositoryDirectoryInterface that will be the new parent of the job (May be null if a move is not
   *          desired)
   * @param newName
   *          The new name of the job (May be null if a rename is not desired)
   * @return The ObjectId of the job that was moved
   * @throws HopException
   */
  public ObjectId renameJob( ObjectId id_job, String versionComment, RepositoryDirectoryInterface newDirectory,
    String newName ) throws HopException;

  /**
   * Move / rename a job
   *
   * @param id_job
   *          The ObjectId of the job to move
   * @param newDirectory
   *          The RepositoryDirectoryInterface that will be the new parent of the job (May be null if a move is not
   *          desired)
   * @param newName
   *          The new name of the job (May be null if a rename is not desired)
   * @return The ObjectId of the job that was moved
   * @throws HopException
   */
  public ObjectId renameJob( ObjectId id_job, RepositoryDirectoryInterface newDirectory, String newName ) throws HopException;

  public void deleteJob( ObjectId id_job ) throws HopException;

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  // Databases : loading, saving, renaming, etc.
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Load the Database connection Metadata from the repository
   *
   * @param id_database
   *          the id of the database connection to load
   * @param revision
   *          the revision to load. Specify null to load the last version.
   * @throws HopException
   *           in case something goes wrong with database, connection, etc.
   */
  public DatabaseMeta loadDatabaseMeta( ObjectId id_database, String revision ) throws HopException;

  /**
   * Remove a database connection from the repository
   *
   * @param databaseName
   *          The name of the connection to remove
   * @throws HopException
   *           In case something went wrong: database error, insufficient permissions, depending objects, etc.
   */
  public void deleteDatabaseMeta( String databaseName ) throws HopException;

  public ObjectId[] getDatabaseIDs( boolean includeDeleted ) throws HopException;

  public String[] getDatabaseNames( boolean includeDeleted ) throws HopException;

  /**
   * Read all the databases defined in the repository
   *
   * @return a list of all the databases defined in the repository
   * @throws HopException
   */
  public List<DatabaseMeta> readDatabases() throws HopException;

  public ObjectId getDatabaseID( String name ) throws HopException;

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  // ClusterSchema
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  public ClusterSchema loadClusterSchema( ObjectId id_cluster_schema, List<SlaveServer> slaveServers,
    String versionLabel ) throws HopException;

  public ObjectId[] getClusterIDs( boolean includeDeleted ) throws HopException;

  public String[] getClusterNames( boolean includeDeleted ) throws HopException;

  public ObjectId getClusterID( String name ) throws HopException;

  public void deleteClusterSchema( ObjectId id_cluster ) throws HopException;

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  // SlaveServer
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  public SlaveServer loadSlaveServer( ObjectId id_slave_server, String versionLabel ) throws HopException;

  public ObjectId[] getSlaveIDs( boolean includeDeleted ) throws HopException;

  public String[] getSlaveNames( boolean includeDeleted ) throws HopException;

  /**
   * @return a list of all the slave servers in the repository.
   * @throws HopException
   */
  public List<SlaveServer> getSlaveServers() throws HopException;

  public ObjectId getSlaveID( String name ) throws HopException;

  public void deleteSlave( ObjectId id_slave ) throws HopException;

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  // PartitionSchema
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  public PartitionSchema loadPartitionSchema( ObjectId id_partition_schema, String versionLabel ) throws HopException;

  public ObjectId[] getPartitionSchemaIDs( boolean includeDeleted ) throws HopException;

  // public ObjectId[] getPartitionIDs(ObjectId id_partition_schema) throws HopException;

  public String[] getPartitionSchemaNames( boolean includeDeleted ) throws HopException;

  public ObjectId getPartitionSchemaID( String name ) throws HopException;

  public void deletePartitionSchema( ObjectId id_partition_schema ) throws HopException;

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  // Directory stuff
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  public RepositoryDirectoryInterface loadRepositoryDirectoryTree() throws HopException;

  public RepositoryDirectoryInterface findDirectory( String directory ) throws HopException;

  public RepositoryDirectoryInterface findDirectory( ObjectId directory ) throws HopException;

  public void saveRepositoryDirectory( RepositoryDirectoryInterface dir ) throws HopException;

  public void deleteRepositoryDirectory( RepositoryDirectoryInterface dir ) throws HopException;

  /**
   * Move / rename a repository directory
   *
   * @param id
   *          The ObjectId of the repository directory to move
   * @param newParentDir
   *          The RepositoryDirectoryInterface that will be the new parent of the repository directory (May be null if a
   *          move is not desired)
   * @param newName
   *          The new name of the repository directory (May be null if a rename is not desired)
   * @return The ObjectId of the repository directory that was moved
   * @throws HopException
   */
  public ObjectId renameRepositoryDirectory( ObjectId id, RepositoryDirectoryInterface newParentDir, String newName ) throws HopException;

  /**
   * Create a new directory, possibly by creating several sub-directies of / at the same time.
   *
   * @param parentDirectory
   *          the parent directory
   * @param directoryPath
   *          The path to the new Repository Directory, to be created.
   * @return The created sub-directory
   * @throws HopException
   *           In case something goes wrong
   */
  public RepositoryDirectoryInterface createRepositoryDirectory( RepositoryDirectoryInterface parentDirectory,
    String directoryPath ) throws HopException;

  public String[] getTransformationNames( ObjectId id_directory, boolean includeDeleted ) throws HopException;

  public List<RepositoryElementMetaInterface> getJobObjects( ObjectId id_directory, boolean includeDeleted ) throws HopException;

  public List<RepositoryElementMetaInterface> getTransformationObjects( ObjectId id_directory,
    boolean includeDeleted ) throws HopException;

  /**
   * Gets all job and transformation objects in the given directory. (Combines {@link #getJobObjects(ObjectId, boolean)}
   * and {@link #getTransformationObjects(ObjectId, boolean)} into one operation.
   *
   * @param id_directory
   *          directory
   * @param includeDeleted
   *          true to return deleted objects
   * @return list of repository objects
   * @throws HopException
   *           In case something goes wrong
   */
  public List<RepositoryElementMetaInterface> getJobAndTransformationObjects( ObjectId id_directory,
    boolean includeDeleted ) throws HopException;

  public String[] getJobNames( ObjectId id_directory, boolean includeDeleted ) throws HopException;

  /**
   * Returns the child directory names of a parent directory
   *
   * @param id_directory
   *          parent directory id
   * @return array of child names
   * @throws HopException
   */
  public String[] getDirectoryNames( ObjectId id_directory ) throws HopException;

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  // Logging...
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Insert an entry in the audit trail of the repository. This is an optional operation and depends on the capabilities
   * of the underlying repository.
   *
   * @param The
   *          description to be put in the audit trail of the repository.
   */
  public ObjectId insertLogEntry( String description ) throws HopException;

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  // Relationships between objects !!!!!!!!!!!!!!!!!!!!!! <-----------------
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  public void insertStepDatabase( ObjectId id_transformation, ObjectId id_step, ObjectId id_database ) throws HopException;

  public void insertJobEntryDatabase( ObjectId id_job, ObjectId id_jobentry, ObjectId id_database ) throws HopException;

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  // Condition
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * This method saves the object ID of the condition object (if not null) in the step attributes
   *
   * @param id_step
   * @param code
   * @param condition
   */
  public void saveConditionStepAttribute( ObjectId id_transformation, ObjectId id_step, String code,
    Condition condition ) throws HopException;

  /**
   * Load a condition from the repository with the Object ID stored in a step attribute.
   *
   * @param id_step
   * @param code
   * @return
   * @throws HopException
   */
  public Condition loadConditionFromStepAttribute( ObjectId id_step, String code ) throws HopException;

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  // Attributes for steps...
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  public boolean getStepAttributeBoolean( ObjectId id_step, int nr, String code, boolean def ) throws HopException;

  public boolean getStepAttributeBoolean( ObjectId id_step, int nr, String code ) throws HopException;

  public boolean getStepAttributeBoolean( ObjectId id_step, String code ) throws HopException;

  public long getStepAttributeInteger( ObjectId id_step, int nr, String code ) throws HopException;

  public long getStepAttributeInteger( ObjectId id_step, String code ) throws HopException;

  public String getStepAttributeString( ObjectId id_step, int nr, String code ) throws HopException;

  public String getStepAttributeString( ObjectId id_step, String code ) throws HopException;

  public void saveStepAttribute( ObjectId id_transformation, ObjectId id_step, int nr, String code, String value ) throws HopException;

  public void saveStepAttribute( ObjectId id_transformation, ObjectId id_step, String code, String value ) throws HopException;

  public void saveStepAttribute( ObjectId id_transformation, ObjectId id_step, int nr, String code, boolean value ) throws HopException;

  public void saveStepAttribute( ObjectId id_transformation, ObjectId id_step, String code, boolean value ) throws HopException;

  public void saveStepAttribute( ObjectId id_transformation, ObjectId id_step, int nr, String code, long value ) throws HopException;

  public void saveStepAttribute( ObjectId id_transformation, ObjectId id_step, String code, long value ) throws HopException;

  public void saveStepAttribute( ObjectId id_transformation, ObjectId id_step, int nr, String code, double value ) throws HopException;

  public void saveStepAttribute( ObjectId id_transformation, ObjectId id_step, String code, double value ) throws HopException;

  public int countNrStepAttributes( ObjectId id_step, String code ) throws HopException;

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  // Attributes for job entries...
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  public int countNrJobEntryAttributes( ObjectId id_jobentry, String code ) throws HopException;

  public boolean getJobEntryAttributeBoolean( ObjectId id_jobentry, String code ) throws HopException;

  public boolean getJobEntryAttributeBoolean( ObjectId id_jobentry, int nr, String code ) throws HopException;

  public boolean getJobEntryAttributeBoolean( ObjectId id_jobentry, String code, boolean def ) throws HopException;

  public long getJobEntryAttributeInteger( ObjectId id_jobentry, String code ) throws HopException;

  public long getJobEntryAttributeInteger( ObjectId id_jobentry, int nr, String code ) throws HopException;

  public String getJobEntryAttributeString( ObjectId id_jobentry, String code ) throws HopException;

  public String getJobEntryAttributeString( ObjectId id_jobentry, int nr, String code ) throws HopException;

  public void saveJobEntryAttribute( ObjectId id_job, ObjectId id_jobentry, int nr, String code, String value ) throws HopException;

  public void saveJobEntryAttribute( ObjectId id_job, ObjectId id_jobentry, String code, String value ) throws HopException;

  public void saveJobEntryAttribute( ObjectId id_job, ObjectId id_jobentry, int nr, String code, boolean value ) throws HopException;

  public void saveJobEntryAttribute( ObjectId id_job, ObjectId id_jobentry, String code, boolean value ) throws HopException;

  public void saveJobEntryAttribute( ObjectId id_job, ObjectId id_jobentry, int nr, String code, long value ) throws HopException;

  public void saveJobEntryAttribute( ObjectId id_job, ObjectId id_jobentry, String code, long value ) throws HopException;

  /**
   * This method is introduced to avoid having to go over an integer/string/whatever in the interface and the step code.
   *
   * @param id_step
   * @param code
   * @return
   * @throws HopException
   */
  public DatabaseMeta loadDatabaseMetaFromStepAttribute( ObjectId id_step, String code,
    List<DatabaseMeta> databases ) throws HopException;

  /**
   * This method saves the object ID of the database object (if not null) in the step attributes
   *
   * @param id_transformation
   * @param id_step
   * @param code
   * @param database
   */
  public void saveDatabaseMetaStepAttribute( ObjectId id_transformation, ObjectId id_step, String code,
    DatabaseMeta database ) throws HopException;

  /**
   * This method is introduced to avoid having to go over an integer/string/whatever in the interface and the job entry
   * code.
   *
   * @param id_step
   * @param code
   * @return
   * @throws HopException
   */
  public DatabaseMeta loadDatabaseMetaFromJobEntryAttribute( ObjectId id_jobentry, String nameCode, String idCode,
    List<DatabaseMeta> databases ) throws HopException;

  /**
   * This method is introduced to avoid having to go over an integer/string/whatever in the interface and the job entry
   * code.
   *
   * @param id_entry
   * @param nameCode
   * @param nr
   * @param idcode
   * @param databases
   * @return
   * @throws HopException
   */
  public DatabaseMeta loadDatabaseMetaFromJobEntryAttribute( ObjectId id_jobentry, String nameCode, int nr,
    String idCode, List<DatabaseMeta> databases ) throws HopException;

  /**
   * This method saves the object ID of the database object (if not null) in the job entry attributes
   *
   * @param id_job
   * @param id_jobentry
   * @param idCode
   * @param database
   */
  public void saveDatabaseMetaJobEntryAttribute( ObjectId id_job, ObjectId id_jobentry, String nameCode,
    String idCode, DatabaseMeta database ) throws HopException;

  /**
   * This method saves the object ID of the database object (if not null) in the job entry attributes
   *
   * @param id_job
   * @param id_jobentry
   * @param nr
   * @param code
   * @param database
   */
  public void saveDatabaseMetaJobEntryAttribute( ObjectId id_job, ObjectId id_jobentry, int nr, String nameCode,
    String idCode, DatabaseMeta database ) throws HopException;

  /**
   * Removes he deleted flag from a repository element in the repository. If it wasn't deleted, it remains untouched.
   *
   * @param element
   *          the repository element to restore
   * @throws HopException
   *           get throws in case something goes horribly wrong.
   */
  public void undeleteObject( RepositoryElementMetaInterface repositoryObject ) throws HopException;

  /**
   * Retrieves the current list of of IRepository Services.
   *
   * @return List of repository services
   * @throws HopException
   *           in case something goes horribly wrong.
   */
  public List<Class<? extends IRepositoryService>> getServiceInterfaces() throws HopException;

  /**
   * Retrieves a given repository service
   *
   * @param service
   *          class name
   * @return repository service
   *
   * @throws HopException
   *           in case something goes horribly wrong.
   */
  public IRepositoryService getService( Class<? extends IRepositoryService> clazz ) throws HopException;

  /**
   * Checks whether a given repository service is available or not
   *
   * @param repository
   *          service class that needs to be checked for support
   * @throws HopException
   *           in case something goes horribly wrong.
   */
  public boolean hasService( Class<? extends IRepositoryService> clazz ) throws HopException;

  /**
   * Get more information about a certain object ID in the form of the RepositoryObject
   *
   * @param objectId
   *          The ID of the object to get more information about.
   * @param objectType
   *          The type of object to investigate.
   * @return The repository object or null if nothing could be found.
   * @throws HopException
   *           In case there was a loading problem.
   */
  public RepositoryObject getObjectInformation( ObjectId objectId, RepositoryObjectType objectType ) throws HopException;

  /**
   * This is an informational message that a repository can display on connecting within Spoon. If a null is returned,
   * no message is displayed to the end user.
   *
   * @return message
   */
  public String getConnectMessage();

  /**
   * Get the repository version.
   *
   * @return The repository version as a string
   */
  public String[] getJobsUsingDatabase( ObjectId id_database ) throws HopException;

  public String[] getTransformationsUsingDatabase( ObjectId id_database ) throws HopException;

  /**
   * @return the importer that will handle imports into this repository
   */
  public IRepositoryImporter getImporter();

  /**
   * @return the exporter that will handle exports from this repository
   */
  public IRepositoryExporter getExporter() throws HopException;

  /**
   * @return the Metastore that is implemented in this Repository. Return null if this repository doesn't implement a
   *         Metastore.
   */
  public IMetaStore getMetaStore();

}
