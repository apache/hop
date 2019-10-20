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

package org.apache.hop.trans.steps.loadsave;

import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
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
import org.apache.hop.repository.AbstractRepository;
import org.apache.hop.repository.IRepositoryExporter;
import org.apache.hop.repository.IRepositoryImporter;
import org.apache.hop.repository.IRepositoryService;
import org.apache.hop.repository.IUser;
import org.apache.hop.repository.LongObjectId;
import org.apache.hop.repository.ObjectId;
import org.apache.hop.repository.RepositoryDirectoryInterface;
import org.apache.hop.repository.RepositoryElementInterface;
import org.apache.hop.repository.RepositoryElementMetaInterface;
import org.apache.hop.repository.RepositoryMeta;
import org.apache.hop.repository.RepositoryObject;
import org.apache.hop.repository.RepositoryObjectType;
import org.apache.hop.repository.RepositorySecurityManager;
import org.apache.hop.repository.RepositorySecurityProvider;
import org.apache.hop.repository.StringObjectId;
import org.apache.hop.shared.SharedObjects;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.metastore.api.IMetaStore;

public class MemoryRepository extends AbstractRepository {
  private final Map<ObjectId, Map<Integer, Map<String, String>>> stepAttributeMap =
      new HashMap<ObjectId, Map<Integer, Map<String, String>>>();
  private final Map<ObjectId, Map<Integer, Map<String, String>>> jobAttributeMap =
      new HashMap<ObjectId, Map<Integer, Map<String, String>>>();

  public MemoryRepository() {

  }

  private void populateMap( Map<ObjectId, Map<Integer, Map<String, String>>> attributeMap, JSONObject jsonObject ) {
    for ( Object objectId : jsonObject.keySet() ) {
      JSONObject nrsObject = (JSONObject) jsonObject.get( objectId );
      for ( Object nrKey : nrsObject.keySet() ) {
        JSONObject nrObject = (JSONObject) nrsObject.get( nrKey );
        for ( Object stringKey : nrObject.keySet() ) {
          setAttribute( attributeMap, new StringObjectId( objectId.toString() ), Integer.valueOf( nrKey.toString() ),
              stringKey.toString(), nrObject.get( stringKey ).toString() );
        }
      }
    }
  }

  public MemoryRepository( String json ) throws ParseException {
    Object repoObj = new JSONParser().parse( json );
    JSONObject jsonRepoObj = (JSONObject) repoObj;
    populateMap( stepAttributeMap, (JSONObject) jsonRepoObj.get( "step" ) );
    populateMap( jobAttributeMap, (JSONObject) jsonRepoObj.get( "job" ) );
  }

  private String getAttribute( Map<ObjectId, Map<Integer, Map<String, String>>> attributeMap, ObjectId id, int nr,
      String code, String def ) {
    String value = null;
    Map<Integer, Map<String, String>> stepMap = attributeMap.get( id );
    if ( stepMap != null ) {
      Map<String, String> numberMap = stepMap.get( nr );
      if ( numberMap != null ) {
        value = numberMap.get( code );
      }
    }
    return value == null ? def : value;
  }

  private void setAttribute( Map<ObjectId, Map<Integer, Map<String, String>>> attributeMap, ObjectId id, int nr,
      String code, String value ) {
    Map<Integer, Map<String, String>> stepMap = attributeMap.get( id );
    if ( stepMap == null ) {
      stepMap = new HashMap<Integer, Map<String, String>>();
      attributeMap.put( id, stepMap );
    }
    Map<String, String> numberMap = stepMap.get( nr );
    if ( numberMap == null ) {
      numberMap = new HashMap<String, String>();
      stepMap.put( nr, numberMap );
    }
    if ( numberMap.containsKey( code ) ) {
      // PDI-15793
      throw new RuntimeException(
        "Tried to insert code [" + code + "] twice, which may not be supported by all repository types." );
    }
    numberMap.put( code, value );
  }

  private String getStepAttribute( ObjectId id_step, int nr, String code, String def ) {
    return getAttribute( stepAttributeMap, id_step, nr, code, def );
  }

  private void setStepAttribute( ObjectId id_transformation, ObjectId id_step, int nr, String code, String value ) {
    setAttribute( stepAttributeMap, id_step, nr, code, value );
  }

  private String getJobAttribute( ObjectId id_job, int nr, String code, String def ) {
    return getAttribute( jobAttributeMap, id_job, nr, code, def );
  }

  private void setJobAttribute( ObjectId id_job, int nr, String code, String value ) {
    setAttribute( jobAttributeMap, id_job, nr, code, value );
  }

  @Override
  public String getName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getVersion() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RepositoryMeta getRepositoryMeta() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public IUser getUserInfo() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RepositorySecurityProvider getSecurityProvider() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RepositorySecurityManager getSecurityManager() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public LogChannelInterface getLog() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void connect( String username, String password ) throws HopException, HopSecurityException {
    // TODO Auto-generated method stub

  }

  @Override
  public void disconnect() {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean isConnected() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void init( RepositoryMeta repositoryMeta ) {
    // TODO Auto-generated method stub
  }

  @Override
  public boolean
    exists( String name, RepositoryDirectoryInterface repositoryDirectory, RepositoryObjectType objectType ) throws HopException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public ObjectId getTransformationID( String name, RepositoryDirectoryInterface repositoryDirectory )
    throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ObjectId getJobId( String name, RepositoryDirectoryInterface repositoryDirectory ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void save( RepositoryElementInterface repositoryElement, String versionComment,
      ProgressMonitorListener monitor, boolean overwrite ) throws HopException {
    // TODO Auto-generated method stub

  }

  @Override
  public void save( RepositoryElementInterface repositoryElement, String versionComment, Calendar versionDate,
      ProgressMonitorListener monitor, boolean overwrite ) throws HopException {
    // TODO Auto-generated method stub

  }

  @Override
  public RepositoryDirectoryInterface getDefaultSaveDirectory( RepositoryElementInterface repositoryElement )
    throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RepositoryDirectoryInterface getUserHomeDirectory() throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void clearSharedObjectCache() {
    // TODO Auto-generated method stub

  }

  @Override
  public TransMeta loadTransformation( String transname, RepositoryDirectoryInterface repdir,
      ProgressMonitorListener monitor, boolean setInternalVariables, String revision ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TransMeta loadTransformation( ObjectId id_transformation, String versionLabel ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SharedObjects readTransSharedObjects( TransMeta transMeta ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ObjectId renameTransformation( ObjectId id_transformation, RepositoryDirectoryInterface newDirectory,
      String newName ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ObjectId renameTransformation( ObjectId id_transformation, String versionComment,
    RepositoryDirectoryInterface newDirectory, String newName ) throws HopException {
    return null;
  }

  @Override
  public void deleteTransformation( ObjectId id_transformation ) throws HopException {
    // TODO Auto-generated method stub

  }

  @Override
  public JobMeta loadJob( String jobname, RepositoryDirectoryInterface repdir, ProgressMonitorListener monitor,
      String revision ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public JobMeta loadJob( ObjectId id_job, String versionLabel ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SharedObjects readJobMetaSharedObjects( JobMeta jobMeta ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ObjectId renameJob( ObjectId id_job, RepositoryDirectoryInterface newDirectory, String newName )
    throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ObjectId renameJob( ObjectId id_job, String versionComment, RepositoryDirectoryInterface newDirectory,
    String newName ) throws HopException {
    return null;
  }

  @Override
  public void deleteJob( ObjectId id_job ) throws HopException {
    // TODO Auto-generated method stub

  }

  @Override
  public DatabaseMeta loadDatabaseMeta( ObjectId id_database, String revision ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void deleteDatabaseMeta( String databaseName ) throws HopException {
    // TODO Auto-generated method stub

  }

  @Override
  public ObjectId[] getDatabaseIDs( boolean includeDeleted ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String[] getDatabaseNames( boolean includeDeleted ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<DatabaseMeta> readDatabases() throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ObjectId getDatabaseID( String name ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ClusterSchema loadClusterSchema( ObjectId id_cluster_schema, List<SlaveServer> slaveServers,
      String versionLabel ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ObjectId[] getClusterIDs( boolean includeDeleted ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String[] getClusterNames( boolean includeDeleted ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ObjectId getClusterID( String name ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void deleteClusterSchema( ObjectId id_cluster ) throws HopException {
    // TODO Auto-generated method stub

  }

  @Override
  public SlaveServer loadSlaveServer( ObjectId id_slave_server, String versionLabel ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ObjectId[] getSlaveIDs( boolean includeDeleted ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String[] getSlaveNames( boolean includeDeleted ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<SlaveServer> getSlaveServers() throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ObjectId getSlaveID( String name ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void deleteSlave( ObjectId id_slave ) throws HopException {
    // TODO Auto-generated method stub

  }

  @Override
  public PartitionSchema loadPartitionSchema( ObjectId id_partition_schema, String versionLabel )
    throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ObjectId[] getPartitionSchemaIDs( boolean includeDeleted ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String[] getPartitionSchemaNames( boolean includeDeleted ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ObjectId getPartitionSchemaID( String name ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void deletePartitionSchema( ObjectId id_partition_schema ) throws HopException {
    // TODO Auto-generated method stub

  }

  @Override
  public RepositoryDirectoryInterface loadRepositoryDirectoryTree() throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RepositoryDirectoryInterface findDirectory( String directory ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RepositoryDirectoryInterface findDirectory( ObjectId directory ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void saveRepositoryDirectory( RepositoryDirectoryInterface dir ) throws HopException {
    // TODO Auto-generated method stub

  }

  @Override
  public void deleteRepositoryDirectory( RepositoryDirectoryInterface dir ) throws HopException {
    // TODO Auto-generated method stub

  }

  @Override
  public ObjectId renameRepositoryDirectory( ObjectId id, RepositoryDirectoryInterface newParentDir, String newName )
    throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RepositoryDirectoryInterface createRepositoryDirectory( RepositoryDirectoryInterface parentDirectory,
      String directoryPath ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String[] getTransformationNames( ObjectId id_directory, boolean includeDeleted ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<RepositoryElementMetaInterface> getJobObjects( ObjectId id_directory, boolean includeDeleted )
    throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<RepositoryElementMetaInterface> getTransformationObjects( ObjectId id_directory, boolean includeDeleted )
    throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<RepositoryElementMetaInterface> getJobAndTransformationObjects( ObjectId id_directory,
      boolean includeDeleted ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String[] getJobNames( ObjectId id_directory, boolean includeDeleted ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String[] getDirectoryNames( ObjectId id_directory ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ObjectId insertLogEntry( String description ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void insertStepDatabase( ObjectId id_transformation, ObjectId id_step, ObjectId id_database )
    throws HopException {
    // TODO Auto-generated method stub

  }

  @Override
  public void insertJobEntryDatabase( ObjectId id_job, ObjectId id_jobentry, ObjectId id_database )
    throws HopException {
    // TODO Auto-generated method stub

  }

  @Override
  public void saveConditionStepAttribute( ObjectId id_transformation, ObjectId id_step, String code, Condition condition )
    throws HopException {
    this.saveStepAttribute( id_transformation, id_step, code, condition.getXML() );
  }

  @Override
  public Condition loadConditionFromStepAttribute( ObjectId id_step, String code ) throws HopException {
    // TODO Auto-generated method stub
    String tmp = this.getStepAttributeString( id_step, code );
    return new Condition( tmp );
  }

  @Override
  public boolean getStepAttributeBoolean( ObjectId id_step, int nr, String code, boolean def ) throws HopException {
    return "Y".equalsIgnoreCase( getStepAttribute( id_step, nr, code, def ? "Y" : "N" ) );
  }

  @Override
  public long getStepAttributeInteger( ObjectId id_step, int nr, String code ) throws HopException {
    return Long.valueOf( getStepAttribute( id_step, nr, code, "0" ) );
  }

  @Override
  public String getStepAttributeString( ObjectId id_step, int nr, String code ) throws HopException {
    return getStepAttribute( id_step, nr, code, null );
  }

  @Override
  public void saveStepAttribute( ObjectId id_transformation, ObjectId id_step, int nr, String code, String value )
    throws HopException {
    setStepAttribute( id_transformation, id_step, nr, code, value );
  }

  @Override
  public void saveStepAttribute( ObjectId id_transformation, ObjectId id_step, int nr, String code, boolean value )
    throws HopException {
    setStepAttribute( id_transformation, id_step, nr, code, value ? "Y" : "N" );
  }

  @Override
  public void saveStepAttribute( ObjectId id_transformation, ObjectId id_step, int nr, String code, long value )
    throws HopException {
    setStepAttribute( id_transformation, id_step, nr, code, Long.toString( value ) );
  }

  @Override
  public void saveStepAttribute( ObjectId id_transformation, ObjectId id_step, int nr, String code, double value )
    throws HopException {
    setStepAttribute( id_transformation, id_step, nr, code, Double.toString( value ) );
  }

  @Override
  public int countNrStepAttributes( ObjectId id_step, String code ) throws HopException {
    Map<Integer, Map<String, String>> stepMap = stepAttributeMap.get( id_step );
    int count = 0;
    if ( stepMap != null ) {
      for ( Entry<Integer, Map<String, String>> entry : stepMap.entrySet() ) {
        Map<String, String> value = entry.getValue();
        if ( value != null && value.get( code ) != null ) {
          count++;
        }
      }
    }

    return count;
  }

  @Override
  public int countNrJobEntryAttributes( ObjectId id_jobentry, String code ) throws HopException {
    Map<Integer, Map<String, String>> jobMap = jobAttributeMap.get( id_jobentry );
    int count = 0;
    if  ( jobMap != null ) {
      for ( Entry<Integer, Map<String, String>> entry : jobMap.entrySet() ) {
        Map<String, String> value = entry.getValue();
        if ( value != null && value.get( code ) != null ) {
          count++;
        }
      }
    }

    return count;
  }

  @Override
  public long getJobEntryAttributeInteger( ObjectId id_jobentry, int nr, String code ) throws HopException {
    return Long.parseLong( getJobAttribute( id_jobentry, nr, code, "0" ) );
  }

  @Override
  public String getJobEntryAttributeString( ObjectId id_jobentry, int nr, String code ) throws HopException {
    return getJobAttribute( id_jobentry, nr, code, null );
  }

  @Override
  public void saveJobEntryAttribute( ObjectId id_job, ObjectId id_jobentry, int nr, String code, String value )
    throws HopException {
    setJobAttribute( id_jobentry, nr, code, value );
  }

  @Override
  public void saveJobEntryAttribute( ObjectId id_job, ObjectId id_jobentry, int nr, String code, boolean value )
    throws HopException {
    setJobAttribute( id_jobentry, nr, code, value ? "Y" : "N" );
  }

  @Override
  public void saveJobEntryAttribute( ObjectId id_job, ObjectId id_jobentry, int nr, String code, long value )
    throws HopException {
    setJobAttribute( id_jobentry, nr, code, Long.toString( value ) );
  }

  @Override
  public DatabaseMeta loadDatabaseMetaFromStepAttribute( ObjectId id_step, String code, List<DatabaseMeta> databases )
    throws HopException {
    long id_database = getStepAttributeInteger( id_step, code );
    if ( id_database <= 0 ) {
      return null;
    }
    return DatabaseMeta.findDatabase( databases, new LongObjectId( id_database ) );
  }

  @Override
  public void saveDatabaseMetaStepAttribute( ObjectId id_transformation, ObjectId id_step, String code,
      DatabaseMeta database ) throws HopException {
    ObjectId id = null;
    if ( database != null ) {
      id = database.getObjectId();
      Long id_database = id == null ? Long.valueOf( -1L ) : new LongObjectId( id ).longValue();
      saveStepAttribute( id_transformation, id_step, code, id_database );
    }
  }

  @Override
  public DatabaseMeta loadDatabaseMetaFromJobEntryAttribute( ObjectId id_jobentry, String nameCode, int nr,
      String idCode, List<DatabaseMeta> databases ) throws HopException {
    long id_database = getJobEntryAttributeInteger( id_jobentry, nr, idCode );
    if ( id_database <= 0 ) {
      String name = getJobEntryAttributeString( id_jobentry, nr, nameCode );
      if ( name == null ) {
        return null;
      }
      return DatabaseMeta.findDatabase( databases, name );
    }
    return DatabaseMeta.findDatabase( databases, new LongObjectId( id_database ) );
  }

  @Override
  public void saveDatabaseMetaJobEntryAttribute( ObjectId id_job, ObjectId id_jobentry, int nr, String nameCode,
      String idCode, DatabaseMeta database ) throws HopException {
    ObjectId id = null;
    if ( database != null ) {
      id = database.getObjectId();
      Long id_database = id == null ? Long.valueOf( -1L ) : new LongObjectId( id ).longValue();

      // Save both the ID and the name of the database connection...
      //
      saveJobEntryAttribute( id_job, id_jobentry, nr, idCode, id_database );
      saveJobEntryAttribute( id_job, id_jobentry, nr, nameCode, id_database );

      insertJobEntryDatabase( id_job, id_jobentry, id );
    }
  }

  @Override
  public void undeleteObject( RepositoryElementMetaInterface repositoryObject ) throws HopException {
    // TODO Auto-generated method stub

  }

  @Override
  public List<Class<? extends IRepositoryService>> getServiceInterfaces() throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public IRepositoryService getService( Class<? extends IRepositoryService> clazz ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean hasService( Class<? extends IRepositoryService> clazz ) throws HopException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public RepositoryObject getObjectInformation( ObjectId objectId, RepositoryObjectType objectType )
    throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getConnectMessage() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String[] getJobsUsingDatabase( ObjectId id_database ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String[] getTransformationsUsingDatabase( ObjectId id_database ) throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public IRepositoryImporter getImporter() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public IRepositoryExporter getExporter() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public IMetaStore getMetaStore() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean getJobEntryAttributeBoolean( ObjectId id_jobentry, int nr, String code, boolean def )
    throws HopException {
    return "Y".equalsIgnoreCase( getJobAttribute( id_jobentry, nr, code, def ? "Y" : "N" ) );
  }
}
