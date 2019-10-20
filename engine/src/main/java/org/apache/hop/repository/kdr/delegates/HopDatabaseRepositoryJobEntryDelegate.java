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

package org.apache.hop.repository.kdr.delegates;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.JobEntryPluginType;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entries.missing.MissingEntry;
import org.apache.hop.job.entry.JobEntryBase;
import org.apache.hop.job.entry.JobEntryCopy;
import org.apache.hop.job.entry.JobEntryInterface;
import org.apache.hop.repository.LongObjectId;
import org.apache.hop.repository.ObjectId;
import org.apache.hop.repository.Repository;
import org.apache.hop.repository.kdr.HopDatabaseRepository;
import org.apache.hop.repository.kdr.delegates.metastore.HopDatabaseRepositoryMetaStore;

public class HopDatabaseRepositoryJobEntryDelegate extends HopDatabaseRepositoryBaseDelegate {
  // private static Class<?> PKG = JobEntryCopy.class; // for i18n purposes, needed by Translator2!!

  public static final String JOBENTRY_ATTRIBUTE_PREFIX = "_ATTR_" + '\t';

  public HopDatabaseRepositoryJobEntryDelegate( HopDatabaseRepository repository ) {
    super( repository );
  }

  public RowMetaAndData getJobEntry( ObjectId id_jobentry ) throws HopException {
    return repository.connectionDelegate.getOneRow(
      quoteTable( HopDatabaseRepository.TABLE_R_JOBENTRY ),
      quote( HopDatabaseRepository.FIELD_JOBENTRY_ID_JOBENTRY ), id_jobentry );
  }

  public RowMetaAndData getJobEntryCopy( ObjectId id_jobentry_copy ) throws HopException {
    return repository.connectionDelegate.getOneRow(
      quoteTable( HopDatabaseRepository.TABLE_R_JOBENTRY_COPY ),
      quote( HopDatabaseRepository.FIELD_JOBENTRY_COPY_ID_JOBENTRY_COPY ), id_jobentry_copy );
  }

  public RowMetaAndData getJobEntryType( ObjectId id_jobentry_type ) throws HopException {
    return repository.connectionDelegate.getOneRow(
      quoteTable( HopDatabaseRepository.TABLE_R_JOBENTRY_TYPE ),
      quote( HopDatabaseRepository.FIELD_JOBENTRY_ID_JOBENTRY_TYPE ), id_jobentry_type );
  }

  public synchronized ObjectId getJobEntryID( String name, ObjectId id_job ) throws HopException {
    return repository.connectionDelegate.getIDWithValue(
      quoteTable( HopDatabaseRepository.TABLE_R_JOBENTRY ),
      quote( HopDatabaseRepository.FIELD_JOBENTRY_ID_JOBENTRY ),
      quote( HopDatabaseRepository.FIELD_JOBENTRY_NAME ), name,
      quote( HopDatabaseRepository.FIELD_JOBENTRY_ID_JOB ), id_job );
  }

  public synchronized ObjectId getJobEntryTypeID( String code ) throws HopException {
    return repository.connectionDelegate.getIDWithValue(
      quoteTable( HopDatabaseRepository.TABLE_R_JOBENTRY_TYPE ),
      quote( HopDatabaseRepository.FIELD_JOBENTRY_TYPE_ID_JOBENTRY_TYPE ),
      quote( HopDatabaseRepository.FIELD_JOBENTRY_TYPE_CODE ), code );
  }

  /**
   * Load the chef graphical entry from repository We load type, name & description if no entry can be found.
   *
   * @param log
   *          the logging channel
   * @param rep
   *          the Repository
   * @param jobId
   *          The job ID
   * @param jobEntryCopyId
   *          The jobentry copy ID
   * @param jobentries
   *          A list with all jobentries
   * @param databases
   *          A list with all defined databases
   */
  public JobEntryCopy loadJobEntryCopy( ObjectId jobId, ObjectId jobEntryCopyId,
    List<JobEntryInterface> jobentries, List<DatabaseMeta> databases, List<SlaveServer> slaveServers, String jobname )
        throws HopException {
    JobEntryCopy jobEntryCopy = new JobEntryCopy();

    try {
      jobEntryCopy.setObjectId( jobEntryCopyId );

      // Handle GUI information: nr, location, ...
      RowMetaAndData r = getJobEntryCopy( jobEntryCopyId );
      if ( r != null ) {
        // These are the jobentry_copy fields...
        //
        ObjectId jobEntryId =
          new LongObjectId( r.getInteger( HopDatabaseRepository.FIELD_JOBENTRY_COPY_ID_JOBENTRY, 0 ) );
        ObjectId jobEntryTypeId =
          new LongObjectId( r.getInteger( HopDatabaseRepository.FIELD_JOBENTRY_COPY_ID_JOBENTRY_TYPE, 0 ) );
        jobEntryCopy.setNr( (int) r.getInteger( HopDatabaseRepository.FIELD_JOBENTRY_COPY_NR, 0 ) );
        int locx = (int) r.getInteger( HopDatabaseRepository.FIELD_JOBENTRY_COPY_GUI_LOCATION_X, 0 );
        int locy = (int) r.getInteger( HopDatabaseRepository.FIELD_JOBENTRY_COPY_GUI_LOCATION_Y, 0 );
        boolean isdrawn = r.getBoolean( HopDatabaseRepository.FIELD_JOBENTRY_COPY_GUI_DRAW, false );
        boolean isparallel = r.getBoolean( HopDatabaseRepository.FIELD_JOBENTRY_COPY_PARALLEL, false );

        // Do we have the jobentry already?
        //
        jobEntryCopy.setEntry( JobMeta.findJobEntry( jobentries, jobEntryId ) );
        if ( jobEntryCopy.getEntry() == null ) {
          // What type of jobentry do we load now?
          // Get the jobentry type code
          //
          RowMetaAndData rt = getJobEntryType( new LongObjectId( jobEntryTypeId ) );
          if ( rt != null ) {
            String jet_code = rt.getString( HopDatabaseRepository.FIELD_JOBENTRY_TYPE_CODE, null );
            JobEntryInterface jobEntry = null;
            PluginRegistry registry = PluginRegistry.getInstance();
            PluginInterface jobPlugin = registry.findPluginWithId( JobEntryPluginType.class, jet_code );
            if ( jobPlugin == null ) {
              jobEntry = new MissingEntry( jobname, jet_code );
            } else {
              jobEntry = (JobEntryInterface) registry.loadClass( jobPlugin );
            }
            if ( jobEntry != null ) {
              jobEntryCopy.setEntry( jobEntry );
              // Load the attributes for that jobentry
              //
              // THIS IS THE PLUGIN/JOB-ENTRY BEING LOADED!
              //

              // If you extended the JobEntryBase class, you're fine.
              // Otherwise you're on your own.
              //
              if ( jobEntry instanceof JobEntryBase ) {
                loadJobEntryBase( (JobEntryBase) jobEntry, jobEntryId, databases, slaveServers );
                ( (JobEntryBase) jobEntry ).setAttributesMap( loadJobEntryAttributesMap( jobId, jobEntryId ) );
              }

              compatibleJobEntryLoadRep( jobEntry, repository, jobEntryTypeId, databases, slaveServers );
              jobEntry.loadRep( repository, repository.metaStore, jobEntryId, databases, slaveServers );

              jobEntryCopy.getEntry().setObjectId( jobEntryId );

              jobentries.add( jobEntryCopy.getEntry() );
            } else {
              throw new HopException( "JobEntryLoader was unable to find Job Entry Plugin with description ["
                + jet_code + "]." );
            }
          } else {
            throw new HopException( "Unable to find Job Entry Type with id="
              + jobEntryTypeId + " in the repository" );
          }
        }

        jobEntryCopy.setLocation( locx, locy );
        jobEntryCopy.setDrawn( isdrawn );
        jobEntryCopy.setLaunchingInParallel( isparallel );

        return jobEntryCopy;
      } else {
        throw new HopException( "Unable to find job entry copy in repository with id_jobentry_copy="
          + jobEntryCopyId );
      }
    } catch ( HopDatabaseException dbe ) {
      throw new HopException( "Unable to load job entry copy from repository with id_jobentry_copy="
        + jobEntryCopyId, dbe );
    }
  }

  @SuppressWarnings( "deprecation" )
  private void compatibleJobEntryLoadRep( JobEntryInterface jobEntry, HopDatabaseRepository repository,
    ObjectId id_jobentry_type, List<DatabaseMeta> databases, List<SlaveServer> slaveServers ) throws HopException {

    jobEntry.loadRep( repository, id_jobentry_type, databases, slaveServers );

  }

  public void saveJobEntryCopy( JobEntryCopy copy, ObjectId id_job, HopDatabaseRepositoryMetaStore metaStore ) throws HopException {
    try {
      JobEntryInterface entry = copy.getEntry();
      /*
       * --1-- Save the JobEntryCopy details... --2-- If we don't find a id_jobentry, save the jobentry (meaning: only
       * once)
       */

      // See if an entry with the same name is already available...
      ObjectId id_jobentry = getJobEntryID( copy.getName(), id_job );
      if ( id_jobentry == null ) {
        insertJobEntry( id_job, (JobEntryBase) entry );

        // THIS IS THE PLUGIN/JOB-ENTRY BEING SAVED!
        //
        entry.saveRep( repository, metaStore, id_job );
        compatibleEntrySaveRep( entry, repository, id_job );

        // Save the attribute groups map
        //
        if ( entry instanceof JobEntryBase ) {
          saveAttributesMap( id_job, copy.getObjectId(), ( (JobEntryBase) entry ).getAttributesMap() );
        }

        id_jobentry = entry.getObjectId();
      }

      // OK, the entry is saved.
      // Get the entry type...
      //
      ObjectId id_jobentry_type = getJobEntryTypeID( entry.getPluginId() );

      // Oops, not found: update the repository!
      if ( id_jobentry_type == null ) {
        repository.updateJobEntryTypes();

        // Try again!
        id_jobentry_type = getJobEntryTypeID( entry.getPluginId() );
      }

      // Save the entry copy..
      //
      copy.setObjectId( insertJobEntryCopy( id_job, id_jobentry, id_jobentry_type, copy.getNr(), copy
        .getLocation().x, copy.getLocation().y, copy.isDrawn(), copy.isLaunchingInParallel() ) );

    } catch ( HopDatabaseException dbe ) {
      throw new HopException( "Unable to save job entry copy to the repository, id_job=" + id_job, dbe );
    }
  }

  @SuppressWarnings( "deprecation" )
  private void compatibleEntrySaveRep( JobEntryInterface entry, Repository repository, ObjectId id_job ) throws HopException {
    entry.saveRep( repository, id_job );
  }

  public synchronized ObjectId insertJobEntry( ObjectId id_job, JobEntryBase jobEntryBase ) throws HopException {
    ObjectId id = repository.connectionDelegate.getNextJobEntryID();

    ObjectId id_jobentry_type = getJobEntryTypeID( jobEntryBase.getPluginId() );

    log.logDebug( "ID_JobEntry_type = " + id_jobentry_type + " for type = [" + jobEntryBase.getPluginId() + "]" );

    RowMetaAndData table = new RowMetaAndData();

    table.addValue( new ValueMetaInteger(
      HopDatabaseRepository.FIELD_JOBENTRY_ID_JOBENTRY ), id );
    table.addValue(
      new ValueMetaInteger( HopDatabaseRepository.FIELD_JOBENTRY_ID_JOB ), id_job );
    table
      .addValue(
        new ValueMetaInteger(
          HopDatabaseRepository.FIELD_JOBENTRY_ID_JOBENTRY_TYPE ),
        id_jobentry_type );
    table.addValue(
      new ValueMetaString( HopDatabaseRepository.FIELD_JOBENTRY_NAME ),
      jobEntryBase.getName() );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_JOBENTRY_DESCRIPTION ), jobEntryBase
      .getDescription() );

    repository.connectionDelegate.getDatabase().prepareInsert(
      table.getRowMeta(), HopDatabaseRepository.TABLE_R_JOBENTRY );
    repository.connectionDelegate.getDatabase().setValuesInsert( table );
    repository.connectionDelegate.getDatabase().insertRow();
    repository.connectionDelegate.getDatabase().closeInsert();

    jobEntryBase.setObjectId( id );

    return id;
  }

  public synchronized ObjectId insertJobEntryCopy( ObjectId id_job, ObjectId id_jobentry,
    ObjectId id_jobentry_type, int nr, long gui_location_x, long gui_location_y, boolean gui_draw,
    boolean parallel ) throws HopException {
    ObjectId id = repository.connectionDelegate.getNextJobEntryCopyID();

    RowMetaAndData table = new RowMetaAndData();

    //CHECKSTYLE:LineLength:OFF
    table.addValue( new ValueMetaInteger( HopDatabaseRepository.FIELD_JOBENTRY_COPY_ID_JOBENTRY_COPY ), id );
    table.addValue( new ValueMetaInteger( HopDatabaseRepository.FIELD_JOBENTRY_COPY_ID_JOBENTRY ), id_jobentry );
    table.addValue( new ValueMetaInteger( HopDatabaseRepository.FIELD_JOBENTRY_COPY_ID_JOB ), id_job );
    table.addValue( new ValueMetaInteger( HopDatabaseRepository.FIELD_JOBENTRY_COPY_ID_JOBENTRY_TYPE ), id_jobentry_type );
    table.addValue( new ValueMetaInteger( HopDatabaseRepository.FIELD_JOBENTRY_COPY_NR ), new Long( nr ) );
    table.addValue( new ValueMetaInteger( HopDatabaseRepository.FIELD_JOBENTRY_COPY_GUI_LOCATION_X ), new Long( gui_location_x ) );
    table.addValue( new ValueMetaInteger( HopDatabaseRepository.FIELD_JOBENTRY_COPY_GUI_LOCATION_Y ), new Long( gui_location_y ) );
    table.addValue( new ValueMetaBoolean( HopDatabaseRepository.FIELD_JOBENTRY_COPY_GUI_DRAW ), Boolean.valueOf( gui_draw ) );
    table.addValue( new ValueMetaBoolean( HopDatabaseRepository.FIELD_JOBENTRY_COPY_PARALLEL ), Boolean.valueOf( parallel ) );

    repository.connectionDelegate.getDatabase().prepareInsert( table.getRowMeta(), HopDatabaseRepository.TABLE_R_JOBENTRY_COPY );
    repository.connectionDelegate.getDatabase().setValuesInsert( table );
    repository.connectionDelegate.getDatabase().insertRow();
    repository.connectionDelegate.getDatabase().closeInsert();

    return id;
  }

  public synchronized int getNrJobEntries( ObjectId id_job ) throws HopException {
    int retval = 0;

    RowMetaAndData par = repository.connectionDelegate.getParameterMetaData( id_job );
    String sql =
      "SELECT COUNT(*) FROM "
        + quoteTable( HopDatabaseRepository.TABLE_R_JOBENTRY ) + " WHERE "
        + quote( HopDatabaseRepository.FIELD_JOBENTRY_ID_JOB ) + " = ? ";
    RowMetaAndData r = repository.connectionDelegate.getOneRow( sql, par.getRowMeta(), par.getData() );
    if ( r != null ) {
      retval = (int) r.getInteger( 0, 0L );
    }

    return retval;
  }

  public void loadJobEntryBase( JobEntryBase jobEntryBase, ObjectId id_jobentry, List<DatabaseMeta> databases,
    List<SlaveServer> slaveServers ) throws HopException {
    try {
      RowMetaAndData r = getJobEntry( id_jobentry );
      if ( r != null ) {
        jobEntryBase.setName( r.getString( "NAME", null ) );

        jobEntryBase.setDescription( r.getString( "DESCRIPTION", null ) );
        long id_jobentry_type = r.getInteger( "ID_JOBENTRY_TYPE", 0 );
        RowMetaAndData jetrow = getJobEntryType( new LongObjectId( id_jobentry_type ) );
        if ( jetrow != null ) {
          jobEntryBase.setPluginId( jetrow.getString( "CODE", null ) );
        }
      }
    } catch ( HopDatabaseException dbe ) {
      throw new HopException( "Unable to load base job entry information from the repository for id_jobentry="
        + id_jobentry, dbe );
    }
  }

  private void saveAttributesMap( ObjectId jobId, ObjectId entryId, Map<String, Map<String, String>> attributesMap ) throws HopException {
    for ( final String groupName : attributesMap.keySet() ) {
      Map<String, String> attributes = attributesMap.get( groupName );
      for ( final String key : attributes.keySet() ) {
        final String value = attributes.get( key );
        if ( key != null && value != null ) {
          repository.connectionDelegate.insertJobEntryAttribute( jobId, entryId, 0, JOBENTRY_ATTRIBUTE_PREFIX
            + groupName + '\t' + value, 0, value );
        }
      }
    }
  }

  private Map<String, Map<String, String>> loadJobEntryAttributesMap( ObjectId jobId, Object jobEntryId ) throws HopException {
    Map<String, Map<String, String>> attributesMap = new HashMap<String, Map<String, String>>();

    List<Object[]> attributeRows =
      repository.connectionDelegate.getJobEntryAttributesWithPrefix( jobId, jobId, JOBENTRY_ATTRIBUTE_PREFIX );
    RowMetaInterface rowMeta = repository.connectionDelegate.getReturnRowMeta();
    for ( Object[] attributeRow : attributeRows ) {
      String code = rowMeta.getString( attributeRow, HopDatabaseRepository.FIELD_JOBENTRY_ATTRIBUTE_CODE, null );
      String value =
        rowMeta.getString( attributeRow, HopDatabaseRepository.FIELD_JOBENTRY_ATTRIBUTE_VALUE_STR, null );
      if ( code != null && value != null ) {
        code = code.substring( JOBENTRY_ATTRIBUTE_PREFIX.length() );
        int tabIndex = code.indexOf( '\t' );
        if ( tabIndex > 0 ) {
          String groupName = code.substring( 0, tabIndex );
          String key = code.substring( tabIndex + 1 );
          Map<String, String> attributes = attributesMap.get( groupName );
          if ( attributes == null ) {
            attributes = new HashMap<String, String>();
            attributesMap.put( groupName, attributes );
          }
          attributes.put( key, value );
        }
      }
    }

    return attributesMap;
  }
}
