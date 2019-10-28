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
package org.apache.hop.ui.hopui;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.hop.base.AbstractMeta;
import org.apache.hop.cluster.ClusterSchema;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.EngineMetaInterface;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.job.JobMeta;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.repository.RepositoryElementInterface;
import org.apache.hop.shared.SharedObjectInterface;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.ui.hopui.delegates.HopUiDelegates;

/**
 * This utility provides methods for synchronization of kettle's shared objects.
 * 
 */
public class SharedObjectSyncUtil {

  private final ConnectionSynchronizationHandler connectionSynchronizationHandler =
      new ConnectionSynchronizationHandler();

  private final SlaveServerSynchronizationHandler slaveServerSynchronizationHandler =
      new SlaveServerSynchronizationHandler();

  private final ClusterSchemaSynchronizationHandler clusterSchemaSynchronizationHandler =
      new ClusterSchemaSynchronizationHandler();

  private final PartitionSchemaSynchronizationHandler partitionSchemaSynchronizationHandler =
      new PartitionSchemaSynchronizationHandler();

  private final StepMetaSynchronizationHandler stepMetaSynchronizationHandler = new StepMetaSynchronizationHandler();

  private final HopUiDelegates hopUiDelegates;
  private final HopUi hopUi;

  public SharedObjectSyncUtil( HopUi hopUi ) {
    this.hopUiDelegates = hopUi.delegates;
    this.hopUi = hopUi;
    hopUiDelegates.db.setSharedObjectSyncUtil( this );
    hopUiDelegates.slaves.setSharedObjectSyncUtil( this );
    hopUiDelegates.clusters.setSharedObjectSyncUtil( this );
    hopUiDelegates.partitions.setSharedObjectSyncUtil( this );
  }

  public synchronized void synchronizeConnections( DatabaseMeta database ) {
    synchronizeConnections( database, database.getName() );
  }
  /**
   * Synchronizes data from <code>database</code> to shared databases.
   * 
   * @param database
   *          data to share
   */
  public synchronized void synchronizeConnections( DatabaseMeta database, String originalName ) {
    if ( !database.isShared() ) {
      return;
    }
    synchronizeJobs( database, connectionSynchronizationHandler, originalName );
    synchronizeTransformations( database, connectionSynchronizationHandler, originalName );
    saveSharedObjects();
  }

  private void saveSharedObjects() {
    try {
      // flush to file for newly opened
      hopUi.getActiveMeta().saveSharedObjects();
    } catch ( HopException e ) {
      hopUi.getLog().logError( e.getLocalizedMessage(), e );
    }
  }

  /**
   * Synchronizes data from <code>slaveServer</code> to shared slave servers.
   * 
   * @param slaveServer
   *          data to share
   */
  public synchronized void synchronizeSlaveServers( SlaveServer slaveServer ) {
    synchronizeSlaveServers( slaveServer, slaveServer.getName() );
  }

  public synchronized void synchronizeSlaveServers( SlaveServer slaveServer, String originalName ) {
    if ( slaveServer.isShared() ) {
      synchronizeJobs( slaveServer, slaveServerSynchronizationHandler, originalName );
      synchronizeTransformations( slaveServer, slaveServerSynchronizationHandler, originalName );
      saveSharedObjects();
    }
    if ( slaveServer.getObjectId() != null ) {
      updateRepositoryObjects( slaveServer, slaveServerSynchronizationHandler );
    }
  }

  public synchronized void deleteSlaveServer( SlaveServer removed ) {
    synchronizeAll( true, meta -> meta.getSlaveServers().remove( removed ) );
  }

  public synchronized void deleteClusterSchema( ClusterSchema removed ) {
    synchronizeTransformations( true, transMeta -> transMeta.getClusterSchemas().remove( removed ) );
  }

  public synchronized void deletePartitionSchema( PartitionSchema removed ) {
    synchronizeTransformations( true, transMeta -> transMeta.getPartitionSchemas().remove( removed ) );
  }

  private <T extends SharedObjectInterface & RepositoryElementInterface>
    void updateRepositoryObjects( T updatedObject, SynchronizationHandler<T> handler ) {
    synchronizeJobs( true, job -> synchronizeByObjectId( updatedObject, handler.getObjectsForSyncFromJob( job ), handler ) );
    synchronizeTransformations( true, trans ->
      synchronizeByObjectId( updatedObject, handler.getObjectsForSyncFromTransformation( trans ), handler ) );
  }

  public synchronized void reloadTransformationRepositoryObjects( boolean includeActive ) {
    if ( hopUi.rep != null ) {
      synchronizeTransformations( includeActive, transMeta -> {
        try {
          hopUi.rep.readTransSharedObjects( transMeta );
        } catch ( HopException e ) {
          logError( e );
        }
      } );
    }
  }

  public synchronized void reloadJobRepositoryObjects( boolean includeActive ) {
    if ( hopUi.rep != null ) {
      synchronizeJobs( includeActive, jobMeta -> {
        try {
          hopUi.rep.readJobMetaSharedObjects( jobMeta );
        } catch ( HopException e ) {
          logError( e );
        }
      } );
    }
  }

  public synchronized void reloadSharedObjects() {
    synchronizeAll( false, meta -> {
      try {
        meta.setSharedObjects( meta.readSharedObjects() );
      } catch ( HopException e ) {
        logError( e );
      }
    } );
  }

  private synchronized void synchronizeJobs( boolean includeActive, Consumer<JobMeta> synchronizeAction ) {
    JobMeta current = hopUi.getActiveJob();
    for ( JobMeta job : hopUiDelegates.jobs.getLoadedJobs() ) {
      if ( includeActive || job != current ) {
        synchronizeAction.accept( job );
      }
    }
  }

  private synchronized void synchronizeTransformations( boolean includeActive, Consumer<TransMeta> synchronizeAction  ) {
    TransMeta current = hopUi.getActiveTransformation();
    for ( TransMeta trans : hopUiDelegates.trans.getLoadedTransformations() ) {
      if ( includeActive || trans != current ) {
        synchronizeAction.accept( trans );
      }
    }
  }

  private synchronized void synchronizeAll( boolean includeActive, Consumer<AbstractMeta> synchronizeAction  ) {
    EngineMetaInterface current = hopUi.getActiveMeta();
    for ( TransMeta trans : hopUiDelegates.trans.getLoadedTransformations() ) {
      if ( includeActive || trans != current ) {
        synchronizeAction.accept( trans );
      }
    }
    for ( JobMeta job : hopUiDelegates.jobs.getLoadedJobs() ) {
      if ( includeActive || job != current ) {
        synchronizeAction.accept( job );
      }
    }
  }

  /**
   * Synchronizes data from <code>clusterSchema</code> to shared cluster schemas.
   * 
   * @param clusterSchema
   *          data to share
   */
  public synchronized void synchronizeClusterSchemas( ClusterSchema clusterSchema ) {
    synchronizeClusterSchemas( clusterSchema, clusterSchema.getName() );
  }

  public synchronized void synchronizeClusterSchemas( ClusterSchema clusterSchema, String originalName ) {
    if ( clusterSchema.isShared() ) {
      synchronizeTransformations( clusterSchema, clusterSchemaSynchronizationHandler, originalName );
    }
    if ( clusterSchema.getObjectId() != null ) {
      updateRepositoryObjects( clusterSchema, clusterSchemaSynchronizationHandler );
    }
  }

  /**
   * Synchronizes data from <code>clusterSchema</code> to shared partition schemas.
   * 
   * @param partitionSchema
   *          data to share
   */
  public synchronized void synchronizePartitionSchemas( PartitionSchema partitionSchema ) {
    synchronizePartitionSchemas( partitionSchema, partitionSchema.getName() );
  }

  public synchronized void synchronizePartitionSchemas( PartitionSchema partitionSchema, String originalName ) {
    if ( partitionSchema.isShared() ) {
      synchronizeTransformations( partitionSchema, partitionSchemaSynchronizationHandler, originalName );
    }
    if ( partitionSchema.getObjectId() != null ) {
      updateRepositoryObjects( partitionSchema, partitionSchemaSynchronizationHandler );
    }
  }

  /**
   * Synchronizes data from <code>clusterSchema</code> to shared steps.
   * 
   * @param step
   *          data to shares
   */
  public synchronized void synchronizeSteps( StepMeta step ) {
    synchronizeSteps( step, step.getName() );
  }

  public synchronized void synchronizeSteps( StepMeta step, String originalName ) {
    if ( !step.isShared() ) {
      return;
    }
    synchronizeTransformations( step, stepMetaSynchronizationHandler, step.getName() );
  }

  private void logError( HopException e ) {
    if ( hopUi.getLog() != null ) {
      hopUi.getLog().logError( e.getLocalizedMessage(), e );
    }
  }

  private <T extends SharedObjectInterface> void synchronizeJobs( T sourceObject, SynchronizationHandler<T> handler,
      String originalName ) {
    for ( JobMeta currentJob : hopUiDelegates.jobs.getLoadedJobs() ) {
      List<T> objectsForSync = handler.getObjectsForSyncFromJob( currentJob );
      synchronizeShared( sourceObject, originalName, objectsForSync, handler );
    }
  }

  private <T extends SharedObjectInterface> void synchronizeTransformations( T object,
      SynchronizationHandler<T> handler, String originalName ) {
    for ( TransMeta currentTransformation : hopUiDelegates.trans.getLoadedTransformations() ) {
      List<T> objectsForSync =
          handler.getObjectsForSyncFromTransformation( currentTransformation );
      synchronizeShared( object, originalName, objectsForSync, handler );
    }
  }

  private static <T extends SharedObjectInterface> void synchronizeShared(
      T object, String name, List<T> objectsForSync, SynchronizationHandler<T> handler ) {
    synchronize( object, toSync -> toSync.isShared() && toSync.getName().equals( name ), objectsForSync, handler );
  }

  private static <T extends SharedObjectInterface & RepositoryElementInterface>
    void synchronizeByObjectId( T object, List<T> objectsForSync, SynchronizationHandler<T> handler ) {
    synchronize( object, toSync -> object.getObjectId().equals( toSync.getObjectId() ), objectsForSync, handler );
  }

  private static <T extends SharedObjectInterface> void synchronize( T object, Predicate<T> pred, List<T> objectsForSync,
      SynchronizationHandler<T> handler ) {
    for ( T objectToSync : objectsForSync ) {
      if ( pred.test( objectToSync ) && object != objectToSync ) {
        handler.doSynchronization( object, objectToSync );
      }
    }
  }

  protected static interface SynchronizationHandler<T extends SharedObjectInterface> {

    List<T> getObjectsForSyncFromJob( JobMeta job );

    List<T> getObjectsForSyncFromTransformation( TransMeta transformation );

    void doSynchronization( T source, T target );

  }

  private static class ConnectionSynchronizationHandler implements SynchronizationHandler<DatabaseMeta> {

    @Override
    public List<DatabaseMeta> getObjectsForSyncFromJob( JobMeta job ) {
      return job.getDatabases();
    }

    @Override
    public List<DatabaseMeta> getObjectsForSyncFromTransformation( TransMeta transformation ) {
      return transformation.getDatabases();
    }

    @Override
    public void doSynchronization( DatabaseMeta source, DatabaseMeta target ) {
      target.replaceMeta( source );
    }

  }

  private static class SlaveServerSynchronizationHandler implements SynchronizationHandler<SlaveServer> {

    @Override
    public List<SlaveServer> getObjectsForSyncFromJob( JobMeta job ) {
      return job.getSlaveServers();
    }

    @Override
    public List<SlaveServer> getObjectsForSyncFromTransformation( TransMeta transformation ) {
      return transformation.getSlaveServers();
    }

    @Override
    public void doSynchronization( SlaveServer source, SlaveServer target ) {
      target.replaceMeta( source );
    }

  }

  private static class ClusterSchemaSynchronizationHandler implements SynchronizationHandler<ClusterSchema> {

    @Override
    public List<ClusterSchema> getObjectsForSyncFromJob( JobMeta job ) {
      return Collections.emptyList();
    }

    @Override
    public List<ClusterSchema> getObjectsForSyncFromTransformation( TransMeta transformation ) {
      return transformation.getClusterSchemas();
    }

    @Override
    public void doSynchronization( ClusterSchema source, ClusterSchema target ) {
      target.replaceMeta( source );
    }

  }

  private static class PartitionSchemaSynchronizationHandler implements SynchronizationHandler<PartitionSchema> {

    @Override
    public List<PartitionSchema> getObjectsForSyncFromJob( JobMeta job ) {
      return Collections.emptyList();
    }

    @Override
    public List<PartitionSchema> getObjectsForSyncFromTransformation( TransMeta transformation ) {
      return transformation.getPartitionSchemas();
    }

    @Override
    public void doSynchronization( PartitionSchema source, PartitionSchema target ) {
      target.replaceMeta( source );
    }

  }

  private static class StepMetaSynchronizationHandler implements SynchronizationHandler<StepMeta> {

    @Override
    public List<StepMeta> getObjectsForSyncFromJob( JobMeta job ) {
      return Collections.emptyList();
    }

    @Override
    public List<StepMeta> getObjectsForSyncFromTransformation( TransMeta transformation ) {
      return transformation.getSteps();
    }

    @Override
    public void doSynchronization( StepMeta source, StepMeta target ) {
      target.replaceMeta( source );
    }

  }

}
