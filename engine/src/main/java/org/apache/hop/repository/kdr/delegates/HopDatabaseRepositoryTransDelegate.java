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

package org.apache.hop.repository.kdr.delegates;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.hop.cluster.ClusterSchema;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.ProgressMonitorListener;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogTableInterface;
import org.apache.hop.core.logging.TransLogTable;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.repository.LongObjectId;
import org.apache.hop.repository.ObjectId;
import org.apache.hop.repository.RepositoryAttributeInterface;
import org.apache.hop.repository.RepositoryDirectory;
import org.apache.hop.repository.RepositoryDirectoryInterface;
import org.apache.hop.repository.RepositoryObjectType;
import org.apache.hop.repository.kdr.HopDatabaseRepository;
import org.apache.hop.shared.SharedObjects;
import org.apache.hop.trans.TransDependency;
import org.apache.hop.trans.TransHopMeta;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.TransMeta.TransformationType;
import org.apache.hop.trans.step.StepErrorMeta;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.trans.step.StepPartitioningMeta;
import org.apache.hop.trans.steps.missing.MissingTrans;

public class HopDatabaseRepositoryTransDelegate extends HopDatabaseRepositoryBaseDelegate {

  private static Class<?> PKG = TransMeta.class; // for i18n purposes, needed by Translator2!!

  private static final String TRANS_ATTRIBUTE_PREFIX_DELIMITER = "_";

  public static final String TRANS_ATTRIBUTE_PREFIX = "_ATTR_" + TRANS_ATTRIBUTE_PREFIX_DELIMITER;

  public HopDatabaseRepositoryTransDelegate( HopDatabaseRepository repository ) {
    super( repository );
  }

  public RowMetaAndData getTransformation( ObjectId id_transformation ) throws HopException {
    return repository.connectionDelegate.getOneRow(
      quoteTable( HopDatabaseRepository.TABLE_R_TRANSFORMATION ),
      quote( HopDatabaseRepository.FIELD_TRANSFORMATION_ID_TRANSFORMATION ), id_transformation );
  }

  public RowMetaAndData getTransHop( ObjectId id_trans_hop ) throws HopException {
    return repository.connectionDelegate.getOneRow(
      quoteTable( HopDatabaseRepository.TABLE_R_TRANS_HOP ),
      quote( HopDatabaseRepository.FIELD_TRANS_HOP_ID_TRANS_HOP ), id_trans_hop );
  }

  public RowMetaAndData getTransDependency( ObjectId id_dependency ) throws HopException {
    return repository.connectionDelegate.getOneRow(
      quoteTable( HopDatabaseRepository.TABLE_R_DEPENDENCY ),
      quote( HopDatabaseRepository.FIELD_DEPENDENCY_ID_DEPENDENCY ), id_dependency );
  }

  public boolean existsTransMeta( String name, RepositoryDirectoryInterface repositoryDirectory,
    RepositoryObjectType objectType ) throws HopException {
    try {
      return ( getTransformationID( name, repositoryDirectory.getObjectId() ) != null );
    } catch ( HopException e ) {
      throw new HopException( "Unable to verify if the transformation with name ["
        + name + "] in directory [" + repositoryDirectory + "] exists", e );
    }
  }

  public synchronized ObjectId getTransformationID( String name, ObjectId id_directory ) throws HopException {
    return repository.connectionDelegate.getIDWithValue(
      quoteTable( HopDatabaseRepository.TABLE_R_TRANSFORMATION ),
      quote( HopDatabaseRepository.FIELD_TRANSFORMATION_ID_TRANSFORMATION ),
      quote( HopDatabaseRepository.FIELD_TRANSFORMATION_NAME ), name,
      quote( HopDatabaseRepository.FIELD_TRANSFORMATION_ID_DIRECTORY ), id_directory );
  }

  public synchronized ObjectId getTransHopID( ObjectId id_transformation, ObjectId id_step_from,
    ObjectId id_step_to ) throws HopException {
    String[] lookupkey =
      new String[] {
        quote( HopDatabaseRepository.FIELD_TRANS_HOP_ID_TRANSFORMATION ),
        quote( HopDatabaseRepository.FIELD_TRANS_HOP_ID_STEP_FROM ),
        quote( HopDatabaseRepository.FIELD_TRANS_HOP_ID_STEP_TO ), };
    ObjectId[] key = new ObjectId[] { id_transformation, id_step_from, id_step_to };

    return repository.connectionDelegate.getIDWithValue(
      quoteTable( HopDatabaseRepository.TABLE_R_TRANS_HOP ),
      quote( HopDatabaseRepository.FIELD_TRANS_HOP_ID_TRANS_HOP ), lookupkey, key );
  }

  public synchronized ObjectId getDependencyID( ObjectId id_transformation, ObjectId id_database, String tablename ) throws HopException {

    String[] lookupkey =
      new String[] {
        quote( HopDatabaseRepository.FIELD_DEPENDENCY_ID_TRANSFORMATION ),
        quote( HopDatabaseRepository.FIELD_DEPENDENCY_ID_DATABASE ), };
    ObjectId[] key = new ObjectId[] { id_transformation, id_database };

    return repository.connectionDelegate.getIDWithValue(
      quoteTable( HopDatabaseRepository.TABLE_R_DEPENDENCY ),
      quote( HopDatabaseRepository.FIELD_DEPENDENCY_ID_DEPENDENCY ),
      quote( HopDatabaseRepository.FIELD_DEPENDENCY_TABLE_NAME ), tablename, lookupkey, key );
  }

  /**
   * Saves the transformation to a repository.
   *
   * @param transMeta
   *          the transformation metadata to store
   * @param monitor
   *          the way we report progress to the user, can be null if no UI is present
   * @param overwriteAssociated
   *          Overwrite existing object(s)?
   * @throws HopException
   *           if an error occurs.
   */
  public void saveTransformation( TransMeta transMeta, String versionComment, ProgressMonitorListener monitor,
    boolean overwriteAssociated ) throws HopException {
    try {
      if ( monitor != null ) {
        monitor.subTask( BaseMessages.getString( PKG, "TransMeta.Monitor.LockingRepository" ) );
      }

      repository.insertLogEntry( "save transformation '" + transMeta.getName() + "'" );

      // Clear attribute id cache
      repository.connectionDelegate.clearNextIDCounters(); // force repository lookup.

      // Do we have a valid directory?
      if ( transMeta.getRepositoryDirectory().getObjectId() == null ) {
        throw new HopException( BaseMessages.getString(
          PKG, "TransMeta.Exception.PlsSelectAValidDirectoryBeforeSavingTheTransformation" ) );
      }

      int nrWorks =
        2 + transMeta.nrDatabases() + transMeta.nrNotes() + transMeta.nrSteps() + transMeta.nrTransHops();
      if ( monitor != null ) {
        monitor.beginTask( BaseMessages.getString( PKG, "TransMeta.Monitor.SavingTransformationTask.Title" )
          + transMeta.getPathAndName(), nrWorks );
      }
      if ( log.isDebug() ) {
        log.logDebug( BaseMessages.getString( PKG, "TransMeta.Log.SavingOfTransformationStarted" ) );
      }

      if ( monitor != null && monitor.isCanceled() ) {
        throw new HopDatabaseException();
      }

      // Before we start, make sure we have a valid transformation ID!
      // Two possibilities:
      // 1) We have a ID: keep it
      // 2) We don't have an ID: look it up.
      // If we find a transformation with the same name: ask!
      //
      if ( monitor != null ) {
        monitor.subTask( BaseMessages.getString(
          PKG, "TransMeta.Monitor.HandlingOldVersionTransformationTask.Title" ) );
        // transMeta.setObjectId(getTransformationID(transMeta.getName(),
        // transMeta.getRepositoryDirectory().getObjectId()));
      }

      // If no valid id is available in the database, assign one...
      if ( transMeta.getObjectId() == null ) {
        transMeta.setObjectId( repository.connectionDelegate.getNextTransformationID() );
      } else {
        // If we have a valid ID, we need to make sure everything is cleared out
        // of the database for this id_transformation, before we put it back in...
        if ( monitor != null ) {
          monitor.subTask( BaseMessages.getString(
            PKG, "TransMeta.Monitor.DeletingOldVersionTransformationTask.Title" ) );
        }
        if ( log.isDebug() ) {
          log.logDebug( BaseMessages.getString( PKG, "TransMeta.Log.DeletingOldVersionTransformation" ) );
        }
        repository.deleteTransformation( transMeta.getObjectId() );
        if ( log.isDebug() ) {
          log.logDebug( BaseMessages.getString( PKG, "TransMeta.Log.OldVersionOfTransformationRemoved" ) );
        }
      }
      if ( monitor != null ) {
        monitor.worked( 1 );
      }

      if ( log.isDebug() ) {
        log.logDebug( BaseMessages.getString( PKG, "TransMeta.Log.SavingNotes" ) );
      }
      for ( int i = 0; i < transMeta.nrNotes(); i++ ) {
        if ( monitor != null && monitor.isCanceled() ) {
          throw new HopDatabaseException( BaseMessages.getString( PKG, "TransMeta.Log.UserCancelledTransSave" ) );
        }

        // if (monitor != null) monitor.subTask(BaseMessages.getString(PKG, "TransMeta.Monitor.SavingNoteTask.Title") +
        // (i + 1) + "/" + transMeta.nrNotes());
        NotePadMeta ni = transMeta.getNote( i );
        repository.saveNotePadMeta( ni, transMeta.getObjectId() );
        if ( ni.getObjectId() != null ) {
          repository.insertTransNote( transMeta.getObjectId(), ni.getObjectId() );
        }
        if ( monitor != null ) {
          monitor.worked( 1 );
        }
      }

      if ( log.isDebug() ) {
        log.logDebug( BaseMessages.getString( PKG, "TransMeta.Log.SavingDatabaseConnections" ) );
      }
      for ( int i = 0; i < transMeta.nrDatabases(); i++ ) {
        if ( monitor != null && monitor.isCanceled() ) {
          throw new HopDatabaseException( BaseMessages.getString( PKG, "TransMeta.Log.UserCancelledTransSave" ) );
        }

        // if (monitor != null) monitor.subTask(BaseMessages.getString(PKG,
        // "TransMeta.Monitor.SavingDatabaseTask.Title") + (i + 1) + "/" + transMeta.nrDatabases());
        DatabaseMeta databaseMeta = transMeta.getDatabase( i );
        // Save the database connection if we're overwriting objects or (it has changed and nothing was saved in the
        // repository)
        if ( overwriteAssociated || databaseMeta.hasChanged() || databaseMeta.getObjectId() == null ) {
          repository.save( databaseMeta, versionComment, monitor, overwriteAssociated );
        }
        if ( monitor != null ) {
          monitor.worked( 1 );
        }
      }

      // Before saving the steps, make sure we have all the step-types.
      // It is possible that we received another step through a plugin.
      if ( log.isDebug() ) {
        log.logDebug( BaseMessages.getString( PKG, "TransMeta.Log.CheckingStepTypes" ) );
      }
      repository.updateStepTypes();
      repository.updateDatabaseTypes();

      if ( log.isDebug() ) {
        log.logDebug( BaseMessages.getString( PKG, "TransMeta.Log.SavingSteps" ) );
      }
      for ( int i = 0; i < transMeta.nrSteps(); i++ ) {
        if ( monitor != null && monitor.isCanceled() ) {
          throw new HopDatabaseException( BaseMessages.getString( PKG, "TransMeta.Log.UserCancelledTransSave" ) );
        }

        // if (monitor != null) monitor.subTask(BaseMessages.getString(PKG, "TransMeta.Monitor.SavingStepTask.Title") +
        // (i + 1) + "/" + transMeta.nrSteps());
        StepMeta stepMeta = transMeta.getStep( i );
        repository.stepDelegate.saveStepMeta( stepMeta, transMeta.getObjectId() );

        if ( monitor != null ) {
          monitor.worked( 1 );
        }
      }
      repository.connectionDelegate.closeStepAttributeInsertPreparedStatement();

      if ( log.isDebug() ) {
        log.logDebug( BaseMessages.getString( PKG, "TransMeta.Log.SavingHops" ) );
      }
      for ( int i = 0; i < transMeta.nrTransHops(); i++ ) {
        if ( monitor != null && monitor.isCanceled() ) {
          throw new HopDatabaseException( BaseMessages.getString( PKG, "TransMeta.Log.UserCancelledTransSave" ) );
        }

        // if (monitor != null) monitor.subTask(BaseMessages.getString(PKG, "TransMeta.Monitor.SavingHopTask.Title") +
        // (i + 1) + "/" + transMeta.nrTransHops());
        TransHopMeta hi = transMeta.getTransHop( i );
        saveTransHopMeta( hi, transMeta.getObjectId() );
        if ( monitor != null ) {
          monitor.worked( 1 );
        }
      }

      // if (monitor != null) monitor.subTask(BaseMessages.getString(PKG, "TransMeta.Monitor.FinishingTask.Title"));
      if ( log.isDebug() ) {
        log.logDebug( BaseMessages.getString( PKG, "TransMeta.Log.SavingTransformationInfo" ) );
      }

      insertTransformation( transMeta ); // save the top level information for the transformation

      saveTransParameters( transMeta );

      repository.connectionDelegate.closeTransAttributeInsertPreparedStatement();

      // Save the partition schemas
      //
      for ( int i = 0; i < transMeta.getPartitionSchemas().size(); i++ ) {
        if ( monitor != null && monitor.isCanceled() ) {
          throw new HopDatabaseException( BaseMessages.getString( PKG, "TransMeta.Log.UserCancelledTransSave" ) );
        }

        PartitionSchema partitionSchema = transMeta.getPartitionSchemas().get( i );
        // See if this transformation really is a consumer of this object
        // It might be simply loaded as a shared object from the repository
        //
        boolean isUsedByTransformation = transMeta.isUsingPartitionSchema( partitionSchema );
        repository.save(
          partitionSchema, versionComment, null, transMeta.getObjectId(), isUsedByTransformation,
          overwriteAssociated );
      }

      // Save the slaves
      //
      for ( int i = 0; i < transMeta.getSlaveServers().size(); i++ ) {
        if ( monitor != null && monitor.isCanceled() ) {
          throw new HopDatabaseException( BaseMessages.getString( PKG, "TransMeta.Log.UserCancelledTransSave" ) );
        }

        SlaveServer slaveServer = transMeta.getSlaveServers().get( i );
        boolean isUsedByTransformation = transMeta.isUsingSlaveServer( slaveServer );
        repository.save(
          slaveServer, versionComment, null, transMeta.getObjectId(), isUsedByTransformation,
          overwriteAssociated );
      }

      // Save the clustering schemas
      for ( int i = 0; i < transMeta.getClusterSchemas().size(); i++ ) {
        if ( monitor != null && monitor.isCanceled() ) {
          throw new HopDatabaseException( BaseMessages.getString( PKG, "TransMeta.Log.UserCancelledTransSave" ) );
        }

        ClusterSchema clusterSchema = transMeta.getClusterSchemas().get( i );
        boolean isUsedByTransformation = transMeta.isUsingClusterSchema( clusterSchema );
        repository.save(
          clusterSchema, versionComment, null, transMeta.getObjectId(), isUsedByTransformation,
          overwriteAssociated );
      }

      if ( log.isDebug() ) {
        log.logDebug( BaseMessages.getString( PKG, "TransMeta.Log.SavingDependencies" ) );
      }
      for ( int i = 0; i < transMeta.nrDependencies(); i++ ) {
        if ( monitor != null && monitor.isCanceled() ) {
          throw new HopDatabaseException( BaseMessages.getString( PKG, "TransMeta.Log.UserCancelledTransSave" ) );
        }

        TransDependency td = transMeta.getDependency( i );
        saveTransDependency( td, transMeta.getObjectId() );
      }

      saveTransAttributesMap( transMeta.getObjectId(), transMeta.getAttributesMap() );

      // Save the step error handling information as well!
      for ( int i = 0; i < transMeta.nrSteps(); i++ ) {
        StepMeta stepMeta = transMeta.getStep( i );
        StepErrorMeta stepErrorMeta = stepMeta.getStepErrorMeta();
        if ( stepErrorMeta != null ) {
          repository.stepDelegate.saveStepErrorMeta( stepErrorMeta, transMeta.getObjectId(), stepMeta
            .getObjectId() );
        }
      }
      repository.connectionDelegate.closeStepAttributeInsertPreparedStatement();

      if ( log.isDebug() ) {
        log.logDebug( BaseMessages.getString( PKG, "TransMeta.Log.SavingFinished" ) );
      }

      if ( monitor != null ) {
        monitor.subTask( BaseMessages.getString( PKG, "TransMeta.Monitor.UnlockingRepository" ) );
      }
      repository.unlockRepository();

      // Perform a commit!
      repository.commit();

      transMeta.clearChanged();
      if ( monitor != null ) {
        monitor.worked( 1 );
      }
      if ( monitor != null ) {
        monitor.done();
      }
    } catch ( HopDatabaseException dbe ) {
      // Oops, roll back!
      repository.rollback();

      log.logError( BaseMessages.getString( PKG, "TransMeta.Log.ErrorSavingTransformationToRepository" )
        + Const.CR + dbe.getMessage() );
      throw new HopException( BaseMessages.getString(
        PKG, "TransMeta.Log.ErrorSavingTransformationToRepository" ), dbe );
    }
  }

  /**
   * Save the parameters of this transformation to the repository.
   *
   *
   * @throws HopException
   *           Upon any error.
   *
   *
   *           TODO: Move this code over to the Repository class for refactoring...
   */
  public void saveTransParameters( TransMeta transMeta ) throws HopException {
    String[] paramKeys = transMeta.listParameters();
    for ( int idx = 0; idx < paramKeys.length; idx++ ) {
      String desc = transMeta.getParameterDescription( paramKeys[idx] );
      String defaultValue = transMeta.getParameterDefault( paramKeys[idx] );
      insertTransParameter( transMeta.getObjectId(), idx, paramKeys[idx], defaultValue, desc );
    }
  }

  /**
   * Read a transformation with a certain name from a repository
   *
   * @param transname
   *          The name of the transformation.
   * @param repdir
   *          the path to the repository directory
   * @param monitor
   *          The progress monitor to display the progress of the file-open operation in a dialog
   * @param setInternalVariables
   *          true if you want to set the internal variables based on this transformation information
   */
  public TransMeta loadTransformation( TransMeta transMeta, String transname, RepositoryDirectoryInterface repdir,
    ProgressMonitorListener monitor, boolean setInternalVariables ) throws HopException {
    transMeta.setRepository( repository );
    transMeta.setMetaStore( repository.metaStore );

    synchronized ( repository ) {
      try {
        String pathAndName =
          repdir.isRoot() ? repdir + transname : repdir + RepositoryDirectory.DIRECTORY_SEPARATOR + transname;

        transMeta.setName( transname );
        transMeta.setRepositoryDirectory( repdir );

        // Get the transformation id
        if ( log.isDetailed() ) {
          log.logDetailed( BaseMessages.getString(
            PKG, "TransMeta.Log.LookingForTransformation", transname, repdir.getPath() ) );
        }

        if ( monitor != null ) {
          monitor.subTask( BaseMessages.getString( PKG, "TransMeta.Monitor.ReadingTransformationInfoTask.Title" ) );
        }
        transMeta.setObjectId( getTransformationID( transname, repdir.getObjectId() ) );
        if ( monitor != null ) {
          monitor.worked( 1 );
        }

        // If no valid id is available in the database, then give error...
        if ( transMeta.getObjectId() != null ) {
          ObjectId[] noteids = repository.getTransNoteIDs( transMeta.getObjectId() );
          ObjectId[] stepids = repository.getStepIDs( transMeta.getObjectId() );
          ObjectId[] hopids = getTransHopIDs( transMeta.getObjectId() );

          int nrWork = 3 + noteids.length + stepids.length + hopids.length;

          if ( monitor != null ) {
            monitor.beginTask( BaseMessages.getString( PKG, "TransMeta.Monitor.LoadingTransformationTask.Title" )
              + pathAndName, nrWork );
          }

          if ( log.isDetailed() ) {
            log.logDetailed( BaseMessages.getString( PKG, "TransMeta.Log.LoadingTransformation", transMeta
              .getName() ) );
          }

          // Load the common database connections
          if ( monitor != null ) {
            monitor.subTask( BaseMessages.getString(
              PKG, "TransMeta.Monitor.ReadingTheAvailableSharedObjectsTask.Title" ) );
          }
          try {
            transMeta.setSharedObjects( readTransSharedObjects( transMeta ) );
          } catch ( Exception e ) {
            log.logError( BaseMessages
              .getString( PKG, "TransMeta.ErrorReadingSharedObjects.Message", e.toString() ) );
            log.logError( Const.getStackTracker( e ) );
          }

          if ( monitor != null ) {
            monitor.worked( 1 );
          }

          // Load the notes...
          if ( monitor != null ) {
            monitor.subTask( BaseMessages.getString( PKG, "TransMeta.Monitor.ReadingNoteTask.Title" ) );
          }
          for ( int i = 0; i < noteids.length; i++ ) {
            NotePadMeta ni = repository.notePadDelegate.loadNotePadMeta( noteids[i] );
            if ( transMeta.indexOfNote( ni ) < 0 ) {
              transMeta.addNote( ni );
            }
            if ( monitor != null ) {
              monitor.worked( 1 );
            }
          }

          if ( monitor != null ) {
            monitor.subTask( BaseMessages.getString( PKG, "TransMeta.Monitor.ReadingStepsTask.Title" ) );
          }
          repository.connectionDelegate.fillStepAttributesBuffer( transMeta.getObjectId() ); // read all the attributes
                                                                                             // on one go!
          for ( int i = 0; i < stepids.length; i++ ) {
            if ( log.isDetailed() ) {
              log.logDetailed( BaseMessages.getString( PKG, "TransMeta.Log.LoadingStepWithID" ) + stepids[i] );
            }
            if ( monitor != null ) {
              monitor.subTask( BaseMessages.getString( PKG, "TransMeta.Monitor.ReadingStepTask.Title" )
                + ( i + 1 ) + "/" + ( stepids.length ) );
            }
            StepMeta stepMeta =
              repository.stepDelegate.loadStepMeta( stepids[i], transMeta.getDatabases(), transMeta
                .getPartitionSchemas() );
            if ( stepMeta.isMissing() ) {
              transMeta.addMissingTrans( (MissingTrans) stepMeta.getStepMetaInterface() );
            }
            // In this case, we just add or replace the shared steps.
            // The repository is considered "more central"
            transMeta.addOrReplaceStep( stepMeta );

            if ( monitor != null ) {
              monitor.worked( 1 );
            }
          }
          if ( monitor != null ) {
            monitor.worked( 1 );
          }
          repository.connectionDelegate.setStepAttributesBuffer( null ); // clear the buffer (should be empty anyway)

          // Have all StreamValueLookups, etc. reference the correct source steps...
          for ( int i = 0; i < transMeta.nrSteps(); i++ ) {
            StepMetaInterface sii = transMeta.getStep( i ).getStepMetaInterface();
            sii.searchInfoAndTargetSteps( transMeta.getSteps() );
          }

          if ( monitor != null ) {
            monitor.subTask( BaseMessages.getString(
              PKG, "TransMeta.Monitor.LoadingTransformationDetailsTask.Title" ) );
          }
          loadRepTrans( transMeta );
          if ( monitor != null ) {
            monitor.worked( 1 );
          }

          if ( monitor != null ) {
            monitor.subTask( BaseMessages.getString( PKG, "TransMeta.Monitor.ReadingHopTask.Title" ) );
          }
          for ( int i = 0; i < hopids.length; i++ ) {
            TransHopMeta hi = loadTransHopMeta( hopids[i], transMeta.getSteps() );
            if ( hi != null ) {
              transMeta.addTransHop( hi );
            }
            if ( monitor != null ) {
              monitor.worked( 1 );
            }
          }

          // Have all step partitioning meta-data reference the correct schemas that we just loaded
          //
          for ( int i = 0; i < transMeta.nrSteps(); i++ ) {
            StepPartitioningMeta stepPartitioningMeta = transMeta.getStep( i ).getStepPartitioningMeta();
            if ( stepPartitioningMeta != null ) {
              stepPartitioningMeta.setPartitionSchemaAfterLoading( transMeta.getPartitionSchemas() );
            }
          }

          // Have all step clustering schema meta-data reference the correct cluster schemas that we just loaded
          //
          for ( int i = 0; i < transMeta.nrSteps(); i++ ) {
            transMeta.getStep( i ).setClusterSchemaAfterLoading( transMeta.getClusterSchemas() );
          }

          if ( monitor != null ) {
            monitor.subTask( BaseMessages.getString( PKG, "TransMeta.Monitor.ReadingTheDependenciesTask.Title" ) );
          }
          ObjectId[] depids = repository.getTransDependencyIDs( transMeta.getObjectId() );
          for ( int i = 0; i < depids.length; i++ ) {
            TransDependency td = loadTransDependency( depids[i], transMeta.getDatabases() );
            transMeta.addDependency( td );
          }
          if ( monitor != null ) {
            monitor.worked( 1 );
          }

          // Load the group attributes map
          //
          transMeta.setAttributesMap( loadTransAttributesMap( transMeta.getObjectId() ) );

          // Also load the step error handling metadata
          //
          for ( int i = 0; i < transMeta.nrSteps(); i++ ) {
            StepMeta stepMeta = transMeta.getStep( i );
            String sourceStep =
              repository.getStepAttributeString( stepMeta.getObjectId(), "step_error_handling_source_step" );
            if ( sourceStep != null ) {
              StepErrorMeta stepErrorMeta =
                repository.stepDelegate.loadStepErrorMeta( transMeta, stepMeta, transMeta.getSteps() );
              stepErrorMeta.getSourceStep().setStepErrorMeta( stepErrorMeta ); // a bit of a trick, I know.
            }
          }

          // Load all the log tables for the transformation...
          //
          RepositoryAttributeInterface attributeInterface =
            new HopDatabaseRepositoryTransAttribute( repository.connectionDelegate, transMeta.getObjectId() );
          for ( LogTableInterface logTable : transMeta.getLogTables() ) {
            logTable.loadFromRepository( attributeInterface );
          }

          if ( monitor != null ) {
            monitor.subTask( BaseMessages.getString( PKG, "TransMeta.Monitor.SortingStepsTask.Title" ) );
          }
          transMeta.sortSteps();
          if ( monitor != null ) {
            monitor.worked( 1 );
          }
          if ( monitor != null ) {
            monitor.done();
          }
        } else {
          throw new HopException( BaseMessages
            .getString( PKG, "TransMeta.Exception.TransformationDoesNotExist" )
            + transMeta.getName() );
        }
        if ( log.isDetailed() ) {
          log.logDetailed( BaseMessages.getString( PKG, "TransMeta.Log.LoadedTransformation2", transname, String
            .valueOf( transMeta.getRepositoryDirectory() == null ) ) );
          log
            .logDetailed( BaseMessages.getString(
              PKG, "TransMeta.Log.LoadedTransformation", transname, transMeta.getRepositoryDirectory().getPath() ) );
        }

        // close prepared statements, minimize locking etc.
        //
        repository.connectionDelegate.closeAttributeLookupPreparedStatements();

        return transMeta;
      } catch ( HopDatabaseException e ) {
        log.logError( BaseMessages.getString( PKG, "TransMeta.Log.DatabaseErrorOccuredReadingTransformation" )
          + Const.CR + e );
        throw new HopException( BaseMessages.getString(
          PKG, "TransMeta.Exception.DatabaseErrorOccuredReadingTransformation" ), e );
      } catch ( Exception e ) {
        log.logError( BaseMessages.getString( PKG, "TransMeta.Log.DatabaseErrorOccuredReadingTransformation" )
          + Const.CR + e );
        throw new HopException( BaseMessages.getString(
          PKG, "TransMeta.Exception.DatabaseErrorOccuredReadingTransformation2" ), e );
      } finally {
        transMeta.initializeVariablesFrom( null );
        if ( setInternalVariables ) {
          transMeta.setInternalHopVariables();
        }
      }
    }
  }

  /**
   * Load the transformation name & other details from a repository.
   */
  private void loadRepTrans( TransMeta transMeta ) throws HopException {
    try {
      RowMetaAndData r = getTransformation( transMeta.getObjectId() );

      if ( r != null ) {
        transMeta.setName( r.getString( HopDatabaseRepository.FIELD_TRANSFORMATION_NAME, null ) );

        // Trans description
        transMeta.setDescription( r.getString( HopDatabaseRepository.FIELD_TRANSFORMATION_DESCRIPTION, null ) );
        transMeta.setExtendedDescription( r.getString(
          HopDatabaseRepository.FIELD_TRANSFORMATION_EXTENDED_DESCRIPTION, null ) );
        transMeta
          .setTransversion( r.getString( HopDatabaseRepository.FIELD_TRANSFORMATION_TRANS_VERSION, null ) );
        transMeta.setTransstatus( (int) r.getInteger(
          HopDatabaseRepository.FIELD_TRANSFORMATION_TRANS_STATUS, -1L ) );

        TransLogTable logTable = transMeta.getTransLogTable();
        logTable.findField( TransLogTable.ID.LINES_READ ).setSubject(
          StepMeta.findStep( transMeta.getSteps(), new LongObjectId( r.getInteger(
            HopDatabaseRepository.FIELD_TRANSFORMATION_ID_STEP_READ, -1L ) ) ) );

        logTable.findField( TransLogTable.ID.LINES_READ ).setSubject(
          StepMeta.findStep( transMeta.getSteps(), new LongObjectId( r.getInteger(
            HopDatabaseRepository.FIELD_TRANSFORMATION_ID_STEP_WRITE, -1L ) ) ) );
        logTable.findField( TransLogTable.ID.LINES_READ ).setSubject(
          StepMeta.findStep( transMeta.getSteps(), new LongObjectId( r.getInteger(
            HopDatabaseRepository.FIELD_TRANSFORMATION_ID_STEP_INPUT, -1L ) ) ) );
        logTable.findField( TransLogTable.ID.LINES_READ ).setSubject(
          StepMeta.findStep( transMeta.getSteps(), new LongObjectId( r.getInteger(
            HopDatabaseRepository.FIELD_TRANSFORMATION_ID_STEP_OUTPUT, -1L ) ) ) );
        logTable.findField( TransLogTable.ID.LINES_READ ).setSubject(
          StepMeta.findStep( transMeta.getSteps(), new LongObjectId( r.getInteger(
            HopDatabaseRepository.FIELD_TRANSFORMATION_ID_STEP_UPDATE, -1L ) ) ) );

        long id_rejected =
          getTransAttributeInteger(
            transMeta.getObjectId(), 0, HopDatabaseRepository.TRANS_ATTRIBUTE_ID_STEP_REJECTED );
        if ( id_rejected > 0 ) {
          logTable.findField( TransLogTable.ID.LINES_REJECTED ).setSubject(
            StepMeta.findStep( transMeta.getSteps(), new LongObjectId( id_rejected ) ) );
        }

        DatabaseMeta logDb =
          DatabaseMeta.findDatabase( transMeta.getDatabases(), new LongObjectId( r.getInteger(
            HopDatabaseRepository.FIELD_TRANSFORMATION_ID_DATABASE_LOG, -1L ) ) );
        if ( logDb != null ) {
          logTable.setConnectionName( logDb.getName() );
          // TODO: save/load name as a string, allow variables!
        }

        logTable.setTableName( r.getString( HopDatabaseRepository.FIELD_TRANSFORMATION_TABLE_NAME_LOG, null ) );
        logTable.setBatchIdUsed( r.getBoolean( HopDatabaseRepository.FIELD_TRANSFORMATION_USE_BATCHID, false ) );
        logTable
          .setLogFieldUsed( r.getBoolean( HopDatabaseRepository.FIELD_TRANSFORMATION_USE_LOGFIELD, false ) );

        transMeta.setMaxDateConnection( DatabaseMeta.findDatabase( transMeta.getDatabases(), new LongObjectId( r
          .getInteger( HopDatabaseRepository.FIELD_TRANSFORMATION_ID_DATABASE_MAXDATE, -1L ) ) ) );
        transMeta.setMaxDateTable( r.getString(
          HopDatabaseRepository.FIELD_TRANSFORMATION_TABLE_NAME_MAXDATE, null ) );
        transMeta.setMaxDateField( r.getString(
          HopDatabaseRepository.FIELD_TRANSFORMATION_FIELD_NAME_MAXDATE, null ) );
        transMeta.setMaxDateOffset( r
          .getNumber( HopDatabaseRepository.FIELD_TRANSFORMATION_OFFSET_MAXDATE, 0.0 ) );
        transMeta.setMaxDateDifference( r.getNumber(
          HopDatabaseRepository.FIELD_TRANSFORMATION_DIFF_MAXDATE, 0.0 ) );

        transMeta.setCreatedUser( r.getString( HopDatabaseRepository.FIELD_TRANSFORMATION_CREATED_USER, null ) );
        transMeta.setCreatedDate( r.getDate( HopDatabaseRepository.FIELD_TRANSFORMATION_CREATED_DATE, null ) );

        transMeta
          .setModifiedUser( r.getString( HopDatabaseRepository.FIELD_TRANSFORMATION_MODIFIED_USER, null ) );
        transMeta.setModifiedDate( r.getDate( HopDatabaseRepository.FIELD_TRANSFORMATION_MODIFIED_DATE, null ) );

        // Optional:
        transMeta.setSizeRowset( Const.ROWS_IN_ROWSET );
        Long val_size_rowset = r.getInteger( HopDatabaseRepository.FIELD_TRANSFORMATION_SIZE_ROWSET );
        if ( val_size_rowset != null ) {
          transMeta.setSizeRowset( val_size_rowset.intValue() );
        }

        long id_directory = r.getInteger( HopDatabaseRepository.FIELD_TRANSFORMATION_ID_DIRECTORY, -1L );
        if ( id_directory >= 0 ) {
          if ( log.isDetailed() ) {
            log.logDetailed( "ID_DIRECTORY=" + id_directory );
          }
          // always reload the folder structure
          //
          transMeta.setRepositoryDirectory( repository.loadRepositoryDirectoryTree().findDirectory(
            new LongObjectId( id_directory ) ) );
        }

        transMeta.setUsingUniqueConnections( getTransAttributeBoolean(
          transMeta.getObjectId(), 0, HopDatabaseRepository.TRANS_ATTRIBUTE_UNIQUE_CONNECTIONS ) );
        transMeta.setFeedbackShown( !"N".equalsIgnoreCase( getTransAttributeString(
          transMeta.getObjectId(), 0, HopDatabaseRepository.TRANS_ATTRIBUTE_FEEDBACK_SHOWN ) ) );
        transMeta.setFeedbackSize( (int) getTransAttributeInteger(
          transMeta.getObjectId(), 0, HopDatabaseRepository.TRANS_ATTRIBUTE_FEEDBACK_SIZE ) );
        transMeta.setUsingThreadPriorityManagment( !"N".equalsIgnoreCase( getTransAttributeString( transMeta
          .getObjectId(), 0, HopDatabaseRepository.TRANS_ATTRIBUTE_USING_THREAD_PRIORITIES ) ) );

        // Performance monitoring for steps...
        //
        transMeta.setCapturingStepPerformanceSnapShots( getTransAttributeBoolean(
          transMeta.getObjectId(), 0, HopDatabaseRepository.TRANS_ATTRIBUTE_CAPTURE_STEP_PERFORMANCE ) );
        transMeta
          .setStepPerformanceCapturingDelay( getTransAttributeInteger(
            transMeta.getObjectId(), 0,
            HopDatabaseRepository.TRANS_ATTRIBUTE_STEP_PERFORMANCE_CAPTURING_DELAY ) );
        transMeta.setStepPerformanceCapturingSizeLimit( getTransAttributeString(
          transMeta.getObjectId(), 0,
          HopDatabaseRepository.TRANS_ATTRIBUTE_STEP_PERFORMANCE_CAPTURING_SIZE_LIMIT ) );
        transMeta.getPerformanceLogTable().setTableName(
          getTransAttributeString(
            transMeta.getObjectId(), 0, HopDatabaseRepository.TRANS_ATTRIBUTE_STEP_PERFORMANCE_LOG_TABLE ) );
        transMeta.getTransLogTable().setLogSizeLimit(
          getTransAttributeString(
            transMeta.getObjectId(), 0, HopDatabaseRepository.TRANS_ATTRIBUTE_LOG_SIZE_LIMIT ) );
        transMeta.getTransLogTable().setLogInterval(
          getTransAttributeString(
            transMeta.getObjectId(), 0, HopDatabaseRepository.TRANS_ATTRIBUTE_LOG_INTERVAL ) );
        transMeta.setTransformationType( TransformationType.getTransformationTypeByCode( getTransAttributeString(
          transMeta.getObjectId(), 0, HopDatabaseRepository.TRANS_ATTRIBUTE_TRANSFORMATION_TYPE ) ) );
        transMeta.setSleepTimeEmpty( (int) getTransAttributeInteger(
          transMeta.getObjectId(), 0, HopDatabaseRepository.TRANS_ATTRIBUTE_SLEEP_TIME_EMPTY ) );
        transMeta.setSleepTimeFull( (int) getTransAttributeInteger(
          transMeta.getObjectId(), 0, HopDatabaseRepository.TRANS_ATTRIBUTE_SLEEP_TIME_FULL ) );

        loadRepParameters( transMeta );
      }
    } catch ( HopDatabaseException dbe ) {
      throw new HopException( BaseMessages.getString(
        PKG, "TransMeta.Exception.UnableToLoadTransformationInfoFromRepository" ), dbe );
    } finally {
      transMeta.initializeVariablesFrom( null );
      transMeta.setInternalHopVariables();
    }
  }

  /**
   * Load the parameters of this transformation from the repository. The current ones already loaded will be erased.
   *
   * @throws HopException
   *           Upon any error.
   */
  private void loadRepParameters( TransMeta transMeta ) throws HopException {
    transMeta.eraseParameters();

    int count = countTransParameter( transMeta.getObjectId() );
    for ( int idx = 0; idx < count; idx++ ) {
      String key = getTransParameterKey( transMeta.getObjectId(), idx );
      String def = getTransParameterDefault( transMeta.getObjectId(), idx );
      String desc = getTransParameterDescription( transMeta.getObjectId(), idx );
      transMeta.addParameterDefinition( key, def, desc );
    }
  }

  /**
   * Count the number of parameters of a transaction.
   *
   * @param id_transformation
   *          transformation id
   * @return the number of transactions
   *
   * @throws HopException
   *           Upon any error.
   */
  public int countTransParameter( ObjectId id_transformation ) throws HopException {
    return repository.connectionDelegate.countNrTransAttributes(
      id_transformation, HopDatabaseRepository.TRANS_ATTRIBUTE_PARAM_KEY );
  }

  /**
   * Get a transformation parameter key. You can count the number of parameters up front.
   *
   * @param id_transformation
   *          transformation id
   * @param nr
   *          number of the parameter
   * @return they key/name of specified parameter
   *
   * @throws HopException
   *           Upon any error.
   */
  public String getTransParameterKey( ObjectId id_transformation, int nr ) throws HopException {
    return repository.connectionDelegate.getTransAttributeString(
      id_transformation, nr, HopDatabaseRepository.TRANS_ATTRIBUTE_PARAM_KEY );
  }

  /**
   * Get a transformation parameter default. You can count the number of parameters up front.
   *
   * @param id_transformation
   *          transformation id
   * @param nr
   *          number of the parameter
   * @return
   *
   * @throws HopException
   *           Upon any error.
   */
  public String getTransParameterDefault( ObjectId id_transformation, int nr ) throws HopException {
    return repository.connectionDelegate.getTransAttributeString(
      id_transformation, nr, HopDatabaseRepository.TRANS_ATTRIBUTE_PARAM_DEFAULT );
  }

  /**
   * Get a transformation parameter description. You can count the number of parameters up front.
   *
   * @param id_transformation
   *          transformation id
   * @param nr
   *          number of the parameter
   * @return
   *
   * @throws HopException
   *           Upon any error.
   */
  public String getTransParameterDescription( ObjectId id_transformation, int nr ) throws HopException {
    return repository.connectionDelegate.getTransAttributeString(
      id_transformation, nr, HopDatabaseRepository.TRANS_ATTRIBUTE_PARAM_DESCRIPTION );
  }

  /**
   * Insert a parameter for a transformation in the repository.
   *
   * @param id_transformation
   *          transformation id
   * @param nr
   *          number of the parameter to insert
   * @param key
   *          key to insert
   * @param defValue
   *          default value
   * @param description
   *          description to insert
   *
   * @throws HopException
   *           Upon any error.
   */
  public void insertTransParameter( ObjectId id_transformation, long nr, String key, String defValue,
    String description ) throws HopException {
    repository.connectionDelegate.insertTransAttribute(
      id_transformation, nr, HopDatabaseRepository.TRANS_ATTRIBUTE_PARAM_KEY, 0, key != null ? key : "" );
    repository.connectionDelegate.insertTransAttribute(
      id_transformation, nr, HopDatabaseRepository.TRANS_ATTRIBUTE_PARAM_DEFAULT, 0, defValue != null
        ? defValue : "" );
    repository.connectionDelegate.insertTransAttribute(
      id_transformation, nr, HopDatabaseRepository.TRANS_ATTRIBUTE_PARAM_DESCRIPTION, 0, description != null
        ? description : "" );
  }

  /**
   * Read all the databases from the repository, insert into the TransMeta object, overwriting optionally
   *
   * @param transMeta
   *          The transformation to load into.
   * @param overWriteShared
   *          if an object with the same name exists, overwrite
   * @throws HopException
   */
  public void readDatabases( TransMeta transMeta, boolean overWriteShared ) throws HopException {
    try {
      ObjectId[] dbids = repository.getDatabaseIDs( false );
      for ( int i = 0; i < dbids.length; i++ ) {
        DatabaseMeta databaseMeta = repository.loadDatabaseMeta( dbids[i], null ); // reads last version
        databaseMeta.shareVariablesWith( transMeta );

        DatabaseMeta check = transMeta.findDatabase( databaseMeta.getName() ); // Check if there already is one in the
                                                                               // transformation
        if ( check == null || overWriteShared ) { // We only add, never overwrite database connections.
          if ( databaseMeta.getName() != null ) {
            transMeta.addOrReplaceDatabase( databaseMeta );
            if ( !overWriteShared ) {
              databaseMeta.setChanged( false );
            }
          }
        }
      }
      transMeta.clearChangedDatabases();
    } catch ( HopDatabaseException dbe ) {
      throw new HopException( BaseMessages.getString(
        PKG, "TransMeta.Log.UnableToReadDatabaseIDSFromRepository" ), dbe );
    } catch ( HopException ke ) {
      throw new HopException(
        BaseMessages.getString( PKG, "TransMeta.Log.UnableToReadDatabasesFromRepository" ), ke );
    }
  }

  /**
   * Read the clusters in the repository and add them to this transformation if they are not yet present.
   *
   * @param transMeta
   *          The transformation to load into.
   * @param overWriteShared
   *          if an object with the same name exists, overwrite
   * @throws HopException
   */
  public void readClusters( TransMeta transMeta, boolean overWriteShared ) throws HopException {
    try {
      ObjectId[] dbids = repository.getClusterIDs( false );
      for ( int i = 0; i < dbids.length; i++ ) {
        ClusterSchema clusterSchema = repository.loadClusterSchema( dbids[i], transMeta.getSlaveServers(), null );
        clusterSchema.shareVariablesWith( transMeta );
        // Check if there already is one in the transformation
        ClusterSchema check = transMeta.findClusterSchema( clusterSchema.getName() );
        if ( check == null || overWriteShared ) {
          if ( !Utils.isEmpty( clusterSchema.getName() ) ) {
            transMeta.addOrReplaceClusterSchema( clusterSchema );
            if ( !overWriteShared ) {
              clusterSchema.setChanged( false );
            }
          }
        }
      }
    } catch ( HopDatabaseException dbe ) {
      throw new HopException(
        BaseMessages.getString( PKG, "TransMeta.Log.UnableToReadClustersFromRepository" ), dbe );
    }
  }

  /**
   * Read the partitions in the repository and add them to this transformation if they are not yet present.
   *
   * @param transMeta
   *          The transformation to load into.
   * @param overWriteShared
   *          if an object with the same name exists, overwrite
   * @throws HopException
   */
  public void readPartitionSchemas( TransMeta transMeta, boolean overWriteShared ) throws HopException {
    try {
      ObjectId[] dbids = repository.getPartitionSchemaIDs( false );
      for ( int i = 0; i < dbids.length; i++ ) {
        PartitionSchema partitionSchema = repository.loadPartitionSchema( dbids[i], null ); // Load last version
        PartitionSchema check = transMeta.findPartitionSchema( partitionSchema.getName() ); // Check if there already is
                                                                                            // one in the transformation
        if ( check == null || overWriteShared ) {
          if ( !Utils.isEmpty( partitionSchema.getName() ) ) {
            transMeta.addOrReplacePartitionSchema( partitionSchema );
            if ( !overWriteShared ) {
              partitionSchema.setChanged( false );
            }
          }
        }
      }
    } catch ( HopException dbe ) {
      throw new HopException( BaseMessages.getString(
        PKG, "TransMeta.Log.UnableToReadPartitionSchemaFromRepository" ), dbe );
    }
  }

  /**
   * Read the slave servers in the repository and add them to this transformation if they are not yet present.
   *
   * @param transMeta
   *          The transformation to load into.
   * @param overWriteShared
   *          if an object with the same name exists, overwrite
   * @throws HopException
   */
  public void readSlaves( TransMeta transMeta, boolean overWriteShared ) throws HopException {
    try {
      ObjectId[] dbids = repository.getSlaveIDs( false );
      for ( int i = 0; i < dbids.length; i++ ) {
        SlaveServer slaveServer = repository.loadSlaveServer( dbids[i], null ); // Load last version
        slaveServer.shareVariablesWith( transMeta );
        SlaveServer check = transMeta.findSlaveServer( slaveServer.getName() ); // Check if there already is one in the
                                                                                // transformation
        if ( check == null || overWriteShared ) {
          if ( !Utils.isEmpty( slaveServer.getName() ) ) {
            transMeta.addOrReplaceSlaveServer( slaveServer );
            if ( !overWriteShared ) {
              slaveServer.setChanged( false );
            }
          }
        }
      }
    } catch ( HopDatabaseException dbe ) {
      throw new HopException( BaseMessages.getString(
        PKG, "TransMeta.Log.UnableToReadSlaveServersFromRepository" ), dbe );
    }
  }

  public TransDependency loadTransDependency( ObjectId id_dependency, List<DatabaseMeta> databases ) throws HopException {
    TransDependency transDependency = new TransDependency();

    try {
      transDependency.setObjectId( id_dependency );

      RowMetaAndData r = getTransDependency( id_dependency );

      if ( r != null ) {
        long id_connection = r.getInteger( "ID_DATABASE", 0 );
        transDependency.setDatabase( DatabaseMeta.findDatabase( databases, new LongObjectId( id_connection ) ) );
        transDependency.setTablename( r.getString( "TABLE_NAME", null ) );
        transDependency.setFieldname( r.getString( "FIELD_NAME", null ) );
      }

      return transDependency;
    } catch ( HopException dbe ) {
      throw new HopException( BaseMessages.getString(
        PKG, "TransDependency.Exception.UnableToLoadTransformationDependency" )
        + id_dependency, dbe );
    }
  }

  public void saveTransDependency( TransDependency transDependency, ObjectId id_transformation ) throws HopException {
    try {
      ObjectId id_database =
        transDependency.getDatabase() == null ? null : transDependency.getDatabase().getObjectId();

      transDependency.setObjectId( insertDependency( id_transformation, id_database, transDependency
        .getTablename(), transDependency.getFieldname() ) );
    } catch ( HopException dbe ) {
      throw new HopException( BaseMessages.getString(
        PKG, "TransDependency.Exception.UnableToSaveTransformationDepency" )
        + id_transformation, dbe );
    }
  }

  public void saveTransHopMeta( TransHopMeta transHopMeta, ObjectId id_transformation ) throws HopException {
    try {
      // See if a transformation hop with the same fromstep and tostep is
      // already available...
      ObjectId id_step_from = transHopMeta.getFromStep() == null ? null : transHopMeta.getFromStep().getObjectId();
      ObjectId id_step_to = transHopMeta.getToStep() == null ? null : transHopMeta.getToStep().getObjectId();

      // Insert new transMeta hop in repository
      transHopMeta.setObjectId( insertTransHop( id_transformation, id_step_from, id_step_to, transHopMeta
        .isEnabled() ) );
    } catch ( HopDatabaseException dbe ) {
      throw new HopException( BaseMessages.getString(
        PKG, "TransHopMeta.Exception.UnableToSaveTransformationHopInfo" )
        + id_transformation, dbe );
    }
  }

  public TransHopMeta loadTransHopMeta( ObjectId id_trans_hop, List<StepMeta> steps ) throws HopException {
    TransHopMeta hopTransMeta = new TransHopMeta();
    try {
      hopTransMeta.setObjectId( id_trans_hop );

      RowMetaAndData r = getTransHop( id_trans_hop );

      hopTransMeta.setEnabled( r.getBoolean( "ENABLED", false ) );

      long id_step_from = r.getInteger( "ID_STEP_FROM", 0 );
      long id_step_to = r.getInteger( "ID_STEP_TO", 0 );

      StepMeta fromStep = StepMeta.findStep( steps, new LongObjectId( id_step_from ) );

      // Links to a shared objects, try again by looking up the name...
      //
      if ( fromStep == null && id_step_from > 0 ) {
        // Simply load this, we only want the name, we don't care about the
        // rest...
        //
        StepMeta stepMeta =
          repository.stepDelegate.loadStepMeta(
            new LongObjectId( id_step_from ), new ArrayList<DatabaseMeta>(), new ArrayList<PartitionSchema>() );
        fromStep = StepMeta.findStep( steps, stepMeta.getName() );
      }

      if ( fromStep == null ) {
        log.logError( "Unable to determine source step of transformation hop with ID: " + id_trans_hop );
        return null; // Invalid hop, simply ignore. See: PDI-2446
      }
      hopTransMeta.setFromStep( fromStep );

      hopTransMeta.getFromStep().setDraw( true );

      hopTransMeta.setToStep( StepMeta.findStep( steps, new LongObjectId( id_step_to ) ) );

      // Links to a shared objects, try again by looking up the name...
      //
      if ( hopTransMeta.getToStep() == null && id_step_to > 0 ) {
        // Simply load this, we only want the name, we don't care about
        // the rest...
        StepMeta stepMeta =
          repository.stepDelegate.loadStepMeta(
            new LongObjectId( id_step_to ), new ArrayList<DatabaseMeta>(), new ArrayList<PartitionSchema>() );
        hopTransMeta.setToStep( StepMeta.findStep( steps, stepMeta.getName() ) );
      }

      if ( hopTransMeta.getFromStep() == null ) {
        // This not a valid hop. Skipping it is better than refusing to load the transformation. PDI-5519
        //
        return null;
      }
      hopTransMeta.getToStep().setDraw( true );

      return hopTransMeta;
    } catch ( HopDatabaseException dbe ) {
      throw new HopException( BaseMessages.getString( PKG, "TransHopMeta.Exception.LoadTransformationHopInfo" )
        + id_trans_hop, dbe );
    }
  }

  public synchronized int getNrTransformations( ObjectId id_directory ) throws HopException {
    int retval = 0;

    RowMetaAndData par = repository.connectionDelegate.getParameterMetaData( id_directory );
    String sql =
      "SELECT COUNT(*) FROM "
        + quoteTable( HopDatabaseRepository.TABLE_R_TRANSFORMATION ) + " WHERE "
        + quote( HopDatabaseRepository.FIELD_TRANSFORMATION_ID_DIRECTORY ) + " = ? ";
    RowMetaAndData r = repository.connectionDelegate.getOneRow( sql, par.getRowMeta(), par.getData() );
    if ( r != null ) {
      retval = (int) r.getInteger( 0, 0L );
    }

    return retval;
  }

  public synchronized int getNrTransHops( ObjectId id_transformation ) throws HopException {
    int retval = 0;

    RowMetaAndData par = repository.connectionDelegate.getParameterMetaData( id_transformation );
    String sql =
      "SELECT COUNT(*) FROM "
        + quoteTable( HopDatabaseRepository.TABLE_R_TRANS_HOP ) + " WHERE "
        + quote( HopDatabaseRepository.FIELD_TRANS_HOP_ID_TRANSFORMATION ) + " = ? ";
    RowMetaAndData r = repository.connectionDelegate.getOneRow( sql, par.getRowMeta(), par.getData() );
    if ( r != null ) {
      retval = (int) r.getInteger( 0, 0L );
    }

    return retval;
  }

  public synchronized int getNrTransDependencies( ObjectId id_transformation ) throws HopException {
    int retval = 0;

    RowMetaAndData par = repository.connectionDelegate.getParameterMetaData( id_transformation );
    String sql =
      "SELECT COUNT(*) FROM "
        + quoteTable( HopDatabaseRepository.TABLE_R_DEPENDENCY ) + " WHERE "
        + quote( HopDatabaseRepository.FIELD_DEPENDENCY_ID_TRANSFORMATION ) + " = ? ";
    RowMetaAndData r = repository.connectionDelegate.getOneRow( sql, par.getRowMeta(), par.getData() );
    if ( r != null ) {
      retval = (int) r.getInteger( 0, 0L );
    }

    return retval;
  }

  public String[] getTransformationsWithIDList( List<Object[]> list, RowMetaInterface rowMeta ) throws HopException {
    String[] transList = new String[list.size()];
    for ( int i = 0; i < list.size(); i++ ) {
      long id_transformation =
        rowMeta.getInteger(
          list.get( i ), quote( HopDatabaseRepository.FIELD_TRANSFORMATION_ID_TRANSFORMATION ), -1L );
      if ( id_transformation > 0 ) {
        RowMetaAndData transRow = getTransformation( new LongObjectId( id_transformation ) );
        if ( transRow != null ) {
          String transName =
            transRow.getString( HopDatabaseRepository.FIELD_TRANSFORMATION_NAME, "<name not found>" );
          long id_directory =
            transRow.getInteger( HopDatabaseRepository.FIELD_TRANSFORMATION_ID_DIRECTORY, -1L );
          RepositoryDirectoryInterface dir =
            repository.loadRepositoryDirectoryTree().findDirectory( new LongObjectId( id_directory ) );

          transList[i] = dir.getPathObjectCombination( transName );
        }
      }
    }

    return transList;
  }

  public String[] getTransformationsWithIDList( ObjectId[] ids ) throws HopException {
    String[] transList = new String[ids.length];
    for ( int i = 0; i < ids.length; i++ ) {
      ObjectId id_transformation = ids[i];
      if ( id_transformation != null ) {
        RowMetaAndData transRow = getTransformation( id_transformation );
        if ( transRow != null ) {
          String transName =
            transRow.getString( HopDatabaseRepository.FIELD_TRANSFORMATION_NAME, "<name not found>" );
          long id_directory =
            transRow.getInteger( HopDatabaseRepository.FIELD_TRANSFORMATION_ID_DIRECTORY, -1L );
          RepositoryDirectoryInterface dir =
            repository.loadRepositoryDirectoryTree().findDirectory( new LongObjectId( id_directory ) );

          transList[i] = dir.getPathObjectCombination( transName );
        }
      }
    }

    return transList;
  }

  public boolean existsTransMeta( String transname, RepositoryDirectory directory ) throws HopException {
    return getTransformationID( transname, directory.getObjectId() ) != null;
  }

  public ObjectId[] getTransHopIDs( ObjectId id_transformation ) throws HopException {
    return repository.connectionDelegate.getIDs( "SELECT "
      + quote( HopDatabaseRepository.FIELD_TRANS_HOP_ID_TRANS_HOP ) + " FROM "
      + quoteTable( HopDatabaseRepository.TABLE_R_TRANS_HOP ) + " WHERE "
      + quote( HopDatabaseRepository.FIELD_TRANS_HOP_ID_TRANSFORMATION ) + " = ? ", id_transformation );
  }

  //CHECKSTYLE:LineLength:OFF
  private synchronized void insertTransformation( TransMeta transMeta ) throws HopException {
    RowMetaAndData table = new RowMetaAndData();

    table.addValue( new ValueMetaInteger( HopDatabaseRepository.FIELD_TRANSFORMATION_ID_TRANSFORMATION ), new LongObjectId( transMeta.getObjectId() ) );
    table.addValue( new ValueMetaString( HopDatabaseRepository.FIELD_TRANSFORMATION_NAME ), transMeta.getName() );
    table.addValue( new ValueMetaString( HopDatabaseRepository.FIELD_TRANSFORMATION_DESCRIPTION ), transMeta.getDescription() );
    table.addValue( new ValueMetaString( HopDatabaseRepository.FIELD_TRANSFORMATION_EXTENDED_DESCRIPTION ), transMeta.getExtendedDescription() );
    table.addValue( new ValueMetaString( HopDatabaseRepository.FIELD_TRANSFORMATION_TRANS_VERSION ), transMeta.getTransversion() );
    table.addValue( new ValueMetaInteger( HopDatabaseRepository.FIELD_TRANSFORMATION_TRANS_STATUS ), new Long( transMeta.getTransstatus() < 0 ? -1L : transMeta.getTransstatus() ) );
    TransLogTable logTable = transMeta.getTransLogTable();
    StepMeta step = (StepMeta) logTable.getSubject( TransLogTable.ID.LINES_READ );
    table.addValue( new ValueMetaInteger( HopDatabaseRepository.FIELD_TRANSFORMATION_ID_STEP_READ ), step == null ? null : step.getObjectId() );
    step = (StepMeta) logTable.getSubject( TransLogTable.ID.LINES_WRITTEN );
    table.addValue( new ValueMetaInteger( HopDatabaseRepository.FIELD_TRANSFORMATION_ID_STEP_WRITE ), step == null ? null : step.getObjectId() );
    step = (StepMeta) logTable.getSubject( TransLogTable.ID.LINES_INPUT );
    table.addValue( new ValueMetaInteger( HopDatabaseRepository.FIELD_TRANSFORMATION_ID_STEP_INPUT ), step == null ? null : step.getObjectId() );
    step = (StepMeta) logTable.getSubject( TransLogTable.ID.LINES_OUTPUT );
    table.addValue( new ValueMetaInteger( HopDatabaseRepository.FIELD_TRANSFORMATION_ID_STEP_OUTPUT ), step == null ? null : step.getObjectId() );
    step = (StepMeta) logTable.getSubject( TransLogTable.ID.LINES_UPDATED );
    table.addValue( new ValueMetaInteger( HopDatabaseRepository.FIELD_TRANSFORMATION_ID_STEP_UPDATE ), step == null ? null : step.getObjectId() );

    table.addValue( new ValueMetaInteger( HopDatabaseRepository.FIELD_TRANSFORMATION_ID_DATABASE_LOG ), logTable.getDatabaseMeta() == null ? new LongObjectId( -1L ).longValue() : new LongObjectId( logTable.getDatabaseMeta().getObjectId() ).longValue() );
    table.addValue( new ValueMetaString( HopDatabaseRepository.FIELD_TRANSFORMATION_TABLE_NAME_LOG ), logTable.getDatabaseMeta() );
    table.addValue( new ValueMetaBoolean( HopDatabaseRepository.FIELD_TRANSFORMATION_USE_BATCHID ), Boolean.valueOf( logTable.isBatchIdUsed() ) );
    table.addValue( new ValueMetaBoolean( HopDatabaseRepository.FIELD_TRANSFORMATION_USE_LOGFIELD ), Boolean.valueOf( logTable.isLogFieldUsed() ) );
    table.addValue( new ValueMetaInteger( HopDatabaseRepository.FIELD_TRANSFORMATION_ID_DATABASE_MAXDATE ), transMeta.getMaxDateConnection() == null ? new LongObjectId( -1L ).longValue() : new LongObjectId( transMeta.getMaxDateConnection().getObjectId() ).longValue() );
    table.addValue( new ValueMetaString( HopDatabaseRepository.FIELD_TRANSFORMATION_TABLE_NAME_MAXDATE ), transMeta.getMaxDateTable() );
    table.addValue( new ValueMetaString( HopDatabaseRepository.FIELD_TRANSFORMATION_FIELD_NAME_MAXDATE ), transMeta.getMaxDateField() );
    table.addValue( new ValueMetaNumber( HopDatabaseRepository.FIELD_TRANSFORMATION_OFFSET_MAXDATE ), new Double( transMeta.getMaxDateOffset() ) );
    table.addValue( new ValueMetaNumber( HopDatabaseRepository.FIELD_TRANSFORMATION_DIFF_MAXDATE ), new Double( transMeta.getMaxDateDifference() ) );

    table.addValue( new ValueMetaString( HopDatabaseRepository.FIELD_TRANSFORMATION_CREATED_USER ), transMeta.getCreatedUser() );
    table.addValue( new ValueMetaDate( HopDatabaseRepository.FIELD_TRANSFORMATION_CREATED_DATE ), transMeta.getCreatedDate() );

    table.addValue( new ValueMetaString( HopDatabaseRepository.FIELD_TRANSFORMATION_MODIFIED_USER ), transMeta.getModifiedUser() );
    table.addValue( new ValueMetaDate( HopDatabaseRepository.FIELD_TRANSFORMATION_MODIFIED_DATE ), transMeta.getModifiedDate() );
    table.addValue( new ValueMetaInteger( HopDatabaseRepository.FIELD_TRANSFORMATION_SIZE_ROWSET ), new Long( transMeta.getSizeRowset() ) );
    table.addValue( new ValueMetaInteger( HopDatabaseRepository.FIELD_TRANSFORMATION_ID_DIRECTORY ), transMeta.getRepositoryDirectory().getObjectId() );

    repository.connectionDelegate.getDatabase().prepareInsert( table.getRowMeta(), HopDatabaseRepository.TABLE_R_TRANSFORMATION );
    repository.connectionDelegate.getDatabase().setValuesInsert( table );
    repository.connectionDelegate.getDatabase().insertRow();
    repository.connectionDelegate.getDatabase().closeInsert();

    step = (StepMeta) logTable.getSubject( TransLogTable.ID.LINES_REJECTED );
    if ( step != null ) {
      ObjectId rejectedId = step.getObjectId();
      Preconditions.checkNotNull( rejectedId );
      repository.connectionDelegate.insertTransAttribute(
        transMeta.getObjectId(), 0, HopDatabaseRepository.TRANS_ATTRIBUTE_ID_STEP_REJECTED,
        Long.valueOf( rejectedId.toString() ), null );
    }

    repository.connectionDelegate.insertTransAttribute(
      transMeta.getObjectId(), 0, HopDatabaseRepository.TRANS_ATTRIBUTE_UNIQUE_CONNECTIONS, 0, transMeta
        .isUsingUniqueConnections() ? "Y" : "N" );
    repository.connectionDelegate.insertTransAttribute(
      transMeta.getObjectId(), 0, HopDatabaseRepository.TRANS_ATTRIBUTE_FEEDBACK_SHOWN, 0, transMeta
        .isFeedbackShown() ? "Y" : "N" );
    repository.connectionDelegate.insertTransAttribute(
      transMeta.getObjectId(), 0, HopDatabaseRepository.TRANS_ATTRIBUTE_FEEDBACK_SIZE, transMeta
        .getFeedbackSize(), "" );
    repository.connectionDelegate.insertTransAttribute(
      transMeta.getObjectId(), 0, HopDatabaseRepository.TRANS_ATTRIBUTE_USING_THREAD_PRIORITIES, 0, transMeta
        .isUsingThreadPriorityManagment() ? "Y" : "N" );
    repository.connectionDelegate.insertTransAttribute(
      transMeta.getObjectId(), 0, HopDatabaseRepository.TRANS_ATTRIBUTE_SHARED_FILE, 0, transMeta
        .getSharedObjectsFile() );

    repository.connectionDelegate.insertTransAttribute(
      transMeta.getObjectId(), 0, HopDatabaseRepository.TRANS_ATTRIBUTE_CAPTURE_STEP_PERFORMANCE, 0,
      transMeta.isCapturingStepPerformanceSnapShots() ? "Y" : "N" );
    repository.connectionDelegate.insertTransAttribute(
      transMeta.getObjectId(), 0, HopDatabaseRepository.TRANS_ATTRIBUTE_STEP_PERFORMANCE_CAPTURING_DELAY,
      transMeta.getStepPerformanceCapturingDelay(), "" );
    repository.connectionDelegate.insertTransAttribute(
      transMeta.getObjectId(), 0,
      HopDatabaseRepository.TRANS_ATTRIBUTE_STEP_PERFORMANCE_CAPTURING_SIZE_LIMIT, 0, transMeta
        .getStepPerformanceCapturingSizeLimit() );
    repository.connectionDelegate.insertTransAttribute(
      transMeta.getObjectId(), 0, HopDatabaseRepository.TRANS_ATTRIBUTE_STEP_PERFORMANCE_LOG_TABLE, 0,
      transMeta.getPerformanceLogTable().getTableName() );

    repository.connectionDelegate.insertTransAttribute(
      transMeta.getObjectId(), 0, HopDatabaseRepository.TRANS_ATTRIBUTE_LOG_SIZE_LIMIT, 0, transMeta
        .getTransLogTable().getLogSizeLimit() );
    repository.connectionDelegate.insertTransAttribute(
      transMeta.getObjectId(), 0, HopDatabaseRepository.TRANS_ATTRIBUTE_LOG_INTERVAL, 0, transMeta
        .getTransLogTable().getLogInterval() );
    repository.connectionDelegate.insertTransAttribute(
      transMeta.getObjectId(), 0, HopDatabaseRepository.TRANS_ATTRIBUTE_TRANSFORMATION_TYPE, 0, transMeta
        .getTransformationType().getCode() );

    repository.connectionDelegate.insertTransAttribute(
      transMeta.getObjectId(), 0, HopDatabaseRepository.TRANS_ATTRIBUTE_SLEEP_TIME_EMPTY, transMeta
        .getSleepTimeEmpty(), null );
    repository.connectionDelegate.insertTransAttribute(
      transMeta.getObjectId(), 0, HopDatabaseRepository.TRANS_ATTRIBUTE_SLEEP_TIME_FULL, transMeta
        .getSleepTimeFull(), null );

    // Save the logging connection link...
    if ( logTable.getDatabaseMeta() != null ) {
      repository.insertStepDatabase( transMeta.getObjectId(), null, logTable.getDatabaseMeta().getObjectId() );
    }

    // Save the maxdate connection link...
    if ( transMeta.getMaxDateConnection() != null ) {
      repository
        .insertStepDatabase( transMeta.getObjectId(), null, transMeta.getMaxDateConnection().getObjectId() );
    }

    // Save the logging tables too..
    //
    RepositoryAttributeInterface attributeInterface =
      new HopDatabaseRepositoryTransAttribute( repository.connectionDelegate, transMeta.getObjectId() );
    transMeta.getTransLogTable().saveToRepository( attributeInterface );
    transMeta.getStepLogTable().saveToRepository( attributeInterface );
    transMeta.getPerformanceLogTable().saveToRepository( attributeInterface );
    transMeta.getChannelLogTable().saveToRepository( attributeInterface );
  }

  private synchronized ObjectId insertTransHop( ObjectId id_transformation, ObjectId id_step_from,
    ObjectId id_step_to, boolean enabled ) throws HopException {
    ObjectId id = repository.connectionDelegate.getNextTransHopID();

    RowMetaAndData table = new RowMetaAndData();

    table.addValue( new ValueMetaInteger(
      HopDatabaseRepository.FIELD_TRANS_HOP_ID_TRANS_HOP ), id );
    table
      .addValue(
        new ValueMetaInteger(
          HopDatabaseRepository.FIELD_TRANS_HOP_ID_TRANSFORMATION ),
        id_transformation );
    table.addValue( new ValueMetaInteger(
      HopDatabaseRepository.FIELD_TRANS_HOP_ID_STEP_FROM ), id_step_from );
    table.addValue( new ValueMetaInteger(
      HopDatabaseRepository.FIELD_TRANS_HOP_ID_STEP_TO ), id_step_to );
    table.addValue( new ValueMetaBoolean(
      HopDatabaseRepository.FIELD_TRANS_HOP_ENABLED ), Boolean
      .valueOf( enabled ) );

    repository.connectionDelegate.getDatabase().prepareInsert(
      table.getRowMeta(), HopDatabaseRepository.TABLE_R_TRANS_HOP );
    repository.connectionDelegate.getDatabase().setValuesInsert( table );
    repository.connectionDelegate.getDatabase().insertRow();
    repository.connectionDelegate.getDatabase().closeInsert();

    return id;
  }

  private synchronized ObjectId insertDependency( ObjectId id_transformation, ObjectId id_database,
    String tablename, String fieldname ) throws HopException {
    ObjectId id = repository.connectionDelegate.getNextDepencencyID();

    RowMetaAndData table = new RowMetaAndData();

    table.addValue( new ValueMetaInteger(
      HopDatabaseRepository.FIELD_DEPENDENCY_ID_DEPENDENCY ), id );
    table.addValue(
      new ValueMetaInteger(
        HopDatabaseRepository.FIELD_DEPENDENCY_ID_TRANSFORMATION ),
      id_transformation );
    table.addValue( new ValueMetaInteger(
      HopDatabaseRepository.FIELD_DEPENDENCY_ID_DATABASE ), id_database );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_DEPENDENCY_TABLE_NAME ), tablename );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_DEPENDENCY_FIELD_NAME ), fieldname );

    repository.connectionDelegate.getDatabase().prepareInsert(
      table.getRowMeta(), HopDatabaseRepository.TABLE_R_DEPENDENCY );
    repository.connectionDelegate.getDatabase().setValuesInsert( table );
    repository.connectionDelegate.getDatabase().insertRow();
    repository.connectionDelegate.getDatabase().closeInsert();

    return id;
  }

  public boolean getTransAttributeBoolean( ObjectId id_transformation, int nr, String code ) throws HopException {
    return repository.connectionDelegate.getTransAttributeBoolean( id_transformation, nr, code );
  }

  public String getTransAttributeString( ObjectId id_transformation, int nr, String code ) throws HopException {
    return repository.connectionDelegate.getTransAttributeString( id_transformation, nr, code );
  }

  public long getTransAttributeInteger( ObjectId id_transformation, int nr, String code ) throws HopException {
    return repository.connectionDelegate.getTransAttributeInteger( id_transformation, nr, code );
  }

  public SharedObjects readTransSharedObjects( TransMeta transMeta ) throws HopException {

    transMeta.setSharedObjectsFile( getTransAttributeString( transMeta.getObjectId(), 0, "SHARED_FILE" ) );

    transMeta.setSharedObjects( transMeta.readSharedObjects() );

    // Repository objects take priority so let's overwrite them...
    //
    readDatabases( transMeta, true );
    readPartitionSchemas( transMeta, true );
    readSlaves( transMeta, true );
    readClusters( transMeta, true );

    return transMeta.getSharedObjects();
  }

  public synchronized void moveTransformation( String transname, ObjectId id_directory_from,
    ObjectId id_directory_to ) throws HopException {
    String nameField = quote( HopDatabaseRepository.FIELD_TRANSFORMATION_NAME );
    String sql =
      "UPDATE "
        + quoteTable( HopDatabaseRepository.TABLE_R_TRANSFORMATION ) + " SET "
        + quote( HopDatabaseRepository.FIELD_TRANSFORMATION_ID_DIRECTORY ) + " = ? WHERE " + nameField
        + " = ? AND " + quote( HopDatabaseRepository.FIELD_TRANSFORMATION_ID_DIRECTORY ) + " = ?";

    RowMetaAndData par = new RowMetaAndData();
    par
      .addValue(
        new ValueMetaInteger(
          HopDatabaseRepository.FIELD_TRANSFORMATION_ID_DIRECTORY ),
        id_directory_to );
    par.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_TRANSFORMATION_NAME ), transname );
    par
      .addValue(
        new ValueMetaInteger(
          HopDatabaseRepository.FIELD_TRANSFORMATION_ID_DIRECTORY ),
        id_directory_from );

    repository.connectionDelegate.getDatabase().execStatement( sql, par.getRowMeta(), par.getData() );
  }

  public synchronized void renameTransformation( ObjectId id_transformation,
    RepositoryDirectoryInterface newParentDir, String newname ) throws HopException {
    if ( newParentDir != null || newname != null ) {
      RowMetaAndData table = new RowMetaAndData();
      String sql = "UPDATE " + quoteTable( HopDatabaseRepository.TABLE_R_TRANSFORMATION ) + " SET ";

      boolean additionalParameter = false;

      if ( newname != null ) {
        additionalParameter = true;
        sql += quote( HopDatabaseRepository.FIELD_TRANSFORMATION_NAME ) + " = ? ";
        table.addValue( new ValueMetaString(
          HopDatabaseRepository.FIELD_TRANSFORMATION_NAME ), newname );
      }

      if ( newParentDir != null ) {
        if ( additionalParameter ) {
          sql += ", ";
        }
        sql += quote( HopDatabaseRepository.FIELD_TRANSFORMATION_ID_DIRECTORY ) + " = ? ";
        table.addValue(
          new ValueMetaInteger(
            HopDatabaseRepository.FIELD_TRANSFORMATION_ID_DIRECTORY ),
          newParentDir.getObjectId() );
      }

      sql += "WHERE " + quote( HopDatabaseRepository.FIELD_TRANSFORMATION_ID_TRANSFORMATION ) + " = ?";
      table.addValue(
        new ValueMetaInteger(
          HopDatabaseRepository.FIELD_TRANSFORMATION_ID_TRANSFORMATION ),
        id_transformation );

      log.logBasic( "sql = [" + sql + "]" );
      log.logBasic( "row = [" + table + "]" );

      repository.connectionDelegate.getDatabase().execStatement( sql, table.getRowMeta(), table.getData() );
      repository.connectionDelegate.getDatabase().commit();
    }
  }

  private void saveTransAttributesMap( ObjectId transformationId, Map<String, Map<String, String>> attributesMap ) throws HopException {

    for ( final String groupName : attributesMap.keySet() ) {
      Map<String, String> attributes = attributesMap.get( groupName );
      for ( final String key : attributes.keySet() ) {
        final String value = attributes.get( key );
        if ( key != null && value != null ) {
          repository.connectionDelegate.insertTransAttribute( transformationId, 0, TRANS_ATTRIBUTE_PREFIX
            + groupName + TRANS_ATTRIBUTE_PREFIX_DELIMITER + key, 0, value );
        }
      }
    }
  }

  private Map<String, Map<String, String>> loadTransAttributesMap( ObjectId transformationId ) throws HopException {
    Map<String, Map<String, String>> attributesMap = new HashMap<String, Map<String, String>>();

    List<Object[]> attributeRows =
      repository.connectionDelegate.getTransAttributesWithPrefix( transformationId, TRANS_ATTRIBUTE_PREFIX );
    RowMetaInterface rowMeta = repository.connectionDelegate.getReturnRowMeta();
    for ( Object[] attributeRow : attributeRows ) {
      String code = rowMeta.getString( attributeRow, HopDatabaseRepository.FIELD_TRANS_ATTRIBUTE_CODE, null );
      String value =
        rowMeta.getString( attributeRow, HopDatabaseRepository.FIELD_TRANS_ATTRIBUTE_VALUE_STR, null );
      if ( code != null && value != null ) {
        code = code.substring( TRANS_ATTRIBUTE_PREFIX.length() );
        int tabIndex = code.indexOf( TRANS_ATTRIBUTE_PREFIX_DELIMITER );
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
