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

import java.util.List;

import org.apache.hop.core.ProgressMonitorListener;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;

/**
 * Implementing convenience methods that can be described in terms of other methods in the interface
 */
public abstract class AbstractRepository implements Repository {

  @Override
  public long getJobEntryAttributeInteger( ObjectId id_jobentry, String code ) throws HopException {
    return getJobEntryAttributeInteger( id_jobentry, 0, code );
  }

  @Override
  public String getJobEntryAttributeString( ObjectId id_jobentry, String code ) throws HopException {
    return getJobEntryAttributeString( id_jobentry, 0, code );
  }

  @Override
  public boolean getJobEntryAttributeBoolean( ObjectId id_jobentry, String code ) throws HopException {
    return getJobEntryAttributeBoolean( id_jobentry, 0, code );
  }

  @Override
  public boolean getJobEntryAttributeBoolean( ObjectId id_jobentry, String code, boolean def ) throws HopException {
    return getJobEntryAttributeBoolean( id_jobentry, 0, code, def );
  }

  @Override
  public boolean getJobEntryAttributeBoolean( ObjectId id_jobentry, int nr, String code ) throws HopException {
    return getJobEntryAttributeBoolean( id_jobentry, nr, code, false );
  }

  public abstract boolean getJobEntryAttributeBoolean( ObjectId id_jobentry, int nr, String code, boolean def ) throws HopException;

  @Override
  public boolean getStepAttributeBoolean( ObjectId id_step, String code ) throws HopException {
    return getStepAttributeBoolean( id_step, 0, code );
  }

  @Override
  public boolean getStepAttributeBoolean( ObjectId id_step, int nr, String code ) throws HopException {
    return getStepAttributeBoolean( id_step, nr, code, false );
  }

  @Override
  public long getStepAttributeInteger( ObjectId id_step, String code ) throws HopException {
    return getStepAttributeInteger( id_step, 0, code );
  }

  @Override
  public String getStepAttributeString( ObjectId id_step, String code ) throws HopException {
    return getStepAttributeString( id_step, 0, code );
  }

  @Override
  public void saveStepAttribute( ObjectId id_transformation, ObjectId id_step, String code, boolean value ) throws HopException {
    saveStepAttribute( id_transformation, id_step, 0, code, value );
  }

  @Override
  public void saveStepAttribute( ObjectId id_transformation, ObjectId id_step, String code, double value ) throws HopException {
    saveStepAttribute( id_transformation, id_step, 0, code, value );
  }

  @Override
  public void saveStepAttribute( ObjectId id_transformation, ObjectId id_step, String code, long value ) throws HopException {
    saveStepAttribute( id_transformation, id_step, 0, code, value );
  }

  @Override
  public void saveStepAttribute( ObjectId id_transformation, ObjectId id_step, String code, String value ) throws HopException {
    saveStepAttribute( id_transformation, id_step, 0, code, value );
  }

  @Override
  public void saveJobEntryAttribute( ObjectId id_job, ObjectId id_jobentry, String code, boolean value ) throws HopException {
    saveJobEntryAttribute( id_job, id_jobentry, 0, code, value );
  }

  @Override
  public void saveJobEntryAttribute( ObjectId id_job, ObjectId id_jobentry, String code, long value ) throws HopException {
    saveJobEntryAttribute( id_job, id_jobentry, 0, code, value );
  }

  @Override
  public void saveJobEntryAttribute( ObjectId id_job, ObjectId id_jobentry, String code, String value ) throws HopException {
    saveJobEntryAttribute( id_job, id_jobentry, 0, code, value );
  }

  @Override
  public DatabaseMeta loadDatabaseMetaFromJobEntryAttribute( ObjectId id_jobentry, String nameCode, String idCode,
    List<DatabaseMeta> databases ) throws HopException {
    return loadDatabaseMetaFromJobEntryAttribute( id_jobentry, nameCode, 0, idCode, databases );
  }

  @Override
  public void save( RepositoryElementInterface repoElement, String versionComment, ProgressMonitorListener monitor ) throws HopException {
    save( repoElement, versionComment, monitor, false );
  }

  @Override
  public void saveDatabaseMetaJobEntryAttribute( ObjectId id_job, ObjectId id_jobentry, String nameCode,
    String idCode, DatabaseMeta database ) throws HopException {
    saveDatabaseMetaJobEntryAttribute( id_job, id_jobentry, 0, nameCode, idCode, database );
  }

  public boolean test() {
    return true;
  }

  public void create() {

  }

}
