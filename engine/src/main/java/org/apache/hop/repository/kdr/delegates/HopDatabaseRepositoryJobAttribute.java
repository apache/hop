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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.repository.ObjectId;
import org.apache.hop.repository.RepositoryAttributeInterface;

public class HopDatabaseRepositoryJobAttribute implements RepositoryAttributeInterface {

  private HopDatabaseRepositoryConnectionDelegate connectionDelegate;
  private ObjectId jobObjectId;

  public HopDatabaseRepositoryJobAttribute( HopDatabaseRepositoryConnectionDelegate connectionDelegate,
    ObjectId jobObjectId ) {
    this.connectionDelegate = connectionDelegate;
    this.jobObjectId = jobObjectId;
  }

  public boolean getAttributeBoolean( String code ) throws HopException {
    return connectionDelegate.getJobAttributeBoolean( jobObjectId, 0, code );
  }

  public long getAttributeInteger( String code ) throws HopException {
    return connectionDelegate.getJobAttributeInteger( jobObjectId, 0, code );
  }

  public String getAttributeString( String code ) throws HopException {
    return connectionDelegate.getJobAttributeString( jobObjectId, 0, code );
  }

  public void setAttribute( String code, String value ) throws HopException {
    connectionDelegate.insertJobAttribute( jobObjectId, 0, code, 0, value );
  }

  public void setAttribute( String code, boolean value ) throws HopException {
    connectionDelegate.insertJobAttribute( jobObjectId, 0, code, 0, value ? "Y" : "N" );
  }

  public void setAttribute( String code, long value ) throws HopException {
    connectionDelegate.insertJobAttribute( jobObjectId, 0, code, value, null );
  }
}
