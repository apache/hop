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

import org.apache.hop.core.exception.HopException;

/**
 * This interface defines any security management related APIs that are required for a repository.
 *
 */
public interface RepositorySecurityManager extends IRepositoryService {

  public List<IUser> getUsers() throws HopException;

  public void setUsers( List<IUser> users ) throws HopException;

  public ObjectId getUserID( String login ) throws HopException;

  public void delUser( ObjectId id_user ) throws HopException;

  public void delUser( String name ) throws HopException;

  public ObjectId[] getUserIDs() throws HopException;

  public void saveUserInfo( IUser user ) throws HopException;

  public void renameUser( ObjectId id_user, String newname ) throws HopException;

  public IUser constructUser() throws HopException;

  public void updateUser( IUser user ) throws HopException;

  public void deleteUsers( List<IUser> users ) throws HopException;

  public IUser loadUserInfo( String username ) throws HopException;

  public boolean isManaged() throws HopException;
}
