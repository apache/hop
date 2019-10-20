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

package org.apache.hop.repository.kdr;

import java.util.ArrayList;
import java.util.List;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopSecurityException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.repository.BaseRepositorySecurityProvider;
import org.apache.hop.repository.IUser;
import org.apache.hop.repository.ObjectId;
import org.apache.hop.repository.RepositoryCapabilities;
import org.apache.hop.repository.RepositoryCommonValidations;
import org.apache.hop.repository.RepositoryMeta;
import org.apache.hop.repository.RepositoryOperation;
import org.apache.hop.repository.RepositorySecurityManager;
import org.apache.hop.repository.RepositorySecurityProvider;
import org.apache.hop.repository.RepositorySecurityUserValidator;
import org.apache.hop.repository.UserInfo;
import org.apache.hop.repository.kdr.delegates.HopDatabaseRepositoryConnectionDelegate;
import org.apache.hop.repository.kdr.delegates.HopDatabaseRepositoryUserDelegate;

public class HopDatabaseRepositorySecurityProvider extends BaseRepositorySecurityProvider implements
  RepositorySecurityProvider, RepositorySecurityManager, RepositorySecurityUserValidator {

  private RepositoryCapabilities capabilities;

  private HopDatabaseRepository repository;

  private HopDatabaseRepositoryUserDelegate userDelegate;

  private HopDatabaseRepositoryConnectionDelegate connectionDelegate;

  /**
   * @param repository
   * @param userInfo
   */
  public HopDatabaseRepositorySecurityProvider( HopDatabaseRepository repository,
                                                   RepositoryMeta repositoryMeta, IUser userInfo ) {
    super( repositoryMeta, userInfo );
    this.repository = repository;
    this.capabilities = repositoryMeta.getRepositoryCapabilities();

    // This object is initialized last in the HopDatabaseRepository constructor.
    // As such it's safe to keep references here to the delegates...
    //
    userDelegate = repository.userDelegate;
    connectionDelegate = repository.connectionDelegate;
  }

  public boolean isReadOnly() {
    return capabilities.isReadOnly();
  }

  public boolean isLockingPossible() {
    return capabilities.supportsLocking();
  }

  public boolean allowsVersionComments( String fullPath ) {
    return false;
  }

  public boolean isVersionCommentMandatory() {
    return false;
  }

  // UserInfo

  public IUser loadUserInfo( String login ) throws HopException {
    return userDelegate.loadUserInfo( new UserInfo(), login );
  }

  /**
   * This method creates new user after all validations have been done. For updating user's data please use {@linkplain
   * #updateUser(IUser)}.
   *
   * @param userInfo user's info
   * @throws HopException
   * @throws IllegalArgumentException if {@code userInfo.getObjectId() != null}
   */
  public void saveUserInfo( IUser userInfo ) throws HopException {
    normalizeUserInfo( userInfo );
    if ( !validateUserInfo( userInfo ) ) {
      throw new HopException( BaseMessages.getString( HopDatabaseRepositorySecurityProvider.class,
        "HopDatabaseRepositorySecurityProvider.ERROR_0001_UNABLE_TO_CREATE_USER" ) );
    }

    if ( userInfo.getObjectId() != null ) {
      // not a message for UI
      throw new IllegalArgumentException( "Use updateUser() for updating" );
    }

    String userLogin = userInfo.getLogin();
    ObjectId exactMatch = userDelegate.getUserID( userLogin );
    if ( exactMatch != null ) {
      // found the corresponding record in db, prohibit creation!
      throw new HopException( BaseMessages.getString( HopDatabaseRepositorySecurityProvider.class,
        "HopDatabaseRepositorySecurityProvider.ERROR_0001_USER_NAME_ALREADY_EXISTS" ) );
    }

    userDelegate.saveUserInfo( userInfo );
  }

  public void validateAction( RepositoryOperation... operations ) throws HopException, HopSecurityException {

  }

  public synchronized void delUser( ObjectId id_user ) throws HopException {
    repository.connectionDelegate.performDelete( "DELETE FROM "
      + repository.quoteTable( HopDatabaseRepository.TABLE_R_USER ) + " WHERE "
      + repository.quote( HopDatabaseRepository.FIELD_USER_ID_USER ) + " = ? ", id_user );
  }

  public synchronized ObjectId getUserID( String login ) throws HopException {
    return userDelegate.getUserID( login );
  }

  public ObjectId[] getUserIDs() throws HopException {
    return connectionDelegate.getIDs( "SELECT "
      + repository.quote( HopDatabaseRepository.FIELD_USER_ID_USER ) + " FROM "
      + repository.quoteTable( HopDatabaseRepository.TABLE_R_USER ) );
  }

  public synchronized String[] getUserLogins() throws HopException {
    String loginField = repository.quote( HopDatabaseRepository.FIELD_USER_LOGIN );
    return connectionDelegate.getStrings( "SELECT "
      + loginField + " FROM " + repository.quoteTable( HopDatabaseRepository.TABLE_R_USER ) + " ORDER BY "
      + loginField );
  }

  public synchronized void renameUser( ObjectId id_user, String newname ) throws HopException {
    userDelegate.renameUser( id_user, newname );
  }

  public void deleteUsers( List<IUser> users ) throws HopException {
    throw new UnsupportedOperationException();
  }

  public List<IUser> getUsers() throws HopException {
    String[] userLogins = getUserLogins();
    List<IUser> users = new ArrayList<IUser>();
    for ( String userLogin : userLogins ) {
      users.add( loadUserInfo( userLogin ) );
    }
    return users;
  }

  public void setUsers( List<IUser> users ) throws HopException {
    throw new UnsupportedOperationException();
  }

  public void delUser( String name ) throws HopException {
    delUser( getUserID( name ) );
  }

  public void updateUser( IUser user ) throws HopException {
    userDelegate.saveUserInfo( user );
  }

  public IUser constructUser() throws HopException {
    return new UserInfo();
  }

  public List<String> getAllRoles() throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  public List<String> getAllUsers() throws HopException {
    // TODO Auto-generated method stub
    return null;
  }

  public boolean isManaged() throws HopException {
    return true;
  }

  @Override
  public boolean isVersioningEnabled( String fullPath ) {
    return false;
  }


  @Override
  public boolean validateUserInfo( IUser user ) {
    return RepositoryCommonValidations.checkUserInfo( user );
  }

  @Override
  public void normalizeUserInfo( IUser user ) {
    RepositoryCommonValidations.normalizeUserInfo( user );
  }
}
