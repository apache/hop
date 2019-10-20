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

package org.apache.hop.repository.kdr;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.repository.StringObjectId;
import org.apache.hop.repository.UserInfo;
import org.apache.hop.repository.kdr.delegates.HopDatabaseRepositoryUserDelegate;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
/**
 * @author Andrey Khayrutdinov
 */
public class HopDatabaseRepositorySecurityProviderTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void initHop() throws Exception {
    HopEnvironment.init();
  }


  private HopDatabaseRepository repository;
  private HopDatabaseRepositorySecurityProvider provider;

  @Before
  public void setUp() {
    repository = new HopDatabaseRepository();
    repository.userDelegate = mock( HopDatabaseRepositoryUserDelegate.class );
    provider =
      new HopDatabaseRepositorySecurityProvider( repository, new HopDatabaseRepositoryMeta(), new UserInfo() );
  }


  @Test( expected = HopException.class )
  public void saveUserInfo_NormalizesInfo_FailsIfStillBreaches() throws Exception {
    provider.saveUserInfo( new UserInfo( "    " ) );
  }

  @Test( expected = HopException.class )
  public void saveUserInfo_CheckDuplication_FailsIfFoundSame() throws Exception {
    testSaveUserInfo_Passes( "login", "login", "login" );
  }


  @Test
  public void saveUserInfo_CheckDuplication_PassesIfFoundDifferenceInCase() throws Exception {
    testSaveUserInfo_Passes( "login", "login", "LOGIN" );
  }

  @Test
  public void saveUserInfo_NormalizesInfo_PassesIfNoViolations() throws Exception {
    testSaveUserInfo_Passes( "login    ", "login" );
  }

  @Test
  public void saveUserInfo_CheckDuplication_PassesIfFoundNothing() throws Exception {
    testSaveUserInfo_Passes( "login", "login" );
  }

  private void testSaveUserInfo_Passes( String login, String expectedLogin ) throws Exception {
    testSaveUserInfo_Passes( login, expectedLogin, "prefix_" + login );
  }

  private void testSaveUserInfo_Passes( String login, String expectedLogin, String existing ) throws Exception {
    doReturn( new StringObjectId( existing ) ).when( repository.userDelegate ).getUserID( eq( existing ) );

    provider.saveUserInfo( new UserInfo( login ) );

    ArgumentCaptor<UserInfo> captor = ArgumentCaptor.forClass( UserInfo.class );
    verify( repository.userDelegate ).saveUserInfo( captor.capture() );

    assertEquals( "UserInfo should be passed", expectedLogin, captor.getValue().getLogin() );
  }
}
