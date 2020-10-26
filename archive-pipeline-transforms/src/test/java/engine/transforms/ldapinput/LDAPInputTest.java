/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.pipeline.transforms.ldapinput;

import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests LDAP Input Transform
 *
 * @author nhudak
 */
public class LDAPInputTest {
  private static TransformMockHelper<LDAPInputMeta, LDAPInputData> transformMockHelper;

  @BeforeClass
  public static void setup() {
    transformMockHelper =
      new TransformMockHelper<LDAPInputMeta, LDAPInputData>(
        "LDAP INPUT TEST", LDAPInputMeta.class, LDAPInputData.class );
    when( transformMockHelper.logChannelFactory.create( any(), any( LoggingObjectInterface.class ) ) )
      .thenReturn( transformMockHelper.logChannelInterface );
    when( transformMockHelper.pipeline.isRunning() ).thenReturn( true );
  }

  private LDAPInputMeta mockMeta() {
    LDAPInputMeta meta = mock( LDAPInputMeta.class );
    return meta;
  }

  @AfterClass
  public static void tearDown() {
    transformMockHelper.cleanUp();
  }

  @Test
  public void testRowProcessing() throws Exception {
    //Setup transform
    LDAPInput ldapInput = new LDAPInput(
      transformMockHelper.transformMeta, transformMockHelper.iTransformData,
      0, transformMockHelper.pipelineMeta, transformMockHelper.pipeline );
    LDAPInputData data = new LDAPInputData();
    LDAPInputMeta meta = mockMeta();

    //Mock fields
    LDAPInputField[] fields = new LDAPInputField[] {
      new LDAPInputField( "dn" ),
      new LDAPInputField( "cn" ),
      new LDAPInputField( "role" )
    };
    int sortedField = 1;
    fields[ sortedField ].setSortedKey( true );
    when( meta.getInputFields() ).thenReturn( fields );

    //Mock LDAP Connection
    when( meta.getProtocol() ).thenReturn( LdapMockProtocol.getName() );
    when( meta.getHost() ).thenReturn( "host.mock" );
    when( meta.getDerefAliases() ).thenReturn( "never" );
    when( meta.getReferrals() ).thenReturn( "ignore" );
    LdapMockProtocol.setup();

    try {
      //Run Initialization
      assertTrue( "Input Initialization Failed", ldapInput.init();

      //Verify
      assertEquals( "Field not marked as sorted", 1, data.connection.getSortingAttributes().size() );
      assertEquals( "Field not marked as sorted", data.attrReturned[ sortedField ], data.connection.getSortingAttributes().get( 0 ) );
      assertNotNull( data.attrReturned[ sortedField ] );
    } finally {
      LdapMockProtocol.cleanup();
    }
  }
}
