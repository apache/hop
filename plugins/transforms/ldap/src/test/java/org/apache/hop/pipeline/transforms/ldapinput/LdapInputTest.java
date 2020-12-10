/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.pipeline.transforms.ldapinput;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests LDAP Input Transform
 *
 * @author nhudak
 */
public class LdapInputTest {
  private static TransformMockHelper<LdapInputMeta, LdapInputData> mockHelper;

  @BeforeClass
  public static void setup() {
    mockHelper =
      new TransformMockHelper<>(
        "LDAP INPUT TEST", LdapInputMeta.class, LdapInputData.class );
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel );
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterClass
  public static void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  public void testRowProcessing() throws Exception {

    // Setup transform
    LdapInputMeta meta = mock(LdapInputMeta.class);
    LdapInputData data = new LdapInputData();
    LdapInput ldapInput =
        new LdapInput(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);

    // Mock fields
    LdapInputField[] fields =
        new LdapInputField[] {
          new LdapInputField("dn"), new LdapInputField("cn"), new LdapInputField("role")
        };
    int sortedField = 1;
    fields[sortedField].setSortedKey(true);
    meta.setInputFields(fields);
    when(meta.getInputFields()).thenReturn(fields);

    // Mock LDAP Connection
    when(meta.getProtocol()).thenReturn(LdapMockProtocol.getName());
    when(meta.getHost()).thenReturn("host.mock");
    when(meta.getDerefAliases()).thenReturn("never");
    when(meta.getReferrals()).thenReturn("ignore");
    LdapMockProtocol.setup();

    try {
      // Run Initialization
      assertTrue("Input Initialization Failed", ldapInput.init());

      // Verify
      assertEquals("Field not marked as sorted", 1, data.connection.getSortingAttributes().size());
      assertEquals(
          "Field not marked as sorted",
          data.attrReturned[sortedField],
          data.connection.getSortingAttributes().get(0));
      assertNotNull(data.attrReturned[sortedField]);
    } finally {
      LdapMockProtocol.cleanup();
    }
  }
}
