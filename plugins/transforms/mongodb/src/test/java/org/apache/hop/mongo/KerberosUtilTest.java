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

package org.apache.hop.mongo;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class KerberosUtilTest {

  @Test
  public void testJaasAuthenticationMode() throws MongoDbException {
    assertEquals(
        KerberosUtil.JaasAuthenticationMode.EXTERNAL,
        KerberosUtil.JaasAuthenticationMode.byName("EXTERNAL"));
    assertEquals(
        KerberosUtil.JaasAuthenticationMode.KERBEROS_KEYTAB,
        KerberosUtil.JaasAuthenticationMode.byName("KERBEROS_KEYTAB"));
    assertEquals(
        KerberosUtil.JaasAuthenticationMode.KERBEROS_USER,
        KerberosUtil.JaasAuthenticationMode.byName("KERBEROS_USER"));
    // KERBEROS_USER should be default
    assertEquals(
        KerberosUtil.JaasAuthenticationMode.KERBEROS_USER,
        KerberosUtil.JaasAuthenticationMode.byName(null));

    try {
      KerberosUtil.JaasAuthenticationMode.byName("Invalid");
      fail();
    } catch (MongoDbException e) {
      assertEquals(
          "HOP_JAAS_AUTH_MODE is incorrect.  "
              + "Should be one of [KERBEROS_USER, KERBEROS_KEYTAB, EXTERNAL], found 'Invalid'.",
          e.getMessage());
    }
  }
}
