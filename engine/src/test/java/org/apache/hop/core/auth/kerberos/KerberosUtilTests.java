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

package org.apache.hop.core.auth.kerberos;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.TextOutputCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import org.junit.jupiter.api.Test;

/** Unit test for {@link KerberosUtil} */
class KerberosUtilTests {
  private final KerberosUtil kerberos = new KerberosUtil();

  @Test
  void testGetLoginContextFromKeytab() throws Exception {
    String principal = "test@EXAMPLE.COM";
    String keytab = "/tmp/test.keytab";

    LoginContext context = kerberos.getLoginContextFromKeytab(principal, keytab);
    assertNotNull(context);
  }

  @Test
  void testGetLoginContextFromUsernamePassword_callback() throws Exception {
    String principal = "user@EXAMPLE.COM";
    String password = "123456";

    LoginContext context = kerberos.getLoginContextFromUsernamePassword(principal, password);
    assertNotNull(context);

    // JAAS callback
    CallbackHandler handler =
        callbacks -> {
          for (Callback callback : callbacks) {
            switch (callback) {
              case NameCallback nc ->
                  assertEquals(
                      principal, nc.getDefaultName() == null ? principal : nc.getDefaultName());
              case PasswordCallback pc -> {
                pc.setPassword(password.toCharArray());
                assertArrayEquals(password.toCharArray(), pc.getPassword());
              }
              default -> fail("Unexpected callback: " + callback.getClass());
            }
          }
        };

    handler.handle(
        new Callback[] {new NameCallback("username"), new PasswordCallback("password", false)});
  }

  @Test
  void testGetLoginContextFromKerberosCache() throws Exception {
    String principal = "cache@EXAMPLE.COM";

    LoginContext context = kerberos.getLoginContextFromKerberosCache(principal);
    assertNotNull(context);
  }

  @Test
  void testUnsupportedCallback() {
    String principal = "user@EXAMPLE.COM";
    String password = "123456";

    assertThrows(
        UnsupportedCallbackException.class,
        () -> {
          CallbackHandler handler =
              callbacks -> {
                for (Callback callback : callbacks) {
                  switch (callback) {
                    case NameCallback nc -> nc.setName(principal);
                    case PasswordCallback pc -> pc.setPassword(password.toCharArray());
                    default -> throw new UnsupportedCallbackException(callback);
                  }
                }
              };

          handler.handle(
              new Callback[] {new TextOutputCallback(TextOutputCallback.INFORMATION, "test")});
        });
  }
}
