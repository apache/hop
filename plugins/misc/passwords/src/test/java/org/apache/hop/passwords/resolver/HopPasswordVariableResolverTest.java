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
 *
 */

package org.apache.hop.passwords.resolver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HopPasswordVariableResolverTest {

  private HopPasswordVariableResolver resolver;
  private Variables variables;

  @BeforeAll
  static void initEnvironment() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    // Re-bind the default Hop encoder so this class is not polluted by AES encoder tests that
    // share the JVM (surefire reuseForks).
    System.setProperty(org.apache.hop.core.Const.HOP_PASSWORD_ENCODER_PLUGIN, "Hop");
    Encr.init("Hop");
    resolver = new HopPasswordVariableResolver();
    variables = new Variables();
    variables.initializeFrom(null);
  }

  @Test
  void pluginMetadata() {
    assertEquals("HopPassword", resolver.getPluginId());
    assertEquals("Hop Password Variable Resolver", resolver.getPluginName());
    assertTrue(resolver.isResolveAsVariable());
    assertFalse(resolver.isFailIfVariableNotDefined());
  }

  @Test
  void resolveLiteralEncryptedPassword() throws Exception {
    resolver.setResolveAsVariable(false);
    String plain = "mypass";
    String encoded = Encr.encryptPasswordIfNotUsingVariables(plain);

    assertEquals(plain, resolver.resolve(encoded, variables));
  }

  @Test
  void resolveLiteralPlaintextPassthrough() throws Exception {
    resolver.setResolveAsVariable(false);
    assertEquals("already-plain", resolver.resolve("already-plain", variables));
  }

  @Test
  void resolveVariableNameToEncryptedValue() throws Exception {
    resolver.setResolveAsVariable(true);
    String plain = "sftp-secret";
    String encoded = Encr.encryptPasswordIfNotUsingVariables(plain);
    variables.setVariable("SFTP_PASSWORD", encoded);

    assertEquals(plain, resolver.resolve("SFTP_PASSWORD", variables));
  }

  @Test
  void resolveVariableNameWithWhitespace() throws Exception {
    resolver.setResolveAsVariable(true);
    String plain = "trim-me";
    String encoded = Encr.encryptPasswordIfNotUsingVariables(plain);
    variables.setVariable("PWD", encoded);

    assertEquals(plain, resolver.resolve("  PWD  ", variables));
  }

  @Test
  void resolveMissingVariableReturnsNullByDefault() throws Exception {
    resolver.setResolveAsVariable(true);
    resolver.setFailIfVariableNotDefined(false);
    assertNull(resolver.resolve("DOES_NOT_EXIST", variables));
  }

  @Test
  void resolveMissingVariableThrowsWhenFailEnabled() {
    resolver.setResolveAsVariable(true);
    resolver.setFailIfVariableNotDefined(true);
    HopException ex =
        assertThrows(HopException.class, () -> resolver.resolve("DOES_NOT_EXIST", variables));
    assertTrue(ex.getMessage().contains("DOES_NOT_EXIST"));
  }

  @Test
  void resolveOptionalDollarBraceVariableForm() throws Exception {
    resolver.setResolveAsVariable(true);
    String plain = "brace-form";
    String encoded = Encr.encryptPasswordIfNotUsingVariables(plain);
    variables.setVariable("SFTP_PASSWORD", encoded);

    assertEquals(plain, resolver.resolve("${SFTP_PASSWORD}", variables));
  }

  @Test
  void resolveEmptyArgument() throws Exception {
    assertNull(resolver.resolve(null, variables));
    assertEquals("", resolver.resolve("", variables));
  }

  @Test
  void resolveUsesHopEncryptedPrefix() throws Exception {
    resolver.setResolveAsVariable(false);
    HopTwoWayPasswordEncoder encoder = new HopTwoWayPasswordEncoder();
    String encoded = encoder.encode("abcd", true);
    assertTrue(encoded.startsWith(HopTwoWayPasswordEncoder.PASSWORD_ENCRYPTED_PREFIX));
    assertEquals("abcd", resolver.resolve(encoded, variables));
  }
}
