/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.core.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nimbusds.jwt.JWTClaimsSet;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HopJdbcTokenServiceTest {

  @BeforeEach
  void pinSecret() {
    byte[] secret = new byte[32];
    new SecureRandom().nextBytes(secret);
    HopJdbcTokenService.overrideSecretForTests(secret);
  }

  @AfterEach
  void clearSecret() {
    HopJdbcTokenService.overrideSecretForTests(null);
  }

  @Test
  void roundTripPreservesSubjectAndRoles() throws Exception {
    HopJdbcTokenService.IssuedToken issued =
        HopJdbcTokenService.issue(
            "matt@example.com", List.of("admin", "hop-admin"), Duration.ofMinutes(5));
    JWTClaimsSet claims = HopJdbcTokenService.verify(issued.token());
    assertEquals("matt@example.com", claims.getSubject());
    assertEquals(HopJdbcTokenService.ISSUER, claims.getIssuer());
    assertTrue(claims.getAudience().contains(HopJdbcTokenService.AUDIENCE));
    assertEquals(Set.of("admin", "hop-admin"), HopJdbcTokenService.roleNames(claims));
    assertTrue(issued.expiresInSeconds() > 0);
    assertTrue(HopJdbcTokenService.isHopJdbcToken(issued.token()));
  }

  @Test
  void expiredTokenIsRejected() throws Exception {
    HopJdbcTokenService.IssuedToken issued =
        HopJdbcTokenService.issue("user", List.of("user"), Duration.ofMillis(1));
    Thread.sleep(20);
    assertThrows(Exception.class, () -> HopJdbcTokenService.verify(issued.token()));
    assertFalse(HopJdbcTokenService.isHopJdbcToken(issued.token()));
  }

  @Test
  void randomJwtIsNotAHopToken() {
    assertFalse(HopJdbcTokenService.isHopJdbcToken("not-a-jwt"));
    assertFalse(HopJdbcTokenService.isHopJdbcToken("a.b.c"));
  }

  @Test
  void verifyCanBeRepeatedWithoutReloadingTheSecret() throws Exception {
    HopJdbcTokenService.IssuedToken issued =
        HopJdbcTokenService.issue("user", List.of("user"), Duration.ofMinutes(5));
    HopJdbcTokenService.verify(issued.token());
    JWTClaimsSet again = HopJdbcTokenService.verify(issued.token());
    assertEquals("user", again.getSubject());
  }

  @Test
  void blankUsernameIsRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> HopJdbcTokenService.issue(" ", List.of("user"), Duration.ofMinutes(1)));
  }
}
