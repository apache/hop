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

package org.apache.hop.core.security.oidc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nimbusds.jwt.JWTClaimsSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.security.HopRole;
import org.apache.hop.core.security.HopSecurityConfig;
import org.apache.hop.core.security.HopSecurityContext;
import org.apache.hop.core.security.Permission;
import org.junit.jupiter.api.Test;

class HopOidcClientTest {

  @Test
  void pkceChallengeIsS256() {
    String verifier = HopOidcClient.newCodeVerifier();
    String challenge = HopOidcClient.codeChallengeS256(verifier);
    assertTrue(challenge.length() >= 40);
    assertEquals(challenge, HopOidcClient.codeChallengeS256(verifier));
  }

  @Test
  void extractsGroupsAndMapsRoles() throws Exception {
    HopSecurityConfig config = new HopSecurityConfig();
    config.setOauthRoleClaim("groups");
    config.setOauthUsernameClaim("preferred_username");
    config.getRoleMappings().put("my-admins", "admin");

    JWTClaimsSet claims =
        new JWTClaimsSet.Builder()
            .subject("sub-1")
            .claim("preferred_username", "alice")
            .claim("groups", List.of("my-admins", "other"))
            .build();

    HopOidcClient client = new HopOidcClient(config);
    assertEquals("alice", client.extractUsername(claims));
    Set<String> roles = client.extractRoleNames(claims);
    assertTrue(roles.contains("my-admins"));

    HopSecurityContext ctx = client.toSecurityContext(claims);
    assertEquals("alice", ctx.getUsername());
    assertTrue(ctx.allows(Permission.SECURITY_MANAGE));
    assertTrue(ctx.getRoleIds().contains(HopRole.ADMIN.getId()));
  }

  @Test
  void extractsKeycloakRealmRoles() throws Exception {
    HopSecurityConfig config = new HopSecurityConfig();
    config.setOauthRoleClaim("realm_access.roles");

    JWTClaimsSet claims =
        new JWTClaimsSet.Builder()
            .claim("preferred_username", "bob")
            .claim("realm_access", Map.of("roles", List.of("hop-operator", "offline_access")))
            .build();

    HopOidcClient client = new HopOidcClient(config);
    Set<String> roles = client.extractRoleNames(claims);
    assertTrue(roles.contains("hop-operator"));
    HopSecurityContext ctx = client.toSecurityContext(claims);
    assertTrue(ctx.allows(Permission.RUN_EXECUTE));
    assertTrue(!ctx.allows(Permission.FILE_SAVE));
  }
}
