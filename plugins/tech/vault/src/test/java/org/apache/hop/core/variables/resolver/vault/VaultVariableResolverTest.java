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

package org.apache.hop.core.variables.resolver.vault;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.jopenlibs.vault.Vault;
import io.github.jopenlibs.vault.VaultConfig;
import io.github.jopenlibs.vault.VaultException;
import io.github.jopenlibs.vault.api.Logical;
import io.github.jopenlibs.vault.response.AuthResponse;
import io.github.jopenlibs.vault.response.LogicalResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class VaultVariableResolverTest {

  private VaultVariableResolver resolver;
  private IVariables variables;
  private Vault vault;
  private Logical logical;

  @TempDir Path tempDir;

  @BeforeAll
  static void initHopEnvironment() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    vault = mock(Vault.class);
    logical = mock(Logical.class);
    LogicalResponse response = mock(LogicalResponse.class);
    when(vault.logical()).thenReturn(logical);
    when(logical.read(anyString())).thenReturn(response);
    when(response.getData()).thenReturn(Map.of("data", "{\"password\":\"secret\"}"));

    resolver =
        new VaultVariableResolver() {
          @Override
          protected Vault createVault(VaultConfig vaultConfig) {
            return vault;
          }
        };
    resolver.setVaultAddress("http://vault:8200");
    resolver.setVaultToken("s.token");
    variables = new Variables();
  }

  @Test
  void testPluginMetadata() {
    assertEquals("Vault-Variable-Resolver", resolver.getPluginId());
    assertEquals("Hashicorp Vault Variable Resolver", resolver.getPluginName());
  }

  @Test
  void testDefaultAuthenticationTypeIsToken() {
    assertEquals(VaultAuthType.TOKEN.name(), new VaultVariableResolver().getAuthenticationType());
  }

  @ParameterizedTest
  @NullAndEmptySource
  void testEmptyAuthTypeFallsBackToToken(String authType) throws Exception {
    assertEquals(VaultAuthType.TOKEN, resolver.parseAuthType(authType));
  }

  @Test
  void testAuthTypeIsCaseInsensitive() throws Exception {
    assertEquals(VaultAuthType.KUBERNETES, resolver.parseAuthType("kubernetes"));
    assertEquals(VaultAuthType.TOKEN, resolver.parseAuthType("Token"));
  }

  @Test
  void testAuthTypeParseUsesRootLocale() throws Exception {
    Locale previous = Locale.getDefault();
    try {
      Locale.setDefault(Locale.forLanguageTag("tr-TR"));
      assertEquals(VaultAuthType.KUBERNETES, resolver.parseAuthType("kubernetes"));
    } finally {
      Locale.setDefault(previous);
    }
  }

  @Test
  void testUnknownAuthTypeIsRejected() {
    HopException e = assertThrows(HopException.class, () -> resolver.parseAuthType("APPROLE"));
    assertTrue(e.getMessage().contains("TOKEN"));
    assertTrue(e.getMessage().contains("KUBERNETES"));
  }

  @ParameterizedTest
  @NullAndEmptySource
  void testResolveWithoutSecretPath(String secretPath) throws Exception {
    assertNull(resolver.resolve(secretPath, variables));
  }

  @Test
  void testTokenAuthReadsSecret() throws Exception {
    assertEquals("{\"password\":\"secret\"}", resolver.resolve("secret/data/hop", variables));
    verify(logical).read("secret/data/hop");
  }

  @Test
  void testPathPrefixIsPrepended() throws Exception {
    resolver.setPathPrefix("secret/data/");
    resolver.resolve("hop", variables);
    verify(logical).read("secret/data/hop");
  }

  @Test
  void testTokenAuthWithoutTokenFails() throws Exception {
    resolver.setVaultToken("");
    assertNull(resolver.resolve("secret/data/hop", variables));
  }

  @Test
  void testKubernetesAuthWithoutRoleFails() throws Exception {
    resolver.setAuthenticationType(VaultAuthType.KUBERNETES.name());
    resolver.setKubernetesRole("");
    resolver.setKubernetesJwt("header.payload.sig");
    assertNull(resolver.resolve("secret/data/hop", variables));
  }

  @Test
  void testLoadJwtFromInlineString() throws Exception {
    resolver.setKubernetesJwt("  inline-jwt  ");
    assertEquals("inline-jwt", resolver.loadServiceAccountJwt(variables));
  }

  @Test
  void testLoadJwtFromFile() throws Exception {
    Path jwtFile = tempDir.resolve("token");
    Files.writeString(jwtFile, "file-jwt\n");
    resolver.setKubernetesJwt("");
    resolver.setKubernetesJwtPath(jwtFile.toString());
    assertEquals("file-jwt", resolver.loadServiceAccountJwt(variables));
  }

  @Test
  void testLoadJwtPrefersInlineOverFile() throws Exception {
    Path jwtFile = tempDir.resolve("token");
    Files.writeString(jwtFile, "file-jwt");
    resolver.setKubernetesJwt("inline-jwt");
    resolver.setKubernetesJwtPath(jwtFile.toString());
    assertEquals("inline-jwt", resolver.loadServiceAccountJwt(variables));
  }

  @Test
  void testLoadJwtUsesDefaultPathWhenEmpty() {
    resolver.setKubernetesJwt("");
    resolver.setKubernetesJwtPath("");
    HopException e =
        assertThrows(HopException.class, () -> resolver.loadServiceAccountJwt(variables));
    assertTrue(e.getMessage().contains(BaseVaultVariableResolver.DEFAULT_KUBERNETES_JWT_PATH));
  }

  @Test
  void testJwtAndRoleVariablesAreResolved() throws Exception {
    variables.setVariable("ROLE", "hop");
    variables.setVariable("JWT", "resolved-jwt");
    resolver.setKubernetesRole("${ROLE}");
    resolver.setKubernetesJwt("${JWT}");
    assertEquals("resolved-jwt", resolver.loadServiceAccountJwt(variables));
    assertEquals("hop", variables.resolve(resolver.getKubernetesRole()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "kubernetes"})
  void testDefaultKubernetesLoginPath(String mount) {
    assertEquals("auth/kubernetes", BaseVaultVariableResolver.kubernetesLoginPath(mount));
  }

  @Test
  void testCustomKubernetesLoginPath() {
    assertEquals("auth/k8s", BaseVaultVariableResolver.kubernetesLoginPath("k8s"));
    assertEquals("auth/k8s", BaseVaultVariableResolver.kubernetesLoginPath("auth/k8s"));
  }

  @Test
  void testClientIsCachedAcrossResolves() throws Exception {
    CountingResolver counting = new CountingResolver(vault);
    counting.setVaultAddress("http://vault:8200");
    counting.setVaultToken("s.token");

    counting.resolve("secret/data/hop", variables);
    counting.resolve("secret/data/hop", variables);

    assertEquals(1, counting.builds);
    verify(logical, times(2)).read("secret/data/hop");
  }

  @Test
  void testExpiredTokenRebuildsTheClient() throws Exception {
    CountingResolver counting = new CountingResolver(vault);
    counting.setVaultAddress("http://vault:8200");
    counting.setVaultToken("s.token");

    counting.resolve("secret/data/hop", variables);
    counting.expireCachedToken();
    counting.resolve("secret/data/hop", variables);

    assertEquals(2, counting.builds);
  }

  @Test
  void testNonRenewableTokenRefreshesBeforeExpiry() throws Exception {
    CountingResolver counting = new CountingResolver(vault);
    counting.setVaultAddress("http://vault:8200");
    counting.setVaultToken("s.token");

    counting.resolve("secret/data/hop", variables);
    counting.setCachedLeaseForTest(System.currentTimeMillis() + 10_000L, false);
    counting.resolve("secret/data/hop", variables);

    assertEquals(2, counting.builds);
  }

  @Test
  void testKubernetesAuthFailureRetriesOnce() throws Exception {
    when(logical.read(anyString()))
        .thenThrow(new VaultException("permission denied", 403))
        .thenReturn(response("{\"password\":\"secret\"}"));

    CountingResolver kubernetes = new CountingResolver(vault, true);
    kubernetes.setVaultAddress("http://vault:8200");
    kubernetes.setAuthenticationType(VaultAuthType.KUBERNETES.name());
    kubernetes.setKubernetesRole("hop");
    kubernetes.setKubernetesJwt("header.payload.sig");

    assertEquals("{\"password\":\"secret\"}", kubernetes.resolve("secret/data/hop", variables));
    assertEquals(2, kubernetes.builds);
    verify(logical, times(2)).read("secret/data/hop");
  }

  @Test
  void testTokenAuthFailureDoesNotRetry() throws Exception {
    when(logical.read(anyString())).thenThrow(new VaultException("permission denied", 403));

    CountingResolver counting = new CountingResolver(vault);
    counting.setVaultAddress("http://vault:8200");
    counting.setVaultToken("s.token");

    assertNull(counting.resolve("secret/data/hop", variables));
    assertEquals(1, counting.builds);
    verify(logical, times(1)).read("secret/data/hop");
  }

  @Test
  void testClearUnusedCredentialsDropsVaultTokenForKubernetes() {
    resolver.setAuthenticationType(VaultAuthType.KUBERNETES.name());
    resolver.setVaultToken("s.leftover");
    resolver.setKubernetesJwt("inline-jwt");
    resolver.clearUnusedCredentials();
    assertEquals("", resolver.getVaultToken());
    assertEquals("inline-jwt", resolver.getKubernetesJwt());
  }

  @Test
  void testClearUnusedCredentialsDropsJwtForToken() {
    resolver.setAuthenticationType(VaultAuthType.TOKEN.name());
    resolver.setVaultToken("s.token");
    resolver.setKubernetesJwt("inline-jwt");
    resolver.clearUnusedCredentials();
    assertEquals("s.token", resolver.getVaultToken());
    assertEquals("", resolver.getKubernetesJwt());
  }

  @Test
  void testClearUnusedCredentialsKeepsBothWhenAuthTypeIsAVariable() {
    resolver.setAuthenticationType("${VAULT_AUTH_TYPE}");
    resolver.setVaultToken("s.token");
    resolver.setKubernetesJwt("inline-jwt");
    resolver.clearUnusedCredentials();
    assertEquals("s.token", resolver.getVaultToken());
    assertEquals("inline-jwt", resolver.getKubernetesJwt());
  }

  private static LogicalResponse response(String data) {
    LogicalResponse logicalResponse = mock(LogicalResponse.class);
    when(logicalResponse.getData()).thenReturn(Map.of("data", data));
    return logicalResponse;
  }

  private static final class CountingResolver extends VaultVariableResolver {
    private final Vault vault;
    private final boolean kubernetesLogin;
    private int builds;

    private CountingResolver(Vault vault) {
      this(vault, false);
    }

    private CountingResolver(Vault vault, boolean kubernetesLogin) {
      this.vault = vault;
      this.kubernetesLogin = kubernetesLogin;
    }

    @Override
    protected Vault buildVault(IVariables variables) throws HopException {
      builds++;
      return super.buildVault(variables);
    }

    @Override
    protected Vault createVault(VaultConfig vaultConfig) {
      return vault;
    }

    @Override
    protected AuthResponse loginByKubernetes(Vault vault, String role, String jwt, String loginPath)
        throws HopException {
      if (!kubernetesLogin) {
        return super.loginByKubernetes(vault, role, jwt, loginPath);
      }
      AuthResponse response = mock(AuthResponse.class);
      when(response.getAuthClientToken()).thenReturn("s.k8s");
      when(response.getAuthLeaseDuration()).thenReturn(3600L);
      when(response.isAuthRenewable()).thenReturn(false);
      return response;
    }
  }
}
