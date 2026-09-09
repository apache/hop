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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jopenlibs.vault.Vault;
import io.github.jopenlibs.vault.response.AuthResponse;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers(disabledWithoutDocker = true)
class VaultKubernetesAuthIT {

  private static final String ROOT_TOKEN = "myroot";
  private static final String SECRET_PATH = "secret/data/hop";
  private static final HttpClient HTTP = HttpClient.newHttpClient();
  static final KubernetesTokenReviewMock TOKEN_REVIEW = startTokenReview();

  @Container
  static GenericContainer<?> vault =
      new GenericContainer<>(DockerImageName.parse("hashicorp/vault:1.19.0"))
          .withExposedPorts(8200)
          .withEnv("VAULT_DEV_ROOT_TOKEN_ID", ROOT_TOKEN)
          .withEnv("VAULT_DEV_LISTEN_ADDRESS", "0.0.0.0:8200")
          .withEnv("SKIP_SETCAP", "true")
          .withAccessToHost(true)
          .waitingFor(
              Wait.forHttp("/v1/sys/health")
                  .forPort(8200)
                  .forStatusCodeMatching(code -> code == 200 || code == 429)
                  .withStartupTimeout(Duration.ofMinutes(2)));

  @TempDir static Path tempDir;

  @BeforeAll
  static void configureVault() throws Exception {
    HopLogStore.init();
    String kubernetesHost = "http://host.testcontainers.internal:" + TOKEN_REVIEW.getPort();

    vaultWrite("/v1/sys/auth/kubernetes", "{\"type\":\"kubernetes\"}");
    vaultWrite(
        "/v1/auth/kubernetes/config",
        """
        {
          "kubernetes_host": "%s",
          "disable_iss_validation": true,
          "disable_local_ca_jwt": true,
          "token_reviewer_jwt": "dummy"
        }
        """
            .formatted(kubernetesHost));
    vaultWrite(
        "/v1/sys/policies/acl/hop-read",
        """
        {
          "policy": "path \\"secret/data/*\\" { capabilities = [\\"read\\"] }"
        }
        """);
    vaultWrite(
        "/v1/auth/kubernetes/role/hop",
        """
        {
          "bound_service_account_names": "hop",
          "bound_service_account_namespaces": "default",
          "policies": ["hop-read"],
          "ttl": "1h"
        }
        """);
    vaultWrite("/v1/sys/auth/k8s", "{\"type\":\"kubernetes\"}");
    vaultWrite(
        "/v1/auth/k8s/config",
        """
        {
          "kubernetes_host": "%s",
          "disable_iss_validation": true,
          "disable_local_ca_jwt": true,
          "token_reviewer_jwt": "dummy"
        }
        """
            .formatted(kubernetesHost));
    vaultWrite(
        "/v1/auth/k8s/role/hop",
        """
        {
          "bound_service_account_names": "hop",
          "bound_service_account_namespaces": "default",
          "policies": ["hop-read"],
          "ttl": "1h"
        }
        """);
    vaultWrite(
        "/v1/" + SECRET_PATH,
        """
        {
          "data": {
            "hostname": "localhost",
            "password": "some-password"
          }
        }
        """);

    var probe = vault.execInContainer("wget", "-qO-", "-T", "5", kubernetesHost + "/healthz");
    if (probe.getExitCode() != 0) {
      throw new IllegalStateException(
          "Vault cannot reach the TokenReview mock at "
              + kubernetesHost
              + ": exit="
              + probe.getExitCode()
              + " stdout="
              + probe.getStdout()
              + " stderr="
              + probe.getStderr()
              + " vaultLogs="
              + vault.getLogs());
    }
  }

  @AfterAll
  static void stopMock() {
    TOKEN_REVIEW.close();
  }

  private static KubernetesTokenReviewMock startTokenReview() {
    try {
      KubernetesTokenReviewMock mock = new KubernetesTokenReviewMock();
      org.testcontainers.Testcontainers.exposeHostPorts(mock.getPort());
      return mock;
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  @Test
  void tokenAuthStillReadsTheSecret() throws Exception {
    VaultVariableResolver resolver = tokenResolver();
    String value = resolver.resolve(SECRET_PATH, variables());
    assertNotNull(value);
    assertTrue(value.contains("some-password"), value);
  }

  @Test
  void kubernetesAuthWithInlineJwtReadsTheSecret() throws Exception {
    VaultVariableResolver resolver = kubernetesResolver();
    resolver.setKubernetesJwt(KubernetesServiceAccountJwt.create("default", "hop"));
    String value = resolveOrThrow(resolver);
    assertTrue(value.contains("some-password"), value);
  }

  @Test
  void kubernetesAuthWithJwtFileReadsTheSecret() throws Exception {
    Path jwtFile = tempDir.resolve("token");
    Files.writeString(jwtFile, KubernetesServiceAccountJwt.create("default", "hop"));

    VaultVariableResolver resolver = kubernetesResolver();
    resolver.setKubernetesJwt("");
    resolver.setKubernetesJwtPath(jwtFile.toString());
    String value = resolveOrThrow(resolver);
    assertTrue(value.contains("some-password"), value);
  }

  @Test
  void kubernetesAuthWithCustomMountPathReadsTheSecret() throws Exception {
    VaultVariableResolver resolver = kubernetesResolver();
    resolver.setKubernetesAuthPath("k8s");
    resolver.setKubernetesJwt(KubernetesServiceAccountJwt.create("default", "hop"));
    String value = resolveOrThrow(resolver);
    assertTrue(value.contains("some-password"), value);
  }

  @Test
  void kubernetesAuthWithWrongRoleReturnsNull() throws Exception {
    VaultVariableResolver resolver = kubernetesResolver();
    resolver.setKubernetesRole("missing-role");
    resolver.setKubernetesJwt(KubernetesServiceAccountJwt.create("default", "hop"));
    assertNull(resolver.resolve(SECRET_PATH, variables()));
  }

  @Test
  void kubernetesAuthWithUnauthenticatedJwtReturnsNull() throws Exception {
    VaultVariableResolver resolver = kubernetesResolver();
    resolver.setKubernetesJwt("not-a-jwt");
    assertNull(resolver.resolve(SECRET_PATH, variables()));
  }

  @Test
  void kubernetesClientIsCachedAcrossResolves() throws Exception {
    CountingLoginResolver resolver = new CountingLoginResolver();
    resolver.setVaultAddress(vaultAddress());
    resolver.setVerifyingSsl(false);
    resolver.setAuthenticationType(VaultAuthType.KUBERNETES.name());
    resolver.setKubernetesRole("hop");
    resolver.setKubernetesJwt(KubernetesServiceAccountJwt.create("default", "hop"));

    IVariables variables = variables();
    assertNotNull(resolveOrThrow(resolver, variables));
    assertNotNull(resolveOrThrow(resolver, variables));
    assertTrue(resolver.logins >= 1);
    assertTrue(
        resolver.logins < 3, "expected the Vault client to be reused, logins=" + resolver.logins);
  }

  private static VaultVariableResolver tokenResolver() {
    VaultVariableResolver resolver = new VaultVariableResolver();
    resolver.setVaultAddress(vaultAddress());
    resolver.setVaultToken(ROOT_TOKEN);
    resolver.setVerifyingSsl(false);
    resolver.setAuthenticationType(VaultAuthType.TOKEN.name());
    return resolver;
  }

  private static VaultVariableResolver kubernetesResolver() {
    VaultVariableResolver resolver = new VaultVariableResolver();
    resolver.setVaultAddress(vaultAddress());
    resolver.setVerifyingSsl(false);
    resolver.setAuthenticationType(VaultAuthType.KUBERNETES.name());
    resolver.setKubernetesRole("hop");
    return resolver;
  }

  private static IVariables variables() {
    return new Variables();
  }

  private static String vaultAddress() {
    return "http://" + vault.getHost() + ":" + vault.getMappedPort(8200);
  }

  private static String resolveOrThrow(VaultVariableResolver resolver) throws Exception {
    return resolveOrThrow(resolver, variables());
  }

  private static String resolveOrThrow(VaultVariableResolver resolver, IVariables variables)
      throws Exception {
    resolver.getVault(variables);
    String value = resolver.resolve(SECRET_PATH, variables);
    assertNotNull(value, "secret lookup returned null after a successful login");
    return value;
  }

  private static void vaultWrite(String path, String json) throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(vaultAddress() + path))
            .timeout(Duration.ofSeconds(15))
            .header("X-Vault-Token", ROOT_TOKEN)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(json))
            .build();
    HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() >= 300) {
      throw new IllegalStateException(
          "Vault " + path + " returned " + response.statusCode() + ": " + response.body());
    }
  }

  private static final class CountingLoginResolver extends VaultVariableResolver {
    private int logins;

    @Override
    protected AuthResponse loginByKubernetes(Vault vault, String role, String jwt, String loginPath)
        throws org.apache.hop.core.exception.HopException {
      logins++;
      return super.loginByKubernetes(vault, role, jwt, loginPath);
    }
  }
}
