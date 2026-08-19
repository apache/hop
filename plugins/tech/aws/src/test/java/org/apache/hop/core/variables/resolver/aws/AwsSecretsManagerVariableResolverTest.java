/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.variables.resolver.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;

/**
 * Unit tests for the AWS Secrets Manager variable resolver. The Secrets Manager client is mocked so
 * that no AWS account is needed.
 */
@ExtendWith(MockitoExtension.class)
class AwsSecretsManagerVariableResolverTest {

  @Mock private SecretsManagerClient mockClient;

  private AwsSecretsManagerVariableResolver resolver;
  private IVariables variables;

  @BeforeAll
  static void initHopEnvironment() throws HopException {
    HopLogStore.init();
    // The resolver decrypts secret keys through Encr, which needs its encoder plugin registered.
    PluginRegistry.addPluginType(TwoWayPasswordEncoderPluginType.getInstance());
    PluginRegistry.init();
    Encr.init(Const.NVL(EnvUtil.getSystemProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN), "Hop"));
  }

  @BeforeEach
  void setUp() {
    // Hand the resolver the mocked client instead of letting it build a real one.
    resolver =
        new AwsSecretsManagerVariableResolver() {
          @Override
          protected SecretsManagerClient getClient(IVariables variables) {
            return mockClient;
          }
        };
    variables = new Variables();
  }

  private void respondWithString(String value) {
    when(mockClient.getSecretValue(any(GetSecretValueRequest.class)))
        .thenReturn(GetSecretValueResponse.builder().secretString(value).build());
  }

  private GetSecretValueRequest captureRequest() {
    ArgumentCaptor<GetSecretValueRequest> captor =
        ArgumentCaptor.forClass(GetSecretValueRequest.class);
    verify(mockClient).getSecretValue(captor.capture());
    return captor.getValue();
  }

  @Test
  void testPluginMetadata() {
    assertEquals("AwsSecretsManager", resolver.getPluginId());
    assertEquals("AWS Secrets Manager Variable Resolver", resolver.getPluginName());
  }

  @Test
  void testDefaults() {
    assertEquals(AwsSecretsManagerAuthType.AUTOMATIC.name(), resolver.getAuthenticationType());
    assertEquals("AWSCURRENT", resolver.getVersionStage());
    assertEquals("0", resolver.getCacheTtlSeconds());
  }

  @Test
  void testGettersAndSetters() {
    resolver.setRegion("eu-west-1");
    resolver.setAccessKey("AKIAEXAMPLE");
    resolver.setSecretKey("secret");
    resolver.setSessionToken("token");
    resolver.setCredentialsFile("/tmp/credentials");
    resolver.setProfileName("hop");
    resolver.setEndpointOverride("http://localhost:4566");
    resolver.setSecretNamePrefix("production/");

    assertEquals("eu-west-1", resolver.getRegion());
    assertEquals("AKIAEXAMPLE", resolver.getAccessKey());
    assertEquals("secret", resolver.getSecretKey());
    assertEquals("token", resolver.getSessionToken());
    assertEquals("/tmp/credentials", resolver.getCredentialsFile());
    assertEquals("hop", resolver.getProfileName());
    assertEquals("http://localhost:4566", resolver.getEndpointOverride());
    assertEquals("production/", resolver.getSecretNamePrefix());
  }

  @ParameterizedTest
  @NullAndEmptySource
  void testResolveWithoutSecretName(String secretName) throws Exception {
    assertNull(resolver.resolve(secretName, variables));
    verify(mockClient, never()).getSecretValue(any(GetSecretValueRequest.class));
  }

  @Test
  void testResolveRejectsArn() throws Exception {
    // An ARN contains colons, which the variable resolver expression syntax splits on. Rather than
    // silently looking up a truncated name we refuse it.
    String arn = "arn:aws:secretsmanager:eu-west-1:123456789012:secret:my-secret-AbCdEf";

    assertNull(resolver.resolve(arn, variables));
    verify(mockClient, never()).getSecretValue(any(GetSecretValueRequest.class));
  }

  @Test
  void testResolveRejectsTruncatedArn() throws Exception {
    // What an ARN actually looks like by the time it reaches the resolver: core splits the
    // expression #{aws-secrets:arn:aws:secretsmanager:...} on ':' and hands us only "arn".
    assertNull(resolver.resolve("arn", variables));
    verify(mockClient, never()).getSecretValue(any(GetSecretValueRequest.class));
  }

  @Test
  void testResolveRejectsArnEvenWithPrefixConfigured() throws Exception {
    resolver.setSecretNamePrefix("production/");

    assertNull(resolver.resolve("arn", variables));
    verify(mockClient, never()).getSecretValue(any(GetSecretValueRequest.class));
  }

  @Test
  void testResolveReturnsSecretString() throws Exception {
    respondWithString("p@ssw0rd");

    assertEquals("p@ssw0rd", resolver.resolve("my-secret", variables));
    assertEquals("my-secret", captureRequest().secretId());
  }

  @Test
  void testResolveReturnsJsonUntouched() throws Exception {
    // Picking a single key out of the JSON is done by the variable resolver machinery in core, so
    // the resolver hands back the payload as stored.
    String json = "{\"username\":\"hop\",\"password\":\"secret\"}";
    respondWithString(json);

    assertEquals(json, resolver.resolve("db-credentials", variables));
  }

  @Test
  void testResolveAppliesSecretNamePrefix() throws Exception {
    resolver.setSecretNamePrefix("production/");
    respondWithString("value");

    resolver.resolve("database", variables);

    assertEquals("production/database", captureRequest().secretId());
  }

  @Test
  void testResolveResolvesVariablesInPrefix() throws Exception {
    variables.setVariable("ENVIRONMENT", "staging");
    resolver.setSecretNamePrefix("${ENVIRONMENT}/");
    respondWithString("value");

    resolver.resolve("database", variables);

    assertEquals("staging/database", captureRequest().secretId());
  }

  @Test
  void testResolveUsesAwsCurrentByDefault() throws Exception {
    respondWithString("value");

    resolver.resolve("my-secret", variables);

    assertEquals("AWSCURRENT", captureRequest().versionStage());
  }

  @Test
  void testResolveUsesConfiguredVersionStage() throws Exception {
    resolver.setVersionStage("AWSPREVIOUS");
    respondWithString("value");

    resolver.resolve("my-secret", variables);

    assertEquals("AWSPREVIOUS", captureRequest().versionStage());
  }

  @Test
  void testResolveFallsBackToAwsCurrentOnEmptyVersionStage() throws Exception {
    resolver.setVersionStage("");
    respondWithString("value");

    resolver.resolve("my-secret", variables);

    assertEquals("AWSCURRENT", captureRequest().versionStage());
  }

  @Test
  void testResolveEncodesBinarySecret() throws Exception {
    byte[] binary = {1, 2, 3, 4, 5};
    when(mockClient.getSecretValue(any(GetSecretValueRequest.class)))
        .thenReturn(
            GetSecretValueResponse.builder().secretBinary(SdkBytes.fromByteArray(binary)).build());

    assertEquals(
        Base64.getEncoder().encodeToString(binary), resolver.resolve("binary-secret", variables));
  }

  @Test
  void testResolveDecodesBinarySecretBackToOriginal() throws Exception {
    String original = "keystore-content";
    when(mockClient.getSecretValue(any(GetSecretValueRequest.class)))
        .thenReturn(
            GetSecretValueResponse.builder()
                .secretBinary(SdkBytes.fromUtf8String(original))
                .build());

    String resolved = resolver.resolve("binary-secret", variables);

    assertEquals(
        original, new String(Base64.getDecoder().decode(resolved), StandardCharsets.UTF_8));
  }

  @Test
  void testResolveReturnsNullWhenSecretNotFound() throws Exception {
    when(mockClient.getSecretValue(any(GetSecretValueRequest.class)))
        .thenThrow(ResourceNotFoundException.builder().message("not found").build());

    assertNull(resolver.resolve("missing-secret", variables));
  }

  @Test
  void testResolveReturnsNullOnFailure() throws Exception {
    when(mockClient.getSecretValue(any(GetSecretValueRequest.class)))
        .thenThrow(new RuntimeException("network down"));

    assertNull(resolver.resolve("my-secret", variables));
  }

  @Test
  void testCachingIsOffByDefault() throws Exception {
    respondWithString("value");

    resolver.resolve("my-secret", variables);
    resolver.resolve("my-secret", variables);

    verify(mockClient, times(2)).getSecretValue(any(GetSecretValueRequest.class));
  }

  @Test
  void testCachingServesRepeatedLookupsFromCache() throws Exception {
    resolver.setCacheTtlSeconds("60");
    respondWithString("value");

    assertEquals("value", resolver.resolve("my-secret", variables));
    assertEquals("value", resolver.resolve("my-secret", variables));

    verify(mockClient, times(1)).getSecretValue(any(GetSecretValueRequest.class));
  }

  @Test
  void testCachingIsPerSecret() throws Exception {
    resolver.setCacheTtlSeconds("60");
    respondWithString("value");

    resolver.resolve("first-secret", variables);
    resolver.resolve("second-secret", variables);

    verify(mockClient, times(2)).getSecretValue(any(GetSecretValueRequest.class));
  }

  @Test
  void testExpiredCacheEntryIsLookedUpAgain() throws Exception {
    // A TTL that has already elapsed by the time the entry is read.
    resolver.setCacheTtlSeconds("-1");
    respondWithString("value");

    resolver.resolve("my-secret", variables);
    resolver.resolve("my-secret", variables);

    verify(mockClient, times(2)).getSecretValue(any(GetSecretValueRequest.class));
  }

  @Test
  void testAutomaticAuthTypeUsesCredentialChain() throws Exception {
    assertInstanceOf(
        DefaultCredentialsProvider.class,
        resolver.buildCredentialsProvider(null, null, null, null, null, null));
  }

  @Test
  void testAccessKeysAuthTypeUsesStaticCredentials() throws Exception {
    AwsCredentialsProvider provider =
        resolver.buildCredentialsProvider("ACCESS_KEYS", "AKIAEXAMPLE", "secret", null, null, null);

    assertInstanceOf(StaticCredentialsProvider.class, provider);
    // Asserting the provider type alone would still pass with the key and the secret swapped.
    AwsCredentials credentials = provider.resolveCredentials();
    assertEquals("AKIAEXAMPLE", credentials.accessKeyId());
    assertEquals("secret", credentials.secretAccessKey());
  }

  @Test
  void testAccessKeysAuthTypeAcceptsSessionToken() throws Exception {
    AwsCredentialsProvider provider =
        resolver.buildCredentialsProvider(
            "ACCESS_KEYS", "AKIAEXAMPLE", "secret", "session-token", null, null);

    assertInstanceOf(StaticCredentialsProvider.class, provider);
    AwsCredentials credentials = provider.resolveCredentials();
    assertInstanceOf(AwsSessionCredentials.class, credentials);
    assertEquals("AKIAEXAMPLE", credentials.accessKeyId());
    assertEquals("secret", credentials.secretAccessKey());
    assertEquals("session-token", ((AwsSessionCredentials) credentials).sessionToken());
  }

  @Test
  void testAccessKeysAreDecryptedWhenStoredEncrypted() throws Exception {
    // Hop's metadata serializer normally decrypts these, but a hand written or older metadata file
    // can still carry the encrypted form.
    AwsCredentialsProvider provider =
        resolver.buildCredentialsProvider(
            "ACCESS_KEYS",
            "AKIAEXAMPLE",
            Encr.encryptPasswordIfNotUsingVariables("secret"),
            null,
            null,
            null);

    assertEquals("secret", provider.resolveCredentials().secretAccessKey());
  }

  @Test
  void testAccessKeysAuthTypeRequiresBothKeys() {
    HopException e =
        assertThrows(
            HopException.class,
            () ->
                resolver.buildCredentialsProvider(
                    "ACCESS_KEYS", "AKIAEXAMPLE", null, null, null, null));
    assertTrue(e.getMessage().contains("ACCESS_KEYS"));
  }

  @Test
  void testCredentialsFileAuthTypeUsesProfileProvider() throws Exception {
    assertInstanceOf(
        ProfileCredentialsProvider.class,
        resolver.buildCredentialsProvider("CREDENTIALS_FILE", null, null, null, null, "hop"));
  }

  @Test
  void testAuthTypeIsCaseInsensitive() throws Exception {
    assertEquals(AwsSecretsManagerAuthType.ACCESS_KEYS, resolver.parseAuthType("access_keys"));
  }

  @Test
  void testEmptyAuthTypeFallsBackToDefault() throws Exception {
    assertEquals(AwsSecretsManagerAuthType.AUTOMATIC, resolver.parseAuthType(null));
    assertEquals(AwsSecretsManagerAuthType.AUTOMATIC, resolver.parseAuthType(""));
  }

  @Test
  void testUnknownAuthTypeIsRejected() {
    HopException e = assertThrows(HopException.class, () -> resolver.parseAuthType("KERBEROS"));
    assertTrue(e.getMessage().contains("KERBEROS"));
  }

  @Test
  void testRegionComboShowsCodeAndDescription() {
    assertTrue(resolver.getRegions(null, null).contains("eu-west-1 - Europe (Ireland)"));
    // The empty entry leaves the region to the environment.
    assertTrue(resolver.getRegions(null, null).contains(""));
  }

  @Test
  void testRegionCodeIsReadBackFromTheDisplayedValue() {
    assertEquals(
        "eu-west-1", AwsSecretsManagerVariableResolver.regionCode("eu-west-1 - Europe (Ireland)"));
    // A hand written or variable driven value carries no description.
    assertEquals("eu-west-1", AwsSecretsManagerVariableResolver.regionCode("eu-west-1"));
    assertEquals("", AwsSecretsManagerVariableResolver.regionCode(""));
    assertNull(AwsSecretsManagerVariableResolver.regionCode(null));
  }

  @Test
  void testComboBoxesArePopulated() {
    assertEquals(
        java.util.List.of("AUTOMATIC", "ACCESS_KEYS", "CREDENTIALS_FILE"),
        resolver.getAuthenticationTypes(null, null));
  }
}
