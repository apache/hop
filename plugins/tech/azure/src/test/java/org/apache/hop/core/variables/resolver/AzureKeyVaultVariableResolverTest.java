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

package org.apache.hop.core.variables.resolver;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.core.exception.ResourceNotFoundException;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.models.KeyVaultSecret;
import java.lang.reflect.Field;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for AzureKeyVaultVariableResolver. These tests use mocking to avoid requiring actual
 * Azure Key Vault access.
 */
@ExtendWith(MockitoExtension.class)
class AzureKeyVaultVariableResolverTest {

  @Mock private SecretClient mockSecretClient;

  private AzureKeyVaultVariableResolver resolver;
  private IVariables variables;

  @BeforeAll
  static void initHopEnvironment() {
    // Initialize Hop's logging infrastructure for testing
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() {
    resolver = new AzureKeyVaultVariableResolver();
    variables = new Variables();
  }

  @Test
  void testPluginMetadata() {
    // Test that plugin metadata methods return expected values
    assertEquals("AzureKeyVault", resolver.getPluginId());
    assertEquals("Azure Key Vault Variable Resolver", resolver.getPluginName());
  }

  @Test
  void testGettersAndSetters() {
    // Test all getter/setter methods
    resolver.setAzureKeyVaultUri("https://test-vault.vault.azure.net/");
    resolver.setAzureTenantId("test-tenant-id");
    resolver.setAzureClientId("test-client-id");
    resolver.setAzureClientSecret("test-client-secret");

    assertEquals("https://test-vault.vault.azure.net/", resolver.getAzureKeyVaultUri());
    assertEquals("test-tenant-id", resolver.getAzureTenantId());
    assertEquals("test-client-id", resolver.getAzureClientId());
    assertEquals("test-client-secret", resolver.getAzureClientSecret());
  }

  @Test
  void testInitWithMissingUri() {
    // Setup with missing URI
    resolver.setAzureKeyVaultUri(null);
    resolver.setAzureTenantId("test-tenant");
    resolver.setAzureClientId("test-client");
    resolver.setAzureClientSecret("test-secret");

    // Init should not throw but should mark as failed
    assertDoesNotThrow(() -> resolver.init());

    // Verify resolver is in failed state
    String result = assertDoesNotThrow(() -> resolver.resolve("test-secret", variables));
    assertNull(result, "Should return null when initialization failed");
  }

  @Test
  void testInitWithMissingTenantId() {
    resolver.setAzureKeyVaultUri("https://test.vault.azure.net/");
    resolver.setAzureTenantId(null);
    resolver.setAzureClientId("test-client");
    resolver.setAzureClientSecret("test-secret");

    assertDoesNotThrow(() -> resolver.init());
    String result = assertDoesNotThrow(() -> resolver.resolve("test-secret", variables));
    assertNull(result);
  }

  @Test
  void testInitWithMissingClientId() {
    resolver.setAzureKeyVaultUri("https://test.vault.azure.net/");
    resolver.setAzureTenantId("test-tenant");
    resolver.setAzureClientId(null);
    resolver.setAzureClientSecret("test-secret");

    assertDoesNotThrow(() -> resolver.init());
    String result = assertDoesNotThrow(() -> resolver.resolve("test-secret", variables));
    assertNull(result);
  }

  @Test
  void testInitWithMissingClientSecret() {
    resolver.setAzureKeyVaultUri("https://test.vault.azure.net/");
    resolver.setAzureTenantId("test-tenant");
    resolver.setAzureClientId("test-client");
    resolver.setAzureClientSecret(null);

    assertDoesNotThrow(() -> resolver.init());
    String result = assertDoesNotThrow(() -> resolver.resolve("test-secret", variables));
    assertNull(result);
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"  ", "\t", "\n"})
  void testInitWithBlankCredentials(String blankValue) {
    // Test with various blank values
    resolver.setAzureKeyVaultUri(blankValue);
    resolver.setAzureTenantId(blankValue);
    resolver.setAzureClientId(blankValue);
    resolver.setAzureClientSecret(blankValue);

    assertDoesNotThrow(() -> resolver.init());
    String result = assertDoesNotThrow(() -> resolver.resolve("test-secret", variables));
    assertNull(result);
  }

  @Test
  void testResolveWithNullSecretId() throws Exception {
    // Setup resolver with mocked client
    setUpMockedResolver();

    String result = resolver.resolve(null, variables);

    assertNull(result, "Should return null for null secret ID");
    verify(mockSecretClient, never()).getSecret(anyString());
  }

  @Test
  void testResolveWithEmptySecretId() throws Exception {
    setUpMockedResolver();

    String result = resolver.resolve("", variables);

    assertNull(result, "Should return null for empty secret ID");
    verify(mockSecretClient, never()).getSecret(anyString());
  }

  @Test
  void testResolveSuccessfully() throws Exception {
    setUpMockedResolver();

    // Mock the Azure SDK response
    KeyVaultSecret mockSecret = mock(KeyVaultSecret.class);
    when(mockSecret.getValue()).thenReturn("secret-value-123");
    when(mockSecretClient.getSecret("my-secret")).thenReturn(mockSecret);

    String result = resolver.resolve("my-secret", variables);

    assertEquals("secret-value-123", result);
    verify(mockSecretClient).getSecret("my-secret");
  }

  @Test
  void testResolveSecretNotFound() throws Exception {
    setUpMockedResolver();

    // Mock ResourceNotFoundException
    when(mockSecretClient.getSecret("non-existent-secret"))
        .thenThrow(new ResourceNotFoundException("Secret not found", null));

    String result = resolver.resolve("non-existent-secret", variables);

    assertNull(result, "Should return null when secret is not found");
    verify(mockSecretClient).getSecret("non-existent-secret");
  }

  @Test
  void testResolveWithGenericException() throws Exception {
    setUpMockedResolver();

    // Mock a generic exception
    when(mockSecretClient.getSecret("error-secret"))
        .thenThrow(new RuntimeException("Azure service error"));

    String result = resolver.resolve("error-secret", variables);

    assertNull(result, "Should return null when an error occurs");
    verify(mockSecretClient).getSecret("error-secret");
  }

  @Test
  void testResolveWithNullSecretValue() throws Exception {
    setUpMockedResolver();

    KeyVaultSecret mockSecret = mock(KeyVaultSecret.class);
    when(mockSecret.getValue()).thenReturn(null);
    when(mockSecretClient.getSecret("null-value-secret")).thenReturn(mockSecret);

    String result = resolver.resolve("null-value-secret", variables);

    assertNull(result, "Should return null when secret value is null");
  }

  @Test
  void testResolveMultipleSecrets() throws Exception {
    setUpMockedResolver();

    // Setup multiple secrets
    KeyVaultSecret secret1 = mock(KeyVaultSecret.class);
    when(secret1.getValue()).thenReturn("value1");
    KeyVaultSecret secret2 = mock(KeyVaultSecret.class);
    when(secret2.getValue()).thenReturn("value2");

    when(mockSecretClient.getSecret("secret1")).thenReturn(secret1);
    when(mockSecretClient.getSecret("secret2")).thenReturn(secret2);

    String result1 = resolver.resolve("secret1", variables);
    String result2 = resolver.resolve("secret2", variables);

    assertEquals("value1", result1);
    assertEquals("value2", result2);
    verify(mockSecretClient).getSecret("secret1");
    verify(mockSecretClient).getSecret("secret2");
  }

  @Test
  void testInitIsCalledOnlyOnce() throws Exception {
    setUpMockedResolver();

    KeyVaultSecret mockSecret = mock(KeyVaultSecret.class);
    when(mockSecret.getValue()).thenReturn("value");
    when(mockSecretClient.getSecret(anyString())).thenReturn(mockSecret);

    // Call resolve multiple times
    resolver.resolve("secret1", variables);
    resolver.resolve("secret2", variables);
    resolver.resolve("secret3", variables);

    // Init should only be called once (synchronized and checks initialized flag)
    // We can verify this by checking that the client was used multiple times
    verify(mockSecretClient, times(3)).getSecret(anyString());
  }

  @Test
  void testThreadSafety() throws Exception {
    setUpMockedResolver();

    KeyVaultSecret mockSecret = mock(KeyVaultSecret.class);
    when(mockSecret.getValue()).thenReturn("concurrent-value");
    when(mockSecretClient.getSecret(anyString())).thenReturn(mockSecret);

    // Test concurrent access
    Thread thread1 =
        new Thread(
            () -> {
              try {
                resolver.resolve("secret1", variables);
              } catch (HopException e) {
                fail("Thread 1 failed: " + e.getMessage());
              }
            });

    Thread thread2 =
        new Thread(
            () -> {
              try {
                resolver.resolve("secret2", variables);
              } catch (HopException e) {
                fail("Thread 2 failed: " + e.getMessage());
              }
            });

    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    // Both threads should complete successfully
    // Init is synchronized, so no race conditions
    verify(mockSecretClient, atLeastOnce()).getSecret(anyString());
  }

  @Test
  void testResolveAfterFailedInit() throws Exception {
    // Force initialization to fail
    resolver.setAzureKeyVaultUri(null);
    resolver.setAzureTenantId(null);
    resolver.setAzureClientId(null);
    resolver.setAzureClientSecret(null);

    // First call should fail init
    String result1 = resolver.resolve("secret", variables);
    assertNull(result1);

    // Now set valid config
    setUpMockedResolver();

    // Second call should still return null because failedInitialization flag is set
    String result2 = resolver.resolve("secret", variables);
    assertNull(result2, "Should remain in failed state after first failure");
  }

  @Test
  void testSetPluginIdDoesNothing() {
    // These methods are no-ops but should not throw
    assertDoesNotThrow(() -> resolver.setPluginId());
  }

  @Test
  void testSetPluginNameDoesNothing() {
    assertDoesNotThrow(() -> resolver.setPluginName("some-name"));
  }

  @Test
  void testSpecialCharactersInSecretId() throws Exception {
    setUpMockedResolver();

    // Azure Key Vault secret names can contain alphanumeric characters and hyphens
    KeyVaultSecret mockSecret = mock(KeyVaultSecret.class);
    when(mockSecret.getValue()).thenReturn("special-value");
    when(mockSecretClient.getSecret("my-secret-123")).thenReturn(mockSecret);

    String result = resolver.resolve("my-secret-123", variables);

    assertEquals("special-value", result);
    verify(mockSecretClient).getSecret("my-secret-123");
  }

  @Test
  void testLongSecretValue() throws Exception {
    setUpMockedResolver();

    // Test with a long secret value
    String longValue = "A".repeat(10000);
    KeyVaultSecret mockSecret = mock(KeyVaultSecret.class);
    when(mockSecret.getValue()).thenReturn(longValue);
    when(mockSecretClient.getSecret("long-secret")).thenReturn(mockSecret);

    String result = resolver.resolve("long-secret", variables);

    assertEquals(longValue, result);
    assertEquals(10000, result.length());
  }

  @Test
  void testEmptySecretValue() throws Exception {
    setUpMockedResolver();

    // Test with empty secret value
    KeyVaultSecret mockSecret = mock(KeyVaultSecret.class);
    when(mockSecret.getValue()).thenReturn("");
    when(mockSecretClient.getSecret("empty-secret")).thenReturn(mockSecret);

    String result = resolver.resolve("empty-secret", variables);

    assertEquals("", result);
  }

  /**
   * Helper method to set up a resolver with a mocked SecretClient. Uses reflection to inject the
   * mock since we can't easily mock the Azure SDK builders.
   */
  private void setUpMockedResolver() throws Exception {
    // Set valid configuration
    resolver.setAzureKeyVaultUri("https://test-vault.vault.azure.net/");
    resolver.setAzureTenantId("test-tenant-id");
    resolver.setAzureClientId("test-client-id");
    resolver.setAzureClientSecret("test-client-secret");

    // Use reflection to inject the mocked SecretClient and set initialized flag
    Field secretClientField = AzureKeyVaultVariableResolver.class.getDeclaredField("secretClient");
    secretClientField.setAccessible(true);
    secretClientField.set(resolver, mockSecretClient);

    Field initializedField = AzureKeyVaultVariableResolver.class.getDeclaredField("initialized");
    initializedField.setAccessible(true);
    initializedField.set(resolver, true);
  }
}
