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

package org.apache.hop.ui.hopgui.notifications;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.notifications.INotificationProvider;
import org.apache.hop.ui.hopgui.notifications.config.NotificationSourceConfig;
import org.apache.hop.ui.hopgui.notifications.providers.TestNotificationProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Unit tests for NotificationPluginHelper. */
public class NotificationPluginHelperTest {

  private MockedStatic<HopConfig> mockedHopConfig;

  @BeforeClass
  public static void initHopLogStore() {
    if (!HopLogStore.isInitialized()) {
      HopLogStore.init();
    }
  }

  private HopConfig mockHopConfigInstance;

  @Before
  public void setUp() throws Exception {
    resetNotificationServiceSingleton();
    mockHopConfigInstance = mock(HopConfig.class);
    doNothing().when(mockHopConfigInstance).saveOption(anyString(), any());
    doNothing().when(mockHopConfigInstance).saveToFile();

    mockedHopConfig = Mockito.mockStatic(HopConfig.class);
    mockedHopConfig.when(HopConfig::getInstance).thenReturn(mockHopConfigInstance);
  }

  @After
  public void tearDown() throws Exception {
    if (mockedHopConfig != null) {
      mockedHopConfig.close();
    }
    NotificationService service = NotificationService.getInstance();
    if (service != null) {
      service.stop();
    }
    resetNotificationServiceSingleton();
  }

  private void resetNotificationServiceSingleton() throws Exception {
    Field instanceField = NotificationService.class.getDeclaredField("instance");
    instanceField.setAccessible(true);
    instanceField.set(null, null);
  }

  @Test
  public void testHasNotificationSource_returnsFalseWhenEmptyConfig() {
    mockedHopConfig
        .when(() -> HopConfig.readOptionString(eq("notification.sources"), eq(null)))
        .thenReturn(null);

    assertFalse(NotificationPluginHelper.hasNotificationSource("my-plugin"));
  }

  @Test
  public void testHasNotificationSource_returnsTrueWhenSourceExistsById() throws Exception {
    NotificationSourceConfig source = new NotificationSourceConfig();
    source.setId("my-plugin");
    source.setName("My Plugin");
    source.setType(NotificationSourceConfig.SourceType.CUSTOM_PLUGIN);

    String json =
        new com.fasterxml.jackson.databind.ObjectMapper()
            .writeValueAsString(java.util.List.of(source));

    mockedHopConfig
        .when(() -> HopConfig.readOptionString(eq("notification.sources"), eq(null)))
        .thenReturn(json);

    assertTrue(NotificationPluginHelper.hasNotificationSource("my-plugin"));
  }

  @Test
  public void testHasNotificationSource_returnsTrueWhenSourceExistsByPluginId() throws Exception {
    NotificationSourceConfig source = new NotificationSourceConfig();
    source.setId("source-xyz");
    source.setPluginId("my-plugin");
    source.setName("My Plugin");
    source.setType(NotificationSourceConfig.SourceType.CUSTOM_PLUGIN);

    String json =
        new com.fasterxml.jackson.databind.ObjectMapper()
            .writeValueAsString(java.util.List.of(source));

    mockedHopConfig
        .when(() -> HopConfig.readOptionString(eq("notification.sources"), eq(null)))
        .thenReturn(json);

    assertTrue(NotificationPluginHelper.hasNotificationSource("my-plugin"));
  }

  @Test
  public void testRegisterPluginNotifications_registersProviderAndCreatesSource() {
    mockedHopConfig
        .when(() -> HopConfig.readOptionString(eq("notification.sources"), eq(null)))
        .thenReturn(null); // No existing sources

    INotificationProvider provider = new TestNotificationProvider();
    boolean result =
        NotificationPluginHelper.registerPluginNotifications(
            "test-provider", "Test Notifications", provider);

    assertTrue(result);
    assertNotNull(NotificationService.getInstance().getProvider("test-provider"));
    verify(mockHopConfigInstance).saveOption(eq("notification.sources"), any());
  }

  @Test
  public void testRegisterPluginNotifications_rejectsNullPluginId() {
    INotificationProvider provider = new TestNotificationProvider();
    boolean result = NotificationPluginHelper.registerPluginNotifications(null, "Name", provider);

    assertFalse(result);
  }

  @Test
  public void testRegisterPluginNotifications_rejectsMismatchedPluginIdAndProviderId() {
    INotificationProvider provider = new TestNotificationProvider(); // id = "test-provider"
    boolean result =
        NotificationPluginHelper.registerPluginNotifications("wrong-id", "Name", provider);

    assertFalse(result);
  }
}
