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

package org.apache.hop.www.api.v1.resources;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.ws.rs.core.Response;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class PluginsResourceTest {

  private final PluginsResource resource = new PluginsResource();

  @BeforeAll
  static void initHop() throws Exception {
    HopClientEnvironment.init();
  }

  @SuppressWarnings("unchecked")
  private List<Class<?>> registeredTypes() {
    return (List<Class<?>>) resource.getTypes().getEntity();
  }

  @Test
  void typesAreListedFromTheRegistry() {
    Response response = resource.getTypes();

    assertEquals(200, response.getStatus());
    assertFalse(registeredTypes().isEmpty(), "the registry should report at least one plugin type");
  }

  @Test
  void aRegisteredTypeClassListsItsPlugins() throws Exception {
    String typeClassName = registeredTypes().get(0).getName();

    Response response = resource.listPlugins(typeClassName);

    assertEquals(200, response.getStatus());
    assertTrue(response.getEntity() instanceof List);
  }

  @Test
  void anArbitraryClassNameIsRefused() {
    // Guards against having any class on the classpath constructed by name.
    HopException thrown =
        assertThrows(HopException.class, () -> resource.listPlugins("java.lang.Runtime"));

    assertTrue(thrown.getMessage().contains("not available in the plugin registry"));
  }

  @Test
  void anUnknownClassNameIsRefusedBeforeItIsLoaded() {
    HopException thrown =
        assertThrows(HopException.class, () -> resource.listPlugins("com.example.NoSuchType"));

    assertTrue(thrown.getMessage().contains("not available in the plugin registry"));
  }

  @Test
  void aBlankClassNameIsRefused() {
    assertThrows(HopException.class, () -> resource.listPlugins(""));
  }
}
