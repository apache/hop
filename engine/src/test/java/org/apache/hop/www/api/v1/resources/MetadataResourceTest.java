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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.core.Response;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.www.api.HopApiNotFoundException;
import org.apache.hop.www.api.HopServerApiContext;
import org.apache.hop.www.service.WebService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Covers metadata reads and writes over the JSON API. */
class MetadataResourceTest {

  private MetadataResource resource;
  private MultiMetadataProvider metadataProvider;
  private IHopMetadataSerializer<IHopMetadata> serializer;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() throws Exception {
    metadataProvider = mock(MultiMetadataProvider.class);
    serializer = mock(IHopMetadataSerializer.class);

    HopServerApiContext context = mock(HopServerApiContext.class);
    when(context.getMetadataProvider()).thenReturn(metadataProvider);
    when(context.getVariables()).thenReturn(new Variables());

    resource = new MetadataResource();
    resource.context = context;
  }

  @SuppressWarnings("unchecked")
  private void withKey(String key) throws HopException {
    when(metadataProvider.getMetadataClassForKey(key)).thenReturn((Class) WebService.class);
    when(metadataProvider.getSerializer((Class) WebService.class))
        .thenReturn((IHopMetadataSerializer) serializer);
  }

  @Test
  void typesAreListedFromTheProviderOnTheContext() {
    when(metadataProvider.getMetadataClasses()).thenReturn(List.of((Class) WebService.class));

    Response response = resource.getTypes();

    assertEquals(200, response.getStatus());
    assertEquals(List.of("web-service"), response.getEntity());
  }

  @Test
  void namesAreListedForAKnownType() throws Exception {
    withKey("web-service");
    when(serializer.listObjectNames()).thenReturn(List.of("one", "two"));

    Response response = resource.listNames("web-service");

    assertEquals(List.of("one", "two"), response.getEntity());
  }

  @Test
  void anUnknownTypeKeyIs404NotAServerError() throws Exception {
    // getMetadataClassForKey throws for an unregistered key; that is a missing resource.
    when(metadataProvider.getMetadataClassForKey("nosuchtype"))
        .thenThrow(new HopException("no plugin for key"));

    assertThrows(HopApiNotFoundException.class, () -> resource.listNames("nosuchtype"));
  }

  @Test
  void aNullTypeClassIsAlso404() throws Exception {
    when(metadataProvider.getMetadataClassForKey("nosuchtype")).thenReturn(null);

    assertThrows(HopApiNotFoundException.class, () -> resource.listNames("nosuchtype"));
  }

  @Test
  void aMissingElementIs404() throws Exception {
    withKey("web-service");
    when(serializer.load("nope")).thenReturn(null);

    HopApiNotFoundException thrown =
        assertThrows(
            HopApiNotFoundException.class, () -> resource.getElement("web-service", "nope"));

    assertTrue(thrown.getMessage().contains("nope"));
  }

  @Test
  void deletingRemovesTheElementAndReportsItsName() throws Exception {
    withKey("web-service");

    Response response = resource.deleteElement("web-service", "gone");

    assertEquals(200, response.getStatus());
    assertEquals("gone", response.getEntity());
    verify(serializer).delete("gone");
  }

  @Test
  void deletingAnUnknownTypeIs404() throws Exception {
    when(metadataProvider.getMetadataClassForKey("nosuchtype"))
        .thenThrow(new HopException("no plugin for key"));

    assertThrows(
        HopApiNotFoundException.class, () -> resource.deleteElement("nosuchtype", "whatever"));
  }

  @Test
  void savingMalformedJsonIsReportedAsAnError() throws Exception {
    withKey("web-service");

    assertThrows(HopException.class, () -> resource.saveElement("web-service", "{not json"));
  }

  @Test
  void savingToAnUnknownTypeIs404() throws Exception {
    when(metadataProvider.getMetadataClassForKey("nosuchtype"))
        .thenThrow(new HopException("no plugin for key"));

    assertThrows(HopApiNotFoundException.class, () -> resource.saveElement("nosuchtype", "{}"));
  }
}
