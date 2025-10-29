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

package org.apache.hop.workflow.actions.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.resource.IResourceHolder;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.action.ActionBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** FtpHelper test */
@ExtendWith(MockitoExtension.class)
class FtpHelperTests {
  @Mock private ActionBase action;
  @Mock private IResourceHolder holder;
  private List<ResourceReference> references;

  @BeforeEach
  void setUp() {
    references = new ArrayList<>();
  }

  @Test
  void shouldAddServerResourceWhenServerNameIsValid() {
    String serverName = "test_server";
    String resolveServer = "resolve_server";

    when(action.resolve(serverName)).thenReturn(resolveServer);
    FtpHelper.addServerResourceReferenceIfPresent(references, serverName, action, holder);
    assertEquals(1, references.size());

    ResourceReference reference = references.get(0);
    assertEquals(holder, reference.getReferenceHolder());
    assertEquals(1, reference.getEntries().size());

    ResourceEntry entry = reference.getEntries().get(0);
    assertEquals(resolveServer, entry.getResource());
    assertEquals(ResourceEntry.ResourceType.SERVER, entry.getResourcetype());
  }

  @Test
  void shouldNotAddReferenceWhenServerNameIsEmpty() {
    FtpHelper.addServerResourceReferenceIfPresent(references, null, action, holder);
    FtpHelper.addServerResourceReferenceIfPresent(references, Const.EMPTY_STRING, action, holder);

    assertTrue(references.isEmpty());
    verify(action, never()).resolve(anyString());
  }

  @Test
  void shouldUseResolveMethodToExpandVariables() {
    String serverName = "test_server";
    String resolveServer = "resolve_server";

    when(action.resolve(serverName)).thenReturn(resolveServer);
    FtpHelper.addServerResourceReferenceIfPresent(references, serverName, action, holder);

    verify(action, only()).resolve(serverName);
    assertEquals(resolveServer, references.get(0).getEntries().get(0).getResource());
  }
}
