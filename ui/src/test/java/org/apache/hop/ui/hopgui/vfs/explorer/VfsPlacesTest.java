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

package org.apache.hop.ui.hopgui.vfs.explorer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;

class VfsPlacesTest {

  @Test
  void theProjectIsListedOnlyWhenAProjectIsEnabled() {
    Variables variables = new Variables();
    assertTrue(VfsPlaces.project(variables, "Project home").isEmpty());

    variables.setVariable("PROJECT_HOME", "/work/p");
    List<VfsPlaces.Place> places = VfsPlaces.project(variables, "Project home");
    assertEquals(1, places.size());
    assertEquals("/work/p", places.get(0).getUri());
    assertEquals(VfsPlaces.Kind.PROJECT, places.get(0).getKind());
  }

  @Test
  void bookmarksAreSortedByName() {
    List<VfsPlaces.Place> places =
        VfsPlaces.bookmarks(Map.of("zeta", "/z", "Alpha", "/a", "beta", "s3:///b"));
    assertEquals(
        List.of("Alpha", "beta", "zeta"), places.stream().map(VfsPlaces.Place::getName).toList());
  }

  @Test
  void localPlacesStartWithTheUserHome() {
    List<VfsPlaces.Place> places = VfsPlaces.local("Home");
    assertFalse(places.isEmpty());
    assertEquals(System.getProperty("user.home"), places.get(0).getUri());
  }

  @Test
  void withoutAMetadataProviderThereAreNoConnections() {
    assertTrue(VfsPlaces.connections(null, new Variables()).isEmpty());
  }

  @Test
  void aDropIsRefusedOntoItselfItsChildOrItsOwnFolder() {
    List<String> dragged = List.of("/data/in", "/data/notes.txt");
    assertFalse(VfsFileExplorerLocation.canDropInto(dragged, "/data/in"));
    assertFalse(VfsFileExplorerLocation.canDropInto(dragged, "file:///data/in/sub"));
    assertFalse(VfsFileExplorerLocation.canDropInto(dragged, "/data"));
    assertTrue(VfsFileExplorerLocation.canDropInto(dragged, "/archive"));
    assertTrue(VfsFileExplorerLocation.canDropInto(List.of(), "/data"));
  }
}
