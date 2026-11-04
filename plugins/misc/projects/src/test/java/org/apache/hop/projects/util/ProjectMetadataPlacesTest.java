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

package org.apache.hop.projects.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.metadata.util.MetadataPlace;
import org.apache.hop.projects.util.ProjectMetadataPlaces.NamedProject;
import org.junit.jupiter.api.Test;

class ProjectMetadataPlacesTest {

  @Test
  void labelsNameTheParentAndTheActiveProject() {
    Variables variables = new Variables();
    HopTwoWayPasswordEncoder encoder = new HopTwoWayPasswordEncoder();
    JsonMetadataProvider parent = new JsonMetadataProvider(encoder, "/parent/metadata", variables);
    JsonMetadataProvider project = new JsonMetadataProvider(encoder, "/child/metadata", variables);
    MultiMetadataProvider multi =
        new MultiMetadataProvider(encoder, List.of(parent, project), variables);

    ProjectMetadataPlaces.apply(
        multi,
        new NamedProject("retail", "${PROJECT_HOME}/metadata"),
        List.of(new NamedProject("acme", "${PROJECT_HOME}/metadata")));

    List<MetadataPlace> places = multi.getPlaces();
    assertEquals(2, places.size());
    assertEquals(
        "Parent project metadata (acme) : ${PROJECT_HOME}/metadata", places.get(0).label());
    assertFalse(places.get(0).active());
    assertEquals(parent.getDescription(), places.get(0).providerDescription());
    assertEquals("Project metadata : ${PROJECT_HOME}/metadata", places.get(1).label());
    assertTrue(places.get(1).active());
    assertTrue(places.get(0).tooltip().contains("acme"));
    assertTrue(places.get(1).tooltip().contains("/child/metadata"));
  }
}
