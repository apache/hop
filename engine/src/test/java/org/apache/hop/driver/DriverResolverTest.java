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

package org.apache.hop.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.eclipse.aether.graph.Exclusion;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DriverResolver#toExclusions}, the {@code groupId:artifactId} exclude
 * parsing a database plugin can declare (e.g. to avoid pulling a jar already shipped in lib/core).
 * No network resolution involved.
 */
class DriverResolverTest {

  @Test
  void nullOrEmptyExcludesProduceNoExclusions() {
    assertTrue(DriverResolver.toExclusions(null).isEmpty());
    assertTrue(DriverResolver.toExclusions(List.of()).isEmpty());
  }

  @Test
  void groupAndArtifactAreParsed() {
    Collection<Exclusion> exclusions =
        DriverResolver.toExclusions(List.of("com.google.protobuf:protobuf-java"));

    assertEquals(1, exclusions.size());
    Exclusion exclusion = exclusions.iterator().next();
    assertEquals("com.google.protobuf", exclusion.getGroupId());
    assertEquals("protobuf-java", exclusion.getArtifactId());
    assertEquals("*", exclusion.getClassifier());
    assertEquals("*", exclusion.getExtension());
  }

  @Test
  void artifactWildcardIsHonoured() {
    Exclusion exclusion = DriverResolver.toExclusions(List.of("org.slf4j:*")).iterator().next();
    assertEquals("org.slf4j", exclusion.getGroupId());
    assertEquals("*", exclusion.getArtifactId());
  }

  @Test
  void aGroupOnlyEntryExcludesEveryArtifactInThatGroup() {
    Exclusion exclusion =
        DriverResolver.toExclusions(List.of("com.google.guava")).iterator().next();
    assertEquals("com.google.guava", exclusion.getGroupId());
    assertEquals("*", exclusion.getArtifactId(), "a missing artifactId defaults to the wildcard");
  }

  @Test
  void blankAndNullEntriesAreSkippedButValidOnesKept() {
    Collection<Exclusion> exclusions =
        DriverResolver.toExclusions(
            Arrays.asList("  ", null, "com.example:a", "", "com.example:b"));

    assertEquals(2, exclusions.size());
  }
}
