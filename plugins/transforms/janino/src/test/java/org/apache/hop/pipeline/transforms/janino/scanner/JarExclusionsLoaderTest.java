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
package org.apache.hop.pipeline.transforms.janino.scanner;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

class JarExclusionsLoaderTest {
  @Test
  void loadsFromResourceFile() {
    var factory = new JarExclusionsLoader();
    Predicate<String> keep = factory.load("ClassLoaderScanner.ignored-jars.txt");
    assertFalse(keep.test("asm-9.8.jar"));
    assertFalse(keep.test("hop-transform-rowgenerator-2.19.0.jar"));
    assertTrue(keep.test("hop-transform-janino-2.19.0.jar"));
  }

  @Test
  void excludesKnownPrefix() {
    var keep = exclusions("asm");
    assertFalse(keep.test("asm-9.8.jar"));
    assertFalse(keep.test("asm"));
    assertTrue(keep.test("jackson-core-2.21.1.jar"));
  }

  @Test
  void notIgnoredPrefixTakesPrecedence() {
    var keep = exclusions("hop-transform-\n!hop-transform-janino");
    assertFalse(keep.test("hop-transform-rowgenerator-2.19.0.jar"));
    assertTrue(keep.test("hop-transform-janino-2.19.0.jar"));
  }

  @Test
  void emptyContentKeepsEverything() {
    var keep = exclusions("");
    assertTrue(keep.test("anything.jar"));
  }

  @Test
  void skipsBlankLines() {
    var keep = exclusions("\n\nasm\n\n");
    assertFalse(keep.test("asm-9.8.jar"));
    assertTrue(keep.test("jackson-core.jar"));
  }

  @Test
  void multipleIgnoresAndNotIgnores() {
    var keep = exclusions("jackson-\n!jackson-databind\nasm\n!asm-analysis");
    assertFalse(keep.test("jackson-core-2.21.1.jar"));
    assertTrue(keep.test("jackson-databind-2.21.1.jar"));
    assertFalse(keep.test("asm-9.8.jar"));
    assertTrue(keep.test("asm-analysis-9.8.jar"));
  }

  private Predicate<String> exclusions(String content) {
    return new JarExclusionsLoader()
        .load(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));
  }
}
