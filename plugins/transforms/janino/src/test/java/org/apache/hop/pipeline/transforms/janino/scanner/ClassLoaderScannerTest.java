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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.Collection;
import org.apache.hop.pipeline.transforms.janino.function.JaninoFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ClassLoaderScannerTest {
  @Test
  void findsAnnotatedMethodsInExplodedDirectory(@TempDir Path tempDir) throws Exception {
    Path classesDir = tempDir.resolve("classes");
    FunctionGenerator.writeToDirectory(classesDir);

    try (URLClassLoader cl =
        new URLClassLoader(
            new URL[] {classesDir.toUri().toURL()},
            Thread.currentThread().getContextClassLoader())) {
      ClassLoaderScanner scanner = new ClassLoaderScanner();
      Collection<Method> methods =
          scanner.findMethodsWithAnnotationInPackage(
              cl, "org.apache.hop.pipeline.transforms.janino.test", JaninoFunction.class);

      assertFalse(methods.isEmpty(), "Should find @JaninoFunction methods in exploded directory");
      assertTrue(
          methods.stream().anyMatch(m -> "nvl".equals(m.getName())), "Should include nvl method");
    }
  }

  @Test
  void findsAnnotatedMethodsInJar(@TempDir Path tempDir) throws Exception {
    Path jarFile = tempDir.resolve("test-functions.jar");
    FunctionGenerator.writeToJar(jarFile);

    try (URLClassLoader cl =
        new URLClassLoader(
            new URL[] {jarFile.toUri().toURL()}, Thread.currentThread().getContextClassLoader())) {
      ClassLoaderScanner scanner = new ClassLoaderScanner();
      Collection<Method> methods =
          scanner.findMethodsWithAnnotationInPackage(
              cl, "org.apache.hop.pipeline.transforms.janino.test", JaninoFunction.class);

      assertFalse(methods.isEmpty(), "Should find @JaninoFunction methods in jar");
      assertTrue(
          methods.stream().anyMatch(m -> "nvl".equals(m.getName())), "Should include nvl method");
    }
  }

  @Test
  void returnsEmptyForPackageWithNoMatchingAnnotation(@TempDir Path tempDir) throws Exception {
    Path classesDir = tempDir.resolve("empty");
    FunctionGenerator.writeToDirectory(classesDir);

    try (URLClassLoader cl =
        new URLClassLoader(
            new URL[] {classesDir.toUri().toURL()},
            Thread.currentThread().getContextClassLoader())) {
      ClassLoaderScanner scanner = new ClassLoaderScanner();
      Collection<Method> methods =
          scanner.findMethodsWithAnnotationInPackage(
              cl, "org.apache.hop.pipeline.transforms.janino.scanner", JaninoFunction.class);

      assertTrue(methods.isEmpty(), "Should find no @JaninoFunction when scanning wrong package");
    }
  }

  @Test
  void everyReturnedMethodCarriesTheAnnotation(@TempDir Path tempDir) throws Exception {
    Path classesDir = tempDir.resolve("annotated");
    FunctionGenerator.writeToDirectory(classesDir);

    try (URLClassLoader cl =
        new URLClassLoader(
            new URL[] {classesDir.toUri().toURL()},
            Thread.currentThread().getContextClassLoader())) {
      ClassLoaderScanner scanner = new ClassLoaderScanner();
      Collection<Method> methods =
          scanner.findMethodsWithAnnotationInPackage(
              cl, "org.apache.hop.pipeline.transforms.janino.test", JaninoFunction.class);

      assertFalse(methods.isEmpty());
      for (Method m : methods) {
        assertNotNull(
            m.getAnnotation(JaninoFunction.class),
            "Method " + m + " must be annotated with @JaninoFunction");
      }
    }
  }
}
