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

package org.apache.hop.core.fatjar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.example.hopplugin.IndexSampleWidget;
import com.example.hopplugin.IndexSampleWidgetPlugin;
import com.example.hopplugin.IndexSampleWidgetPluginType;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.JarCache;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.variables.resolver.VariableResolverPluginType;
import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;
import org.jboss.jandex.IndexReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A fat jar's annotation index holds the classes with an org.apache.hop annotation or the
 * annotation of a registered plugin type: the ones the plugin and variable registries look for. A
 * worker JVM reads the whole index at start-up.
 */
class FatJarBuilderIndexTest {

  private static final List<Class<?>> SAMPLES =
      List.of(
          IndexSampleExtensionPoint.class,
          IndexSampleVariables.class,
          IndexSampleHopTypesOnly.class,
          IndexSampleOtherAnnotation.class,
          IndexSampleWidgetPluginType.class,
          IndexSampleWidgetPlugin.class);

  private static final FatJarBuilder.IndexFilter HOP_ONLY = new FatJarBuilder.IndexFilter(Set.of());

  @TempDir Path folder;

  @Test
  void theIndexHoldsOnlyClassesWithAHopAnnotation() throws Exception {
    Index index = buildAndReadIndex(List.of(VariableResolverPluginType.class));
    assertIndexed(index, IndexSampleExtensionPoint.class);
    assertIndexed(index, IndexSampleVariables.class);
    // A plugin type is itself annotated by Hop
    assertIndexed(index, IndexSampleWidgetPluginType.class);
    assertNotIndexed(index, IndexSampleHopTypesOnly.class);
    assertNotIndexed(index, IndexSampleOtherAnnotation.class);
    // Its plugin type isn't registered: nothing looks for this annotation
    assertNotIndexed(index, IndexSampleWidgetPlugin.class);
  }

  @Test
  void pluginsOfAnExternalPluginTypeAreIndexed() throws Exception {
    Index index = buildAndReadIndex(List.of(IndexSampleWidgetPluginType.class));
    assertIndexed(index, IndexSampleWidgetPlugin.class);
    assertIndexed(index, IndexSampleExtensionPoint.class);
    assertNotIndexed(index, IndexSampleOtherAnnotation.class);
  }

  @Test
  void onlyPluginTypeAnnotationsOutsideHopAreCollected() {
    assertEquals(
        Set.of(IndexSampleWidget.class.getName()),
        FatJarBuilder.pluginAnnotations(
            List.of(IndexSampleWidgetPluginType.class, VariableResolverPluginType.class)));
  }

  @Test
  void aClassWithoutHopTypesIsNotParsed() throws Exception {
    // No org.apache.hop descriptor at all: rejected on the bytes
    assertFalse(HOP_ONLY.isIndexed(new byte[] {0x0a, 0x0b}));
  }

  @Test
  void annotationsOnClassesAndFieldsBothCount() throws Exception {
    assertTrue(HOP_ONLY.isIndexed(bytes(IndexSampleExtensionPoint.class)));
    assertTrue(HOP_ONLY.isIndexed(bytes(IndexSampleVariables.class)));
    assertFalse(HOP_ONLY.isIndexed(bytes(IndexSampleHopTypesOnly.class)));
    assertFalse(HOP_ONLY.isIndexed(bytes(IndexSampleOtherAnnotation.class)));
  }

  /** Builds a fat jar of the samples with the given plugin types and reads its index. */
  private Index buildAndReadIndex(List<Class<? extends IPluginType>> pluginTypes) throws Exception {
    Path source = folder.resolve("samples.jar");
    try (ZipOutputStream out = new ZipOutputStream(new FileOutputStream(source.toFile()))) {
      for (Class<?> sample : SAMPLES) {
        out.putNextEntry(new ZipEntry(classFile(sample)));
        try (InputStream in = sample.getResourceAsStream(sample.getSimpleName() + ".class")) {
          in.transferTo(out);
        }
        out.closeEntry();
      }
    }
    Path target = folder.resolve("fat.jar");

    FatJarBuilder builder =
        new FatJarBuilder(
            mock(ILogChannel.class),
            new Variables(),
            target.toString(),
            List.of(source.toString()));
    builder.setPluginTypes(pluginTypes);
    builder.buildTargetJar();

    try (JarFile jar = new JarFile(target.toFile())) {
      // Every class is still in the jar, only the index is smaller
      for (Class<?> sample : SAMPLES) {
        assertNotNull(jar.getEntry(classFile(sample)), sample.getName());
      }
      ZipEntry entry = jar.getEntry(JarCache.ANNOTATION_INDEX_LOCATION);
      assertNotNull(entry);
      try (InputStream in = jar.getInputStream(entry)) {
        return new IndexReader(in).read();
      }
    }
  }

  private static void assertNotIndexed(Index index, Class<?> sample) {
    assertNull(index.getClassByName(DotName.createSimple(sample)), sample.getName());
  }

  private static void assertIndexed(Index index, Class<?> sample) {
    assertNotNull(index.getClassByName(DotName.createSimple(sample)), sample.getName());
  }

  private static String classFile(Class<?> sample) {
    return sample.getName().replace('.', '/') + ".class";
  }

  private static byte[] bytes(Class<?> sample) throws Exception {
    try (InputStream in = sample.getResourceAsStream(sample.getSimpleName() + ".class")) {
      return in.readAllBytes();
    }
  }
}
