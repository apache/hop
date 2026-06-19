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

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.Collection;
import java.util.function.Predicate;
import java.util.jar.JarFile;
import java.util.stream.Stream;
import org.apache.xbean.finder.AnnotationFinder;
import org.apache.xbean.finder.ClassLoaders;
import org.apache.xbean.finder.archive.Archive;
import org.apache.xbean.finder.archive.CompositeArchive;
import org.apache.xbean.finder.archive.FileArchive;
import org.apache.xbean.finder.archive.FilteredArchive;
import org.apache.xbean.finder.archive.JarArchive;
import org.apache.xbean.finder.filter.Filters;

// note: for now manifest.mf classpath are not handled, todo: review if needed (unlikely normally)
public final class ClassLoaderScanner {
  public Collection<Method> findMethodsWithAnnotationInPackage(
      ClassLoader classLoader, String packageName, Class<? extends Annotation> annotation)
      throws IOException {
    final var urls = ClassLoaders.findUrls(classLoader);
    // we cache the result of the scanning so no need to cache the exclusions - should save mem
    final var jarFilter = new JarExclusionsLoader().load("ClassLoaderScanner.ignored-jars.txt");

    // next line would work but doesn't cut fast enough the jar selection
    // and requires a refiltering and iterating over all entries for a poor - almost always 0 - gain
    // final var archives = ClasspathArchive.archives(classLoader, urls);
    final var archives =
        urls.stream()
            .flatMap(
                it ->
                    switch (it.getProtocol()) {
                      case "jar" -> {
                        final var archive = new JarArchive(classLoader, it);
                        yield !jarHasPackage(archive.getUrl(), jarFilter, packageName)
                            ? Stream.<Archive>empty()
                            : Stream.of(archive);
                      }
                      case "file" -> {
                        try { // validate it is a jar else fallback on plain directory
                          final var jarUrl = new URL("jar", "", it.toExternalForm() + "!/");
                          final var juc = (JarURLConnection) jarUrl.openConnection();
                          juc.getJarFile();
                          final var archive = new JarArchive(classLoader, jarUrl);
                          yield !jarHasPackage(archive.getUrl(), jarFilter, packageName)
                              ? Stream.empty()
                              : Stream.of(archive);
                        } catch (final IOException e) {
                          final var archive = new FileArchive(classLoader, it);
                          yield !dirHasPackage(archive.getDir(), packageName)
                              ? Stream.empty()
                              : Stream.of(archive);
                        }
                      }
                      default -> Stream.empty();
                    })
            .toList();

    final var aggregated =
        new FilteredArchive(
            archives.size() == 1 ? archives.getFirst() : new CompositeArchive(archives),
            Filters.packages(packageName));
    try {
      // todo: review if we try to check if META-INF/jandex.idx exists in the jar/dir
      //       and use it instead of doing a bytecode scanning with asm
      //       -> requires dev to use jandex but can be more consistent with plugins
      //          -> likely better: drop all that and do scanning at build time with ServiceLoader
      // registration using a maven plugin or CLI to generate the right metadata
      return new AnnotationFinder(aggregated, true).findAnnotatedMethods(annotation);
    } finally {
      if (aggregated instanceof AutoCloseable a) { // Composite/Jar archives
        try {
          a.close();
        } catch (Exception e) {
          // no-op, ignored
        }
      }
    }
  }

  private boolean jarHasPackage(URL location, Predicate<String> jarFilter, String packageName) {
    File jarFile = null;
    var idx = 0;
    try {
      var jarPath =
          FileArchive.decode(
              "jar".equalsIgnoreCase(location.getProtocol())
                  ? new URL(
                          location.getPath().endsWith("!/")
                              ? location
                                  .getPath()
                                  .substring(0, location.getPath().lastIndexOf("!/"))
                              : location.getPath())
                      .getPath()
                  : location.getPath());
      for (var jp = jarPath;
          !(jarFile = new File(jp)).exists() && (idx = jarPath.indexOf("!/", idx + 1)) > 0;
          jp = jarPath.substring(0, idx)) {}
      try (final var jar = new JarFile(jarFile)) {
        // todo: enhance with mjar support but so unlikely that we don't care for now
        final var hasPck = jar.getEntry(packageName.replace('.', '/') + '/') != null;
        if (!hasPck) {
          return false;
        }

        // if excluded with our default rules try to match an explicit marker
        // this is a marker file enabling to force the jar scan even if excluded by default
        if (!jarFilter.test(jarFile.getName())) {
          return jar.getEntry("META-INF/org.apache.hop.janino") != null;
        }
        return true;
      }
    } catch (IOException e) {
      return false;
    }
  }

  private boolean dirHasPackage(File location, String packageName) {
    return new File(location, packageName.replace('.', '/')).exists();
  }
}
