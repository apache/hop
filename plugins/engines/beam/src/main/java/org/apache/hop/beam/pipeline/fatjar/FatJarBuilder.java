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

package org.apache.hop.beam.pipeline.fatjar;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.io.IOUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.JarCache;
import org.apache.hop.core.variables.IVariables;
import org.jboss.jandex.IndexWriter;
import org.jboss.jandex.Indexer;

@Getter
@Setter
public class FatJarBuilder {

  public static final String CONST_META_INF = "META-INF";
  private ILogChannel log;
  private IVariables variables;
  private String targetJarFile;
  private List<String> jarFiles;
  private String extraTransformPluginClasses;
  private String extraXpPluginClasses;

  @SuppressWarnings("java:S2065") // disable sonar warning on transient
  private transient Map<String, String> fileContentMap;

  @SuppressWarnings("java:S2065") // disable sonar warning on transient
  private transient Map<String, String> classCollisionMap;

  @SuppressWarnings("java:S2065") // disable sonar warning on transient
  private transient Set<String> collisionFileSet;

  @SuppressWarnings("java:S2065") // disable sonar warning on transient
  private transient Set<String> fileSet;

  public FatJarBuilder() {
    jarFiles = new ArrayList<>();
    extraTransformPluginClasses = null;
    extraXpPluginClasses = null;
  }

  public FatJarBuilder(
      ILogChannel log, IVariables variables, String targetJarFile, List<String> jarFiles) {
    this();
    this.log = log;
    this.variables = variables;
    this.targetJarFile = targetJarFile;
    this.jarFiles = jarFiles;
  }

  public void buildTargetJar() throws HopException {
    fileContentMap = new HashMap<>();
    classCollisionMap = new HashMap<>();
    collisionFileSet = new HashSet<>();
    fileSet = new HashSet<>();

    // The real target file to write to...
    //
    String realTargetJarFile = variables.resolve(targetJarFile);

    Indexer indexer = new Indexer();

    try {
      byte[] buffer = new byte[1024];
      try (JarOutputStream zipOutputStream =
          new JarOutputStream(new FileOutputStream(realTargetJarFile))) {

        for (String jarFile : jarFiles) {

          // Let's do a duplicate jar file check on the base name of the file
          //
          File jarFileFile = new File(jarFile);
          String jarFileBaseName = jarFileFile.getName();
          if (fileSet.contains(jarFileBaseName)) {
            // Already processed
            log.logDetailed("Skipping duplicate jar file: " + jarFile);
            continue;
          }
          if (jarFileBaseName.contains("jersey-bundle")) {
            // Skip this file
            // TODO: replace this bundle by separate packages of a more recent jersey glassfish
            // version
            //
            continue;
          }
          fileSet.add(jarFileBaseName);

          try (ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(jarFile))) {
            ZipEntry zipEntry = zipInputStream.getNextEntry();
            while (zipEntry != null) {

              try {
                boolean skip = false;
                boolean merge = false;
                boolean index = false;

                String entryName = zipEntry.getName();

                if (zipEntry.isDirectory()) {
                  skip = true;
                } else if (entryName.contains("META-INF/INDEX.LIST")) {
                  skip = true;
                } else if (entryName.contains("META-INF/MANIFEST.MF")) {
                  skip = true;
                } else if (entryName.startsWith(CONST_META_INF) && entryName.endsWith(".SF")) {
                  skip = true;
                } else if (entryName.startsWith(CONST_META_INF) && entryName.endsWith(".DSA")) {
                  skip = true;
                } else if (entryName.startsWith(CONST_META_INF) && entryName.endsWith(".RSA")) {
                  skip = true;
                } else if (entryName.startsWith("META-INF/services/")) {
                  merge = true;
                  skip = true;
                }
                // Skip module metadata in */module-info.class
                else if (entryName.endsWith("module-info.class")) {
                  skip = true;
                }
                // Skip module metadata in */package-info.class
                else if (entryName.endsWith("package-info.class")) {
                  skip = true;
                }
                // Spark has UnusedStubClass.class in every library
                //
                else if (entryName.endsWith("UnusedStubClass.class")) {
                  skip = true;
                }
                // Skip because we rebuild a new one from cache
                else if (entryName.endsWith("META-INF/jandex.idx")) {
                  skip = true;
                } else if (entryName.endsWith(".class")) {
                  index = true;

                  // Is this class file already in the fat jar?
                  //
                  String otherJar = classCollisionMap.get(entryName);
                  if (otherJar != null) {
                    skip = true;

                    // Did we already warn about this?
                    if (!collisionFileSet.contains(jarFile)) {
                      collisionFileSet.add(jarFile);
                      log.logDetailed(
                          "Duplicate class(es) detected in " + jarFile + " from : " + otherJar);
                      log.logDetailed("    Example class: " + entryName);
                    }
                  } else {
                    log.logDebug("Adding class " + entryName + " for " + jarFile);
                    classCollisionMap.put(entryName, jarFile);
                  }
                }

                // Nothing to index in guava as it gives an error for some reason
                //
                if (zipEntry.getName().contains("$") // skip anonymous inner classes
                    || jarFileBaseName.startsWith("guava")
                    || jarFileBaseName.startsWith("akka-")
                    || jarFileBaseName.startsWith("scala-")
                    || jarFileBaseName.startsWith("flink-runtime")
                    || jarFileBaseName.startsWith("beam-sdks-java-io")
                    || jarFileBaseName.startsWith("beam-runners-spark")
                    || jarFileBaseName.startsWith("beam-runners-direct")
                    || jarFileBaseName.startsWith("beam-runners-flink")
                    || jarFileBaseName.startsWith("beam-sdks-java-core")
                    || jarFileBaseName.startsWith("beam-runners-core")) {
                  index = false;
                }

                if (!skip) {
                  try {
                    zipOutputStream.putNextEntry(new ZipEntry(zipEntry.getName()));
                  } catch (ZipException ze) {
                    // Duplicate entry!
                    //
                    skip = true;
                  }
                }

                if (merge) {
                  String fileContent = IOUtils.toString(zipInputStream, StandardCharsets.UTF_8);
                  fileContentMap.merge(entryName, fileContent, (a, b) -> a + Const.CR + b);
                } else if (!skip) {
                  int len;
                  ByteArrayOutputStream baos = new ByteArrayOutputStream();
                  while ((len = zipInputStream.read(buffer)) > 0) {
                    zipOutputStream.write(buffer, 0, len);
                    if (index) {
                      baos.write(buffer, 0, len);
                    }
                  }

                  if (index) {
                    indexer.index(new ByteArrayInputStream(baos.toByteArray()));
                  }
                }

                zipInputStream.closeEntry();

                if (!skip) {
                  zipOutputStream.closeEntry();
                }

                zipEntry = zipInputStream.getNextEntry();
              } catch (Exception e) {
                throw new HopException("Error adding jar file entry: " + zipEntry.getName(), e);
              }
            }
          } catch (Exception e) {
            throw new HopException("Error adding jar file: " + jarFile, e);
          }
        }

        // Add the META-INF/MANIFEST.MF file:
        addInfoManifestMf(zipOutputStream);

        // Add the META-INF/services files...
        //
        for (String entryName : fileContentMap.keySet()) {
          String fileContent = fileContentMap.get(entryName);
          zipOutputStream.putNextEntry(new ZipEntry(entryName));
          zipOutputStream.write(fileContent.getBytes(StandardCharsets.UTF_8));
          zipOutputStream.closeEntry();
        }

        // Add META-INF/jandex.idx file
        //
        log.logBasic("Adding Jandex index");
        zipOutputStream.putNextEntry(new ZipEntry(JarCache.ANNOTATION_INDEX_LOCATION));
        IndexWriter indexWriter = new IndexWriter(zipOutputStream);
        indexWriter.write(indexer.complete());
        zipOutputStream.closeEntry();
      }
    } catch (Exception e) {
      throw new HopException("Unable to build far jar file '" + realTargetJarFile + "'", e);
    } finally {
      fileContentMap.clear();
    }
  }

  private static void addInfoManifestMf(JarOutputStream zipOutputStream) throws IOException {
    ZipEntry manifestEntry = new ZipEntry("META-INF/MANIFEST.MF");

    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(new Attributes.Name("Multi-Release"), "true");

    zipOutputStream.putNextEntry(manifestEntry);
    manifest.write(zipOutputStream);
    zipOutputStream.closeEntry();
  }
}
