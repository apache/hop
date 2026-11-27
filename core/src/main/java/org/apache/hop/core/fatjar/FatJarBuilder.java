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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.JarCache;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.jboss.jandex.ClassSummary;
import org.jboss.jandex.DotName;
import org.jboss.jandex.IndexWriter;
import org.jboss.jandex.Indexer;

@Getter
@Setter
public class FatJarBuilder {

  public static final String CONST_META_INF = "META-INF";
  private ILogChannel log;

  /** A progress line in the log every this many percent of the jar files. */
  static final int PROGRESS_STEP_PCT = 10;

  private static final Class<?> PKG = FatJarBuilder.class;

  /** The message of the exception a canceled build throws. */
  public static final String CANCELED_MESSAGE = "The fat jar build was canceled";

  /**
   * Optional: gets one unit of work per jar file and the name of the jar being added, and stops the
   * build when canceled. The builder never calls {@link IProgressMonitor#done()}: that is up to the
   * caller, which may have more to do.
   */
  @SuppressWarnings("java:S2065") // disable sonar warning on transient
  private transient IProgressMonitor progressMonitor;

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

  /**
   * The plugin types whose annotations put a class in the index, when they are outside {@code
   * org.apache.hop}. Empty: the plugin types registered in this JVM.
   */
  private List<Class<? extends IPluginType>> pluginTypes = new ArrayList<>();

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
    IndexFilter indexFilter =
        new IndexFilter(
            pluginAnnotations(
                pluginTypes.isEmpty()
                    ? PluginRegistry.getInstance().getPluginTypes()
                    : pluginTypes));
    boolean canceled = false;
    if (progressMonitor != null) {
      progressMonitor.beginTask(
          BaseMessages.getString(PKG, "FatJarBuilder.Progress.AddingJars", jarFiles.size()),
          jarFiles.size());
    }

    try {
      // Buffered on both ends: unbuffered 1 kB writes made a build on a slow (network, Docker
      // bind mount) file system take ten times longer
      byte[] buffer = new byte[64 * 1024];
      try (JarOutputStream zipOutputStream =
          new JarOutputStream(
              new BufferedOutputStream(new FileOutputStream(realTargetJarFile), 1024 * 1024))) {

        int done = 0;
        int nextReport = PROGRESS_STEP_PCT;
        for (String jarFile : jarFiles) {
          if (progressMonitor != null) {
            if (progressMonitor.isCanceled()) {
              canceled = true;
              break;
            }
            progressMonitor.subTask(new File(jarFile).getName());
            progressMonitor.worked(1);
          }
          done++;
          // A build takes a minute: say how far it is, so a log or a progress dialog that shows
          // the last line doesn't look stuck
          int percent = done * 100 / jarFiles.size();
          if (percent >= nextReport) {
            log.logBasic(
                "Fat jar: "
                    + done
                    + " of "
                    + jarFiles.size()
                    + " jar files added ("
                    + percent
                    + "%)");
            nextReport = (percent / PROGRESS_STEP_PCT + 1) * PROGRESS_STEP_PCT;
          }

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

          try (ZipInputStream zipInputStream =
              new ZipInputStream(
                  new BufferedInputStream(new FileInputStream(jarFile), 256 * 1024))) {
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
                    byte[] classBytes = baos.toByteArray();
                    if (indexFilter.isIndexed(classBytes)) {
                      indexer.index(new ByteArrayInputStream(classBytes));
                    }
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

        if (canceled) {
          log.logBasic("Fat jar build canceled after " + done + " of " + jarFiles.size() + " jars");
        } else {
          addIndexAndMetaInf(zipOutputStream, indexer);
        }
      }
    } catch (Exception e) {
      throw new HopException("Unable to build far jar file '" + realTargetJarFile + "'", e);
    } finally {
      fileContentMap.clear();
    }
    if (canceled) {
      // Half a jar is of no use to anyone
      new File(realTargetJarFile).delete();
      throw new HopException(CANCELED_MESSAGE);
    }
  }

  private static final String HOP_PACKAGE = "org.apache.hop.";

  /** The descriptor prefix every type of an org.apache.hop package has in a class file. */
  private static final byte[] HOP_TYPE_DESCRIPTOR =
      "Lorg/apache/hop/".getBytes(StandardCharsets.US_ASCII);

  /**
   * The annotations of the given plugin types that are outside {@code org.apache.hop}: those of
   * plugin types that external plugins define.
   */
  static Set<String> pluginAnnotations(Collection<Class<? extends IPluginType>> pluginTypes) {
    Set<String> annotations = new HashSet<>();
    for (Class<? extends IPluginType> pluginType : pluginTypes) {
      PluginAnnotationType annotationType = pluginType.getAnnotation(PluginAnnotationType.class);
      if (annotationType != null && !annotationType.value().getName().startsWith(HOP_PACKAGE)) {
        annotations.add(annotationType.value().getName());
      }
    }
    return annotations;
  }

  /**
   * Which classes go into the fat jar's annotation index: a class with an annotation from an {@code
   * org.apache.hop} package or of a registered plugin type, on the class or on one of its fields or
   * methods. That is all the plugin registry and the variable registry look for. A class's own
   * package doesn't matter. Every other class of the jar (nearly all of them) would only make the
   * index bigger: a worker JVM reads the whole index at start-up and keeps it in memory.
   *
   * <p>A class without one of those type descriptors in its bytes is skipped without parsing it.
   */
  static final class IndexFilter {
    private final Set<String> pluginAnnotations;
    private final List<byte[]> pluginAnnotationDescriptors = new ArrayList<>();

    /**
     * @param pluginAnnotations the class names of plugin type annotations outside {@code
     *     org.apache.hop}
     */
    IndexFilter(Set<String> pluginAnnotations) {
      this.pluginAnnotations = pluginAnnotations;
      for (String annotation : pluginAnnotations) {
        pluginAnnotationDescriptors.add(
            ("L" + annotation.replace('.', '/') + ";").getBytes(StandardCharsets.UTF_8));
      }
    }

    /**
     * @param classBytes the class file
     * @return true when the class goes into the index
     */
    boolean isIndexed(byte[] classBytes) throws IOException {
      if (!mayCarryAnnotation(classBytes)) {
        return false;
      }
      ClassSummary summary = new Indexer().indexWithSummary(new ByteArrayInputStream(classBytes));
      for (DotName annotation : summary.annotations()) {
        String name = annotation.toString();
        if (name.startsWith(HOP_PACKAGE) || pluginAnnotations.contains(name)) {
          return true;
        }
      }
      return false;
    }

    private boolean mayCarryAnnotation(byte[] classBytes) {
      if (contains(classBytes, HOP_TYPE_DESCRIPTOR)) {
        return true;
      }
      for (byte[] descriptor : pluginAnnotationDescriptors) {
        if (contains(classBytes, descriptor)) {
          return true;
        }
      }
      return false;
    }
  }

  private static boolean contains(byte[] bytes, byte[] part) {
    for (int i = 0; i <= bytes.length - part.length; i++) {
      if (bytes[i] == part[0] && Arrays.equals(bytes, i, i + part.length, part, 0, part.length)) {
        return true;
      }
    }
    return false;
  }

  private void addIndexAndMetaInf(JarOutputStream zipOutputStream, Indexer indexer)
      throws IOException {
    if (progressMonitor != null) {
      progressMonitor.subTask(BaseMessages.getString(PKG, "FatJarBuilder.Progress.Index"));
    }
    addInfoManifestMf(zipOutputStream);

    // The merged META-INF/services files
    for (Map.Entry<String, String> entry : fileContentMap.entrySet()) {
      zipOutputStream.putNextEntry(new ZipEntry(entry.getKey()));
      zipOutputStream.write(entry.getValue().getBytes(StandardCharsets.UTF_8));
      zipOutputStream.closeEntry();
    }

    log.logBasic("Adding Jandex index");
    zipOutputStream.putNextEntry(new ZipEntry(JarCache.ANNOTATION_INDEX_LOCATION));
    IndexWriter indexWriter = new IndexWriter(zipOutputStream);
    indexWriter.write(indexer.complete());
    zipOutputStream.closeEntry();
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
