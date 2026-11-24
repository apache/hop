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
package org.apache.hop.maven;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.plugins.manifest.DependencyList;
import org.apache.hop.core.plugins.manifest.PluginManifest;
import org.apache.hop.core.plugins.manifest.PluginManifestGenerator;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;

/**
 * Adds the plugin manifest to the zip of a Hop plugin: {@code plugin-manifest.json} in the plugin
 * folder, or {@code lib/core/<artifactId>-manifest.json} for a module that only fills {@code
 * lib/core}. It lists every file the zip installs, with its location, Maven coordinates, scope,
 * size and SHA-256, the plugin ids from the Jandex index and the Hop plugins it needs. An
 * installation uses it to tell which plugins a shared {@code lib/core} jar belongs to.
 *
 * <p>Bind it after the assembly that builds the zip, in the same phase:
 *
 * <pre>{@code
 * <plugin>
 *   <groupId>org.apache.hop</groupId>
 *   <artifactId>hop-maven-plugin</artifactId>
 *   <version>${hop.version}</version>
 *   <executions>
 *     <execution>
 *       <phase>package</phase>
 *       <goals><goal>manifest</goal></goals>
 *     </execution>
 *   </executions>
 * </plugin>
 * }</pre>
 *
 * <p>The manifest entry takes its time from the files already in the zip, so a build with {@code
 * project.build.outputTimestamp} stays reproducible.
 */
@Mojo(
    name = "manifest",
    defaultPhase = LifecyclePhase.PACKAGE,
    requiresDependencyResolution = ResolutionScope.TEST,
    threadSafe = true)
public class ManifestMojo extends AbstractMojo {

  private static final Set<String> INCLUDE_SCOPES =
      Set.of("compile", "provided", "runtime", "system", "test");

  @Parameter(defaultValue = "${project}", readonly = true, required = true)
  private MavenProject project;

  /**
   * The zip to describe. By default {@code ${project.build.finalName}.zip} in the build directory,
   * or else the one zip the build attached.
   */
  @Parameter(property = "hop.manifest.zip")
  private File zip;

  /** Where a copy of the manifest is written. */
  @Parameter(
      property = "hop.manifest.outputFile",
      defaultValue = "${project.build.directory}/plugin-manifest.json")
  private File outputFile;

  /**
   * Describe the base of a Hop client zip, the jars it puts under {@code lib/} for itself, instead
   * of one plugin. Only the client assembly uses this.
   */
  @Parameter(defaultValue = "false")
  private boolean base;

  /**
   * Only dependencies in this scope get coordinates, as with {@code dependency:list}: {@code
   * runtime} (compile and runtime), {@code compile} (compile, provided and system), {@code
   * provided}, {@code system} or {@code test}. Empty for all of them.
   */
  @Parameter private String includeScope;

  /** Dependency types to leave out, such as {@code zip} or {@code pom}. */
  @Parameter private List<String> excludeTypes = new ArrayList<>();

  @Parameter(property = "hop.manifest.skip", defaultValue = "false")
  private boolean skip;

  @Override
  public void execute() throws MojoExecutionException {
    if (skip) {
      getLog().info("Plugin manifest skipped");
      return;
    }
    if (includeScope != null && !includeScope.isBlank() && !INCLUDE_SCOPES.contains(includeScope)) {
      throw new MojoExecutionException(
          "includeScope must be runtime, compile, provided, system or test, not " + includeScope);
    }
    Path zipPath = zipToDescribe();
    PluginManifestGenerator generator = new PluginManifestGenerator(getLog()::warn);
    try {
      PluginManifest manifest =
          generator.run(
              zipPath,
              project.getGroupId(),
              project.getArtifactId(),
              project.getVersion(),
              dependencies(),
              project.getFile() == null ? null : project.getFile().toPath(),
              base,
              outputFile == null ? null : outputFile.toPath());
      getLog().info(PluginManifestGenerator.summary(manifest));
    } catch (IOException e) {
      throw new MojoExecutionException("Unable to add the plugin manifest to " + zipPath, e);
    }
  }

  private Path zipToDescribe() throws MojoExecutionException {
    if (zip != null) {
      if (!zip.isFile()) {
        throw new MojoExecutionException("No zip at " + zip);
      }
      return zip.toPath();
    }
    Path named =
        Path.of(project.getBuild().getDirectory(), project.getBuild().getFinalName() + ".zip");
    if (Files.isRegularFile(named)) {
      return named;
    }
    List<File> attached = new ArrayList<>();
    for (Artifact artifact : project.getAttachedArtifacts()) {
      if ("zip".equals(artifact.getType()) && artifact.getFile() != null) {
        attached.add(artifact.getFile());
      }
    }
    if (attached.size() == 1) {
      return attached.get(0).toPath();
    }
    throw new MojoExecutionException(
        (attached.isEmpty() ? "No zip found" : "Several zips found " + attached)
            + ": run hop:manifest after the assembly that builds the plugin zip, in the same"
            + " phase, or name the zip with the zip parameter");
  }

  /** The module's resolved dependencies, as {@code dependency:list} would list them. */
  DependencyList dependencies() {
    List<DependencyList.Artifact> artifacts = new ArrayList<>();
    for (Artifact artifact : project.getArtifacts()) {
      if (inScope(includeScope, artifact.getScope())
          && !excludeTypes.contains(artifact.getType())) {
        artifacts.add(toListArtifact(artifact));
      }
    }
    return DependencyList.of(artifacts);
  }

  static DependencyList.Artifact toListArtifact(Artifact artifact) {
    return new DependencyList.Artifact(
        artifact.getGroupId(),
        artifact.getArtifactId(),
        artifact.getType(),
        artifact.getClassifier(),
        artifact.getBaseVersion(),
        artifact.getScope(),
        artifact.getFile() == null ? null : artifact.getFile().getAbsolutePath());
  }

  /** Whether {@code scope} is part of {@code includeScope}, with the dependency plugin's rules. */
  static boolean inScope(String includeScope, String scope) {
    if (includeScope == null || includeScope.isBlank() || "test".equals(includeScope)) {
      return true;
    }
    return switch (includeScope) {
      case "runtime" -> "compile".equals(scope) || "runtime".equals(scope);
      case "compile" ->
          "compile".equals(scope) || "provided".equals(scope) || "system".equals(scope);
      default -> includeScope.equals(scope);
    };
  }
}
