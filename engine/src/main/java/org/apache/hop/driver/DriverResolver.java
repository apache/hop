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

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.collection.DependencyCollectionContext;
import org.eclipse.aether.collection.DependencySelector;
import org.eclipse.aether.connector.basic.BasicRepositoryConnectorFactory;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.graph.Exclusion;
import org.eclipse.aether.impl.DefaultServiceLocator;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.ArtifactResult;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.resolution.DependencyResult;
import org.eclipse.aether.spi.connector.RepositoryConnectorFactory;
import org.eclipse.aether.spi.connector.transport.TransporterFactory;
import org.eclipse.aether.transport.file.FileTransporterFactory;
import org.eclipse.aether.transport.http.HttpTransporterFactory;
import org.eclipse.aether.util.artifact.JavaScopes;
import org.eclipse.aether.util.filter.DependencyFilterUtils;
import org.eclipse.aether.util.graph.selector.AndDependencySelector;
import org.eclipse.aether.util.graph.selector.ExclusionDependencySelector;
import org.eclipse.aether.util.graph.selector.ScopeDependencySelector;

/**
 * Resolves a Maven coordinate (and its transitive runtime dependencies) to a set of jar files,
 * downloading from a remote repository into a local cache. Wraps the Maven Artifact Resolver
 * (ex-Eclipse Aether).
 *
 * <p>ASF note: this never bundles anything. It downloads, at the caller's explicit request, from
 * Maven Central (or a user-supplied repository) straight onto the user's machine.
 */
public class DriverResolver {

  /** Maven Central. Deliberately NOT an ASF- or Hop-operated mirror (ASF policy on Cat-X bytes). */
  public static final String MAVEN_CENTRAL = "https://repo1.maven.org/maven2/";

  private final File localCacheDir;

  public DriverResolver(File localCacheDir) {
    this.localCacheDir = localCacheDir;
  }

  /** A resolved jar: its Maven artifactId, version and the downloaded file on disk. */
  public record ResolvedArtifact(String artifactId, String version, File file) {}

  /**
   * Resolve a coordinate to the jar plus its transitive runtime dependency jars, downloading as
   * needed.
   *
   * @param coordinate {@code groupId:artifactId:version}
   * @param remoteRepoUrl primary repository base URL, or null for Maven Central
   * @param excludes Maven coordinates ({@code groupId:artifactId}, artifactId may be {@code *}) to
   *     exclude, e.g. dependencies Hop already ships in lib/core. Optional and test/provided
   *     dependencies are always skipped.
   * @param driverRepoUrl optional extra repository to search (for drivers not on Maven Central),
   *     searched in addition to the primary repository; null to use only the primary
   * @return the resolved artifacts: the driver plus its (non-optional) transitive runtime
   *     dependencies, minus the exclusions
   */
  public List<ResolvedArtifact> resolve(
      String coordinate, String remoteRepoUrl, List<String> excludes, String driverRepoUrl)
      throws HopException {
    try {
      RepositorySystem system = newRepositorySystem();
      DefaultRepositorySystemSession session = newSession(system);

      List<RemoteRepository> repositories = new ArrayList<>();
      repositories.add(
          new RemoteRepository.Builder(
                  "primary",
                  "default",
                  (remoteRepoUrl == null || remoteRepoUrl.isBlank())
                      ? MAVEN_CENTRAL
                      : remoteRepoUrl)
              .build());
      if (driverRepoUrl != null && !driverRepoUrl.isBlank()) {
        repositories.add(
            new RemoteRepository.Builder("driver-repo", "default", driverRepoUrl).build());
      }

      Artifact artifact = new DefaultArtifact(coordinate);
      CollectRequest collectRequest = new CollectRequest();
      collectRequest.setRoot(
          new Dependency(artifact, JavaScopes.RUNTIME, false, toExclusions(excludes)));
      collectRequest.setRepositories(repositories);

      DependencyRequest dependencyRequest =
          new DependencyRequest(
              collectRequest, DependencyFilterUtils.classpathFilter(JavaScopes.RUNTIME));

      DependencyResult result = system.resolveDependencies(session, dependencyRequest);

      List<ResolvedArtifact> artifacts = new ArrayList<>();
      for (ArtifactResult artifactResult : result.getArtifactResults()) {
        Artifact resolved = artifactResult.getArtifact();
        if (resolved.getFile() != null) {
          artifacts.add(
              new ResolvedArtifact(
                  resolved.getArtifactId(), resolved.getVersion(), resolved.getFile()));
        }
      }
      return artifacts;
    } catch (Exception e) {
      throw new HopException("Unable to resolve driver artifact '" + coordinate + "'", e);
    }
  }

  @SuppressWarnings("deprecation") // ServiceLocator is the simplest bootstrap on resolver 1.9.x
  private static RepositorySystem newRepositorySystem() {
    DefaultServiceLocator locator = MavenRepositorySystemUtils.newServiceLocator();
    locator.addService(RepositoryConnectorFactory.class, BasicRepositoryConnectorFactory.class);
    locator.addService(TransporterFactory.class, HttpTransporterFactory.class);
    locator.addService(TransporterFactory.class, FileTransporterFactory.class);
    return locator.getService(RepositorySystem.class);
  }

  private DefaultRepositorySystemSession newSession(RepositorySystem system) {
    DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();
    LocalRepository localRepo = new LocalRepository(localCacheDir);
    session.setLocalRepositoryManager(system.newLocalRepositoryManager(session, localRepo));
    // Skip optional and test/provided dependencies, and honour per-dependency exclusions. This is
    // the Maven default selector with OptionalDependencySelector replaced so that even direct
    // optional dependencies (e.g. a driver's Windows-only auth lib) are dropped.
    session.setDependencySelector(
        new AndDependencySelector(
            new ScopeDependencySelector(JavaScopes.TEST, JavaScopes.PROVIDED),
            new SkipOptionalDependencySelector(),
            new ExclusionDependencySelector()));
    return session;
  }

  /**
   * Build Aether exclusions from {@code groupId:artifactId} strings (artifactId may be {@code *}).
   *
   * <p>Package-private so the parsing can be unit tested without resolving anything over the
   * network.
   */
  static Collection<Exclusion> toExclusions(List<String> excludes) {
    if (excludes == null || excludes.isEmpty()) {
      return Collections.emptyList();
    }
    List<Exclusion> exclusions = new ArrayList<>();
    for (String exclude : excludes) {
      if (exclude == null || exclude.isBlank()) {
        continue;
      }
      String[] parts = exclude.split(":");
      String groupId = parts[0].trim();
      String artifactId = parts.length > 1 ? parts[1].trim() : "*";
      exclusions.add(new Exclusion(groupId, artifactId, "*", "*"));
    }
    return exclusions;
  }

  /** A dependency selector that drops every optional dependency, at any depth. */
  private static class SkipOptionalDependencySelector implements DependencySelector {
    @Override
    public boolean selectDependency(Dependency dependency) {
      return !dependency.isOptional();
    }

    @Override
    public DependencySelector deriveChildSelector(DependencyCollectionContext context) {
      return this;
    }
  }
}
