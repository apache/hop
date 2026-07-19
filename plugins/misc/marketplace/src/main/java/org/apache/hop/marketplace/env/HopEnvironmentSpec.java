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

package org.apache.hop.marketplace.env;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

/**
 * Declarative Hop environment file ({@code hop-env.yaml} / {@code hop-env.json}).
 *
 * <pre>
 * version: "1.0"
 * hopVersion: "2.19.0"
 * enforceOnRun: false
 * repositories:
 *   - id: central
 *     url: https://repo1.maven.org/maven2/
 * plugins:
 *   - artifactId: hop-tech-parquet
 *     version: "2.19.0"
 * dependencies:
 *   - groupId: org.postgresql
 *     artifactId: postgresql
 *     version: "42.7.3"
 * </pre>
 */
@Getter
@Setter
public class HopEnvironmentSpec {
  private String version = "1.0";
  private String hopVersion;
  private boolean enforceOnRun;
  private List<RepositoryRef> repositories = new ArrayList<>();
  private List<PluginRef> plugins = new ArrayList<>();
  private List<DependencyRef> dependencies = new ArrayList<>();

  @Getter
  @Setter
  public static class RepositoryRef {
    private String id;
    private String url;
  }

  @Getter
  @Setter
  public static class PluginRef {
    private String groupId;
    private String artifactId;
    private String version;
  }

  @Getter
  @Setter
  public static class DependencyRef {
    private String groupId;
    private String artifactId;
    private String version;

    /** Target directory under Hop home; default {@code lib/jdbc}. */
    private String target = "lib/jdbc";
  }
}
