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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.marketplace.config.MarketplaceSecrets;

/**
 * Declarative Hop install spec file ({@code hop-env.yaml} / {@code hop-env.json}).
 *
 * <pre>
 * version: "1.0"
 * hopVersion: "2.19.0"
 * enforceOnRun: false
 * repositories:
 *   - id: central
 *     url: <a href="https://repo1.maven.org/maven2/">Maven central</a>
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
public class HopInstallSpec {
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

    /** Optional Basic auth username (prefer env HOP_MARKETPLACE_PASSWORD for secrets). */
    private String username;

    /**
     * Optional Basic auth password, obfuscated in the install spec file the same way as in
     * hop-config.json. Install spec files are meant to be shared, so prefer a variable or {@code
     * HOP_MARKETPLACE_PASSWORD} over a password here.
     */
    @JsonSerialize(using = MarketplaceSecrets.Serializer.class)
    @JsonDeserialize(using = MarketplaceSecrets.Deserializer.class)
    private String password;
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
