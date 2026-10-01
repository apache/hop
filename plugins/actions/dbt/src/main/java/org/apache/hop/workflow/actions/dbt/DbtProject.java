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

package org.apache.hop.workflow.actions.dbt;

import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;

/**
 * Shared, reusable definition of a dbt Core project: where it lives, how its profiles are resolved,
 * the default target, and which dbt runtime executable to invoke. Registered once and referenced by
 * many {@link ActionDbt} actions — the same pattern as a database connection.
 *
 * <p>All path/target fields are stored verbatim and may contain Hop variables ({@code ${ENV}}); the
 * action resolves them at execution time.
 */
@HopMetadata(
    key = "dbt-project",
    name = "i18n::DbtProject.Name",
    description = "i18n::DbtProject.Description",
    image = "dbt.svg",
    documentationUrl = "/metadata-types/dbt-project.html")
public class DbtProject extends HopMetadataBase implements IHopMetadata {

  /** Directory containing {@code dbt_project.yml}. */
  @HopMetadataProperty private String projectDirectory;

  /** Directory containing {@code profiles.yml}; empty = dbt default (~/.dbt). */
  @HopMetadataProperty private String profilesDirectory;

  /** Default dbt target when an action does not override it (may be variable-driven). */
  @HopMetadataProperty private String defaultTarget;

  /**
   * The dbt executable used for plain runs (path or name on PATH). Default {@code dbt}. Designed to
   * be swappable so the runtime can later be a container wrapper.
   */
  @HopMetadataProperty private String dbtExecutable;

  /**
   * The OpenLineage-enabled dbt wrapper used when lineage emission is on. Default {@code dbt-ol}
   * (from the {@code openlineage-dbt} package). When lineage is enabled, the action invokes this
   * instead of {@link #dbtExecutable}.
   */
  @HopMetadataProperty private String dbtOlExecutable;

  public DbtProject() {
    this.dbtExecutable = "dbt";
    this.dbtOlExecutable = "dbt-ol";
  }

  public String getProjectDirectory() {
    return projectDirectory;
  }

  public void setProjectDirectory(String projectDirectory) {
    this.projectDirectory = projectDirectory;
  }

  public String getProfilesDirectory() {
    return profilesDirectory;
  }

  public void setProfilesDirectory(String profilesDirectory) {
    this.profilesDirectory = profilesDirectory;
  }

  public String getDefaultTarget() {
    return defaultTarget;
  }

  public void setDefaultTarget(String defaultTarget) {
    this.defaultTarget = defaultTarget;
  }

  public String getDbtExecutable() {
    return dbtExecutable;
  }

  public void setDbtExecutable(String dbtExecutable) {
    this.dbtExecutable = dbtExecutable;
  }

  public String getDbtOlExecutable() {
    return dbtOlExecutable;
  }

  public void setDbtOlExecutable(String dbtOlExecutable) {
    this.dbtOlExecutable = dbtOlExecutable;
  }
}
