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

package org.apache.hop.core.database;

import java.util.List;
import lombok.Builder;
import lombok.Getter;

/**
 * Describes how a database's JDBC driver can be downloaded on demand. A database plugin declares
 * this from {@link IDatabase#getDriverDownload()} so the driver lives in one place - next to the
 * rest of the database metadata - and external plugins can point Hop at their own drivers.
 *
 * <p>This is metadata only; it carries no driver bytes, so it is safe to ship even for restricted
 * (Category X) drivers that Apache Hop may not bundle. Hop downloads the actual jar from Maven
 * Central at the user's explicit request, after the user accepts the vendor license.
 */
@Getter
@Builder
public class DriverDownload {

  /** Maven coordinate without version: {@code groupId:artifactId}. */
  private final String mavenCoordinate;

  /** Version resolved when the caller does not request a specific one. */
  private final String defaultVersion;

  /**
   * ASF license category: {@code A}/{@code B} may be bundled, {@code X} must never be bundled and
   * is only downloaded at the user's explicit request after license acceptance.
   */
  private final String licenseCategory;

  private final String licenseName;
  private final String licenseUrl;
  private final String vendor;
  private final String vendorUrl;
  private final String notes;

  /**
   * Maven coordinates ({@code groupId:artifactId}, artifactId may be {@code *}) to exclude from the
   * download. Use this to drop transitive jars that Hop already ships in lib/core (so the driver
   * uses the shared one via its parent classloader), e.g. {@code
   * com.google.protobuf:protobuf-java}. Optional and test dependencies are skipped automatically
   * and do not need to be listed here.
   */
  @Builder.Default private final List<String> excludes = List.of();

  /**
   * Optional extra Maven repository base URL to resolve this driver from, for drivers that are not
   * on Maven Central (e.g. MonetDB on Clojars). It is searched in addition to Maven Central / the
   * user-supplied repository. Null means Maven Central only.
   */
  private final String repositoryUrl;

  /**
   * @return true when this is a restricted (Category X) driver that must not be bundled.
   */
  public boolean isRestricted() {
    return "X".equalsIgnoreCase(licenseCategory);
  }

  /**
   * @return the full Maven coordinate {@code groupId:artifactId:version}, using {@code version}
   *     when supplied, otherwise {@link #defaultVersion}.
   */
  public String toCoordinate(String version) {
    String v = (version == null || version.isBlank()) ? defaultVersion : version;
    return mavenCoordinate + ":" + v;
  }

  /**
   * @return the artifact id portion of {@link #mavenCoordinate}, used for install detection.
   */
  public String getArtifactId() {
    if (mavenCoordinate == null) {
      return null;
    }
    String[] parts = mavenCoordinate.split(":");
    return parts.length >= 2 ? parts[1] : null;
  }
}
