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
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.hop.core.exception.HopException;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * {@code hop driver install <id>}: download a JDBC driver (and its transitive runtime dependencies)
 * and install it into lib/jdbc.
 *
 * <p>For restricted (Category X) drivers the install is refused unless {@code --accept-license} is
 * given: Apache Hop neither ships nor hosts these drivers; you obtain them from the vendor / Maven
 * Central under the vendor's license, at your own request.
 */
@SuppressWarnings("java:S106")
@Command(
    name = "install",
    description = "Download and install a JDBC driver into the configured JDBC folder",
    mixinStandardHelpOptions = true)
public class DriverInstallCommand implements Callable<Integer> {

  private static final int EXIT_OK = 0;
  private static final int EXIT_LICENSE_REQUIRED = 2;
  private static final int EXIT_UNKNOWN_DRIVER = 3;

  @Parameters(
      index = "0",
      paramLabel = "<driver-id>",
      description = "Driver id from 'hop driver list' (e.g. oracle, mysql, mariadb, db2)")
  private String driverId;

  @Option(
      names = {"--driver-version"},
      description = "Version to install (default: the catalog's default version)")
  private String version;

  @Option(
      names = {"--accept-license"},
      description = "Accept the vendor license. Required for restricted (LIC=X) drivers.")
  private boolean acceptLicense;

  @Option(
      names = {"--repo"},
      description = "Maven repository base URL to download from (default: Maven Central)")
  private String repoUrl;

  @Option(
      names = {"--target"},
      description =
          "Target folder to install into "
              + "(default: the first non-lib/jdbc HOP_SHARED_JDBC_FOLDERS entry, else lib/jdbc)")
  private String targetFolder;

  @Option(
      names = {"--force"},
      description = "Re-download and re-install even if the same version is already installed.")
  private boolean force;

  @Override
  public Integer call() throws HopException {
    DriverCatalog catalog = DriverCatalog.load();
    DriverDefinition driver = catalog.get(driverId);
    if (driver == null) {
      System.err.println("Unknown driver id: '" + driverId + "'.");
      System.err.println("Run 'hop driver list' to see the available drivers.");
      return EXIT_UNKNOWN_DRIVER;
    }

    if (driver.isRestricted() && !acceptLicense) {
      printLicenseNotice(driver);
      return EXIT_LICENSE_REQUIRED;
    }

    File target =
        (targetFolder != null && !targetFolder.isBlank())
            ? new File(targetFolder).getAbsoluteFile()
            : DriverInstaller.defaultInstallFolder();

    String installVersion =
        (version != null && !version.isBlank())
            ? version
            : driver.getDownload().getDefaultVersion();
    String artifactId = driver.getDownload().getArtifactId();
    List<DriverInstaller.InstalledDriver> existing = DriverInstaller.findInstalled(artifactId);

    boolean sameVersionInTarget =
        existing.stream()
            .anyMatch(p -> p.folder().equals(target) && p.version().equals(installVersion));
    if (sameVersionInTarget && !force) {
      System.out.println(
          driver.getName() + " " + installVersion + " is already installed in " + target + ".");
      System.out.println("Use --force to download and re-install it.");
      return EXIT_OK;
    }

    // Warn about copies in other scanned folders (e.g. the bundled lib/jdbc) that are left in
    // place.
    for (DriverInstaller.InstalledDriver present : existing) {
      if (!present.folder().equals(target)) {
        System.out.println(
            "Note: "
                + driver.getName()
                + " "
                + present.version()
                + " is also present in "
                + present.folder()
                + "; it will not be removed, so you may want to delete it to avoid version conflicts.");
      }
    }

    String coordinate = driver.toCoordinate(version);
    System.out.println("Installing driver '" + driver.getId() + "' (" + driver.getName() + ")");
    System.out.println("  artifact : " + coordinate);
    System.out.println(
        "  license  : "
            + driver.getDownload().getLicenseName()
            + " (category "
            + driver.getDownload().getLicenseCategory()
            + ")");
    System.out.println(
        "  from     : " + (repoUrl != null ? repoUrl : DriverResolver.MAVEN_CENTRAL));
    System.out.println("  into     : " + target);
    System.out.println();

    List<File> installed =
        new DriverInstaller().install(driver.getDownload(), version, repoUrl, target);

    System.out.println("Installed " + installed.size() + " jar(s):");
    for (File file : installed) {
      System.out.println("  - " + file.getName());
    }
    System.out.println();
    System.out.println("Restart Hop (or the server/worker) so the driver is picked up.");
    return EXIT_OK;
  }

  private void printLicenseNotice(DriverDefinition driver) {
    System.out.println();
    System.out.println(
        "The "
            + driver.getName()
            + " JDBC driver is licensed under "
            + driver.getDownload().getLicenseName()
            + " by "
            + driver.getDownload().getVendor()
            + ".");
    System.out.println("It is NOT distributed with Apache Hop.");
    System.out.println();
    System.out.println("  Driver   : " + driver.toCoordinate(version));
    System.out.println("  License  : " + driver.getDownload().getLicenseUrl());
    System.out.println("  Vendor   : " + driver.getDownload().getVendorUrl());
    if (driver.getDownload().getNotes() != null) {
      System.out.println("  Notes    : " + driver.getDownload().getNotes());
    }
    System.out.println();
    System.out.println(
        "If you accept the vendor's license, Hop can download it from Maven Central onto this");
    System.out.println("machine only. Re-run with --accept-license:");
    System.out.println();
    System.out.println("  hop driver install " + driver.getId() + " --accept-license");
    System.out.println();
  }
}
