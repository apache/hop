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

/** {@code hop driver list}: print the driver catalog and each driver's install status. */
@SuppressWarnings("java:S106")
@Command(
    name = "list",
    description = "List the JDBC drivers in the catalog and their install status",
    mixinStandardHelpOptions = true)
public class DriverListCommand implements Callable<Integer> {

  @Option(
      names = {"--target"},
      description =
          "Only report install status for this folder "
              + "(default: all HOP_SHARED_JDBC_FOLDERS entries, else lib/jdbc)")
  private String targetFolder;

  @Override
  public Integer call() throws HopException {
    DriverCatalog catalog = DriverCatalog.load();
    List<File> folders =
        (targetFolder != null && !targetFolder.isBlank())
            ? List.of(new File(targetFolder).getAbsoluteFile())
            : DriverInstaller.configuredJdbcFolders();

    System.out.println("JDBC drivers declared by the database plugins");
    System.out.println("Scanned folders: " + folders);
    System.out.println();
    String format = "%-12s %-26s %-5s %-10s %s%n";
    System.out.printf(format, "ID", "DATABASE", "LIC", "INSTALLED", "ARTIFACT:VERSION");
    System.out.printf(format, "----", "--------", "---", "---------", "----------------");

    for (DriverDefinition driver : catalog.list()) {
      boolean installed =
          folders.stream().anyMatch(folder -> DriverCatalog.isInstalled(driver, folder));
      System.out.printf(
          format,
          driver.getId(),
          truncate(driver.getName(), 26),
          driver.getDownload().getLicenseCategory(),
          installed ? "yes" : "no",
          driver.toCoordinate(null));
    }

    System.out.println();
    System.out.println("Install a driver with:");
    System.out.println("  hop driver install <id> [--accept-license]");
    System.out.println("--accept-license is required for restricted (LIC=X) drivers.");
    return 0;
  }

  private static String truncate(String value, int max) {
    if (value == null) {
      return "";
    }
    return value.length() <= max ? value : value.substring(0, max - 1) + "…";
  }
}
