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
package org.apache.hop.junit.batch;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.hop.core.database.DriverDownload;

/**
 * A JDBC driver that Hop downloads on demand instead of shipping (its licence keeps it out of the
 * build), fetched from Maven Central at the version the dialect's {@link DriverDownload} names and
 * loaded in a class loader of its own, so a test runs against exactly the driver users get.
 */
public final class DownloadedJdbcDriver {

  private static final String CENTRAL = "https://repo1.maven.org/maven2/";

  private final Driver driver;

  private DownloadedJdbcDriver(Driver driver) {
    this.driver = driver;
  }

  /**
   * @param download what the dialect says to download
   * @param driverClass the driver class the dialect uses
   * @param cacheFolder where downloaded jars are kept between runs
   * @return the loaded driver
   */
  public static DownloadedJdbcDriver load(
      DriverDownload download, String driverClass, Path cacheFolder) throws Exception {
    String[] coordinate = download.getMavenCoordinate().split(":");
    String groupId = coordinate[0];
    String artifactId = coordinate[1];
    String version = download.getDefaultVersion();
    String jarName = artifactId + "-" + version + ".jar";
    Files.createDirectories(cacheFolder);
    Path jar = cacheFolder.resolve(jarName);
    if (!Files.exists(jar)) {
      String repository =
          download.getRepositoryUrl() == null ? CENTRAL : download.getRepositoryUrl();
      if (!repository.endsWith("/")) {
        repository = repository + "/";
      }
      URL url =
          URI.create(
                  repository
                      + groupId.replace('.', '/')
                      + "/"
                      + artifactId
                      + "/"
                      + version
                      + "/"
                      + jarName)
              .toURL();
      Path partial = cacheFolder.resolve(jarName + ".part");
      try (InputStream in = url.openStream()) {
        Files.copy(in, partial, StandardCopyOption.REPLACE_EXISTING);
      }
      Files.move(partial, jar, StandardCopyOption.REPLACE_EXISTING);
    }
    URLClassLoader loader =
        new URLClassLoader(
            new URL[] {jar.toUri().toURL()}, DownloadedJdbcDriver.class.getClassLoader());
    Driver driver =
        (Driver) Class.forName(driverClass, true, loader).getDeclaredConstructor().newInstance();
    return new DownloadedJdbcDriver(driver);
  }

  /**
   * @return a connection to the URL with the user and password, as the dialect would open it
   */
  public Connection connect(String url, String user, String password) throws SQLException {
    Properties properties = new Properties();
    properties.setProperty("user", user);
    properties.setProperty("password", password);
    Connection connection = driver.connect(url, properties);
    if (connection == null) {
      throw new SQLException("The driver does not accept the URL " + url);
    }
    return connection;
  }

  /**
   * @return the driver's version, for the test log
   */
  public String version() {
    return driver.getMajorVersion() + "." + driver.getMinorVersion();
  }
}
