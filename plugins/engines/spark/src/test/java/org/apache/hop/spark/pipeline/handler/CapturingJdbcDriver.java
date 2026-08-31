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

package org.apache.hop.spark.pipeline.handler;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;

/**
 * Test driver that records the connection properties Spark builds and then delegates to the real
 * database behind the {@link #PREFIX}.
 *
 * <p>Used by {@link SparkFileIoJdbcWriteTest} to check directly that {@code path} never reaches a
 * driver (issue #8138), rather than depending on a driver being strict enough to reject it —
 * Teradata rejects it, but Postgres, MySQL and H2 all ignore properties they do not recognise.
 *
 * <p>Top-level rather than nested on purpose: Spark's {@code DriverRegistry} matches a registered
 * driver by canonical name, which for a nested class does not equal the binary name it is loaded
 * by, and the lookup then fails with an internal error.
 */
public class CapturingJdbcDriver implements Driver {

  public static final String PREFIX = "jdbc:capture:";
  public static final List<Properties> CAPTURED = new CopyOnWriteArrayList<>();

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }
    Properties copy = new Properties();
    copy.putAll(info);
    CAPTURED.add(copy);
    return DriverManager.getConnection(url.substring(PREFIX.length()), info);
  }

  @Override
  public boolean acceptsURL(String url) {
    return url != null && url.startsWith(PREFIX);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
    return new DriverPropertyInfo[0];
  }

  @Override
  public int getMajorVersion() {
    return 1;
  }

  @Override
  public int getMinorVersion() {
    return 0;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() {
    return Logger.getLogger("hop-8138-capturing-driver");
  }
}
