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

import java.util.Locale;
import lombok.Getter;
import org.apache.hop.core.database.DriverDownload;

/**
 * A catalog entry combining a database plugin (its type, name and JDBC driver class) with the
 * {@link DriverDownload} descriptor that plugin declares. Assembled by {@link DriverCatalog} from
 * the database plugins - there is no separate catalog file; the driver download lives next to the
 * rest of the database metadata, so external plugins can contribute their own.
 */
@Getter
public class DriverDefinition {

  /** Command-line id: the database type lowercased, e.g. {@code oracle}. */
  private final String id;

  /** The Hop database plugin type, e.g. {@code ORACLE}. */
  private final String databaseType;

  /** The database plugin's display name. */
  private final String name;

  /** The JDBC driver class, e.g. {@code oracle.jdbc.OracleDriver}. */
  private final String driverClass;

  /** The download descriptor declared by the database plugin. */
  private final DriverDownload download;

  public DriverDefinition(
      String databaseType, String name, String driverClass, DriverDownload download) {
    this.databaseType = databaseType;
    this.id = databaseType == null ? null : databaseType.toLowerCase(Locale.ROOT);
    this.name = name;
    this.driverClass = driverClass;
    this.download = download;
  }

  /**
   * @return true when the driver is restricted (Category X) and must not be bundled.
   */
  public boolean isRestricted() {
    return download != null && download.isRestricted();
  }

  /**
   * @return the full Maven coordinate, honouring an optional version override.
   */
  public String toCoordinate(String version) {
    return download == null ? null : download.toCoordinate(version);
  }
}
