/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.apache.hop.databases.cassandra;

import org.apache.hop.databases.cassandra.driver.datastax.DriverConnection;
import org.apache.hop.databases.cassandra.spi.Connection;

public class ConnectionFactory {
  private static ConnectionFactory s_singleton = new ConnectionFactory();

  public static enum Driver {
    ASTYANAX,
    BINARY_CQL3_PROTOCOL;
  }

  private ConnectionFactory() {}

  public static ConnectionFactory getFactory() {
    return s_singleton;
  }

  public Connection getConnection(Driver d) {
    switch (d) {
      case BINARY_CQL3_PROTOCOL:
        return new DriverConnection();
      default:
        return null;
    }
  }
}
