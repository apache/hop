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
 *
 */

package org.apache.hop.neo4j.shared;

import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.neo4j.driver.Driver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DriverSingleton {

  private static DriverSingleton singleton;

  private Map<String, Driver> driverMap;

  private DriverSingleton() {
    driverMap = new HashMap<>();
  }

  public static DriverSingleton getInstance() {
    if (singleton == null) {
      singleton = new DriverSingleton();
    }
    return singleton;
  }

  public static Driver getDriver(ILogChannel log, IVariables variables, NeoConnection connection) {
    DriverSingleton ds = getInstance();

    String key = getDriverKey(connection, variables);

    Driver driver = ds.driverMap.get(key);
    if (driver == null) {
      driver = connection.getDriver(log, variables);
      ds.driverMap.put(key, driver);
    }

    return driver;
  }

  public static void closeAll() {
    DriverSingleton ds = getInstance();

    List<String> keys = new ArrayList<>(ds.getDriverMap().keySet());
    for (String key : keys) {
      synchronized (ds.getDriverMap()) {
        Driver driver = ds.driverMap.get(key);
        driver.close();
        ds.driverMap.remove(key);
      }
    }
  }

  private static String getDriverKey(NeoConnection connection, IVariables variables) {
    String hostname = variables.resolve(connection.getServer());
    String boltPort = variables.resolve(connection.getBoltPort());
    String username = variables.resolve(connection.getUsername());

    return hostname + ":" + boltPort + "@" + username;
  }

  /**
   * Gets driverMap
   *
   * @return value of driverMap
   */
  public Map<String, Driver> getDriverMap() {
    return driverMap;
  }
}
