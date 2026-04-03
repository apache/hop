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

package org.apache.hop.core.database;

import lombok.Getter;
import lombok.Setter;

/** Class to contain the information needed to partition (cluster): id, hostname, port, database */
@Getter
@Setter
public class PartitionDatabaseMeta {
  String partitionId;

  String hostname;
  String port;
  String databaseName;
  String username;
  String password;

  public PartitionDatabaseMeta() {}

  /**
   * @param partitionId
   * @param hostname
   * @param port
   * @param database
   */
  public PartitionDatabaseMeta(String partitionId, String hostname, String port, String database) {
    super();

    this.partitionId = partitionId;
    this.hostname = hostname;
    this.port = port;
    this.databaseName = database;
  }
}
