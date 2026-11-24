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

package org.apache.hop.pipeline.transforms.sql;

import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.databases.h2.H2DatabaseMeta;

/**
 * H2, declared as a database where a failed statement aborts the transaction and savepoints are
 * used to recover from it, like PostgreSQL. Combined with a connection that really aborts, it lets
 * a test drive the savepoint path of the transform.
 */
@DatabaseMetaPlugin(type = "H2_SAVEPOINTS", typeDescription = "H2 with savepoints")
public class SavepointH2DatabaseMeta extends H2DatabaseMeta {
  @Override
  public boolean isUseSafePoints() {
    return true;
  }
}
