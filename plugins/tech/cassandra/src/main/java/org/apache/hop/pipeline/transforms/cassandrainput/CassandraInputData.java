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
package org.apache.hop.pipeline.transforms.cassandrainput;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.databases.cassandra.metadata.CassandraConnection;
import org.apache.hop.databases.cassandra.spi.Connection;
import org.apache.hop.databases.cassandra.spi.Keyspace;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

/**
 * Data class for the CassandraInput transform. Contains some utility methods for obtaining a
 * connection to cassandra, translating a row from cassandra to Hop and for compressing a query.
 */
public class CassandraInputData extends BaseTransformData implements ITransformData {

  public CassandraConnection cassandraConnection;
  public Connection connection;
  public Keyspace keyspace;
  public IRowMeta outputRowMeta;
}
