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
package org.apache.hop.pipeline.transforms.cassandrasstableoutput.writer;

import java.util.Map;
import org.apache.hop.databases.cassandra.util.CassandraUtils;

public abstract class AbstractSSTableWriter {
  private static final int DEFAULT_BUFFER_SIZE_MB = 16;
  private int bufferSize = DEFAULT_BUFFER_SIZE_MB;
  private String directory = System.getProperty("java.io.tmpdir");
  private String keyspace;
  private String table;
  private String primaryKey;
  private String partitionerClass;

  public abstract void init() throws Exception;

  public abstract void processRow(Map<String, Object> record) throws Exception;

  public abstract void close() throws Exception;

  protected String getDirectory() {
    return directory;
  }

  /**
   * Set the directory to read the sstables from
   *
   * @param directory the directory to read the sstables from
   */
  public void setDirectory(String directory) {
    this.directory = directory;
  }

  protected String getKeyspace() {
    return keyspace;
  }

  /**
   * Set the target keyspace
   *
   * @param keyspace the keyspace to use
   */
  public void setKeyspace(String keyspace) {
    this.keyspace = keyspace;
  }

  protected String getTable() {
    return table;
  }

  /**
   * Set the table to load to. Note: it is assumed that this table exists in the keyspace apriori.
   *
   * @param table the table to load to.
   */
  public void setTable(String table) {
    this.table = table;
  }

  protected int getBufferSize() {
    return bufferSize;
  }

  /**
   * Set the buffer size (Mb) to use. A new table file is written every time the buffer is full.
   *
   * @param bufferSize the size of the buffer to use
   */
  public void setBufferSize(int bufferSize) {
    this.bufferSize = bufferSize;
  }

  protected String getPrimaryKey() {
    return primaryKey;
  }

  public void setPrimaryKey(String primaryKey) {
    this.primaryKey = primaryKey;
  }

  protected String getPartitionerClass() {
    return partitionerClass;
  }

  public void setPartitionerClass(String partitionerClass) {
    this.partitionerClass = partitionerClass;
  }

  protected String getPartitionKey() {
    return CassandraUtils.getPartitionKey(primaryKey);
  }
}
