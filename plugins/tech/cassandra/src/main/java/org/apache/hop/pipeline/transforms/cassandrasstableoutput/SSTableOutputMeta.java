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
package org.apache.hop.pipeline.transforms.cassandrasstableoutput;

import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.plugins.ParentFirst;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Provides metadata for the Cassandra SSTable output transform. */
@Transform(
    id = "SSTableOutput",
    image = "cassandra.svg",
    name = "SSTable output",
    documentationUrl = "/pipeline/transforms/sstable-output.html",
    description = "Writes to a filesystem directory as a Cassandra SSTable",
    keywords = "i18n::SSTableOutputMeta.keyword",
    categoryDescription = "Cassandra")
@ParentFirst(patterns = {".*"})
public class SSTableOutputMeta extends BaseTransformMeta<SSTableOutput, SSTableOutputData> {

  protected static final Class<?> PKG = SSTableOutputMeta.class;

  /** The path to the yaml file */
  @HopMetadataProperty(
      key = "yaml_path",
      injectionKey = "YAML_FILE_PATH",
      injectionKeyDescription = "SSTableOutput.Injection.YAML_FILE_PATH")
  protected String yamlPath;

  /** The directory to output to */
  @HopMetadataProperty(
      key = "output_directory",
      injectionKey = "DIRECTORY",
      injectionKeyDescription = "SSTableOutput.Injection.DIRECTORY")
  protected String directory;

  /** The keyspace (database) to use */
  @HopMetadataProperty(
      key = "cassandra_keyspace",
      injectionKey = "CASSANDRA_KEYSPACE",
      injectionKeyDescription = "SSTableOutput.Injection.CASSANDRA_KEYSPACE")
  protected String cassandraKeyspace;

  /** The table to write to */
  @HopMetadataProperty(
      key = "table",
      injectionKey = "TABLE",
      injectionKeyDescription = "SSTableOutput.Injection.TABLE")
  protected String table = "";

  /** The field in the incoming data to use as the key for inserts */
  @HopMetadataProperty(
      key = "key_field",
      injectionKey = "KEY_FIELD",
      injectionKeyDescription = "SSTableOutput.Injection.KEY_FIELD")
  protected String keyField = "";

  /** Size (MB) of write buffer */
  @HopMetadataProperty(
      key = "buffer_size_mb",
      injectionKey = "BUFFER_SIZE",
      injectionKeyDescription = "SSTableOutput.Injection.BUFFER_SIZE")
  protected String bufferSize = "16";

  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info) {

    CheckResult cr;

    if ((prev == null) || (prev.isEmpty())) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              "Not receiving any fields from previous transforms!",
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              "Transform is connected to previous one, receiving " + prev.size() + " fields",
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              "Transform is receiving info from other transforms.",
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              "No input received from other transforms!",
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public void setDefault() {
    directory = System.getProperty("java.io.tmpdir");
    bufferSize = "16";
    table = "";
  }

  @Override
  public boolean supportsErrorHandling() {
    // enable define error handling option
    return true;
  }

  /**
   * Get the path the the yaml file
   *
   * @return the path to the yaml file
   */
  public String getYamlPath() {
    return yamlPath;
  }

  /**
   * Set the path the the yaml file
   *
   * @param path the path to the yaml file
   */
  public void setYamlPath(String path) {
    yamlPath = path;
  }

  /**
   * Where the SSTables are written to
   *
   * @return String directory
   */
  public String getDirectory() {
    return directory;
  }

  /**
   * Where the SSTables are written to
   *
   * @param directory String
   */
  public void setDirectory(String directory) {
    this.directory = directory;
  }

  /**
   * Set the keyspace (db) to use
   *
   * @param keyspace the keyspace to use
   */
  public void setCassandraKeyspace(String keyspace) {
    cassandraKeyspace = keyspace;
  }

  /**
   * Get the keyspace (db) to use
   *
   * @return the keyspace (db) to use
   */
  public String getCassandraKeyspace() {
    return cassandraKeyspace;
  }

  /**
   * Set the table to write to
   *
   * @param table the name of the table to write to
   */
  public void setTable(String table) {
    this.table = table;
  }

  /**
   * Get the name of the table to write to
   *
   * @return the name of the table to write to
   */
  public String getTable() {
    return table;
  }

  /**
   * Set the incoming field to use as the key for inserts
   *
   * @param keyField the name of the incoming field to use as the key
   */
  public void setKeyField(String keyField) {
    this.keyField = keyField;
  }

  /**
   * Get the name of the incoming field to use as the key for inserts
   *
   * @return the name of the incoming field to use as the key for inserts
   */
  public String getKeyField() {
    return keyField;
  }

  /**
   * Size (MB) of write buffer
   *
   * @return String
   */
  public String getBufferSize() {
    return bufferSize;
  }

  /**
   * Size (MB) of write buffer
   *
   * @param bufferSize String
   */
  public void setBufferSize(String bufferSize) {
    this.bufferSize = bufferSize;
  }
}
