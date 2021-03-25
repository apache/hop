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

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.plugins.ParentFirst;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

/** Provides metadata for the Cassandra SSTable output transform. */
@Transform(
    id = "SSTableOutput",
    image = "Cassandra.svg",
    name = "SSTable output",
    documentationUrl = "Products/SSTable_Output",
    description = "Writes to a filesystem directory as a Cassandra SSTable",
    categoryDescription = "Cassandra")
@InjectionSupported(localizationPrefix = "SSTableOutput.Injection.")
@ParentFirst(patterns = {".*"})
public class SSTableOutputMeta extends BaseTransformMeta
    implements ITransformMeta<SSTableOutput, SSTableOutputData> {

  protected static final Class<?> PKG = SSTableOutputMeta.class;

  /** The path to the yaml file */
  @Injection(name = "YAML_FILE_PATH")
  protected String yamlPath;

  /** The directory to output to */
  @Injection(name = "DIRECTORY")
  protected String directory;

  /** The keyspace (database) to use */
  @Injection(name = "CASSANDRA_KEYSPACE")
  protected String cassandraKeyspace;

  /** The table to write to */
  @Injection(name = "TABLE")
  protected String table = "";

  /** The field in the incoming data to use as the key for inserts */
  @Injection(name = "KEY_FIELD")
  protected String keyField = "";

  /** Size (MB) of write buffer */
  @Injection(name = "BUFFER_SIZE")
  protected String bufferSize = "16";

  /** Whether to use CQL version 3 */
  protected boolean useCql3 = true;

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
  public void setTableName(String table) {
    this.table = table;
  }

  /**
   * Get the name of the table to write to
   *
   * @return the name of the table to write to
   */
  public String getTableName() {
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

  /**
   * Set whether to use CQL version 3 is to be used for CQL IO mode
   *
   * @param cql3 true if CQL version 3 is to be used
   */
  public void setUseCql3(boolean cql3) {
    useCql3 = cql3;
  }

  /**
   * Get whether to use CQL version 3 is to be used for CQL IO mode
   *
   * @return true if CQL version 3 is to be used
   */
  public boolean getUseCql3() {
    return useCql3;
  }

  @Override
  public boolean supportsErrorHandling() {
    // enable define error handling option
    return true;
  }

  @Override
  public String getXml() {
    StringBuffer xml = new StringBuffer();
    xml.append(XmlHandler.addTagValue("yaml_path", yamlPath));
    xml.append(XmlHandler.addTagValue("output_directory", directory));
    xml.append(XmlHandler.addTagValue("cassandra_keyspace", cassandraKeyspace));
    xml.append(XmlHandler.addTagValue("table", table));
    xml.append(XmlHandler.addTagValue("key_field", keyField));
    xml.append(XmlHandler.addTagValue("buffer_size_mb", bufferSize));
    xml.append(XmlHandler.addTagValue("use_cql3", useCql3));
    return xml.toString();
  }

  @Override
  public void loadXml(Node node, IHopMetadataProvider metadataProvider) throws HopXmlException {
    yamlPath = XmlHandler.getTagValue(node, "yaml_path");
    directory = XmlHandler.getTagValue(node, "output_directory");
    cassandraKeyspace = XmlHandler.getTagValue(node, "cassandra_keyspace");
    table = XmlHandler.getTagValue(node, "table");
    keyField = XmlHandler.getTagValue(node, "key_field");
    bufferSize = XmlHandler.getTagValue(node, "buffer_size_mb");
    useCql3 = "Y".equalsIgnoreCase(XmlHandler.getTagValue(node, "use_cql3"));
  }

  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info) {

    CheckResult cr;

    if ((prev == null) || (prev.size() == 0)) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_WARNING,
              "Not receiving any fields from previous transforms!",
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              "Transform is connected to previous one, receiving " + prev.size() + " fields",
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              "Transform is receiving info from other transforms.",
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              "No input received from other transforms!",
              transformMeta);
      remarks.add(cr);
    }
  }

  public void setDefault() {
    directory = System.getProperty("java.io.tmpdir");
    bufferSize = "16";
    table = "";
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      SSTableOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new SSTableOutput(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public SSTableOutputData getTransformData() {
    return new SSTableOutputData();
  }
}
