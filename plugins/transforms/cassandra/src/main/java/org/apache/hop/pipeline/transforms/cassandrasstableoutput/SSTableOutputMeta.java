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
package org.apache.hop.pipeline.transforms.cassandrasstableoutput;

import java.util.List;
import java.util.Map;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Counter;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.plugins.ParentFirst;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

/** Provides metadata for the Cassandra SSTable output step. */
@Transform(
    id = "SSTableOutput",
    image = "Cassandra.svg",
    name = "SSTable output",
    documentationUrl = "Products/SSTable_Output",
    description = "Writes to a filesystem directory as a Cassandra SSTable",
    categoryDescription = "Big Data")
@InjectionSupported(localizationPrefix = "SSTableOutput.Injection.")
@ParentFirst(patterns = {".*"})
public class SSTableOutputMeta extends BaseTransformMeta
    implements ITransformMeta<SSTableOutput, SSTableOutputData> {

  protected static final Class<?> PKG = SSTableOutputMeta.class;

  /** The path to the yaml file */
  @Injection(name = "YAML_FILE_PATH")
  protected String m_yamlPath;

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
  protected boolean m_useCQL3 = true;

  /**
   * Get the path the the yaml file
   *
   * @return the path to the yaml file
   */
  public String getYamlPath() {
    return m_yamlPath;
  }

  /**
   * Set the path the the yaml file
   *
   * @param path the path to the yaml file
   */
  public void setYamlPath(String path) {
    m_yamlPath = path;
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
  public void setUseCQL3(boolean cql3) {
    m_useCQL3 = cql3;
  }

  /**
   * Get whether to use CQL version 3 is to be used for CQL IO mode
   *
   * @return true if CQL version 3 is to be used
   */
  public boolean getUseCQL3() {
    return m_useCQL3;
  }

  @Override
  public boolean supportsErrorHandling() {
    // enable define error handling option
    return true;
  }

  @Override
  public String getXml() {
    StringBuffer retval = new StringBuffer();

    if (!Utils.isEmpty(m_yamlPath)) {
      retval.append("\n    ").append(XmlHandler.addTagValue("yaml_path", m_yamlPath));
    }

    if (!Utils.isEmpty(directory)) {
      retval.append("\n    ").append(XmlHandler.addTagValue("output_directory", directory));
    }

    if (!Utils.isEmpty(cassandraKeyspace)) {
      retval
          .append("\n    ")
          .append(XmlHandler.addTagValue("cassandra_keyspace", cassandraKeyspace));
    }

    if (!Utils.isEmpty(table)) {
      retval.append("\n    ").append(XmlHandler.addTagValue("table", table));
    }

    if (!Utils.isEmpty(keyField)) {
      retval.append("\n    ").append(XmlHandler.addTagValue("key_field", keyField));
    }

    if (!Utils.isEmpty(bufferSize)) {
      retval.append("\n    ").append(XmlHandler.addTagValue("buffer_size_mb", bufferSize));
    }

    retval
        .append("\n    ")
        .append( //$NON-NLS-1$
            XmlHandler.addTagValue("use_cql3", m_useCQL3)); // $NON-NLS-1$

    return retval.toString();
  }

  public void loadXml(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters)
      throws HopXmlException {
    m_yamlPath = XmlHandler.getTagValue(stepnode, "yaml_path");
    directory = XmlHandler.getTagValue(stepnode, "output_directory");
    cassandraKeyspace = XmlHandler.getTagValue(stepnode, "cassandra_keyspace");
    table = XmlHandler.getTagValue(stepnode, "table");
    keyField = XmlHandler.getTagValue(stepnode, "key_field");
    bufferSize = XmlHandler.getTagValue(stepnode, "buffer_size_mb");

    String useCQL3 = XmlHandler.getTagValue(stepnode, "use_cql3"); // $NON-NLS-1$
    if (!Utils.isEmpty(useCQL3)) {
      m_useCQL3 = useCQL3.equalsIgnoreCase("Y"); // $NON-NLS-1$
    }
  }

  public void check(
      List<ICheckResult> remarks,
      PipelineMeta transMeta,
      TransformMeta stepMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info) {

    CheckResult cr;

    if ((prev == null) || (prev.size() == 0)) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_WARNING,
              "Not receiving any fields from previous steps!",
              stepMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              "Transform is connected to previous one, receiving " + prev.size() + " fields",
              stepMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this step!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              "Transform is receiving info from other steps.",
              stepMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR, "No input received from other steps!", stepMeta);
      remarks.add(cr);
    }
  }

  public ITransformData getStepData() {
    return new SSTableOutputData();
  }

  public void setDefault() {
    directory = System.getProperty("java.io.tmpdir");
    bufferSize = "16";
    table = "";
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.di.trans.step.BaseTransformMeta#getDialogClassName()
   */
  @Override
  public String getDialogClassName() {
    return "org.apache.hop.di.trans.steps.cassandrasstableoutput.SSTableOutputDialog";
  }

  @Override
  public ITransform createTransform(
      TransformMeta transMeta,
      SSTableOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    // TODO Auto-generated method stub
    return new SSTableOutput(transMeta, null, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public SSTableOutputData getTransformData() {
    return new SSTableOutputData();
  }
}
