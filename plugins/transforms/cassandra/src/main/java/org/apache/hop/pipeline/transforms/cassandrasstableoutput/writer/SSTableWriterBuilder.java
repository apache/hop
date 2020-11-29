/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.cassandrasstableoutput.writer;

import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.hop.core.row.IRowMeta;

/**
 * Builder is used to create specific SSTableWriter depending mostly on CQL version
 *
 * @author Pavel Sakun
 */
public class SSTableWriterBuilder {
  /**
   * Path to cassandra YAML config
   */
  private String configFilePath;

  /**
   * CQL Version
   */
  private int cqlVersion;

  /**
   * The directory to output to
   */
  private String directory;

  /**
   * The keyspace to use
   */
  private String keyspace;

  /**
   * The name of the table to write to
   */
  private String table;

  /**
   * The primary key used to determine unique keys (IDs) for rows
   */
  private String primaryKey;

  /**
   * Size (MB) of write buffer
   */
  private int bufferSize;

  /**
   * Input row meta
   */
  private IRowMeta rowMeta;

  public SSTableWriterBuilder withConfig( String configFilePath ) {
    if ( !configFilePath.startsWith( "file:" ) ) {
      this.configFilePath = "file:" + configFilePath;
    } else {
      this.configFilePath = configFilePath;
    }
    return this;
  }

  public SSTableWriterBuilder withDirectory( String outputDirectoryPath ) {
    this.directory = outputDirectoryPath;
    return this;
  }

  public SSTableWriterBuilder withKeyspace( String keyspaceName ) {
    this.keyspace = keyspaceName;
    return this;
  }

  public SSTableWriterBuilder withTable( String tableName ) {
    this.table = tableName;
    return this;
  }

  public SSTableWriterBuilder withPrimaryKey( String primaryKey ) {
    this.primaryKey = primaryKey;
    return this;
  }

  public SSTableWriterBuilder withBufferSize( int bufferSize ) {
    this.bufferSize = bufferSize;
    return this;
  }

  public SSTableWriterBuilder withRowMeta( IRowMeta rowMeta ) {
    this.rowMeta = rowMeta;
    return this;
  }

  public SSTableWriterBuilder withCqlVersion( int cqlVersion ) {
    this.cqlVersion = cqlVersion;
    return this;
  }

  public AbstractSSTableWriter build() throws Exception {
    System.setProperty( "cassandra.config", configFilePath );
    AbstractSSTableWriter result;

    CQL3SSTableWriter writer = getCql3SSTableWriter();
    writer.setRowMeta( rowMeta );
    result = writer;
    result.setDirectory( directory );
    result.setKeyspace( keyspace );
    result.setTable( table );
    result.setPrimaryKey( primaryKey );
    result.setBufferSize( bufferSize );
    result.setPartitionerClass( getPartitionerClass() );

    return result;
  }

  String getPartitionerClass() throws ConfigurationException {
    return new YamlConfigurationLoader().loadConfig().partitioner;
  }

  CQL3SSTableWriter getCql3SSTableWriter() {
    return new CQL3SSTableWriter();
  }
}
