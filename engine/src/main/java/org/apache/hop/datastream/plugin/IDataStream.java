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
 *
 */

package org.apache.hop.datastream.plugin;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.datastream.metadata.DataStreamMeta;
import org.apache.hop.metadata.api.HopMetadataObject;
import org.apache.hop.metadata.api.IHopMetadataObjectFactory;
import org.apache.hop.metadata.api.IHopMetadataProvider;

@HopMetadataObject(objectFactory = IDataStream.DataStreamObjectFactory.class)
public interface IDataStream extends Cloneable {

  /**
   * Call this when the data stream needs to be set up.
   *
   * @param variables The variables that this plugin can use
   * @param metadataProvider References to other metadata objects can be found here.
   * @param writing Set to true if you're writing and false if you're reading.
   * @param dataStreamMeta The data stream metadata to reference
   * @throws HopException In case there was an error setting up this data stream.
   */
  void initialize(
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      boolean writing,
      DataStreamMeta dataStreamMeta)
      throws HopException;

  /**
   * Gracefully finish working with this data stream.
   *
   * @throws HopException In case there was an error closing up shop.
   */
  void close() throws HopException;

  /**
   * Get the row metadata of the rows to be sent or to be received. When receiving data this needs
   * to be called first and will block until the data is ready to be read. If null is received that
   * means that the data stream is closed without any rows being received.
   *
   * @return The row metadata of the data stream or null if no data is to be expected.
   * @throws HopException In case something went wrong determining the row metadata.
   */
  IRowMeta getRowMeta() throws HopException;

  /**
   * Define the row metadata of the rows to be sent or received. When sending rows this needs to be
   * used first.
   *
   * @param rowMeta The row metadata to set
   * @throws HopException In case something went wrong defining the row metadata.
   */
  void setRowMeta(IRowMeta rowMeta) throws HopException;

  /**
   * Write a row to the data stream. This blocks until the row is written.
   *
   * @param rowData A row of data to be written
   * @throws HopException In case there was an error writing the row.
   */
  void writeRow(Object[] rowData) throws HopException;

  /**
   * Call this method when you're done writing
   *
   * @throws HopException In case something went wrong writing the last row(s).
   */
  void setOutputDone() throws HopException;

  /**
   * Read a row from the data stream. This operation will block until a row becomes available or
   * null is returned.
   *
   * @return A row from the data stream or null if no more rows are to be received.
   * @throws HopException In case there was an error reading a row from the data stream.
   */
  Object[] readRow() throws HopException;

  /**
   * @return the plugin id of this datastream plugin
   */
  String getPluginId();

  /**
   * @param pluginId set the plugin id of this datastream (after instantiation)
   */
  void setPluginId(String pluginId);

  /**
   * @return The name of the datastream plugin
   */
  String getPluginName();

  /**
   * @param name The name of the datastream plugin
   */
  void setPluginName(String name);

  /**
   * Allow this objects metadata to be copied.
   *
   * @return A copy of this object
   */
  IDataStream clone();

  /** This object factory is used to reference the correct class when serializing this metadata. */
  final class DataStreamObjectFactory implements IHopMetadataObjectFactory {
    @Override
    public Object createObject(String id, Object parentObject) throws HopException {
      PluginRegistry registry = PluginRegistry.getInstance();
      IPlugin plugin = registry.findPluginWithId(DataStreamPluginType.class, id);
      if (plugin == null) {
        throw new HopException(
            "Unable to find the plugin in the context of a datastream plugin for id: " + id);
      }
      IDataStream dataStream = registry.loadClass(plugin, IDataStream.class);

      // Set some plugin information automatically from the annotation
      //
      if (dataStream == null) {
        return null;
      }
      DataStreamPlugin annotation = dataStream.getClass().getAnnotation(DataStreamPlugin.class);
      dataStream.setPluginId(annotation.id());
      dataStream.setPluginName(annotation.name());
      return dataStream;
    }

    @Override
    public String getObjectId(Object object) throws HopException {
      if (!(object instanceof IDataStream dataStream)) {
        throw new HopException(
            "Object provided needs to be of class " + IDataStream.class.getName());
      }
      return dataStream.getPluginId();
    }
  }
}
