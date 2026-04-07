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

package org.apache.hop.arrow.datastream.flight;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.arrow.flight.AsyncPutListener;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightClient.ClientStreamListener;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hop.arrow.datastream.shared.ArrowBaseDataStream;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.datastream.metadata.DataStreamMeta;
import org.apache.hop.datastream.plugin.DataStreamPlugin;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.staticschema.metadata.SchemaDefinition;

@DataStreamPlugin(
    id = "ArrowFlightStream",
    name = "Apache Arrow Flight Stream",
    description =
        "Reference this stream name when sending to, or reading from, the Hop Arrow Flight server")
@Getter
@Setter
@GuiPlugin
public class ArrowFlightDataStream extends ArrowBaseDataStream {
  public static final int DEFAULT_MAX_BUFFER_SIZE = 10000000;

  @GuiWidgetElement(
      order = "20000-arrow-flight-data-stream-buffer-size",
      parentId = DataStreamMeta.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.FILENAME,
      label = "i18n::ArrowFlightDataStream.BufferSize.Label",
      toolTip = "i18n::ArrowFlightDataStream.BufferSize.Tooltip")
  @HopMetadataProperty
  protected String bufferSize;

  @GuiWidgetElement(
      order = "20100-arrow-flight-data-stream-batch-size",
      parentId = DataStreamMeta.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::ArrowFlightDataStream.BatchSize.Label",
      toolTip = "i18n::ArrowFlightDataStream.BatchSize.Tooltip")
  @HopMetadataProperty
  protected String batchSize;

  @GuiWidgetElement(
      order = "20200-arrow-flight-data-stream-schema-definition",
      parentId = DataStreamMeta.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.METADATA,
      metadata = SchemaDefinition.class,
      label = "i18n::ArrowFlightDataStream.SchemaDefinition.Label",
      toolTip = "i18n::ArrowFlightDataStream.SchemaDefinition.Tooltip")
  @HopMetadataProperty(key = "schemaDefinition")
  protected String schemaDefinitionName;

  @GuiWidgetElement(
      order = "20300-arrow-flight-data-stream-hostname",
      parentId = DataStreamMeta.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      metadata = SchemaDefinition.class,
      label = "i18n::ArrowFlightDataStream.Hostname.Label",
      toolTip = "i18n::ArrowFlightDataStream.Hostname.Tooltip")
  @HopMetadataProperty(key = "hostname")
  protected String hostname;

  @GuiWidgetElement(
      order = "20400-arrow-flight-data-stream-port",
      parentId = DataStreamMeta.GUI_WIDGETS_PARENT_ID,
      type = GuiElementType.TEXT,
      metadata = SchemaDefinition.class,
      label = "i18n::ArrowFlightDataStream.Port.Label",
      toolTip = "i18n::ArrowFlightDataStream.Port.Tooltip")
  @HopMetadataProperty(key = "port")
  protected String port;

  private int realBufferSize;
  private int realBatchSize;
  private String realHostname;
  private int realPort;
  private SchemaDefinition schemaDefinition;
  private FlightClient flightClient;
  private ClientStreamListener clientStreamListener;
  private FlightInfo readFlightInfo;
  private FlightStream readFlightStream;
  private VectorSchemaRoot readVectorSchemaRoot;
  private Schema readSchema;
  private int readRowIndex;
  private List<FieldVector> readFieldVectors;
  private boolean firstRead;

  public ArrowFlightDataStream() {
    DataStreamPlugin annotation = getClass().getAnnotation(DataStreamPlugin.class);
    this.pluginId = annotation.id();
    this.pluginName = annotation.name();
    rowBuffer = new ArrayList<>();
    bufferSize = Integer.toString(DEFAULT_MAX_BUFFER_SIZE);
    batchSize = "10000";
    hostname = "localhost";
    port = "33333";
  }

  @SuppressWarnings("CopyConstructorMissesField")
  public ArrowFlightDataStream(ArrowFlightDataStream s) {
    this();
    this.bufferSize = s.bufferSize;
    this.batchSize = s.batchSize;
    this.schemaDefinitionName = s.schemaDefinitionName;
  }

  @Override
  public ArrowFlightDataStream clone() {
    return new ArrowFlightDataStream(this);
  }

  @Override
  public void initialize(
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      boolean writing,
      DataStreamMeta dataStreamMeta)
      throws HopException {
    super.initialize(variables, metadataProvider, writing, dataStreamMeta);
    realBufferSize = Const.toInt(variables.resolve(bufferSize), DEFAULT_MAX_BUFFER_SIZE);
    realBatchSize = Const.toInt(variables.resolve(batchSize), 500);
    String realSchemaDefinition = variables.resolve(schemaDefinitionName);
    schemaDefinition =
        metadataProvider.getSerializer(SchemaDefinition.class).load(realSchemaDefinition);
    if (schemaDefinition == null) {
      throw new HopException(
          "The specified schema definition '"
              + realSchemaDefinition
              + "' could not be found in the Hop Flight server metadata.");
    }
    realHostname = variables.resolve(hostname);
    realPort = Const.toInt(variables.resolve(port), 33333);
    firstRead = true;
  }

  @Override
  public void setRowMeta(IRowMeta rowMeta) throws HopException {
    if (!writing) {
      return;
    }
    this.rowMeta = rowMeta;

    initializeStreamWriting();
  }

  private void initializeStreamWriting() throws HopException {
    Schema writeSchema = buildSchema(rowMeta);
    vectorSchemaRoot = VectorSchemaRoot.create(writeSchema, rootAllocator);

    // Allocate room in the field vectors
    //
    allocateFieldVectorsSpace(vectorSchemaRoot, rowMeta, realBatchSize);

    buildFlightClient();

    clientStreamListener =
        flightClient.startPut(
            FlightDescriptor.path(dataStreamMeta.getName()),
            vectorSchemaRoot,
            new AsyncPutListener());
  }

  private void buildFlightClient() throws HopException {
    try {
      // Get a flight client going.
      //
      Location location = Location.forGrpcInsecure(realHostname, realPort);
      flightClient = FlightClient.builder(rootAllocator, location).build();
    } catch (Exception e) {
      throw new HopException(
          "Error connecting to Flight server " + realHostname + ":" + realPort, e);
    }
  }

  @Override
  public void close() {
    if (readVectorSchemaRoot != null) {
      readVectorSchemaRoot.close();
    }
    if (vectorSchemaRoot != null) {
      vectorSchemaRoot.close();
    }
    if (rootAllocator != null) {
      rootAllocator.close();
    }
    if (readFlightStream != null) {
      try {
        readFlightStream.close();
      } catch (Exception e) {
        // Ignore
      }
    }

    if (flightClient != null) {
      try {
        flightClient.close();
      } catch (Exception e) {
        // Ignore
      }
    }
  }

  @Override
  public void writeRow(Object[] rowData) throws HopException {
    rowBuffer.add(rowData);
    if (rowBuffer.size() >= realBatchSize) {
      emptyBuffer();
    }
  }

  private void emptyBuffer() throws HopException {
    try {
      vectorSchemaRoot.setRowCount(rowBuffer.size());

      // Set the data in the field vectors for the rows in the buffer
      //
      for (int rowIndex = 0; rowIndex < rowBuffer.size(); rowIndex++) {
        convertHopRowToFieldVectorIndex(
            vectorSchemaRoot, rowMeta, rowIndex, rowBuffer.get(rowIndex));
      }
      // With values set on all field vectors, we can now write the batch.
      //
      clientStreamListener.putNext();
    } catch (Exception e) {
      throw new HopException("Error writing row to Apache Arrow Flight server", e);
    } finally {
      // We're done. Fill the buffer up again.
      rowBuffer.clear();
    }
  }

  @Override
  public void setOutputDone() throws HopException {
    if (!rowBuffer.isEmpty()) {
      emptyBuffer();
    }
    try {
      clientStreamListener.completed();
    } catch (Exception e) {
      throw new HopException("Error ending arrow file stream", e);
    }
  }

  /**
   * Gets rowMeta
   *
   * @return value of rowMeta
   */
  @Override
  public IRowMeta getRowMeta() throws HopException {
    if (writing) {
      return rowMeta;
    }
    try {
      initializeStreamReading();
    } catch (Exception e) {
      throw new HopException("Error reading row metadata from Apache Arrow Flight server", e);
    }
    return this.rowMeta;
  }

  private void initializeStreamReading() throws HopException {
    buildFlightClient();
    readFlightInfo = flightClient.getInfo(FlightDescriptor.path(dataStreamMeta.getName()));
    if (readFlightInfo.getEndpoints().isEmpty()) {
      throw new HopException(
          "No endpoint tickets found in flight server matching " + dataStreamMeta.getName());
    }
    readFlightStream = flightClient.getStream(readFlightInfo.getEndpoints().get(0).getTicket());
    readVectorSchemaRoot = readFlightStream.getRoot();
    readSchema = readVectorSchemaRoot.getSchema();
    this.rowMeta = buildRowMeta(readSchema);
  }

  @Override
  public Object[] readRow() throws HopException {
    if (writing) {
      throw new HopException("When writing data you can't read rows from the same data stream.");
    }
    try {
      if (firstRead) {
        firstRead = false;
        // Read a first batch
        if (!readNextBatch()) {
          return null;
        }
      }

      // See if there are more rows to populate.
      //
      int schemaBatchSize = readVectorSchemaRoot.getRowCount();

      // If needed, read another batch
      //
      if (readRowIndex < schemaBatchSize || readNextBatch()) {
        return convertFieldVectorsToHopRow(readFieldVectors, rowMeta, readRowIndex++);
      } else {
        // No more data to be expected
        return null;
      }
    } catch (Exception e) {
      throw new HopException(
          "Error while reading a batch of rows from an Apache Arrow Flight stream", e);
    }
  }

  protected boolean readNextBatch() {
    boolean readNext = readFlightStream.next();
    readVectorSchemaRoot = readFlightStream.getRoot();

    // This loop is for the rare cases where the batches contain 0 rows each.
    //
    while (readVectorSchemaRoot.getRowCount() == 0 && readNext) {
      readNext = readFlightStream.next();
      readVectorSchemaRoot = readFlightStream.getRoot();
    }
    readFieldVectors = readVectorSchemaRoot.getFieldVectors();
    readRowIndex = 0;

    return readNext;
  }

  public IRowMeta buildExpectedRowMeta() throws HopException {
    return schemaDefinition.getRowMeta();
  }

  public Schema buildExpectedSchema() throws HopException {
    return ArrowBaseDataStream.buildSchema(schemaDefinition.getRowMeta());
  }
}
