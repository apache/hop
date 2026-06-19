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

package org.apache.hop.arrow.flight;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hop.arrow.datastream.flight.ArrowFlightDataStream;
import org.apache.hop.arrow.datastream.shared.ArrowBaseDataStream;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.Const;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.datastream.metadata.DataStreamMeta;
import org.apache.hop.datastream.plugin.IDataStream;
import org.apache.hop.metadata.api.IHopMetadataProvider;

public class HopFlightProducer extends NoOpFlightProducer {
  public static final int BUFFER_SIZE_OVERSHOOT = 5000;
  public static final int BUFFER_SIZE_ERROR_LEVEL = 4000;
  public static final int MAX_READ_BLOCK_TIME_MS = 60000;

  private final IVariables variables;
  private final IHopMetadataProvider metadataProvider;
  private final RootAllocator rootAllocator;
  private final Map<String, FlightStreamBuffer> streamMap;

  public HopFlightProducer(
      IVariables variables, IHopMetadataProvider metadataProvider, RootAllocator rootAllocator) {
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.rootAllocator = rootAllocator;
    this.streamMap = new HashMap<>();
  }

  @Override
  public Runnable acceptPut(
      CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {

    // 1. Get the descriptor sent by the client
    FlightDescriptor descriptor = flightStream.getDescriptor();

    // 2. Extract the stream name from it
    String streamName = extractStreamName(descriptor);

    try {
      // 3. Do we know about this stream already?
      //
      FlightStreamBuffer streamBuffer = lookupArrowStreamBuffer(streamName);

      // 4. validate the schema
      //
      Schema incomingSchema = flightStream.getSchema();
      if (!incomingSchema.equals(streamBuffer.schema())) {
        ackStream.onError(
            CallStatus.INVALID_ARGUMENT.withDescription("Schema mismatch").toRuntimeException());
        return () -> {};
      }

      // 5. Keep reading data into the row set
      //
      IRowMeta rowMeta = streamBuffer.rowMeta();
      IRowSet rowSet = streamBuffer.rowSet();
      int bufferSize = streamBuffer.bufferSize();
      while (flightStream.next()) {
        VectorSchemaRoot vectorSchemaRoot = flightStream.getRoot();
        List<FieldVector> fieldVectors = vectorSchemaRoot.getFieldVectors();

        int batchRowCount = vectorSchemaRoot.getRowCount();

        if (batchRowCount == 0) continue;

        // Convert Arrow vectorSchemaRoot → Hop rows and push into RowSet
        //
        for (int rowIndex = 0; rowIndex < batchRowCount; rowIndex++) {
          Object[] rowData =
              ArrowBaseDataStream.convertFieldVectorsToHopRow(fieldVectors, rowMeta, rowIndex);
          rowSet.putRow(rowMeta, rowData);

          // If too many rows are kept in memory, throw an error!
          // The client needs to read faster.  This is IPC, not queueing!
          //
          if (rowSet.size() > 0 && rowSet.size() > bufferSize + BUFFER_SIZE_ERROR_LEVEL) {
            throw new HopException(
                "The maximum amount of rows kept in memory is exceeded by reaching "
                    + rowSet.size()
                    + " rows.");
          }
        }
      }

      // Acknowledge successful receipt
      ackStream.onCompleted();

      // Signal the end of affairs to the row set
      //
      rowSet.setDone();
    } catch (Exception e) {
      ackStream.onError(CallStatus.INTERNAL.withCause(e).toRuntimeException());
    }

    return () -> {}; // cleanup if needed
  }

  private FlightStreamBuffer lookupArrowStreamBuffer(String streamName) throws HopException {
    FlightStreamBuffer buffer = streamMap.get(streamName);
    if (buffer == null) {
      DataStreamMeta dataStreamMeta =
          metadataProvider.getSerializer(DataStreamMeta.class).load(streamName);
      if (dataStreamMeta == null) {
        throw new HopException(
            "Stream name '" + streamName + "' could not be found in the metadata as a data stream");
      }
      IDataStream dataStream = dataStreamMeta.getDataStream();
      if (!(dataStream instanceof ArrowFlightDataStream flightDataStream)) {
        throw new HopException(
            "Make sure to reference an Arrow Flight data stream in data stream element '"
                + streamName
                + "'.");
      }
      flightDataStream.initialize(variables, metadataProvider, true, dataStreamMeta);
      IRowMeta rowMeta = flightDataStream.buildExpectedRowMeta();
      Schema expectedSchema = flightDataStream.buildExpectedSchema();

      int bufferSize =
          Const.toInt(
              variables.resolve(flightDataStream.getBufferSize()),
              ArrowFlightDataStream.DEFAULT_MAX_BUFFER_SIZE);
      int batchSize = Const.toInt(variables.resolve(flightDataStream.getBatchSize()), 500);

      // We use a very large queue because we don't ever want to block while writing.
      // We over-size it by 5000 rows and then throw an error if we reach that.
      //
      IRowSet rowSet = new BlockingRowSet(bufferSize + BUFFER_SIZE_OVERSHOOT);

      String hostname = Const.NVL(variables.resolve(flightDataStream.getHostname()), "0.0.0.0");
      int port = Const.toInt(variables.resolve(flightDataStream.getPort()), 33333);
      Location location = Location.forGrpcInsecure(hostname, port);
      buffer =
          new FlightStreamBuffer(expectedSchema, rowMeta, rowSet, bufferSize, batchSize, location);
      streamMap.put(streamName, buffer);
    }
    return buffer;
  }

  @Override
  public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
    try {
      // What's the stream name to read from?
      String streamName = new String(ticket.getBytes(), StandardCharsets.UTF_8);

      // Look it up in the stream map:
      //
      FlightStreamBuffer streamBuffer = lookupArrowStreamBuffer(streamName);

      IRowSet rowSet = streamBuffer.rowSet();
      IRowMeta rowMeta = streamBuffer.rowMeta();
      int batchSize = streamBuffer.batchSize();
      Schema schema = streamBuffer.schema();
      try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator)) {
        ArrowBaseDataStream.allocateFieldVectorsSpace(vectorSchemaRoot, rowMeta, batchSize);
        List<Object[]> rowBuffer = new ArrayList<>();

        // Send the schema once.
        //
        listener.start(vectorSchemaRoot);

        // Example: Pull rows from a readable RowSet and send them back as Arrow batches
        Object[] hopRow = waitForRow(rowSet);
        while (hopRow != null) {

          // Add the row to a batch row buffer:
          //
          rowBuffer.add(hopRow);
          if (rowBuffer.size() >= batchSize) {
            // Fill the batch
            fillBatch(rowBuffer, rowMeta, vectorSchemaRoot);

            // Send the batch
            listener.putNext();
          }
          // Get another row
          hopRow = waitForRow(rowSet);
        }
        // Do we have any rows in the buffer left?
        //
        if (!rowBuffer.isEmpty()) {
          fillBatch(rowBuffer, rowMeta, vectorSchemaRoot);
          listener.putNext();
        }

        listener.completed();

        // Clean up the stream map as well to avoid leaking memory
        //
        streamMap.remove(streamName);
      }
    } catch (Exception e) {
      listener.error(CallStatus.INTERNAL.withCause(e).toRuntimeException());
    }
  }

  private Object[] waitForRow(IRowSet rowSet) throws HopException {
    Object[] row = rowSet.getRowWait(20, TimeUnit.MILLISECONDS);
    long startTime = System.currentTimeMillis();
    while (row == null
        && !rowSet.isDone()
        && System.currentTimeMillis() - startTime < MAX_READ_BLOCK_TIME_MS) {
      row = rowSet.getRowWait(20, TimeUnit.MILLISECONDS);
    }
    return row;
  }

  private void fillBatch(
      List<Object[]> rowBuffer, IRowMeta rowMeta, VectorSchemaRoot vectorSchemaRoot)
      throws HopException {
    vectorSchemaRoot.setRowCount(rowBuffer.size());
    for (int rowIndex = 0; rowIndex < rowBuffer.size(); rowIndex++) {
      Object[] rowData = rowBuffer.get(rowIndex);
      ArrowBaseDataStream.convertHopRowToFieldVectorIndex(
          vectorSchemaRoot, rowMeta, rowIndex, rowData);
    }
    // The data is transferred, we can clear the buffer.
    rowBuffer.clear();
  }

  @Override
  public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
    try {
      // 2. Extract the stream name from it
      String streamName = extractStreamName(descriptor);

      // 3. Do we know about this stream already?
      //
      FlightStreamBuffer streamBuffer = lookupArrowStreamBuffer(streamName);

      FlightEndpoint flightEndpoint =
          new FlightEndpoint(
              new Ticket(descriptor.getPath().get(0).getBytes(StandardCharsets.UTF_8)),
              streamBuffer.location());
      return new FlightInfo(
          streamBuffer.schema(), descriptor, Collections.singletonList(flightEndpoint), -1, -1);
    } catch (Exception e) {
      return new FlightInfo(null, descriptor, java.util.Collections.emptyList(), -1, -1);
    }
  }

  private String extractStreamName(FlightDescriptor descriptor) {
    if (descriptor == null) {
      throw CallStatus.INVALID_ARGUMENT
          .withDescription("No FlightDescriptor provided")
          .toRuntimeException();
    }

    // Option A: Path-based (recommended - clean and widely used)
    if (!descriptor.isCommand() && !descriptor.getPath().isEmpty()) {
      return descriptor.getPath().get(0); // e.g. "python_sales_input"
    }

    throw CallStatus.INVALID_ARGUMENT
        .withDescription("Unsupported FlightDescriptor type: " + descriptor)
        .toRuntimeException();
  }
}
