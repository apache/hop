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

package org.apache.hop.execution;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.row.RowMeta;

/**
 * This class contains execution data in the form of rows of data. These rows are collected for a
 * purpose.
 */
@Setter
@Getter
public class ExecutionData {

  /** The type of execution data captured: Transform or Action */
  private ExecutionType executionType;

  /** Metadata for the stored action */
  private ExecutionDataSetMeta dataSetMeta;

  /** Store the state of the individual executor (action): finished or not */
  private boolean finished;

  /** The time this data was collected */
  private Date collectionDate;

  /**
   * The ID of the pipeline owning the data. This is typically the log channel ID of the pipeline.
   */
  private String parentId;

  /**
   * The ID of the transform owning the data. This is typically the log channel ID of the transform
   * (component) copy.
   */
  private String ownerId;

  /**
   * This is a map with sets of rows ({@link RowBuffer}) per type of data that is collected from a
   * transform. The keys are described in the next map containing the description per key.
   */
  @JsonIgnore private Map<String, RowBuffer> dataSets;

  /** Each set key has a description which is contained in this map. */
  @JsonDeserialize(using = ExecutionDataSetMetaDeserializer.class)
  private Map<String, ExecutionDataSetMeta> setMetaData;

  public ExecutionData() {
    this.collectionDate = new Date();
    this.dataSets = Collections.synchronizedMap(new HashMap<>());
    this.setMetaData = Collections.synchronizedMap(new HashMap<>());
  }

  public ExecutionData(
      Date collectionDate,
      String parentId,
      String ownerId,
      Map<String, RowBuffer> dataSets,
      Map<String, ExecutionDataSetMeta> setDescriptions) {
    this.collectionDate = collectionDate;
    this.parentId = parentId;
    this.ownerId = ownerId;
    this.dataSets = dataSets;
    this.setMetaData = setDescriptions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ExecutionData that = (ExecutionData) o;
    if (!Objects.equals(collectionDate, that.collectionDate)
        || !Objects.equals(parentId, that.parentId)
        || !Objects.equals(ownerId, that.ownerId)) {
      return false;
    }

    if (this.dataSets.size() != that.dataSets.size()) {
      return false;
    }
    if (this.setMetaData.size() != that.setMetaData.size()) {
      return false;
    }

    for (String setKey : this.dataSets.keySet()) {
      RowBuffer thisBuffer = this.dataSets.get(setKey);
      RowBuffer thatBuffer = that.dataSets.get(setKey);
      if (thatBuffer == null) {
        return false;
      }
      // Compare metadata and every row in the buffer:
      //
      if (!thisBuffer.equals(thatBuffer)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public String toString() {
    return "ExecutionData{ "
        + Const.CR
        + "  executionType="
        + executionType
        + Const.CR
        + ", dataSetMeta="
        + dataSetMeta
        + Const.CR
        + ", finished="
        + finished
        + Const.CR
        + ", collectionDate="
        + collectionDate
        + Const.CR
        + ", parentId='"
        + parentId
        + '\''
        + Const.CR
        + ", ownerId='"
        + ownerId
        + '\''
        + Const.CR
        + ", dataSets="
        + dataSets
        + Const.CR
        + ", setMetaData="
        + setMetaData
        + Const.CR
        + '}';
  }

  @Override
  public int hashCode() {
    return Objects.hash(collectionDate, parentId, ownerId, dataSets, setMetaData);
  }

  /**
   * Encode the rows in binary compressed and encoded format fit for inclusion in JSON
   *
   * @return
   */
  @JsonInclude
  public String getRowsBinaryGzipBase64Encoded() throws IOException, HopFileException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    GZIPOutputStream gzos = new GZIPOutputStream(baos);
    DataOutputStream dataOutputStream = new DataOutputStream(gzos);

    synchronized (dataSets) {
      synchronized (setMetaData) {
        // Write the number of data sets...
        //
        dataOutputStream.writeInt(dataSets.keySet().size());
        for (String setKey : dataSets.keySet()) {
          RowBuffer buffer = dataSets.get(setKey);

          // Write the data set key
          //
          dataOutputStream.writeUTF(setKey);

          IRowMeta rowMeta = buffer.getRowMeta();

          if (rowMeta == null) {
            // no information received yet, an empty buffer
            rowMeta = new RowMeta();
          }

          // Write the metadata
          //
          rowMeta.writeMeta(dataOutputStream);

          synchronized (buffer.getBuffer()) {
            List<Object[]> rows = buffer.getBuffer();
            if (rows == null) {
              // Empty buffer
              rows = Collections.emptyList();
            }

            // The number of rows in the buffer
            //
            dataOutputStream.writeInt(rows.size());

            // Write the rows

            for (Object[] row : rows) {
              rowMeta.writeData(dataOutputStream, row);
            }
          }
        }
      }
    }
    dataOutputStream.close();
    gzos.close();

    // We now have a GZIP compressed set of bytes in the byte[]
    //
    byte[] compressedRowBytes = baos.toByteArray();

    // Encode this
    //
    return Base64.getEncoder().encodeToString(compressedRowBytes);
  }

  /**
   * Convert the encoded rows of data back to a list of rows
   *
   * @param encodedString
   * @throws IOException
   * @throws HopFileException
   */
  public void setRowsBinaryGzipBase64Encoded(String encodedString)
      throws IOException, HopFileException {
    dataSets = new HashMap<>();

    byte[] decodedCompressedBytes = Base64.getDecoder().decode(encodedString);
    ByteArrayInputStream bais = new ByteArrayInputStream(decodedCompressedBytes);
    try (GZIPInputStream gcis = new GZIPInputStream(bais)) {
      try (DataInputStream dis = new DataInputStream(gcis)) {

        // Get the number of sets
        int nrSets = dis.readInt();

        for (int i = 0; i < nrSets; i++) {
          // The set key & description
          //
          String setKey = dis.readUTF();

          // The row metadata...
          //
          IRowMeta rowMeta = new RowMeta(dis);

          // How many data rows does this buffer have?
          //
          List<Object[]> rows = new ArrayList<>();
          int nrRows = dis.readInt();
          for (int r = 0; r < nrRows; r++) {
            Object[] row = rowMeta.readData(dis);
            rows.add(row);
          }

          dataSets.put(setKey, new RowBuffer(rowMeta, rows));
        }
      }
    }
  }
}
