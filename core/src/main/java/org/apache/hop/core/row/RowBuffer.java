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

package org.apache.hop.core.row;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Node;

/** This class contains a list of data rows as well as the IRowMeta to describe it. */
public class RowBuffer {

  @JsonIgnore public static final String XML_TAG = "row-buffer";

  private IRowMeta rowMeta;
  private List<Object[]> buffer;

  public RowBuffer() {
    rowMeta = new RowMeta();
    buffer = Collections.synchronizedList(new ArrayList<>());
  }

  /**
   * @param rowMeta
   * @param buffer
   */
  public RowBuffer(IRowMeta rowMeta, List<Object[]> buffer) {
    this.rowMeta = rowMeta;
    this.buffer = buffer;
  }

  /**
   * @param rowMeta
   */
  public RowBuffer(IRowMeta rowMeta) {
    this(rowMeta, Collections.synchronizedList(new ArrayList<>()));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RowBuffer that = (RowBuffer) o;
    if (!this.rowMeta.equals(that.rowMeta)) {
      return false;
    }
    if (this.buffer.size() != that.buffer.size()) {
      return false;
    }
    for (int i = 0; i < this.buffer.size(); i++) {
      Object[] thisRow = this.buffer.get(i);
      Object[] thatRow = that.buffer.get(i);

      for (int v = 0; v < rowMeta.size(); v++) {
        IValueMeta valueMeta = rowMeta.getValueMeta(v);
        try {
          if (valueMeta.compare(thisRow[v], thatRow[v]) != 0) {
            return false;
          }
        } catch (HopValueException e) {
          throw new RuntimeException(
              "Error comparing 2 values in a row buffer row: " + valueMeta.getName(), e);
        }
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash(rowMeta, buffer);
  }

  @JsonIgnore
  public String getXml() throws IOException {
    StringBuilder xml = new StringBuilder();

    xml.append(XmlHandler.openTag(XML_TAG));
    xml.append(rowMeta.getMetaXml());
    for (Object[] row : buffer) {
      xml.append(rowMeta.getDataXml(row));
    }
    xml.append(XmlHandler.closeTag(XML_TAG));

    return xml.toString();
  }

  public RowBuffer(Node node) throws HopException {
    this();
    Node rowMetaNode = XmlHandler.getSubNode(node, RowMeta.XML_META_TAG);
    rowMeta = new RowMeta(rowMetaNode);
    List<Node> dataNodes = XmlHandler.getNodes(node, RowMeta.XML_DATA_TAG);
    for (Node dataNode : dataNodes) {
      buffer.add(rowMeta.getRow(dataNode));
    }
  }

  public int size() {
    return buffer.size();
  }

  public boolean isEmpty() {
    return buffer.isEmpty();
  }

  public void addRow(Object... row) {
    buffer.add(row);
  }

  public void addRow(int index, Object[] row) {
    buffer.add(index, row);
  }

  public Object[] removeRow(int index) {
    return buffer.remove(index);
  }

  public void setRow(int index, Object[] row) {
    buffer.set(index, row);
  }

  /**
   * @return the rowMeta
   */
  public IRowMeta getRowMeta() {
    return rowMeta;
  }

  /**
   * @param rowMeta the rowMeta to set
   */
  public void setRowMeta(IRowMeta rowMeta) {
    this.rowMeta = rowMeta;
  }

  /**
   * @return the buffer
   */
  public List<Object[]> getBuffer() {
    return buffer;
  }

  /**
   * @param buffer the buffer to set
   */
  public void setBuffer(List<Object[]> buffer) {
    this.buffer = buffer;
  }
}
