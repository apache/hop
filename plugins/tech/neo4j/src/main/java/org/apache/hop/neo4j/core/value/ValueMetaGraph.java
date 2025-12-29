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

package org.apache.hop.neo4j.core.value;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.ValueDataUtil;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaPlugin;
import org.apache.hop.neo4j.core.data.GraphData;
import org.w3c.dom.Node;

@ValueMetaPlugin(
    id = "303",
    name = "Graph",
    image = "graph.svg",
    description = "Graph data type containing nodes, relationships and their properties")
public class ValueMetaGraph extends ValueMetaBase {

  /**
   * 303 is the number of the room where the movie "The Matrix" starts and where Neo is short by
   * Agent Smith
   */
  public static final int TYPE_GRAPH = 303;

  public static final String CONST_UNKNOWN_STORAGE_TYPE = " : Unknown storage type ";
  public static final String CONST_SPECIFIED = " specified.";

  public ValueMetaGraph() {
    super(null, TYPE_GRAPH);
  }

  public ValueMetaGraph(String name) {
    super(name, TYPE_GRAPH);
  }

  /**
   * Create the value from an XML node
   *
   * @param node The DOM node to gencsv from
   * @throws HopException
   */
  public ValueMetaGraph(Node node) throws HopException {
    super(node);
  }

  @Override
  public Object getNativeDataType(Object object) throws HopValueException {
    return getGraphData(object);
  }

  public GraphData getGraphData(Object object) throws HopValueException {
    switch (type) {
      case TYPE_GRAPH:
        return switch (storageType) {
          case STORAGE_TYPE_NORMAL -> (GraphData) object;
          default ->
              throw new HopValueException(
                  "Only normal storage type is supported for Graph value : " + toString());
        };
      case TYPE_STRING:
        switch (storageType) {
          case STORAGE_TYPE_NORMAL:
            try {
              return new GraphData((String) object);
            } catch (Exception e) {
              throw new HopValueException(
                  "Error converting a JSON representation of Graph value data to a native representation",
                  e);
            }
          default:
            throw new HopValueException(
                "Only normal storage type is supported for Graph value : " + toString());
        }
      default:
        throw new HopValueException(
            "Unable to convert data type " + toString() + " to Graph value");
    }
  }

  /**
   * Convert Graph model to String...
   *
   * @param object The object to convert to String
   * @return The String representation
   * @throws HopValueException
   */
  @Override
  public String getString(Object object) throws HopValueException {
    try {
      String string;

      switch (type) {
        case TYPE_STRING:
          string =
              switch (storageType) {
                case STORAGE_TYPE_NORMAL -> object == null ? null : object.toString();
                case STORAGE_TYPE_BINARY_STRING ->
                    (String) convertBinaryStringToNativeType((byte[]) object);
                case STORAGE_TYPE_INDEXED ->
                    object == null ? null : (String) index[(Integer) object];
                default ->
                    throw new HopValueException(
                        toString() + CONST_UNKNOWN_STORAGE_TYPE + storageType + CONST_SPECIFIED);
              };
          if (string != null) {
            string = trim(string);
          }
          break;

        case TYPE_DATE:
          throw new HopValueException(
              "You can't convert a Date to a Graph data type for : " + toString());

        case TYPE_NUMBER:
          throw new HopValueException(
              "You can't convert a Number to a Graph data type for : " + toString());

        case TYPE_INTEGER:
          throw new HopValueException(
              "You can't convert an Integer to a Graph data type for : " + toString());

        case TYPE_BIGNUMBER:
          throw new HopValueException(
              "You can't convert a BigNumber to a Graph data type for : " + toString());

        case TYPE_BOOLEAN:
          throw new HopValueException(
              "You can't convert a Boolean to a Graph data type for : " + toString());

        case TYPE_BINARY:
          string =
              switch (storageType) {
                case STORAGE_TYPE_NORMAL -> convertBinaryStringToString((byte[]) object);
                case STORAGE_TYPE_BINARY_STRING -> convertBinaryStringToString((byte[]) object);
                case STORAGE_TYPE_INDEXED ->
                    object == null
                        ? null
                        : convertBinaryStringToString((byte[]) index[(Integer) object]);
                default ->
                    throw new HopValueException(
                        toString() + CONST_UNKNOWN_STORAGE_TYPE + storageType + CONST_SPECIFIED);
              };
          break;

        case TYPE_SERIALIZABLE:
          string =
              switch (storageType) {
                case STORAGE_TYPE_NORMAL ->
                    object == null ? null : object.toString(); // just go for the default toString()
                case STORAGE_TYPE_BINARY_STRING -> convertBinaryStringToString((byte[]) object);
                case STORAGE_TYPE_INDEXED ->
                    object == null
                        ? null
                        : index[(Integer) object].toString(); // just go for the default toString()
                default ->
                    throw new HopValueException(
                        toString() + CONST_UNKNOWN_STORAGE_TYPE + storageType + CONST_SPECIFIED);
              };
          break;

        case TYPE_GRAPH:
          string =
              switch (storageType) {
                case STORAGE_TYPE_NORMAL ->
                    object == null ? null : ((GraphData) object).toJsonString();
                default ->
                    throw new HopValueException(
                        toString()
                            + " : Unsupported storage type "
                            + getStorageTypeDesc()
                            + " for "
                            + toString());
              };
          break;

        default:
          throw new HopValueException(toString() + " : Unknown type " + type + CONST_SPECIFIED);
      }

      if (isOutputPaddingEnabled() && getLength() > 0) {
        string = ValueDataUtil.rightPad(string, getLength());
      }

      return string;
    } catch (ClassCastException e) {
      throw new HopValueException(
          toString()
              + " : There was a data type error: the data type of "
              + object.getClass().getName()
              + " object ["
              + object
              + "] does not correspond to value meta ["
              + toStringMeta()
              + "]");
    }
  }

  @Override
  public Object cloneValueData(Object object) throws HopValueException {
    if (object == null) {
      return null;
    }

    GraphData graphData = getGraphData(object);
    return new GraphData(graphData);
  }

  @Override
  public Class<?> getNativeDataTypeClass() throws HopValueException {
    return GraphData.class;
  }
}
