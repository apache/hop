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

package org.apache.hop.pipeline.transforms.mongodbinput;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.mongo.wrapper.MongoClientWrapper;
import org.apache.hop.mongo.wrapper.collection.MongoCollectionWrapper;
import org.apache.hop.mongo.wrapper.cursor.MongoCursorWrapper;
import org.apache.hop.mongo.wrapper.field.MongoArrayExpansion;
import org.apache.hop.mongo.wrapper.field.MongoField;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** @since 24-jan-2005 */
public class MongoDbInputData extends BaseTransformData implements ITransformData {

  public static final int MONGO_DEFAULT_PORT = 27017;

  public IRowMeta outputRowMeta;

  public MongoClientWrapper clientWrapper;
  // public DB db;
  public MongoCollectionWrapper collection;

  /** cursor for a standard query */
  public MongoCursorWrapper cursor;

  /** results of an aggregation pipeline */
  Iterator<DBObject> m_pipelineResult;

  private List<MongoField> m_userFields;
  private MongoArrayExpansion m_expansionHandler;
  private static MongoDbInputDiscoverFieldsHolder mongoDbInputDiscoverFieldsHolder =
      MongoDbInputDiscoverFieldsHolder.getInstance();

  public static MongoDbInputDiscoverFieldsHolder getMongoDbInputDiscoverFieldsHolder() {
    return mongoDbInputDiscoverFieldsHolder;
  }

  protected void setMongoDbInputDiscoverFieldsHolder(MongoDbInputDiscoverFieldsHolder holder) {
    mongoDbInputDiscoverFieldsHolder = holder;
  }

  protected static MongoArrayExpansion checkFieldPaths(
      List<MongoField> normalFields, IRowMeta outputRowMeta) throws HopException {

    // here we check whether there are any full array expansions
    // specified in the paths (via [*]). If so, we want to make sure
    // that only one is present across all paths. E.g. we can handle
    // multiple fields like $.person[*].first, $.person[*].last etc.
    // but not $.person[*].first, $.person[*].address[*].street.

    String expansion = null;
    List<MongoField> normalList = new ArrayList<>();
    List<MongoField> expansionList = new ArrayList<>();

    for (MongoField f : normalFields) {
      String path = f.fieldPath;

      if (path != null && path.lastIndexOf("[*]") >= 0) {

        if (path.indexOf("[*]") != path.lastIndexOf("[*]")) {
          throw new HopException(
              BaseMessages.getString(
                  MongoDbInputMeta.PKG,
                  "MongoInput.ErrorMessage.PathContainsMultipleExpansions",
                  path));
        }

        String pathPart = path.substring(0, path.lastIndexOf("[*]") + 3);

        if (expansion == null) {
          expansion = pathPart;
        } else {
          if (!expansion.equals(pathPart)) {
            throw new HopException(
                BaseMessages.getString(
                    MongoDbInputMeta.PKG, "MongoDbInput.ErrorMessage.MutipleDifferentExpansions"));
          }
        }

        expansionList.add(f);
      } else {
        normalList.add(f);
      }
    }

    normalFields.clear();
    for (MongoField f : normalList) {
      normalFields.add(f);
    }

    if (expansionList.size() > 0) {

      List<MongoField> subFields = new ArrayList<>();

      for (MongoField ef : expansionList) {
        MongoField subField = new MongoField();
        subField.fieldName = ef.fieldName;
        String path = ef.fieldPath;
        if (path.charAt(path.length() - 2) == '*') {
          path = "dummy"; // pulling a primitive out of the array (path
          // doesn't matter)
        } else {
          path = path.substring(path.lastIndexOf("[*]") + 3, path.length());
          path = "$" + path;
        }

        subField.fieldPath = path;
        subField.indexedValues = ef.indexedValues;
        subField.hopType = ef.hopType;

        subFields.add(subField);
      }

      MongoArrayExpansion exp = new MongoArrayExpansion(subFields);
      exp.expansionPath = expansion;
      exp.outputRowMeta = outputRowMeta;

      return exp;
    }

    return null;
  }

  public MongoDbInputData() {
    super();
  }

  /**
   * Initialize all the paths by locating the index for their field name in the outgoing row
   * structure.
   *
   * @throws HopException
   */
  public void init() throws HopException {
    if (m_userFields != null) {

      // set up array expansion/unwinding (if necessary)
      m_expansionHandler = checkFieldPaths(m_userFields, outputRowMeta);

      for (MongoField f : m_userFields) {
        int outputIndex = outputRowMeta.indexOfValue(f.fieldName);
        f.init(outputIndex);
      }

      if (m_expansionHandler != null) {
        m_expansionHandler.init();
      }
    }
  }

  /**
   * Convert a mongo document to outgoing row field values with respect to the user-specified paths.
   * May return more than one Hop row if an array is being expanded/unwound
   *
   * @param mongoObj the mongo document
   * @param variables variables to use
   * @return populated Hop row(s)
   * @throws HopException if a problem occurs
   */
  public Object[][] mongoDocumentToHop(DBObject mongoObj, IVariables variables) throws HopException {

    Object[][] result = null;

    if (m_expansionHandler != null) {
      m_expansionHandler.reset(variables);

      if (mongoObj instanceof BasicDBObject) {
        result = m_expansionHandler.convertToHopValue((BasicDBObject) mongoObj, variables);
      } else {
        result = m_expansionHandler.convertToHopValue((BasicDBList) mongoObj, variables);
      }
    } else {
      result = new Object[1][];
    }

    // get the normal (non expansion-related fields)
    Object[] normalData = RowDataUtil.allocateRowData(outputRowMeta.size());
    Object value;
    for (MongoField f : m_userFields) {
      value = null;
      f.reset(variables);

      if (mongoObj instanceof BasicDBObject) {
        value = f.convertToHopValue((BasicDBObject) mongoObj);
      } else if (mongoObj instanceof BasicDBList) {
        value = f.convertToHopValue((BasicDBList) mongoObj);
      }

      normalData[f.outputIndex] = value;
    }

    // copy normal fields over to each expansion row (if necessary)
    if (m_expansionHandler == null) {
      result[0] = normalData;
    } else {
      for (int i = 0; i < result.length; i++) {
        Object[] row = result[i];
        for (MongoField f : m_userFields) {
          row[f.outputIndex] = normalData[f.outputIndex];
        }
      }
    }

    return result;
  }

  /**
   * Cleanses a string path by ensuring that any variables names present in the path do not contain
   * "."s (replaces any dots with underscores).
   *
   * @param path the path to cleanse
   * @return the cleansed path
   */
  public static String cleansePath(String path) {
    // look for variables and convert any "." to "_"

    int index = path.indexOf("${");

    int endIndex = 0;
    String tempStr = path;
    while (index >= 0) {
      index += 2;
      endIndex += tempStr.indexOf("}");
      if (endIndex > 0 && endIndex > index + 1) {
        String key = path.substring(index, endIndex);

        String cleanKey = key.replace('.', '_');
        path = path.replace(key, cleanKey);
      } else {
        break;
      }

      if (endIndex + 1 < path.length()) {
        tempStr = path.substring(endIndex + 1, path.length());
      } else {
        break;
      }

      index = tempStr.indexOf("${");

      if (index > 0) {
        index += endIndex;
      }
    }

    return path;
  }

  /**
   * Set user-defined paths for extracting field values from Mongo documents
   *
   * @param fields the field path specifications
   */
  public void setMongoFields(List<MongoField> fields) {
    // copy this list
    m_userFields = new ArrayList<>();

    for (MongoField f : fields) {
      m_userFields.add(f.copy());
    }
  }

  /**
   * Helper function that takes a list of indexed values and returns them as a String in
   * comma-separated form.
   *
   * @param indexedVals a list of indexed values
   * @return the list a String in comma-separated form
   */
  public static String indexedValsList(List<String> indexedVals) {
    StringBuffer temp = new StringBuffer();

    for (int i = 0; i < indexedVals.size(); i++) {
      temp.append(indexedVals.get(i));
      if (i < indexedVals.size() - 1) {
        temp.append(",");
      }
    }

    return temp.toString();
  }

  /**
   * Helper function that takes a comma-separated list in a String and returns a list.
   *
   * @param indexedVals the String containing the lsit
   * @return a List containing the values
   */
  public static List<String> indexedValsList(String indexedVals) {

    String[] parts = indexedVals.split(",");
    List<String> list = new ArrayList<>();
    for (String s : parts) {
      list.add(s.trim());
    }

    return list;
  }
}
