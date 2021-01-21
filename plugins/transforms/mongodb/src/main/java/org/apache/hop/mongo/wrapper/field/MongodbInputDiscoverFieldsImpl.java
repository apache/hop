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

package org.apache.hop.mongo.wrapper.field;

import com.mongodb.AggregationOptions;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.MongoProperties;
import org.apache.hop.mongo.wrapper.MongoClientWrapper;
import org.apache.hop.mongo.wrapper.MongoDBAction;
import org.apache.hop.mongo.wrapper.MongoWrapperUtil;
import org.apache.hop.pipeline.transforms.mongodbinput.DiscoverFieldsCallback;
import org.apache.hop.pipeline.transforms.mongodbinput.MongoDbInputDiscoverFields;
import org.apache.hop.pipeline.transforms.mongodbinput.MongoDbInputMeta;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Created by bryan on 8/7/14. */
public class MongodbInputDiscoverFieldsImpl implements MongoDbInputDiscoverFields {
  private static final Class<?> PKG = MongodbInputDiscoverFieldsImpl.class; // For Translator

  public List<MongoField> discoverFields(
      final MongoProperties.Builder properties,
      final String db,
      final String collection,
      final String query,
      final String fields,
      final boolean isPipeline,
      final int docsToSample,
      MongoDbInputMeta transform)
      throws HopException {
    MongoClientWrapper clientWrapper = null;
    try {
      clientWrapper = MongoWrapperUtil.createMongoClientWrapper(properties, null);
    } catch (MongoDbException e) {
      throw new HopException(e);
    }
    try {
      return clientWrapper.perform(
          db,
          new MongoDBAction<List<MongoField>>() {
            @Override
            public List<MongoField> perform(DB db) throws MongoDbException {
              DBCursor cursor = null;
              int numDocsToSample = docsToSample;
              if (numDocsToSample < 1) {
                numDocsToSample = 100; // default
              }

              List<MongoField> discoveredFields = new ArrayList<>();
              Map<String, MongoField> fieldLookup = new HashMap<>();
              try {
                if (StringUtils.isEmpty(collection)) {
                  throw new HopException(
                      BaseMessages.getString(
                          PKG, "MongoNoAuthWrapper.ErrorMessage.NoCollectionSpecified"));
                }
                DBCollection dbcollection = db.getCollection(collection);

                Iterator<DBObject> pipeSample = null;

                if (isPipeline) {
                  pipeSample = setUpPipelineSample(query, numDocsToSample, dbcollection);
                } else {
                  if (StringUtils.isEmpty(query) && StringUtils.isEmpty(fields)) {
                    cursor = dbcollection.find().limit(numDocsToSample);
                  } else {
                    DBObject dbObject =
                        (DBObject) JSON.parse(StringUtils.isEmpty(query) ? "{}" : query);
                    DBObject dbObject2 = (DBObject) JSON.parse(fields);
                    cursor = dbcollection.find(dbObject, dbObject2).limit(numDocsToSample);
                  }
                }

                int actualCount = 0;
                while (cursor != null ? cursor.hasNext() : pipeSample.hasNext()) {
                  actualCount++;
                  DBObject nextDoc = (cursor != null ? cursor.next() : pipeSample.next());
                  docToFields(nextDoc, fieldLookup);
                }

                postProcessPaths(fieldLookup, discoveredFields, actualCount);

                return discoveredFields;
              } catch (Exception e) {
                throw new MongoDbException(e);
              } finally {
                if (cursor != null) {
                  cursor.close();
                }
              }
            }
          });
    } catch (Exception ex) {
      if (ex instanceof HopException) {
        throw (HopException) ex;
      } else {
        throw new HopException(
            BaseMessages.getString(PKG, "MongoNoAuthWrapper.ErrorMessage.UnableToDiscoverFields"),
            ex);
      }
    } finally {
      try {
        clientWrapper.dispose();
      } catch (MongoDbException e) {
        // Ignore
      }
    }
  }

  @Override
  public void discoverFields(
      final MongoProperties.Builder properties,
      final String db,
      final String collection,
      final String query,
      final String fields,
      final boolean isPipeline,
      final int docsToSample,
      final MongoDbInputMeta transform,
      final DiscoverFieldsCallback discoverFieldsCallback)
      throws HopException {
    new Thread(
            new Runnable() {
              @Override
              public void run() {
                try {
                  discoverFieldsCallback.notifyFields(
                      discoverFields(
                          properties,
                          db,
                          collection,
                          query,
                          fields,
                          isPipeline,
                          docsToSample,
                          transform));
                } catch (HopException e) {
                  discoverFieldsCallback.notifyException(e);
                }
              }
            })
        .run();
  }

  protected static void postProcessPaths(
      Map<String, MongoField> fieldLookup,
      List<MongoField> discoveredFields,
      int numDocsProcessed) {
    List<String> fieldKeys = new ArrayList<>( fieldLookup.keySet() );
    Collections.sort(fieldKeys); // sorting so name clash number assignments will be deterministic
    for (String key : fieldKeys) {
      MongoField m = fieldLookup.get(key);
      m.occurrenceFraction = "" + m.percentageOfSample + "/" + numDocsProcessed;
      setMinArrayIndexes(m);

      // set field names to terminal part and copy any min:max array index
      // info
      if (m.fieldName.contains("[") && m.fieldName.contains(":")) {
        m.arrayIndexInfo = m.fieldName;
      }
      if (m.fieldName.indexOf('.') >= 0) {
        m.fieldName = m.fieldName.substring(m.fieldName.lastIndexOf('.') + 1, m.fieldName.length());
      }

      if (m.disparateTypes) {
        // force type to string if we've seen this path more than once
        // with incompatible types
        m.hopType = IValueMeta.getTypeDescription(ValueMetaInteger.TYPE_STRING);
      }
      discoveredFields.add(m);
    }

    // check for name clashes
    Map<String, Integer> tempM = new HashMap<>();
    for (MongoField m : discoveredFields) {
      if (tempM.get(m.fieldName) != null) {
        Integer toUse = tempM.get(m.fieldName);
        String key = m.fieldName;
        m.fieldName = key + "_" + toUse;
        toUse = new Integer(toUse.intValue() + 1);
        tempM.put(key, toUse);
      } else {
        tempM.put(m.fieldName, 1);
      }
    }
  }

  protected static void setMinArrayIndexes(MongoField m) {
    // set the actual index for each array in the path to the
    // corresponding minimum index
    // recorded in the name

    if (m.fieldName.indexOf('[') < 0) {
      return;
    }

    String temp = m.fieldPath;
    String tempComp = m.fieldName;
    StringBuffer updated = new StringBuffer();

    while (temp.indexOf('[') >= 0) {
      String firstPart = temp.substring(0, temp.indexOf('['));
      String innerPart = temp.substring(temp.indexOf('[') + 1, temp.indexOf(']'));

      if (!innerPart.equals("-")) {
        // terminal primitive specific index
        updated.append(temp); // finished
        temp = "";
        break;
      } else {
        updated.append(firstPart);

        String innerComp = tempComp.substring(tempComp.indexOf('[') + 1, tempComp.indexOf(']'));

        if (temp.indexOf(']') < temp.length() - 1) {
          temp = temp.substring(temp.indexOf(']') + 1, temp.length());
          tempComp = tempComp.substring(tempComp.indexOf(']') + 1, tempComp.length());
        } else {
          temp = "";
        }

        String[] compParts = innerComp.split(":");
        String replace = "[" + compParts[0] + "]";
        updated.append(replace);
      }
    }

    if (temp.length() > 0) {
      // append remaining part
      updated.append(temp);
    }

    m.fieldPath = updated.toString();
  }

  protected static void docToFields(DBObject doc, Map<String, MongoField> lookup) {
    String root = "$";
    String name = "$";

    if (doc instanceof BasicDBObject) {
      processRecord((BasicDBObject) doc, root, name, lookup);
    } else if (doc instanceof BasicDBList) {
      processList((BasicDBList) doc, root, name, lookup);
    }
  }

  private static void processRecord(
      BasicDBObject rec, String path, String name, Map<String, MongoField> lookup) {
    for (String key : rec.keySet()) {
      Object fieldValue = rec.get(key);

      if (fieldValue instanceof BasicDBObject) {
        processRecord((BasicDBObject) fieldValue, path + "." + key, name + "." + key, lookup);
      } else if (fieldValue instanceof BasicDBList) {
        processList((BasicDBList) fieldValue, path + "." + key, name + "." + key, lookup);
      } else {
        // some sort of primitive
        String finalPath = path + "." + key;
        String finalName = name + "." + key;
        if (!lookup.containsKey(finalPath)) {
          MongoField newField = new MongoField();
          int hopType = mongoToHopType(fieldValue);
          // Following suit of mongoToHopType by interpreting null as String type
          newField.mongoType = String.class;
          if (fieldValue != null) {
            newField.mongoType = fieldValue.getClass();
          }
          newField.fieldName = finalName;
          newField.fieldPath = finalPath;
          newField.hopType = IValueMeta.getTypeDescription(hopType);
          newField.percentageOfSample = 1;

          lookup.put(finalPath, newField);
        } else {
          // update max indexes in array parts of name
          MongoField m = lookup.get(finalPath);
          Class<?> fieldClass = String.class;
          if (fieldValue != null) {
            fieldClass = fieldValue.getClass();
          }
          if (!m.mongoType.isAssignableFrom(fieldClass)) {
            m.disparateTypes = true;
          }
          m.percentageOfSample++;
          updateMinMaxArrayIndexes(m, finalName);
        }
      }
    }
  }

  private static void processList(
      BasicDBList list, String path, String name, Map<String, MongoField> lookup) {

    if (list.size() == 0) {
      return; // can't infer anything about an empty list
    }

    String nonPrimitivePath = path + "[-]";
    String primitivePath = path;

    for (int i = 0; i < list.size(); i++) {
      Object element = list.get(i);

      if (element instanceof BasicDBObject) {
        processRecord(
            (BasicDBObject) element, nonPrimitivePath, name + "[" + i + ":" + i + "]", lookup);
      } else if (element instanceof BasicDBList) {
        processList(
            (BasicDBList) element, nonPrimitivePath, name + "[" + i + ":" + i + "]", lookup);
      } else {
        // some sort of primitive
        String finalPath = primitivePath + "[" + i + "]";
        String finalName = name + "[" + i + "]";
        if (!lookup.containsKey(finalPath)) {
          MongoField newField = new MongoField();
          int hopType = mongoToHopType(element);
          // Following suit of mongoToHopType by interpreting null as String type
          newField.mongoType = String.class;
          if (element != null) {
            newField.mongoType = element.getClass();
          }
          newField.fieldName = finalPath;
          newField.fieldPath = finalName;
          newField.hopType = IValueMeta.getTypeDescription(hopType);
          newField.percentageOfSample = 1;

          lookup.put(finalPath, newField);
        } else {
          // update max indexes in array parts of name
          MongoField m = lookup.get(finalPath);
          Class<?> elementClass = String.class;
          if (element != null) {
            elementClass = element.getClass();
          }
          if (!m.mongoType.isAssignableFrom(elementClass)) {
            m.disparateTypes = true;
          }
          m.percentageOfSample++;
          updateMinMaxArrayIndexes(m, finalName);
        }
      }
    }
  }

  protected static void updateMinMaxArrayIndexes(MongoField m, String update) {
    // just look at the second (i.e. max index value) in the array parts
    // of update
    if (m.fieldName.indexOf('[') < 0) {
      return;
    }

    if (m.fieldName.split("\\[").length != update.split("\\[").length) {
      throw new IllegalArgumentException(
          "Field path and update path do not seem to contain " + "the same number of array parts!");
    }

    String temp = m.fieldName;
    String tempComp = update;
    StringBuffer updated = new StringBuffer();

    while (temp.indexOf('[') >= 0) {
      String firstPart = temp.substring(0, temp.indexOf('['));
      String innerPart = temp.substring(temp.indexOf('[') + 1, temp.indexOf(']'));

      if (innerPart.indexOf(':') < 0) {
        // terminal primitive specific index
        updated.append(temp); // finished
        temp = "";
        break;
      } else {
        updated.append(firstPart);

        String innerComp = tempComp.substring(tempComp.indexOf('[') + 1, tempComp.indexOf(']'));

        if (temp.indexOf(']') < temp.length() - 1) {
          temp = temp.substring(temp.indexOf(']') + 1, temp.length());
          tempComp = tempComp.substring(tempComp.indexOf(']') + 1, tempComp.length());
        } else {
          temp = "";
        }

        String[] origParts = innerPart.split(":");
        String[] compParts = innerComp.split(":");
        int origMin = Integer.parseInt(origParts[0]);
        int compMin = Integer.parseInt(compParts[0]);
        int origMax = Integer.parseInt(origParts[1]);
        int compMax = Integer.parseInt(compParts[1]);

        String newRange =
            "["
                + (compMin < origMin ? compMin : origParts[0])
                + ":"
                + (compMax > origMax ? compMax : origParts[1])
                + "]";
        updated.append(newRange);
      }
    }

    if (temp.length() > 0) {
      // append remaining part
      updated.append(temp);
    }

    m.fieldName = updated.toString();
  }

  protected static int mongoToHopType(Object fieldValue) {
    if (fieldValue == null) {
      return IValueMeta.TYPE_STRING;
    }

    if (fieldValue instanceof Symbol
        || fieldValue instanceof String
        || fieldValue instanceof Code
        || fieldValue instanceof ObjectId
        || fieldValue instanceof MinKey
        || fieldValue instanceof MaxKey) {
      return IValueMeta.TYPE_STRING;
    } else if (fieldValue instanceof Date) {
      return IValueMeta.TYPE_DATE;
    } else if (fieldValue instanceof Number) {
      // try to parse as an Integer
      try {
        Integer.parseInt(fieldValue.toString());
        return IValueMeta.TYPE_INTEGER;
      } catch (NumberFormatException e) {
        return IValueMeta.TYPE_NUMBER;
      }
    } else if (fieldValue instanceof Binary) {
      return IValueMeta.TYPE_BINARY;
    } else if (fieldValue instanceof BSONTimestamp) {
      return IValueMeta.TYPE_INTEGER;
    }

    return IValueMeta.TYPE_STRING;
  }

  private static Iterator<DBObject> setUpPipelineSample(
      String query, int numDocsToSample, DBCollection collection) throws HopException {

    query = query + ", {$limit : " + numDocsToSample + "}";
    List<DBObject> samplePipe = jsonPipelineToDBObjectList(query);
    Cursor cursor = collection.aggregate(samplePipe, AggregationOptions.builder().build());
    return cursor;
  }

  public static List<DBObject> jsonPipelineToDBObjectList(String jsonPipeline) throws HopException {
    List<DBObject> pipeline = new ArrayList<>();
    StringBuilder b = new StringBuilder(jsonPipeline.trim());

    // extract the parts of the pipeline
    int bracketCount = -1;
    List<String> parts = new ArrayList<>();
    int i = 0;
    while (i < b.length()) {
      if (b.charAt(i) == '{') {
        if (bracketCount == -1) {
          // trim anything off before this point
          b.delete(0, i);
          bracketCount = 0;
          i = 0;
        }
        bracketCount++;
      }
      if (b.charAt(i) == '}') {
        bracketCount--;
      }
      if (bracketCount == 0) {
        String part = b.substring(0, i + 1);
        parts.add(part);
        bracketCount = -1;

        if (i == b.length() - 1) {
          break;
        }
        b.delete(0, i + 1);
        i = 0;
      }

      i++;
    }

    for (String p : parts) {
      if (!StringUtils.isEmpty(p)) {
        DBObject o = (DBObject) JSON.parse(p);
        pipeline.add(o);
      }
    }

    if (pipeline.size() == 0) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "MongoNoAuthWrapper.ErrorMessage.UnableToParsePipelineOperators"));
    }

    return pipeline;
  }
}
