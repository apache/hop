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

package org.apache.hop.pipeline.transforms.mongodboutput;

import com.fasterxml.jackson.databind.JsonNode;
import com.mongodb.MongoException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.metadata.MongoDbConnection;
import org.apache.hop.mongo.wrapper.MongoClientWrapper;
import org.apache.hop.mongo.wrapper.collection.MongoCollectionWrapper;
import org.apache.hop.mongo.wrapper.cursor.MongoCursorWrapper;
import org.apache.hop.mongo.wrapper.field.MongoField;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.bson.Document;

/** Data class for the MongoDbOutput transform */
@Getter
@Setter
public class MongoDbOutputData extends BaseTransformData implements ITransformData {

  private static final Class<?> PKG = MongoDbOutputMeta.class;

  public static final int MONGO_DEFAULT_PORT = 27017;

  public static final String LOCAL_DB = "local";
  public static final String REPL_SET_COLLECTION = "system.replset";
  public static final String REPL_SET_SETTINGS = "settings";
  public static final String REPL_SET_LAST_ERROR_MODES = "getLastErrorModes";
  public static final String CONST_MONGO_DB_OUTPUT_MESSAGES_ERROR_NO_FIELD_NAME_SPECIFIED_FOR_PATH =
      "MongoDbOutput.Messages.Error.NoFieldNameSpecifiedForPath";
  public static final String CONST_PUSH = "$push";

  /** Shared connection in metadata */
  public MongoDbConnection connection;

  /** Enum for the type of the top level object of the document structure */
  public enum MongoTopLevel {
    RECORD,
    ARRAY,
    INCONSISTENT;
  }

  /** The output row format */
  protected IRowMeta outputRowMeta;

  /** Main entry point to the mongo driver */
  protected MongoClientWrapper clientWrapper;

  /** Collection object for the user-specified document collection */
  protected MongoCollectionWrapper collection;

  protected List<MongoDbOutputMeta.MongoField> userFields;

  /**
   * Map for grouping together $set operations that involve setting complex array-based objects. Key
   * = dot path to array name; value = Document specifying array to set to
   */
  protected Map<String, List<MongoDbOutputMeta.MongoField>> setComplexArrays = new HashMap<>();

  /**
   * Map for grouping together $push operations that involve complex objects. Use [] to indicate the
   * start of the complex object to push. Key - dot path to the array name to push to; value -
   * Document specifying the complex object to push
   */
  protected Map<String, List<MongoDbOutputMeta.MongoField>> pushComplexStructures = new HashMap<>();

  /** all other modifier updates that involve primitive leaf fields */
  protected Map<String, Object[]> primitiveLeafModifiers = new LinkedHashMap<>();

  /**
   * True if the list of paths specifies an incoming Hop field that contains a JSON doc that is
   * intended to be inserted as is (i.e. not added to a field in the document structure defined by
   * the mongo paths)
   */
  protected boolean hasTopLevelJsonDocInsert = false;

  public MongoDbOutputData() {
    super();
  }

  public static boolean scanForInsertTopLevelJSONDoc(List<MongoDbOutputMeta.MongoField> fieldDefs)
      throws HopException {

    int countNonMatchFields = 0;
    boolean hasTopLevelJSONDocInsert = false;

    for (MongoDbOutputMeta.MongoField f : fieldDefs) {
      if (f.inputJson
          && !f.updateMatchField
          && StringUtils.isEmpty(f.mongoDocPath)
          && !f.useIncomingFieldNameAsMongoFieldName) {
        hasTopLevelJSONDocInsert = true;
      }

      if (!f.updateMatchField) {
        countNonMatchFields++;
      }
    }

    if (hasTopLevelJSONDocInsert && countNonMatchFields > 1) {
      throw new HopException(
          "Path specifications contains a top-level document in "
              + "JSON format to be inserted as is, but there are other insert paths "
              + "defined. When a top-level JSON document is to be inserted it must be "
              + "the only non-match field defined in the path specifications");
    }

    return hasTopLevelJSONDocInsert;
  }

  /**
   * Set the field paths to use for creating the document structure
   *
   * @param fields the field paths to use
   */
  public void setMongoFields(List<MongoDbOutputMeta.MongoField> fields) {
    // copy this list
    userFields = new ArrayList<>();

    for (MongoDbOutputMeta.MongoField f : fields) {
      userFields.add(f.copy());
    }
  }

  /** Gets the field paths to use for creating the document structure */
  public List<MongoDbOutputMeta.MongoField> getMongoFields() {
    return userFields;
  }

  /**
   * Initialize field paths
   *
   * @param vars variables to use
   * @throws HopException if a problem occurs
   */
  public void init(IVariables vars) throws HopException {
    if (userFields != null) {
      for (MongoDbOutputMeta.MongoField f : userFields) {
        f.init(vars);
      }
    }
  }

  /**
   * Get the current connection or null if not connected
   *
   * @return the connection or null
   */
  public MongoClientWrapper getConnection() {
    return clientWrapper;
  }

  /**
   * Set the current connection
   *
   * @param clientWrapper the connection to use
   */
  public void setConnection(MongoClientWrapper clientWrapper) {
    this.clientWrapper = clientWrapper;
  }

  /**
   * Create a collection in the current database
   *
   * @param collectionName the name of the collection to create
   * @throws Exception if a problem occurs
   */
  public void createCollection(String db, String collectionName) throws Exception {
    if (clientWrapper == null) {
      throw new Exception(
          BaseMessages.getString(PKG, "MongoDbOutput.Messages.Error.NoDatabaseSet"));
    }

    clientWrapper.createCollection(db, collectionName);
  }

  /**
   * Apply the supplied index operations to the collection. Indexes can be defined on one or more
   * fields in the document. Operation is either create or drop.
   *
   * @param indexes a list of index operations
   * @param log the logging object
   * @param truncate true if the collection was truncated in the current execution - in this case
   *     drop operations are not necessary
   * @throws com.mongodb.MongoException if something goes wrong
   * @throws org.apache.hop.mongo.MongoDbException
   */
  public void applyIndexes(
      List<MongoDbOutputMeta.MongoIndex> indexes, ILogChannel log, boolean truncate)
      throws MongoException, MongoDbException {

    for (MongoDbOutputMeta.MongoIndex index : indexes) {
      String[] indexParts = index.pathToFields.split(",");
      Document mongoIndex = new Document();
      for (String indexKey : indexParts) {
        String[] nameAndDirection = indexKey.split(":");
        int direction = 1;
        if (nameAndDirection.length == 2) {
          direction = Integer.parseInt(nameAndDirection[1].trim());
        }
        String name = nameAndDirection[0];

        // strip off brackets to get actual object name if terminal object
        // is an array
        if (name.contains("[")) {
          name = name.substring(name.indexOf('[') + 1);
        }

        mongoIndex.put(name, direction);
      }

      if (index.drop) {
        if (truncate) {
          log.logBasic(
              BaseMessages.getString(PKG, "MongoDbOutput.Messages.TruncateBeforeInsert", index));
        } else {
          collection.dropIndex(mongoIndex);
        }
        log.logBasic(BaseMessages.getString(PKG, "MongoDbOutput.Messages.DropIndex", index));
      } else {
        Document options = new Document();

        // create indexes in the background
        options.put("background", true);
        options.put("unique", index.unique);
        options.put("sparse", index.sparse);
        collection.createIndex(mongoIndex, options);
        log.logBasic(BaseMessages.getString(PKG, "MongoDbOutput.Messages.CreateIndex", index));
      }
    }
  }

  /**
   * Get an object that encapsulates the fields and modifier operations to use for a modifier
   * update.
   *
   * @param fieldDefs the list of document field definitions
   * @param inputMeta the input row format
   * @param row the current incoming row
   * @param vars environment variables
   * @param topLevelStructure the top level structure of the document
   * @return a Document encapsulating the update to make
   * @throws HopException if a problem occurs
   */
  protected Document getModifierUpdateObject(
      List<MongoDbOutputMeta.MongoField> fieldDefs,
      IRowMeta inputMeta,
      Object[] row,
      IVariables vars,
      MongoTopLevel topLevelStructure)
      throws HopException, MongoDbException {

    boolean haveUpdateFields = false;
    boolean hasNonNullUpdateValues = false;
    String mongoOperatorUpdateAllArray = "$[]";

    // main update object, keyed by $ operator
    Document updateObject = new Document();

    setComplexArrays.clear();
    primitiveLeafModifiers.clear();
    pushComplexStructures.clear();

    // do we need to determine whether this will be an insert or an update?
    boolean checkForMatch = false;
    for (MongoDbOutputMeta.MongoField field : fieldDefs) {
      if (!field.updateMatchField
          && (field.modifierOperationApplyPolicy.equals("Insert")
              || field.modifierOperationApplyPolicy.equals("Update"))) {
        checkForMatch = true;
        break;
      }
    }

    boolean isUpdate = false;
    if (checkForMatch) {
      Document query = getQueryObject(fieldDefs, inputMeta, row, vars, topLevelStructure);

      MongoCursorWrapper cursor = getCollection().find(query).limit(1);
      if (cursor.hasNext()) {
        isUpdate = true;
      }
    }

    for (MongoDbOutputMeta.MongoField field : fieldDefs) {
      // skip query match fields
      if (field.updateMatchField) {
        continue;
      }

      String modifierUpdateOpp = field.environUpdateModifierOperation;

      if (!StringUtils.isEmpty(modifierUpdateOpp) && !modifierUpdateOpp.equals("N/A")) {
        if (checkForMatch) {
          if (isUpdate && field.modifierOperationApplyPolicy.equals("Insert")) {
            continue; // don't apply this opp
          }

          if (!isUpdate && field.modifierOperationApplyPolicy.equals("Update")) {
            continue; // don't apply this opp
          }
        }

        haveUpdateFields = true;

        String incomingFieldName = field.environUpdatedFieldName;
        int index = inputMeta.indexOfValue(incomingFieldName);
        IValueMeta vm = inputMeta.getValueMeta(index);

        if (!vm.isNull(row[index]) || field.insertNull) {
          hasNonNullUpdateValues = true;

          // modifier update objects have fields using "dot" notation to reach
          // into embedded documents
          String path =
              (field.environUpdateMongoDocPath != null) ? field.environUpdateMongoDocPath : "";

          if (path.endsWith("]")
              && modifierUpdateOpp.equals(CONST_PUSH)
              && !field.useIncomingFieldNameAsMongoFieldName) {

            // strip off the brackets as push appends to the end of the named
            // array
            path = path.substring(0, path.indexOf('['));
          }

          boolean hasPath = !StringUtils.isEmpty(path);
          path +=
              ((field.useIncomingFieldNameAsMongoFieldName)
                  ? (hasPath ? "." + incomingFieldName : incomingFieldName)
                  : "");

          // check for array creation
          if (modifierUpdateOpp.equals("$set")
              && path.contains("[")
              && !path.contains(mongoOperatorUpdateAllArray)) {
            String arrayPath = path.substring(0, path.indexOf('['));
            String arraySpec = path.substring(path.indexOf('['));
            MongoDbOutputMeta.MongoField a = new MongoDbOutputMeta.MongoField();
            a.incomingFieldName = field.incomingFieldName;
            a.environUpdatedFieldName = field.environUpdatedFieldName;
            a.mongoDocPath = arraySpec;
            a.environUpdateMongoDocPath = arraySpec;
            // incoming field name has already been appended (if necessary)
            a.useIncomingFieldNameAsMongoFieldName = false;
            a.inputJson = field.inputJson;
            a.init(vars, false);
            List<MongoDbOutputMeta.MongoField> fds = setComplexArrays.get(arrayPath);
            if (fds == null) {
              fds = new ArrayList<>();
              setComplexArrays.put(arrayPath, fds);
            }
            fds.add(a);
          } else if (modifierUpdateOpp.equals(CONST_PUSH)
              && path.contains("[")
              && !path.contains(mongoOperatorUpdateAllArray)) {
            // we ignore any index that might have been specified as $push
            // always appends to the end of the array.
            String arrayPath = path.substring(0, path.indexOf('['));
            String structureToPush = path.substring(path.indexOf(']') + 1);

            // check to see if we're pushing a record at this point in the path
            // or another array...
            if (structureToPush.charAt(0) == '.') {
              // skip the dot
              structureToPush = structureToPush.substring(1);
            }

            MongoDbOutputMeta.MongoField a = new MongoDbOutputMeta.MongoField();
            a.incomingFieldName = field.incomingFieldName;
            a.environUpdatedFieldName = field.environUpdatedFieldName;
            a.mongoDocPath = structureToPush;
            a.environUpdateMongoDocPath = structureToPush;
            // incoming field name has already been appended (if necessary)
            a.useIncomingFieldNameAsMongoFieldName = false;
            a.inputJson = field.inputJson;
            a.init(vars, false);
            List<MongoDbOutputMeta.MongoField> fds = pushComplexStructures.get(arrayPath);
            if (fds == null) {
              fds = new ArrayList<>();
              pushComplexStructures.put(arrayPath, fds);
            }
            fds.add(a);
          } else {
            Object[] params = new Object[4];
            params[0] = modifierUpdateOpp;
            params[1] = index;
            params[2] = field.inputJson;
            params[3] = field.insertNull;
            primitiveLeafModifiers.put(path, params);
          }
        }
      }
    }

    // do the array $sets
    for (String path : setComplexArrays.keySet()) {
      List<MongoDbOutputMeta.MongoField> fds = setComplexArrays.get(path);
      Object valueToSet = hopRowToMongo(fds, inputMeta, row, MongoTopLevel.ARRAY, false);

      Document fieldsToUpdateWithValues;

      if (updateObject.get("$set") != null) {
        fieldsToUpdateWithValues = (Document) updateObject.get("$set");
      } else {
        fieldsToUpdateWithValues = new Document();
      }

      fieldsToUpdateWithValues.put(path, valueToSet);
      updateObject.put("$set", fieldsToUpdateWithValues);
    }

    // now do the $push complex
    for (String path : pushComplexStructures.keySet()) {
      List<MongoDbOutputMeta.MongoField> fds = pushComplexStructures.get(path);

      // check our top-level structure
      MongoTopLevel topLevel = MongoTopLevel.RECORD;
      if (fds.get(0).mongoDocPath.charAt(0) == '[') {
        topLevel = MongoTopLevel.RECORD;
      }

      Object valueToSet = hopRowToMongo(fds, inputMeta, row, topLevel, false);

      Document fieldsToUpdateWithValues = null;

      if (updateObject.get(CONST_PUSH) != null) {
        fieldsToUpdateWithValues = (Document) updateObject.get(CONST_PUSH);
      } else {
        fieldsToUpdateWithValues = new Document();
      }

      fieldsToUpdateWithValues.put(path, valueToSet);
      updateObject.put(CONST_PUSH, fieldsToUpdateWithValues);
    }

    // do the modifiers that involve primitive field values
    for (Map.Entry<String, Object[]> entry : primitiveLeafModifiers.entrySet()) {
      String path = entry.getKey();
      Object[] params = entry.getValue();
      String modifierUpdateOpp = params[0].toString();
      int index = (Integer) params[1];
      boolean isJSON = (Boolean) params[2];
      boolean allowNull = (Boolean) params[3];
      IValueMeta vm = inputMeta.getValueMeta(index);

      Document fieldsToUpdateWithValues = null;

      if (updateObject.get(modifierUpdateOpp) != null) {
        fieldsToUpdateWithValues = (Document) updateObject.get(modifierUpdateOpp);
      } else {
        fieldsToUpdateWithValues = new Document();
      }
      setMongoValueFromHopValue(fieldsToUpdateWithValues, path, vm, row[index], isJSON, allowNull);

      updateObject.put(modifierUpdateOpp, fieldsToUpdateWithValues);
    }

    if (!haveUpdateFields) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "MongoDbOutput.Messages.Error.NoFieldsToUpdateSpecifiedForModifierOpp"));
    }

    if (!hasNonNullUpdateValues) {
      return null;
    }

    return updateObject;
  }

  /**
   * Get an object that encapsulates the query to make for an update/upsert operation
   *
   * @param fieldDefs the list of document field definitions
   * @param inputMeta the input row format
   * @param row the current incoming row
   * @param vars environment variables
   * @return a Document encapsulating the query
   * @throws HopException if something goes wrong
   */
  protected static Document getQueryObject(
      List<MongoDbOutputMeta.MongoField> fieldDefs,
      IRowMeta inputMeta,
      Object[] row,
      IVariables vars,
      MongoTopLevel topLevelStructure)
      throws HopException {
    Document query = new Document();

    boolean haveMatchFields = false;
    boolean hasNonNullMatchValues = false;

    for (MongoDbOutputMeta.MongoField field : fieldDefs) {
      if (field.updateMatchField) {
        haveMatchFields = true;
        String incomingFieldName = field.environUpdatedFieldName;
        int index = inputMeta.indexOfValue(incomingFieldName);
        IValueMeta vm = inputMeta.getValueMeta(index);

        // ignore null fields is not prohibited
        if (vm.isNull(row[index]) && !field.insertNull) {
          continue;
        }

        hasNonNullMatchValues = true;

        if (field.inputJson
            && StringUtils.isEmpty(field.mongoDocPath)
            && !field.useIncomingFieldNameAsMongoFieldName) {
          // We have a query based on a complete incoming JSON doc -
          // i.e. no field processing necessary

          if (vm.isString()) {
            String val = vm.getString(row[index]);
            query = Document.parse(val);
          } else {
            throw new HopException(
                BaseMessages.getString(
                    PKG, "MongoDbOutput.Messages.MatchFieldJSONButIncomingValueNotString"));
          }
          break;
        }

        // query objects have fields using "dot" notation to reach into embedded
        // documents
        String path =
            (field.environUpdateMongoDocPath != null) ? field.environUpdateMongoDocPath : "";

        boolean hasPath = !StringUtils.isEmpty(path);
        path +=
            ((field.useIncomingFieldNameAsMongoFieldName)
                ? (hasPath ? "." + incomingFieldName : incomingFieldName)
                : "");

        // post process arrays to fit the dot notation (if not already done
        // by the user)
        if (path.contains("[")) {
          path = path.replace("[", ".").replace("]", "");
        }

        setMongoValueFromHopValue(query, path, vm, row[index], field.inputJson, field.insertNull);
      }
    }

    if (!haveMatchFields) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "MongoDbOutput.Messages.Error.NoFieldsToUpdateSpecifiedForMatch"));
    }

    if (!hasNonNullMatchValues) {
      // indicates that we don't have anything to match with with respect to
      // this row
      return null;
    }

    return query;
  }

  /**
   * Converts a Hop row to a Mongo Object for inserting/updating
   *
   * @param fieldDefs the document field definitions
   * @param inputMeta the incoming row format
   * @param row the current incoming row
   * @param topLevelStructure the top level structure of the Mongo document
   * @param hasTopLevelJSONDocInsert true if the user-specified paths include a single incoming Hop
   *     field value that contains a JSON document that is to be inserted as is
   * @return a Document encapsulating the document to insert/upsert or null if there are no non-null
   *     incoming fields
   * @throws HopException if a problem occurs
   */
  protected static Object hopRowToMongo(
      List<MongoDbOutputMeta.MongoField> fieldDefs,
      IRowMeta inputMeta,
      Object[] row,
      MongoTopLevel topLevelStructure,
      boolean hasTopLevelJSONDocInsert)
      throws HopException {

    // the easy case
    if (hasTopLevelJSONDocInsert) {
      for (MongoDbOutputMeta.MongoField f : fieldDefs) {
        if (f.inputJson
            && StringUtils.isEmpty(f.mongoDocPath)
            && !f.useIncomingFieldNameAsMongoFieldName) {
          String incomingFieldName = f.environUpdatedFieldName;
          int index = inputMeta.indexOfValue(incomingFieldName);
          IValueMeta vm = inputMeta.getValueMeta(index);
          if (!vm.isNull(row[index])) {
            String jsonDoc = vm.getString(row[index]);
            return Document.parse(jsonDoc);
          } else {
            return null;
          }
        }
      }
    }

    Object root = null;
    if (topLevelStructure == MongoTopLevel.RECORD) {
      root = new Document();
    } else if (topLevelStructure == MongoTopLevel.ARRAY) {
      root = new ArrayList<>();
    }

    boolean haveNonNullFields = false;
    for (MongoDbOutputMeta.MongoField field : fieldDefs) {
      Object current = root;

      field.reset();
      List<String> pathParts = field.tempPathList;
      String incomingFieldName = field.environUpdatedFieldName;
      int index = inputMeta.indexOfValue(incomingFieldName);
      IValueMeta vm = inputMeta.getValueMeta(index);

      Object lookup =
          getPathElementName(pathParts, current, field.useIncomingFieldNameAsMongoFieldName);
      do {
        // array?
        if (lookup != null && lookup instanceof Integer) {
          @SuppressWarnings("unchecked")
          List<Object> temp = (List<Object>) current;
          int idx = (Integer) lookup;
          // Ensure list is large enough
          while (temp.size() <= idx) {
            temp.add(null);
          }
          if (temp.get(idx) == null) {
            if (pathParts.isEmpty() && !field.useIncomingFieldNameAsMongoFieldName) {
              // leaf - primitive element of the array (unless Hop field
              // value is JSON)
              boolean res =
                  setMongoValueFromHopValue(
                      temp, lookup, vm, row[index], field.inputJson, field.insertNull);
              haveNonNullFields = (haveNonNullFields || res);
            } else {
              // must be a record here
              Document newRec = new Document();
              temp.set(idx, newRec);
              current = newRec;

              // end of the path?
              if (pathParts.isEmpty()) {
                if (field.useIncomingFieldNameAsMongoFieldName) {
                  boolean res =
                      setMongoValueFromHopValue(
                          current,
                          incomingFieldName,
                          vm,
                          row[index],
                          field.inputJson,
                          field.insertNull);
                  haveNonNullFields = (haveNonNullFields || res);
                } else {
                  throw new HopException(
                      BaseMessages.getString(
                          PKG,
                          CONST_MONGO_DB_OUTPUT_MESSAGES_ERROR_NO_FIELD_NAME_SPECIFIED_FOR_PATH));
                }
              }
            }
          } else {
            // existing element of the array
            current = temp.get(idx);

            // no more path parts so we must be setting a field in an array
            // element that is a record
            if ((Utils.isEmpty(pathParts)) && current instanceof Document) {
              if (field.useIncomingFieldNameAsMongoFieldName) {
                boolean res =
                    setMongoValueFromHopValue(
                        current,
                        incomingFieldName,
                        vm,
                        row[index],
                        field.inputJson,
                        field.insertNull);
                haveNonNullFields = (haveNonNullFields || res);
              } else {
                throw new HopException(
                    BaseMessages.getString(
                        PKG,
                        CONST_MONGO_DB_OUTPUT_MESSAGES_ERROR_NO_FIELD_NAME_SPECIFIED_FOR_PATH));
              }
            }
          }
        } else {
          // record/object
          if (lookup == null && pathParts.isEmpty()) {
            if (field.useIncomingFieldNameAsMongoFieldName) {
              boolean res =
                  setMongoValueFromHopValue(
                      current,
                      incomingFieldName,
                      vm,
                      row[index],
                      field.inputJson,
                      field.insertNull);
              haveNonNullFields = (haveNonNullFields || res);
            } else {
              throw new HopException(
                  BaseMessages.getString(
                      PKG, CONST_MONGO_DB_OUTPUT_MESSAGES_ERROR_NO_FIELD_NAME_SPECIFIED_FOR_PATH));
            }
          } else {
            if (pathParts.isEmpty()) {
              if (!field.useIncomingFieldNameAsMongoFieldName) {
                boolean res =
                    setMongoValueFromHopValue(
                        current,
                        lookup.toString(),
                        vm,
                        row[index],
                        field.inputJson,
                        field.insertNull);
                haveNonNullFields = (haveNonNullFields || res);
              } else {
                Document doc = (Document) current;
                current = doc.get(lookup.toString());
                boolean res =
                    setMongoValueFromHopValue(
                        current,
                        incomingFieldName,
                        vm,
                        row[index],
                        field.inputJson,
                        field.insertNull);
                haveNonNullFields = (haveNonNullFields || res);
              }
            } else {
              Document doc = (Document) current;
              current = doc.get(lookup.toString());
            }
          }
        }

        lookup = getPathElementName(pathParts, current, field.useIncomingFieldNameAsMongoFieldName);
      } while (lookup != null);
    }

    if (!haveNonNullFields) {
      return null; // nothing has been set!
    }

    return root;
  }

  private static boolean setMongoValueFromHopValue(
      Object mongoObject,
      Object lookup,
      IValueMeta hopType,
      Object hopValue,
      boolean hopValueIsJSON,
      boolean allowNull)
      throws HopValueException {
    if (hopType.isNull(hopValue)) {
      if (allowNull) {
        if (mongoObject instanceof Document doc) {
          doc.put(lookup.toString(), null);
        } else if (mongoObject instanceof List) {
          @SuppressWarnings("unchecked")
          List<Object> list = (List<Object>) mongoObject;
          int idx = (Integer) lookup;
          while (list.size() <= idx) {
            list.add(null);
          }
          list.set(idx, null);
        }
        return true;
      } else {
        return false;
      }
    }

    Object valueToSet = null;

    switch (hopType.getType()) {
      case IValueMeta.TYPE_STRING:
        {
          String val = hopType.getString(hopValue);
          if (hopValueIsJSON) {
            valueToSet = Document.parse(val);
          } else {
            valueToSet = val;
          }
          break;
        }
      case IValueMeta.TYPE_BOOLEAN:
        valueToSet = hopType.getBoolean(hopValue);
        break;
      case IValueMeta.TYPE_INTEGER:
        valueToSet = hopType.getInteger(hopValue);
        break;
      case IValueMeta.TYPE_DATE:
        valueToSet = hopType.getDate(hopValue);
        break;
      case IValueMeta.TYPE_NUMBER:
        valueToSet = hopType.getNumber(hopValue);
        break;
      case IValueMeta.TYPE_BIGNUMBER:
        // use string value - user can use Hop to convert back
        valueToSet = hopType.getString(hopValue);
        break;
      case IValueMeta.TYPE_BINARY:
        valueToSet = hopType.getBinary(hopValue);
        break;
      case IValueMeta.TYPE_JSON:
        {
          JsonNode node = hopType.getJson(hopValue);
          valueToSet = MongoField.toBsonFromJsonNode(node);
          break;
        }
      default:
        {
          // UUID
          try {
            int uuidTypeId = ValueMetaFactory.getIdForValueMeta("UUID");
            if (hopType.getType() == uuidTypeId) {
              valueToSet = (UUID) hopType.convertData(hopType, hopValue);
            }
          } catch (Exception ignore) {
            // UUID plugin not present, fall through
          }
          if (hopType.isSerializableType()) {
            throw new HopValueException(
                BaseMessages.getString(
                    PKG, "MongoDbOutput.Messages.Error.CantStoreHopSerializableVals"));
          }
          break;
        }
    }

    if (valueToSet != null) {
      if (mongoObject instanceof Document doc) {
        doc.put(lookup.toString(), valueToSet);
      } else if (mongoObject instanceof List) {
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) mongoObject;
        int idx = (Integer) lookup;
        while (list.size() <= idx) {
          list.add(null);
        }
        list.set(idx, valueToSet);
      }
      return true;
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  private static Object getPathElementName(
      List<String> pathParts, Object current, boolean incomingAsFieldName) throws HopException {

    if (Utils.isEmpty(pathParts)) {
      return null;
    }

    String part = pathParts.get(0);
    if (part.startsWith("[")) {
      String index = part.substring(1, part.indexOf(']')).trim();
      part = part.substring(part.indexOf(']') + 1).trim();
      if (!part.isEmpty()) {
        // any remaining characters must indicate a multi-dimensional array
        pathParts.set(0, part);

        // does this next array exist?
        if (current instanceof List) {
          List<Object> list = (List<Object>) current;
          int idx = Integer.parseInt(index);
          while (list.size() <= idx) {
            list.add(null);
          }
          if (list.get(idx) == null) {
            List<Object> newArr = new ArrayList<>();
            list.set(idx, newArr);
          }
        } else if (current instanceof Document) {
          Document doc = (Document) current;
          if (doc.get(index) == null) {
            List<Object> newArr = new ArrayList<>();
            doc.put(index, newArr);
          }
        }
      } else {
        // remove - we're finished with this part
        pathParts.remove(0);
      }
      return Integer.valueOf(index);
    } else if (part.endsWith("]")) {
      String fieldName = part.substring(0, part.indexOf('['));
      Object mongoField = null;
      if (current instanceof Document) {
        mongoField = ((Document) current).get(fieldName);
      }
      if (mongoField == null) {
        // create this field
        List<Object> newField = new ArrayList<>();
        if (current instanceof Document) {
          ((Document) current).put(fieldName, newField);
        }
      } else {
        // check type - should be an array
        if (!(mongoField instanceof List)) {
          throw new HopException(
              BaseMessages.getString(
                  PKG, "MongoDbOutput.Messages.Error.FieldExistsButIsntAnArray", part));
        }
      }
      part = part.substring(part.indexOf('['));
      pathParts.set(0, part);

      return fieldName;
    }

    // otherwise this path part is a record (object) or possibly a leaf (if we
    // are not using the incoming Hop field name as the mongo field name)
    Object mongoField = null;
    if (current instanceof Document) {
      mongoField = ((Document) current).get(part);
    }
    if (mongoField == null) {
      if (incomingAsFieldName || pathParts.size() > 1) {
        // create this field
        Document newField = new Document();
        if (current instanceof Document) {
          ((Document) current).put(part, newField);
        }
      }
    } else {
      // check type = should be a record (object)
      if (!(mongoField instanceof Document) && pathParts.size() > 1) {
        throw new HopException(
            BaseMessages.getString(
                PKG, "MongoDbOutput.Messages.Error.FieldExistsButIsntARecord", part));
      }
    }
    pathParts.remove(0);
    return part;
  }

  /**
   * Determines the top level structure of the outgoing Mongo document from the user-specified field
   * paths. This can be either RECORD ( for a top level structure that is an object), ARRAY or
   * INCONSISTENT (if the user has some field paths that start with an array and some that start
   * with an object).
   *
   * @param fieldDefs the list of document field paths
   * @param vars environment variables
   * @return the top level structure
   */
  protected static MongoTopLevel checkTopLevelConsistency(
      List<MongoDbOutputMeta.MongoField> fieldDefs, IVariables vars) throws HopException {

    if (Utils.isEmpty(fieldDefs)) {
      throw new HopException(
          BaseMessages.getString(PKG, "MongoDbOutput.Messages.Error.NoMongoPathsDefined"));
    }

    int numRecords = 0;
    int numArrays = 0;

    for (MongoDbOutputMeta.MongoField field : fieldDefs) {
      String mongoPath = vars.resolve(field.mongoDocPath);

      if (StringUtils.isEmpty(mongoPath)) {
        numRecords++;
      } else if (mongoPath.startsWith("[")) {
        numArrays++;
      } else {
        numRecords++;
      }
    }

    if (numRecords < fieldDefs.size() && numArrays < fieldDefs.size()) {
      return MongoTopLevel.INCONSISTENT;
    }

    if (numRecords > 0) {
      return MongoTopLevel.RECORD;
    }

    return MongoTopLevel.ARRAY;
  }
}
