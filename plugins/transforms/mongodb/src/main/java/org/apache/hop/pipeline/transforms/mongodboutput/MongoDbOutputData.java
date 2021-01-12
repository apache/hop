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

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.wrapper.MongoClientWrapper;
import org.apache.hop.mongo.wrapper.collection.MongoCollectionWrapper;
import org.apache.hop.mongo.wrapper.cursor.MongoCursorWrapper;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Data class for the MongoDbOutput transform */
public class MongoDbOutputData extends BaseTransformData implements ITransformData {

  private static Class<?> PKG = MongoDbOutputMeta.class; // For Translator

  public static final int MONGO_DEFAULT_PORT = 27017;

  public static final String LOCAL_DB = "local";
  public static final String REPL_SET_COLLECTION = "system.replset";
  public static final String REPL_SET_SETTINGS = "settings";
  public static final String REPL_SET_LAST_ERROR_MODES = "getLastErrorModes";

  /** Enum for the type of the top level object of the document structure */
  public enum MongoTopLevel {
    RECORD,
    ARRAY,
    INCONSISTENT;
  }

  /** The output row format */
  protected IRowMeta m_outputRowMeta;

  /** Main entry point to the mongo driver */
  protected MongoClientWrapper clientWrapper;

  /** Collection object for the user-specified document collection */
  protected MongoCollectionWrapper m_collection;

  protected List<MongoDbOutputMeta.MongoField> m_userFields;

  /**
   * Map for grouping together $set operations that involve setting complex array-based objects. Key
   * = dot path to array name; value = DBObject specifying array to set to
   */
  protected Map<String, List<MongoDbOutputMeta.MongoField>> m_setComplexArrays =
    new HashMap<>();

  /**
   * Map for grouping together $push operations that involve complex objects. Use [] to indicate the
   * start of the complex object to push. Key - dot path to the array name to push to; value -
   * DBObject specifying the complex object to push
   */
  protected Map<String, List<MongoDbOutputMeta.MongoField>> m_pushComplexStructures =
    new HashMap<>();

  /** all other modifier updates that involve primitive leaf fields */
  protected Map<String, Object[]> m_primitiveLeafModifiers = new LinkedHashMap<>();

  /**
   * True if the list of paths specifies an incoming Hop field that contains a JSON doc that is
   * intended to be inserted as is (i.e. not added to a field in the document structure defined by
   * the mongo paths)
   */
  protected boolean m_hasTopLevelJSONDocInsert = false;

  public static boolean scanForInsertTopLevelJSONDoc(List<MongoDbOutputMeta.MongoField> fieldDefs)
      throws HopException {

    int countNonMatchFields = 0;
    boolean hasTopLevelJSONDocInsert = false;

    for (MongoDbOutputMeta.MongoField f : fieldDefs) {
      if (f.m_JSON
          && !f.m_updateMatchField
          && StringUtils.isEmpty(f.m_mongoDocPath)
          && !f.m_useIncomingFieldNameAsMongoFieldName) {
        hasTopLevelJSONDocInsert = true;
      }

      if (!f.m_updateMatchField) {
        countNonMatchFields++;
      }
    }

    // Invalid path specification would be one where there is a top level
    // JSON doc to be inserted (as is) but other field paths have been defined.
    // TODO we could allow exactly one top level JSON doc and then have other
    // paths punch data into this document I guess (which is kind of the
    // opposite of the current functionality that allows the document to be
    // defined from the specified paths and then allows non top-level JSON docs
    // to punched into this structure)

    if (hasTopLevelJSONDocInsert && countNonMatchFields > 1) {
      // TODO
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
    m_userFields = new ArrayList<>();

    for (MongoDbOutputMeta.MongoField f : fields) {
      m_userFields.add(f.copy());
    }
  }

  /** Gets the field paths to use for creating the document structure */
  public List<MongoDbOutputMeta.MongoField> getMongoFields() {
    return m_userFields;
  }

  /**
   * Initialize field paths
   *
   * @param vars variables to use
   * @throws HopException if a problem occurs
   */
  public void init(IVariables vars) throws HopException {
    if (m_userFields != null) {
      for (MongoDbOutputMeta.MongoField f : m_userFields) {
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
   * Set the collection to use
   *
   * @param col the collection to use
   */
  public void setCollection(MongoCollectionWrapper col) {
    m_collection = col;
  }

  /**
   * Get the collection in use
   *
   * @return the collection in use
   */
  public MongoCollectionWrapper getCollection() {
    return m_collection;
  }

  /**
   * Set the output row format
   *
   * @param outM the output row format
   */
  public void setOutputRowMeta(IRowMeta outM) {
    m_outputRowMeta = outM;
  }

  /**
   * Get the output row format
   *
   * @return the output row format
   */
  public IRowMeta getOutputRowMeta() {
    return m_outputRowMeta;
  }

  /**
   * Apply the supplied index operations to the collection. Indexes can be defined on one or more
   * fields in the document. Operation is either create or drop.
   *
   * @param indexes a list of index operations
   * @param log the logging object
   * @param truncate true if the collection was truncated in the current execution - in this case
   *     drop operations are not necessary
   * @throws MongoException if something goes wrong
   * @throws HopException
   */
  public void applyIndexes(
      List<MongoDbOutputMeta.MongoIndex> indexes, ILogChannel log, boolean truncate)
      throws MongoException, HopException, MongoDbException {

    for (MongoDbOutputMeta.MongoIndex index : indexes) {
      String[] indexParts = index.m_pathToFields.split(",");
      BasicDBObject mongoIndex = new BasicDBObject();
      for (String indexKey : indexParts) {
        String[] nameAndDirection = indexKey.split(":");
        int direction = 1;
        if (nameAndDirection.length == 2) {
          direction = Integer.parseInt(nameAndDirection[1].trim());
        }
        String name = nameAndDirection[0];

        // strip off brackets to get actual object name if terminal object
        // is an array
        if (name.indexOf('[') > 0) {
          name = name.substring(name.indexOf('[') + 1, name.length());
        }

        mongoIndex.put(name, direction);
      }

      if (index.m_drop) {
        if (truncate) {
          log.logBasic(
              BaseMessages.getString(PKG, "MongoDbOutput.Messages.TruncateBeforeInsert", index));
        } else {
          m_collection.dropIndex(mongoIndex);
        }
        log.logBasic(BaseMessages.getString(PKG, "MongoDbOutput.Messages.DropIndex", index));
      } else {
        BasicDBObject options = new BasicDBObject();

        // create indexes in the background
        options.put("background", true);
        options.put("unique", index.m_unique);
        options.put("sparse", index.m_sparse);
        m_collection.createIndex(mongoIndex, options);
        log.logBasic(BaseMessages.getString(PKG, "MongoDbOutput.Messages.CreateIndex", index));
      }
    }
  }

  /**
   * Get an object that encapsulates the fields and modifier operations to use for a modifier
   * update.
   *
   * <p>NOTE: that with modifier upserts the query conditions get created if the record does not
   * exist (i.e. insert). This is different than straight non- modifier upsert where the query
   * conditions just locate a matching record (if any) and then a complete object replacement is
   * done. So for standard upsert it is necessary to duplicate the query condition paths in order
   * for these fields to be in the object that is inserted/updated.
   *
   * <p>This also means that certain modifier upserts are not possible in the case of insert. E.g.
   * here we are wanting to test if the field "f1" in record "rec1" in the first element of array
   * "two" is set to "george". If so, then we want to push a new record to the end of the array;
   * otherwise create a new document with the array containing just the new record:
   *
   * <p>
   *
   * <p>
   *
   * <pre>
   * db.collection.update({ "one.two.0.rec1.f1" : "george"},
   * { "$push" : { "one.two" : { "rec1" : { "f1" : "bob" , "f2" : "fred"}}}},
   * true)
   * </pre>
   *
   * <p>This does not work and results in a "Cannot apply $push/$pushAll modifier to non-array"
   * error if there is no match (i.e. insert condition). This is because the query conditions get
   * created as well as the modifier opps and, furthermore, they get created first. Since mongo
   * doesn't know whether ".0." indicates an array index or a field name it defaults to creating a
   * field with name "0". This means that "one.two" gets created as a record (not an array) before
   * the $push operation is executed. Hence the error.
   *
   * @param fieldDefs the list of document field definitions
   * @param inputMeta the input row format
   * @param row the current incoming row
   * @param vars environment variables
   * @param topLevelStructure the top level structure of the document
   * @return a DBObject encapsulating the update to make
   * @throws HopException if a problem occurs
   */
  protected DBObject getModifierUpdateObject(
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
    BasicDBObject updateObject = new BasicDBObject();

    m_setComplexArrays.clear();
    m_primitiveLeafModifiers.clear();
    m_pushComplexStructures.clear();

    // do we need to determine whether this will be an insert or an update?
    boolean checkForMatch = false;
    for (MongoDbOutputMeta.MongoField field : fieldDefs) {
      if (!field.m_updateMatchField
          && (field.m_modifierOperationApplyPolicy.equals("Insert")
              || field.m_modifierOperationApplyPolicy.equals("Update"))) {
        checkForMatch = true;
        break;
      }
    }

    boolean isUpdate = false;
    if (checkForMatch) {
      DBObject query = getQueryObject(fieldDefs, inputMeta, row, vars, topLevelStructure);

      MongoCursorWrapper cursor = getCollection().find(query).limit(1);
      if (cursor.hasNext()) {
        isUpdate = true;
      }
    }

    for (MongoDbOutputMeta.MongoField field : fieldDefs) {
      // skip query match fields
      if (field.m_updateMatchField) {
        continue;
      }

      String modifierUpdateOpp = field.environUpdateModifierOperation;

      if (!StringUtils.isEmpty(modifierUpdateOpp) && !modifierUpdateOpp.equals("N/A")) {
        if (checkForMatch) {
          if (isUpdate && field.m_modifierOperationApplyPolicy.equals("Insert")) {
            continue; // don't apply this opp
          }

          if (!isUpdate && field.m_modifierOperationApplyPolicy.equals("Update")) {
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
              && modifierUpdateOpp.equals("$push")
              && !field.m_useIncomingFieldNameAsMongoFieldName) {

            // strip off the brackets as push appends to the end of the named
            // array
            path = path.substring(0, path.indexOf('['));
          }

          boolean hasPath = !StringUtils.isEmpty(path);
          path +=
              ((field.m_useIncomingFieldNameAsMongoFieldName)
                  ? (hasPath ? "." + incomingFieldName : incomingFieldName)
                  : "");

          // check for array creation
          if (modifierUpdateOpp.equals("$set")
              && path.indexOf('[') > 0
              && path.indexOf(mongoOperatorUpdateAllArray) < 0) {
            String arrayPath = path.substring(0, path.indexOf('['));
            String arraySpec = path.substring(path.indexOf('['), path.length());
            MongoDbOutputMeta.MongoField a = new MongoDbOutputMeta.MongoField();
            a.m_incomingFieldName = field.m_incomingFieldName;
            a.environUpdatedFieldName = field.environUpdatedFieldName;
            a.m_mongoDocPath = arraySpec;
            a.environUpdateMongoDocPath = arraySpec;
            // incoming field name has already been appended (if necessary)
            a.m_useIncomingFieldNameAsMongoFieldName = false;
            a.m_JSON = field.m_JSON;
            a.init(vars, false);
            List<MongoDbOutputMeta.MongoField> fds = m_setComplexArrays.get(arrayPath);
            if (fds == null) {
              fds = new ArrayList<>();
              m_setComplexArrays.put(arrayPath, fds);
            }
            fds.add(a);
          } else if (modifierUpdateOpp.equals("$push")
              && path.indexOf('[') > 0
              && path.indexOf(mongoOperatorUpdateAllArray) < 0) {
            // we ignore any index that might have been specified as $push
            // always appends to the end of the array.
            String arrayPath = path.substring(0, path.indexOf('['));
            String structureToPush = path.substring(path.indexOf(']') + 1, path.length());

            // check to see if we're pushing a record at this point in the path
            // or another array...
            if (structureToPush.charAt(0) == '.') {
              // skip the dot
              structureToPush = structureToPush.substring(1, structureToPush.length());
            }

            MongoDbOutputMeta.MongoField a = new MongoDbOutputMeta.MongoField();
            a.m_incomingFieldName = field.m_incomingFieldName;
            a.environUpdatedFieldName = field.environUpdatedFieldName;
            a.m_mongoDocPath = structureToPush;
            a.environUpdateMongoDocPath = structureToPush;
            // incoming field name has already been appended (if necessary)
            a.m_useIncomingFieldNameAsMongoFieldName = false;
            a.m_JSON = field.m_JSON;
            a.init(vars, false);
            List<MongoDbOutputMeta.MongoField> fds = m_pushComplexStructures.get(arrayPath);
            if (fds == null) {
              fds = new ArrayList<>();
              m_pushComplexStructures.put(arrayPath, fds);
            }
            fds.add(a);
          } else {
            Object[] params = new Object[4];
            params[0] = modifierUpdateOpp;
            params[1] = index;
            params[2] = field.m_JSON;
            params[3] = field.insertNull;
            m_primitiveLeafModifiers.put(path, params);
          }
        }
      }
    }

    // do the array $sets
    for (String path : m_setComplexArrays.keySet()) {
      List<MongoDbOutputMeta.MongoField> fds = m_setComplexArrays.get(path);
      DBObject valueToSet = hopRowToMongo(fds, inputMeta, row, MongoTopLevel.ARRAY, false);

      DBObject fieldsToUpdateWithValues;

      if (updateObject.get("$set") != null) {
        // if we have some field(s) already associated with this type of
        // modifier
        // operation then just add to them
        fieldsToUpdateWithValues = (DBObject) updateObject.get("$set");
      } else {
        // otherwise create a new DBObject for this modifier operation
        fieldsToUpdateWithValues = new BasicDBObject();
      }

      fieldsToUpdateWithValues.put(path, valueToSet);
      updateObject.put("$set", fieldsToUpdateWithValues);
    }

    // now do the $push complex
    for (String path : m_pushComplexStructures.keySet()) {
      List<MongoDbOutputMeta.MongoField> fds = m_pushComplexStructures.get(path);

      // check our top-level structure
      MongoTopLevel topLevel = MongoTopLevel.RECORD;
      if (fds.get(0).m_mongoDocPath.charAt(0) == '[') {
        topLevel = MongoTopLevel.RECORD;
      }

      DBObject valueToSet = hopRowToMongo(fds, inputMeta, row, topLevel, false);

      DBObject fieldsToUpdateWithValues = null;

      if (updateObject.get("$push") != null) {
        // if we have some field(s) already associated with this type of
        // modifier
        // operation then just add to them
        fieldsToUpdateWithValues = (DBObject) updateObject.get("$push");
      } else {
        // otherwise create a new DBObject for this modifier operation
        fieldsToUpdateWithValues = new BasicDBObject();
      }

      fieldsToUpdateWithValues.put(path, valueToSet);
      updateObject.put("$push", fieldsToUpdateWithValues);
    }

    // do the modifiers that involve primitive field values
    for (Map.Entry<String, Object[]> entry : m_primitiveLeafModifiers.entrySet()) {
      String path = entry.getKey();
      Object[] params = entry.getValue();
      String modifierUpdateOpp = params[0].toString();
      int index = (Integer) params[1];
      boolean isJSON = (Boolean) params[2];
      boolean allowNull = (Boolean) params[3];
      IValueMeta vm = inputMeta.getValueMeta(index);

      DBObject fieldsToUpdateWithValues = null;

      if (updateObject.get(modifierUpdateOpp) != null) {
        // if we have some field(s) already associated with this type of
        // modifier
        // operation then just add to them
        fieldsToUpdateWithValues = (DBObject) updateObject.get(modifierUpdateOpp);
      } else {
        // otherwise create a new DBObject for this modifier operation
        fieldsToUpdateWithValues = new BasicDBObject();
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
   * @return a DBObject encapsulating the query
   * @throws HopException if something goes wrong
   */
  protected static DBObject getQueryObject(
      List<MongoDbOutputMeta.MongoField> fieldDefs,
      IRowMeta inputMeta,
      Object[] row,
      IVariables vars,
      MongoTopLevel topLevelStructure)
      throws HopException {
    BasicDBObject query = new BasicDBObject();

    boolean haveMatchFields = false;
    boolean hasNonNullMatchValues = false;

    for (MongoDbOutputMeta.MongoField field : fieldDefs) {
      if (field.m_updateMatchField) {
        haveMatchFields = true;
        String incomingFieldName = field.environUpdatedFieldName;
        int index = inputMeta.indexOfValue(incomingFieldName);
        IValueMeta vm = inputMeta.getValueMeta(index);

        // ignore null fields is not prohibited
        if (vm.isNull(row[index]) && !field.insertNull) {
          continue;
        }

        hasNonNullMatchValues = true;

        if (field.m_JSON
            && StringUtils.isEmpty(field.m_mongoDocPath)
            && !field.m_useIncomingFieldNameAsMongoFieldName) {
          // We have a query based on a complete incoming JSON doc -
          // i.e. no field processing necessary

          if (vm.isString()) {
            String val = vm.getString(row[index]);
            query = (BasicDBObject) JSON.parse(val);
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
            (field.environUpdateMongoDocPath != null)
                ? field.environUpdateMongoDocPath
                : ""; //

        boolean hasPath = !StringUtils.isEmpty(path);
        path +=
            ((field.m_useIncomingFieldNameAsMongoFieldName)
                ? (hasPath
                    ? "." //
                        + incomingFieldName
                    : incomingFieldName)
                : ""); //

        // post process arrays to fit the dot notation (if not already done
        // by the user)
        if (path.indexOf('[') > 0) {
          path =
              path.replace("[", ".").replace("]", ""); //   // //
        }

        setMongoValueFromHopValue(query, path, vm, row[index], field.m_JSON, field.insertNull);
      }
    }

    if (!haveMatchFields) {
      throw new HopException(
          BaseMessages.getString(
              PKG,
              "MongoDbOutput.Messages.Error.NoFieldsToUpdateSpecifiedForMatch")); //
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
   * @return a DBObject encapsulating the document to insert/upsert or null if there are no non-null
   *     incoming fields
   * @throws HopException if a problem occurs
   */
  protected static DBObject hopRowToMongo(
      List<MongoDbOutputMeta.MongoField> fieldDefs,
      IRowMeta inputMeta,
      Object[] row,
      MongoTopLevel topLevelStructure,
      boolean hasTopLevelJSONDocInsert)
      throws HopException {

    // the easy case
    if (hasTopLevelJSONDocInsert) {
      for (MongoDbOutputMeta.MongoField f : fieldDefs) {
        if (f.m_JSON
            && StringUtils.isEmpty(f.m_mongoDocPath)
            && !f.m_useIncomingFieldNameAsMongoFieldName) {
          String incomingFieldName = f.environUpdatedFieldName;
          int index = inputMeta.indexOfValue(incomingFieldName);
          IValueMeta vm = inputMeta.getValueMeta(index);
          if (!vm.isNull(row[index])) {
            String jsonDoc = vm.getString(row[index]);
            DBObject docToInsert = (DBObject) JSON.parse(jsonDoc);
            return docToInsert;
          } else {
            return null;
          }
        }
      }
    }

    DBObject root = null;
    if (topLevelStructure == MongoTopLevel.RECORD) {
      root = new BasicDBObject();
    } else if (topLevelStructure == MongoTopLevel.ARRAY) {
      root = new BasicDBList();
    }

    boolean haveNonNullFields = false;
    for (MongoDbOutputMeta.MongoField field : fieldDefs) {
      DBObject current = root;

      field.reset();
      List<String> pathParts = field.m_tempPathList;
      String incomingFieldName = field.environUpdatedFieldName;
      int index = inputMeta.indexOfValue(incomingFieldName);
      IValueMeta vm = inputMeta.getValueMeta(index);

      Object lookup =
          getPathElementName(pathParts, current, field.m_useIncomingFieldNameAsMongoFieldName);
      do {
        // array?
        if (lookup != null && lookup instanceof Integer) {
          BasicDBList temp = (BasicDBList) current;
          if (temp.get(lookup.toString()) == null) {
            if (pathParts.size() == 0 && !field.m_useIncomingFieldNameAsMongoFieldName) {
              // leaf - primitive element of the array (unless Hop field
              // value is JSON)
              boolean res =
                  setMongoValueFromHopValue(
                      temp, lookup, vm, row[index], field.m_JSON, field.insertNull);
              haveNonNullFields = (haveNonNullFields || res);
            } else {
              // must be a record here (since multi-dimensional array creation
              // is handled
              // in getPathElementName())

              // need to create this record/object
              BasicDBObject newRec = new BasicDBObject();
              temp.put(lookup.toString(), newRec);
              current = newRec;

              // end of the path?
              if (pathParts.size() == 0) {
                if (field.m_useIncomingFieldNameAsMongoFieldName) {
                  boolean res =
                      setMongoValueFromHopValue(
                          current,
                          incomingFieldName,
                          vm,
                          row[index],
                          field.m_JSON,
                          field.insertNull);
                  haveNonNullFields = (haveNonNullFields || res);
                } else {
                  throw new HopException(
                      BaseMessages.getString(
                          PKG,
                          "MongoDbOutput.Messages.Error.NoFieldNameSpecifiedForPath")); //
                }
              }
            }
          } else {
            // existing element of the array
            current = (DBObject) temp.get(lookup.toString());

            // no more path parts so we must be setting a field in an array
            // element
            // that is a record
            if (pathParts == null || pathParts.size() == 0) {
              if (current instanceof BasicDBObject) {
                if (field.m_useIncomingFieldNameAsMongoFieldName) {
                  boolean res =
                      setMongoValueFromHopValue(
                          current,
                          incomingFieldName,
                          vm,
                          row[index],
                          field.m_JSON,
                          field.insertNull);
                  haveNonNullFields = (haveNonNullFields || res);
                } else {
                  throw new HopException(
                      BaseMessages.getString(
                          PKG,
                          "MongoDbOutput.Messages.Error.NoFieldNameSpecifiedForPath")); //
                }
              }
            }
          }
        } else {
          // record/object
          if (lookup == null && pathParts.size() == 0) {
            if (field.m_useIncomingFieldNameAsMongoFieldName) {
              boolean res =
                  setMongoValueFromHopValue(
                      current, incomingFieldName, vm, row[index], field.m_JSON, field.insertNull);
              haveNonNullFields = (haveNonNullFields || res);
            } else {
              throw new HopException(
                  BaseMessages.getString(
                      PKG,
                      "MongoDbOutput.Messages.Error.NoFieldNameSpecifiedForPath")); //
            }
          } else {
            if (pathParts.size() == 0) {
              if (!field.m_useIncomingFieldNameAsMongoFieldName) {
                boolean res =
                    setMongoValueFromHopValue(
                        current, lookup.toString(), vm, row[index], field.m_JSON, field.insertNull);
                haveNonNullFields = (haveNonNullFields || res);
              } else {
                current = (DBObject) current.get(lookup.toString());
                boolean res =
                    setMongoValueFromHopValue(
                        current, incomingFieldName, vm, row[index], field.m_JSON, field.insertNull);
                haveNonNullFields = (haveNonNullFields || res);
              }
            } else {
              current = (DBObject) current.get(lookup.toString());
            }
          }
        }

        lookup =
            getPathElementName(pathParts, current, field.m_useIncomingFieldNameAsMongoFieldName);
      } while (lookup != null);
    }

    if (!haveNonNullFields) {
      return null; // nothing has been set!
    }

    return root;
  }

  private static boolean setMongoValueFromHopValue(
      DBObject mongoObject,
      Object lookup,
      IValueMeta hopType,
      Object hopValue,
      boolean hopValueIsJSON,
      boolean allowNull)
      throws HopValueException {
    if (hopType.isNull(hopValue)) {
      if (allowNull) {
        mongoObject.put(lookup.toString(), null);
        return true;
      } else {
        return false;
      }
    }

    if (hopType.isString()) {
      String val = hopType.getString(hopValue);
      if (hopValueIsJSON) {
        Object mongoO = JSON.parse(val);
        mongoObject.put(lookup.toString(), mongoO);
      } else {
        mongoObject.put(lookup.toString(), val);
      }
      return true;
    }
    if (hopType.isBoolean()) {
      Boolean val = hopType.getBoolean(hopValue);
      mongoObject.put(lookup.toString(), val);
      return true;
    }
    if (hopType.isInteger()) {
      Long val = hopType.getInteger(hopValue);
      mongoObject.put(lookup.toString(), val.longValue());
      return true;
    }
    if (hopType.isDate()) {
      Date val = hopType.getDate(hopValue);
      mongoObject.put(lookup.toString(), val);
      return true;
    }
    if (hopType.isNumber()) {
      Double val = hopType.getNumber(hopValue);
      mongoObject.put(lookup.toString(), val.doubleValue());
      return true;
    }
    if (hopType.isBigNumber()) {
      // use string value - user can use Hop to convert back
      String val = hopType.getString(hopValue);
      mongoObject.put(lookup.toString(), val);
      return true;
    }
    if (hopType.isBinary()) {
      byte[] val = hopType.getBinary(hopValue);
      mongoObject.put(lookup.toString(), val);
      return true;
    }
    if (hopType.isSerializableType()) {
      throw new HopValueException(
          BaseMessages.getString(
              PKG, "MongoDbOutput.Messages.Error.CantStoreHopSerializableVals")); //
    }

    return false;
  }

  private static Object getPathElementName(
      List<String> pathParts, DBObject current, boolean incomingAsFieldName) throws HopException {

    if (pathParts == null || pathParts.size() == 0) {
      return null;
    }

    String part = pathParts.get(0);
    if (part.startsWith("[")) { //
      String index = part.substring(1, part.indexOf(']')).trim();
      part = part.substring(part.indexOf(']') + 1).trim();
      if (part.length() > 0) {
        // any remaining characters must indicate a multi-dimensional array
        pathParts.set(0, part);

        // does this next array exist?
        if (current.get(index) == null) {
          BasicDBList newArr = new BasicDBList();
          current.put(index, newArr);
        }
      } else {
        // remove - we're finished with this part
        pathParts.remove(0);
      }
      return new Integer(index);
    } else if (part.endsWith("]")) { //
      String fieldName = part.substring(0, part.indexOf('['));
      Object mongoField = current.get(fieldName);
      if (mongoField == null) {
        // create this field
        BasicDBList newField = new BasicDBList();
        current.put(fieldName, newField);
      } else {
        // check type - should be an array
        if (!(mongoField instanceof BasicDBList)) {
          throw new HopException(
              BaseMessages.getString(
                  PKG,
                  "MongoDbOutput.Messages.Error.FieldExistsButIsntAnArray",
                  part)); //
        }
      }
      part = part.substring(part.indexOf('['));
      pathParts.set(0, part);

      return fieldName;
    }

    // otherwise this path part is a record (object) or possibly a leaf (if we
    // are not
    // using the incoming Hop field name as the mongo field name)
    Object mongoField = current.get(part);
    if (mongoField == null) {
      if (incomingAsFieldName || pathParts.size() > 1) {
        // create this field
        BasicDBObject newField = new BasicDBObject();
        current.put(part, newField);
      }
    } else {
      // check type = should be a record (object)
      if (!(mongoField instanceof BasicDBObject) && pathParts.size() > 1) {
        throw new HopException(
            BaseMessages.getString(
                PKG,
                "MongoDbOutput.Messages.Error.FieldExistsButIsntARecord",
                part)); //
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

    if (fieldDefs == null || fieldDefs.size() == 0) {
      throw new HopException(
          BaseMessages.getString(PKG, "MongoDbOutput.Messages.Error.NoMongoPathsDefined"));
    }

    int numRecords = 0;
    int numArrays = 0;

    for (MongoDbOutputMeta.MongoField field : fieldDefs) {
      String mongoPath = vars.resolve(field.m_mongoDocPath);

      if (StringUtils.isEmpty(mongoPath)) {
        numRecords++;
      } else if (mongoPath.startsWith("[")) { //
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
