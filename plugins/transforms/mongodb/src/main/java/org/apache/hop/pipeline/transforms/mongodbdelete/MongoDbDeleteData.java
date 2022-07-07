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

package org.apache.hop.pipeline.transforms.mongodbdelete;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.mongo.metadata.MongoDbConnection;
import org.apache.hop.mongo.wrapper.MongoClientWrapper;
import org.apache.hop.mongo.wrapper.collection.MongoCollectionWrapper;
import org.apache.hop.mongo.wrapper.cursor.MongoCursorWrapper;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Data class for the MongoDbDelete step
 *
 * @author Maas Dianto (maas.dianto@gmail.com)
 */
public class MongoDbDeleteData extends BaseTransformData implements ITransformData {

    private static Class<?> PKG = MongoDbDeleteMeta.class;
    public static final int MONGO_DEFAULT_PORT = 27017;
    public IRowMeta outputRowMeta;
    public MongoDbConnection connection;
    public MongoClientWrapper clientWrapper;
    // public DB db;
//    public MongoDeleteCollectionWrapper collection;
    public MongoCollectionWrapper collection;
    /** cursor for a standard query */
    public MongoCursorWrapper cursor;
    protected List<MongoDbDeleteField> m_userFields;

    /**
     * Initialize all the paths by locating the index for their field name in the outgoing row structure.
     *
     * @throws HopException
     */
    public void init(IVariables vars) throws HopException {
        if (m_userFields != null) {
            for (MongoDbDeleteField f : m_userFields) {
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
     * @param clientWrapper
     *                      the connection to use
     */
    public void setConnection(MongoClientWrapper clientWrapper) {
        this.clientWrapper = clientWrapper;
    }

    /**
     * Create a collection in the current database
     *
     * @param collectionName
     *                       the name of the collection to create
     * @throws Exception
     * if a problem occurs
     */
    public void createCollection(String db, String collectionName) throws Exception {
        if (clientWrapper == null) {
            throw new Exception(BaseMessages.getString(PKG, "MongoDbDelete.ErrorMessage.NoDatabaseSet"));
        }

        clientWrapper.createCollection(db, collectionName);
    }

    /**
     * Set the collection to use
     *
     * @param col
     *            the collection to use
     */
    public void setCollection(MongoCollectionWrapper col) {
        collection = col;
    }

    /**
     * Get the collection in use
     *
     * @return the collection in use
     */
    public MongoCollectionWrapper getCollection() {
        return collection;
    }

    /**
     * Set the output row format
     *
     * @param outM
     *             the output row format
     */
    public void setOutputRowMeta(IRowMeta outM) {
        outputRowMeta = outM;
    }

    /**
     * Get the output row format
     *
     * @return the output row format
     */
    public IRowMeta getOutputRowMeta() {
        return outputRowMeta;
    }

    /**
     * Set the field paths to use for creating the document structure
     *
     * @param fields
     *               the field paths to use
     */
    public void setMongoFields(List<MongoDbDeleteField> fields) {
        // copy this list
        m_userFields = new ArrayList<MongoDbDeleteField>();

        for (MongoDbDeleteField f : fields) {
            m_userFields.add(f.copy());
        }
    }

    public static DBObject getQueryObject(List<MongoDbDeleteField> fieldDefs, IRowMeta inputMeta,
            Object[] row, IVariables vars) throws HopException {

        DBObject query = new BasicDBObject();

        boolean haveMatchFields = false;
        boolean hasNonNullMatchValues = false;

        for (MongoDbDeleteField field : fieldDefs) {
            haveMatchFields = true;

            hasNonNullMatchValues = true;

            String mongoPath = field.mongoDocPath;
            String path = vars.resolve(mongoPath);
            boolean hasPath = !StringUtil.isEmpty(path);

            if (!hasPath) {
                throw new HopException(BaseMessages.getString(PKG, "MongoDbDelete.ErrorMessage.NoMongoPathsDefined"));
            }

            // post process arrays to fit the dot notation (if not already done
            // by the user)
            if (path.indexOf('[') > 0) {
                path = path.replace("[", ".").replace("]", "");
            }

            if (Comparator.EQUAL.getValue().equals(field.comparator)) {
                String field1 = vars.resolve(field.incomingField1);
                int index = inputMeta.indexOfValue(field1);
                IValueMeta vm = inputMeta.getValueMeta(index);

                // ignore null fields
                if (vm.isNull(row[index])) {
                    continue;
                }

                setMongoValueFromKettleValue(query, path, vm, row[index]);
            } else if (Comparator.NOT_EQUAL.getValue().equals(field.comparator)) {
                String field1 = vars.resolve(field.incomingField1);
                int index = inputMeta.indexOfValue(field1);
                IValueMeta vm = inputMeta.getValueMeta(index);

                // ignore null fields
                if (vm.isNull(row[index])) {
                    continue;
                }
                DBObject notEqual = new BasicDBObject();
                setMongoValueFromKettleValue(notEqual, "$ne", vm, row[index]);
                query.put(path.toString(), notEqual);
            } else if (Comparator.GREATER_THAN.getValue().equals(field.comparator)) {
                String field1 = vars.resolve(field.incomingField1);
                int index = inputMeta.indexOfValue(field1);
                IValueMeta vm = inputMeta.getValueMeta(index);

                // ignore null fields
                if (vm.isNull(row[index])) {
                    continue;
                }
                DBObject greaterThan = new BasicDBObject();
                setMongoValueFromKettleValue(greaterThan, "$gt", vm, row[index]);
                query.put(path.toString(), greaterThan);

            } else if(Comparator.GREATER_THAN_EQUAL.getValue().equals(field.comparator)) {
                String field1 = vars.resolve(field.incomingField1);
                int index = inputMeta.indexOfValue(field1);
                IValueMeta vm = inputMeta.getValueMeta(index);

                // ignore null fields
                if (vm.isNull(row[index])) {
                    continue;
                }
                DBObject greaterThanEqual = new BasicDBObject();
                setMongoValueFromKettleValue(greaterThanEqual, "$gte", vm, row[index]);
                query.put(path.toString(), greaterThanEqual);
            } else if (Comparator.LESS_THAN.getValue().equals(field.comparator)) {
                String field1 = vars.resolve(field.incomingField1);
                int index = inputMeta.indexOfValue(field1);
                IValueMeta vm = inputMeta.getValueMeta(index);

                // ignore null fields
                if (vm.isNull(row[index])) {
                    continue;
                }
                DBObject lessThan = new BasicDBObject();
                setMongoValueFromKettleValue(lessThan, "$lt", vm, row[index]);
                query.put(path.toString(), lessThan);
            } else if (Comparator.LESS_THAN_EQUAL.getValue().equals(field.comparator)) {
                String field1 = vars.resolve(field.incomingField1);
                int index = inputMeta.indexOfValue(field1);
                IValueMeta vm = inputMeta.getValueMeta(index);

                // ignore null fields
                if (vm.isNull(row[index])) {
                    continue;
                }
                DBObject lessThanEqual = new BasicDBObject();
                setMongoValueFromKettleValue(lessThanEqual, "$lte", vm, row[index]);
                query.put(path.toString(), lessThanEqual);
            } else if (Comparator.BETWEEN.getValue().equals(field.comparator)) {

                if (StringUtil.isEmpty(field.incomingField1) || StringUtil.isEmpty(field.incomingField2)) {
                    throw new HopException(BaseMessages.getString(PKG, "MongoDbDelete.ErrorMessage.BetweenTwoFieldsRequired"));
                }

                String field1 = vars.resolve(field.incomingField1);
                int index1 = inputMeta.indexOfValue(field1);
                IValueMeta vm1 = inputMeta.getValueMeta(index1);

                String field2 = vars.resolve(field.incomingField2);
                int index2 = inputMeta.indexOfValue(field2);
                IValueMeta vm2 = inputMeta.getValueMeta(index2);

                // ignore null fields
                if (vm1.isNull(row[index1]) && vm2.isNull(row[index2])) {
                    continue;
                }

                BasicDBObject between = new BasicDBObject();
                setMongoValueFromKettleValue(between, "$gt", vm1, row[index1]);
                setMongoValueFromKettleValue(between, "$lt", vm2, row[index2]);
                query.put(path.toString(), between);

            } else if (Comparator.IS_NULL.getValue().equals(field.comparator)) {
                BasicDBObject exist = new BasicDBObject();
                exist.put("$exists", false);
                query.put(path.toString(), exist);
            } else if (Comparator.IS_NOT_NULL.getValue().equals(field.comparator)) {
                BasicDBObject exist = new BasicDBObject();
                exist.put("$exists", true);
                query.put(path.toString(), exist);
            } else {
                throw new HopException(BaseMessages.getString(PKG, "MongoDbDelete.ErrorMessage.ComparatorNotSupported", new String[]{field.comparator}));
            }

        }

        if (!haveMatchFields) {
            throw new HopException(BaseMessages.getString(PKG,
                    "MongoDbDelete.ErrorMessage.NoFieldsToDeleteSpecifiedForMatch"));
        }

        if (!hasNonNullMatchValues) {
            return null;
        }

        return query;
    }

    private static boolean setMongoValueFromKettleValue(DBObject mongoObject, Object lookup,
                                                        IValueMeta kettleType, Object kettleValue) throws HopValueException {
        if (kettleType.isNull(kettleValue)) {
            return false; // don't insert nulls!
        }

        if (kettleType.isString()) {
            String val = kettleType.getString(kettleValue);
            mongoObject.put(lookup.toString(), val);
            return true;
        }
        if (kettleType.isBoolean()) {
            Boolean val = kettleType.getBoolean(kettleValue);
            mongoObject.put(lookup.toString(), val);
            return true;
        }
        if (kettleType.isInteger()) {
            Long val = kettleType.getInteger(kettleValue);
            mongoObject.put(lookup.toString(), val.longValue());
            return true;
        }
        if (kettleType.isDate()) {
            Date val = kettleType.getDate(kettleValue);
            mongoObject.put(lookup.toString(), val);
            return true;
        }
        if (kettleType.isNumber()) {
            Double val = kettleType.getNumber(kettleValue);
            mongoObject.put(lookup.toString(), val.doubleValue());
            return true;
        }
        if (kettleType.isBigNumber()) {
            // use string value - user can use Kettle to convert back
            String val = kettleType.getString(kettleValue);
            mongoObject.put(lookup.toString(), val);
            return true;
        }
        if (kettleType.isBinary()) {
            byte[] val = kettleType.getBinary(kettleValue);
            mongoObject.put(lookup.toString(), val);
            return true;
        }
        if (kettleType.isSerializableType()) {
            throw new HopValueException(BaseMessages.getString(PKG,
                    "MongoDbDelete.ErrorMessage.CantStoreHopSerializableVals"));
        }

        return false;
    }


    /**
   * Cleanses a string path by ensuring that any variables names present in the path do not contain "."s (replaces any
   * dots with underscores).
   *
   * @param path
   *          the path to cleanse
   * @return the cleansed path
   */
  public static String cleansePath( String path ) {
    // look for variables and convert any "." to "_"

    int index = path.indexOf( "${" ); //$NON-NLS-1$

    int endIndex = 0;
    String tempStr = path;
    while ( index >= 0 ) {
      index += 2;
      endIndex += tempStr.indexOf( "}" ); //$NON-NLS-1$
      if ( endIndex > 0 && endIndex > index + 1 ) {
        String key = path.substring( index, endIndex );

        String cleanKey = key.replace( '.', '_' );
        path = path.replace( key, cleanKey );
      } else {
        break;
      }

      if ( endIndex + 1 < path.length() ) {
        tempStr = path.substring( endIndex + 1, path.length() );
      } else {
        break;
      }

      index = tempStr.indexOf( "${" ); //$NON-NLS-1$

      if ( index > 0 ) {
        index += endIndex;
      }
    }

    return path;
  }
}
