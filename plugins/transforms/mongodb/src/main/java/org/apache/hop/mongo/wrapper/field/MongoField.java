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

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transforms.mongodbinput.MongoDbInputData;
import org.bson.BsonUndefined;
import org.bson.types.Binary;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MongoField implements Comparable<MongoField> {
  protected static Class<?> PKG = MongoField.class; // For Translator

  /** The name the the field will take in the outputted Hop stream */
  @Injection(name = "FIELD_NAME", group = "FIELDS")
  public String fieldName = "";

  /** The path to the field in the Mongo object */
  @Injection(name = "FIELD_PATH", group = "FIELDS")
  public String fieldPath = "";

  /** The Hop type for this field */
  @Injection(name = "FIELD_TYPE", group = "FIELDS")
  public String hopType = "";

  /** User-defined indexed values for String types */
  public List<String> indexedValues;

  /**
   * Temporary variable to hold the min:max array index info for fields determined when sampling
   * documents for paths/types
   */
  @Injection(name = "FIELD_ARRAY_INDEX", group = "FIELDS")
  public transient String arrayIndexInfo;

  /**
   * Temporary variable to hold the number of times this path was seen when sampling documents to
   * determine paths/types.
   */
  @Injection(name = "FIELD_PERCENTAGE", group = "FIELDS")
  public transient int percentageOfSample = -1;

  /**
   * Temporary variable to hold the num times this path was seen/num sampled documents. Note that
   * numerator might be larger than denominator if this path is encountered multiple times in an
   * array within one document.
   */
  public transient String occurrenceFraction = "";

  public transient Class<?> mongoType;

  /**
   * Temporary variable used to indicate that this path occurs multiple times over the sampled
   * documents and that the types differ. In this case we should default to Hop type String as a
   * catch-all
   */
  @Injection(name = "FIELD_DISPARATE_TYPES", group = "FIELDS")
  public transient boolean disparateTypes;

  /** The index that this field is in the output row structure */
  public int outputIndex;

  private IValueMeta tempValueMeta;

  private List<String> pathParts;
  private List<String> tempParts;

  public MongoField copy() {
    MongoField newF = new MongoField();
    newF.fieldName = fieldName;
    newF.fieldPath = fieldPath;
    newF.hopType = hopType;

    // reference doesn't matter here as this list is read only at runtime
    newF.indexedValues = indexedValues;

    return newF;
  }

  /**
   * Initialize this mongo field
   *
   * @param outputIndex the index for this field in the outgoing row structure.
   * @throws HopException if a problem occurs
   */
  public void init(int outputIndex) throws HopException {
    if (StringUtils.isEmpty(fieldPath)) {
      throw new HopException(
          BaseMessages.getString(PKG, "MongoDbOutput.Messages.MongoField.Error.NoPathSet"));
    }

    if (pathParts != null) {
      return;
    }

    String fieldPath = MongoDbInputData.cleansePath(this.fieldPath);

    String[] temp = fieldPath.split("\\.");
    pathParts = new ArrayList<>();
    for (String part : temp) {
      pathParts.add(part);
    }

    if (pathParts.get(0).equals("$")) {
      pathParts.remove(0); // root record indicator
    } else if (pathParts.get(0).startsWith("$[")) {

      // strip leading $ off of array
      String r = pathParts.get(0).substring(1, pathParts.get(0).length());
      pathParts.set(0, r);
    }

    tempParts = new ArrayList<>();
    tempValueMeta = ValueMetaFactory.createValueMeta(ValueMetaFactory.getIdForValueMeta(hopType));
    this.outputIndex = outputIndex;
  }

  /**
   * Reset this field, ready for processing a new document
   *
   * @param variables variables to use
   */
  public void reset(IVariables variables) {
    // first clear because there may be stuff left over from processing
    // the previous mongo document object (especially if a path exited early
    // due to non-existent field or array index out of bounds)
    tempParts.clear();

    for (String part : pathParts) {
      tempParts.add(variables.resolve(part));
    }
  }

  /**
   * Perform Hop type conversions for the Mongo leaf field value.
   *
   * @param fieldValue the leaf value from the Mongo structure
   * @return an Object of the appropriate Hop type
   * @throws HopException if a problem occurs
   */
  public Object getHopValue(Object fieldValue) throws HopException {

    switch (tempValueMeta.getType()) {
      case IValueMeta.TYPE_BIGNUMBER:
        if (fieldValue instanceof Number) {
          fieldValue = BigDecimal.valueOf(((Number) fieldValue).doubleValue());
        } else if (fieldValue instanceof Date) {
          fieldValue = new BigDecimal(((Date) fieldValue).getTime());
        } else {
          fieldValue = new BigDecimal(fieldValue.toString());
        }
        return tempValueMeta.getBigNumber(fieldValue);
      case IValueMeta.TYPE_BINARY:
        if (fieldValue instanceof Binary) {
          fieldValue = ((Binary) fieldValue).getData();
        } else if (fieldValue instanceof byte[]) {
          // Leave fieldValue alone if it is a byte[], or defensively copy uncommenting
          /*
          byte[] castValue = (byte[]) fieldValue;
          byte[] copyValue = new byte[ castValue.length ];
          System.arraycopy( castValue, 0, copyValue, 0, copyValue.length );
          fieldValue = copyValue;
          */
        } else {
          fieldValue = fieldValue.toString().getBytes();
        }
        return tempValueMeta.getBinary(fieldValue);
      case IValueMeta.TYPE_BOOLEAN:
        if (fieldValue instanceof Number) {
          fieldValue = new Boolean(((Number) fieldValue).intValue() != 0);
        } else if (fieldValue instanceof Date) {
          fieldValue = new Boolean(((Date) fieldValue).getTime() != 0);
        } else if (!(fieldValue instanceof Boolean)) {
          fieldValue =
              new Boolean(
                  fieldValue.toString().equalsIgnoreCase("Y")
                      || fieldValue.toString().equalsIgnoreCase("T")
                      || fieldValue.toString().equalsIgnoreCase("1"));
        }
        return tempValueMeta.getBoolean(fieldValue);
      case IValueMeta.TYPE_DATE:
        if (fieldValue instanceof Number) {
          fieldValue = new Date(((Number) fieldValue).longValue());
        } else if (!(fieldValue instanceof Date)) {
          throw new HopException(
              BaseMessages.getString(
                  PKG, "MongoDbInput.ErrorMessage.DateConversion", fieldValue.toString()));
        }
        return tempValueMeta.getDate(fieldValue);
      case IValueMeta.TYPE_INTEGER:
        if (fieldValue instanceof Number) {
          fieldValue = new Long(((Number) fieldValue).intValue());
        } else if (fieldValue instanceof Binary) {
          byte[] b = ((Binary) fieldValue).getData();
          String s = new String(b);
          fieldValue = new Long(s);
        } else {
          fieldValue = new Long(fieldValue.toString());
        }
        return tempValueMeta.getInteger(fieldValue);
      case IValueMeta.TYPE_NUMBER:
        if (fieldValue instanceof Number) {
          fieldValue = new Double(((Number) fieldValue).doubleValue());
        } else if (fieldValue instanceof Binary) {
          byte[] b = ((Binary) fieldValue).getData();
          String s = new String(b);
          fieldValue = new Double(s);
        } else {
          fieldValue = new Double(fieldValue.toString());
        }
        return tempValueMeta.getNumber(fieldValue);
      case IValueMeta.TYPE_STRING:
        return tempValueMeta.getString(fieldValue);
      default:
        return null;
    }
  }

  /**
   * Convert a mongo record object to a Hop field value (for the field defined by this path)
   *
   * @param mongoObject the record to convert
   * @return the Hop field value
   * @throws HopException if a problem occurs
   */
  public Object convertToHopValue(BasicDBObject mongoObject) throws HopException {

    if (mongoObject == null) {
      return null;
    }

    if (tempParts.size() == 0) {
      throw new HopException(
          BaseMessages.getString(PKG, "MongoDbInput.ErrorMessage.MalformedPathRecord"));
    }

    String part = tempParts.remove(0);

    if (part.charAt(0) == '[') {
      // we're not expecting an array at this point - this document does not
      // contain our field
      return null;
    }

    if (part.indexOf('[') > 0) {
      String arrayPart = part.substring(part.indexOf('['));
      part = part.substring(0, part.indexOf('['));

      // put the array section back into location zero
      tempParts.add(0, arrayPart);
    }

    // part is a named field of this record
    Object fieldValue = mongoObject.get(part);
    if (fieldValue == null || fieldValue.getClass().equals(BsonUndefined.class)) {
      return null;
    }

    // what have we got
    if (tempParts.size() == 0) {
      // we're expecting a leaf primitive - lets see if that's what we have
      // here...
      return getHopValue(fieldValue);
    }

    if (fieldValue instanceof BasicDBObject) {
      return convertToHopValue(((BasicDBObject) fieldValue));
    }

    if (fieldValue instanceof BasicDBList) {
      return convertToHopValue(((BasicDBList) fieldValue));
    }

    // must mean we have a primitive here, but we're expecting to process more
    // path so this doesn't match us - return null
    return null;
  }

  /**
   * Convert a mongo array object to a Hop field value (for the field defined in this path)
   *
   * @param mongoList the array to convert
   * @return the Hop field value
   * @throws HopException if a problem occurs
   */
  public Object convertToHopValue(BasicDBList mongoList) throws HopException {

    if (mongoList == null) {
      return null;
    }

    if (tempParts.size() == 0) {
      throw new HopException(
          BaseMessages.getString(PKG, "MongoDbInput.ErrorMessage.MalformedPathArray"));
    }

    String part = tempParts.remove(0);
    if (!(part.charAt(0) == '[')) {
      // we're expecting an array at this point - this document does not
      // contain our field
      return null;
    }

    String index = part.substring(1, part.indexOf(']'));
    int arrayI = 0;
    try {
      arrayI = Integer.parseInt(index.trim());
    } catch (NumberFormatException e) {
      throw new HopException(
          BaseMessages.getString(PKG, "MongoDbInput.ErrorMessage.UnableToParseArrayIndex", index));
    }

    if (part.indexOf(']') < part.length() - 1) {
      // more dimensions to the array
      part = part.substring(part.indexOf(']') + 1, part.length());
      tempParts.add(0, part);
    }

    if (arrayI >= mongoList.size() || arrayI < 0) {
      return null;
    }

    Object element = mongoList.get(arrayI);

    if (element == null) {
      return null;
    }

    if (tempParts.size() == 0) {
      // we're expecting a leaf primitive - let's see if that's what we have
      // here...
      return getHopValue(element);
    }

    if (element instanceof BasicDBObject) {
      return convertToHopValue(((BasicDBObject) element));
    }

    if (element instanceof BasicDBList) {
      return convertToHopValue(((BasicDBList) element));
    }

    // must mean we have a primitive here, but we're expecting to process more
    // path so this doesn't match us - return null
    return null;
  }

  /**
   * Returns the MongoDB path for the field
   *
   * @return String MongoDB Field Path
   */
  public String getPath() {
    String pathName = fieldPath.replace("$.", "");

    pathName = pathName.replaceAll("\\[([0-9]+)\\]", ".$1");
    pathName = pathName.replace("[*]", "");

    return pathName;
  }

  /**
   * Returns the name of the MongoDB field
   *
   * @return String MongoDB Field Name
   */
  public String getName() {
    return fieldName;
  }

  @Override
  public int compareTo(MongoField comp) {
    return fieldName.compareTo(comp.fieldName);
  }

  @Injection(name = "FIELD_INDEXED", group = "FIELDS")
  public void setIndexedVals(String vals) {
    indexedValues = MongoDbInputData.indexedValsList(vals);
  }
}
