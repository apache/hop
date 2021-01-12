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
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transforms.mongodbinput.MongoDbInputData;

import java.util.ArrayList;
import java.util.List;

public class MongoArrayExpansion {
  protected static Class<?> PKG = MongoArrayExpansion.class; // For Translator

  /** The prefix of the full path that defines the expansion */
  public String expansionPath;

  /** Subfield objects that handle the processing of the path after the expansion prefix */
  protected List<MongoField> subFields;

  private List<String> pathParts;
  private List<String> tempParts;

  public IRowMeta outputRowMeta;

  public MongoArrayExpansion(List<MongoField> subFields) {
    this.subFields = subFields;
  }

  /**
   * Initialize this field by parsing the path etc.
   *
   * @throws HopException if a problem occurs
   */
  public void init() throws HopException {
    if (StringUtils.isEmpty(expansionPath)) {
      throw new HopException(BaseMessages.getString(PKG, "MongoDbInput.ErrorMessage.NoPathSet"));
    }
    if (pathParts != null) {
      return;
    }

    String expansionPath = MongoDbInputData.cleansePath(this.expansionPath);

    String[] temp = expansionPath.split("\\.");
    pathParts = new ArrayList<>();
    for (String part : temp) {
      pathParts.add(part);
    }

    if (pathParts.get(0).equals("$")) {
      pathParts.remove(0); // root record indicator
    } else if (pathParts.get(0).startsWith("$[")) {

      // strip leading $ off of array
      String r = pathParts.get(0).substring(1);
      pathParts.set(0, r);
    }
    tempParts = new ArrayList<>();

    // initialize the sub fields
    if (subFields != null) {
      for (MongoField f : subFields) {
        int outputIndex = outputRowMeta.indexOfValue(f.fieldName);
        f.init(outputIndex);
      }
    }
  }

  /**
   * Reset this field. Should be called prior to processing a new field value from the avro file
   *
   * @param variables environment variables (values that environment variables resolve to cannot contain
   *     "."s)
   */
  public void reset(IVariables variables) {
    tempParts.clear();

    for (String part : pathParts) {
      tempParts.add(variables.resolve(part));
    }

    // reset sub fields
    for (MongoField f : subFields) {
      f.reset(variables);
    }
  }

  protected Object[][] nullResult() {
    Object[][] result = new Object[1][outputRowMeta.size() + RowDataUtil.OVER_ALLOCATE_SIZE];

    return result;
  }

  public Object[][] convertToHopValue(BasicDBObject mongoObject, IVariables variables)
      throws HopException {

    if (mongoObject == null) {
      return nullResult();
    }

    if (tempParts.size() == 0) {
      throw new HopException(
          BaseMessages.getString(PKG, "MongoDbInput.ErrorMessage.MalformedPathRecord"));
    }

    String part = tempParts.remove(0);

    if (part.charAt(0) == '[') {
      // we're not expecting an array at this point - this document does not
      // contain our field(s)
      return nullResult();
    }

    if (part.indexOf('[') > 0) {
      String arrayPart = part.substring(part.indexOf('['));
      part = part.substring(0, part.indexOf('['));

      // put the array section back into location zero
      tempParts.add(0, arrayPart);
    }

    // part is a named field of this record
    Object fieldValue = mongoObject.get(part);
    if (fieldValue == null) {
      return nullResult();
    }

    if (fieldValue instanceof BasicDBObject) {
      return convertToHopValue(((BasicDBObject) fieldValue), variables);
    }

    if (fieldValue instanceof BasicDBList) {
      return convertToHopValue(((BasicDBList) fieldValue), variables);
    }

    // must mean we have a primitive here, but we're expecting to process more
    // path so this doesn't match us - return null
    return nullResult();
  }

  public Object[][] convertToHopValue(BasicDBList mongoList, IVariables variables) throws HopException {

    if (mongoList == null) {
      return nullResult();
    }

    if (tempParts.size() == 0) {
      throw new HopException(
          BaseMessages.getString(PKG, "MongoDbInput.ErrorMessage.MalformedPathArray"));
    }

    String part = tempParts.remove(0);
    if (!(part.charAt(0) == '[')) {
      // we're expecting an array at this point - this document does not
      // contain our field
      return nullResult();
    }

    String index = part.substring(1, part.indexOf(']'));

    if (part.indexOf(']') < part.length() - 1) {
      // more dimensions to the array
      part = part.substring(part.indexOf(']') + 1, part.length());
      tempParts.add(0, part);
    }

    if (index.equals("*")) {
      // start the expansion - we delegate conversion to our subfields
      Object[][] result =
          new Object[mongoList.size()][outputRowMeta.size() + RowDataUtil.OVER_ALLOCATE_SIZE];

      for (int i = 0; i < mongoList.size(); i++) {
        Object element = mongoList.get(i);

        for (int j = 0; j < subFields.size(); j++) {
          MongoField sf = subFields.get(j);
          sf.reset(variables);

          // what have we got?
          if (element instanceof BasicDBObject) {
            result[i][sf.outputIndex] = sf.convertToHopValue((BasicDBObject) element);
          } else if (element instanceof BasicDBList) {
            result[i][sf.outputIndex] = sf.convertToHopValue((BasicDBList) element);
          } else {
            // assume a primitive
            result[i][sf.outputIndex] = sf.getHopValue(element);
          }
        }
      }

      return result;
    } else {
      int arrayI = 0;
      try {
        arrayI = Integer.parseInt(index.trim());
      } catch (NumberFormatException e) {
        throw new HopException(
            BaseMessages.getString(
                PKG, "MongoDbInput.ErrorMessage.UnableToParseArrayIndex", index));
      }

      if (arrayI >= mongoList.size() || arrayI < 0) {
        // index is out of bounds
        return nullResult();
      }

      Object element = mongoList.get(arrayI);

      if (element == null) {
        return nullResult();
      }

      if (element instanceof BasicDBObject) {
        return convertToHopValue(((BasicDBObject) element), variables);
      }

      if (element instanceof BasicDBList) {
        return convertToHopValue(((BasicDBList) element), variables);
      }

      // must mean we have a primitive here, but we're expecting to process
      // more
      // path so this doesn't match us - return null
      return nullResult();
    }
  }
}
