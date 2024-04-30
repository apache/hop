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

package org.apache.hop.workflow.actions.truncatetables;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.junit.ClassRule;

public class ActionTruncateTablesLoadSaveTest
    extends WorkflowActionLoadSaveTestSupport<ActionTruncateTables> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<ActionTruncateTables> getActionClass() {
    return ActionTruncateTables.class;
  }

  @Override
  protected List<String> listAttributes() {
    return Arrays.asList(new String[] {"connection", "argFromPrevious", "items"});
  }

  @Override
  protected Map<String, String> createGettersMap() {
    Map<String, String> getterMap =
        new HashMap<String, String>() {
          {
            put("connection", "getConnection");
            put("arg_from_previous", "isArgFromPrevious");
            put("items", "getItems");
          }
        };

    return getterMap;
  }

  @Override
  protected Map<String, String> createSettersMap() {
    Map<String, String> setterMap =
        new HashMap<String, String>() {
          {
            put("connection", "setConnection");
            put("arg_from_previous", "setArgFromPrevious");
            put("items", "setItems");
          }
        };

    return setterMap;
  }

  @Override
  protected Map<String, IFieldLoadSaveValidator<?>> createAttributeValidatorsMap() {
    Map<String, IFieldLoadSaveValidator<?>> validators = new HashMap<>();

    validators.put(
        "items",
        new ListLoadSaveValidator<TruncateTableItem>(new TruncateTableItemLoadSaveValidator()));

    return validators;
  }

  public class TruncateTableItemLoadSaveValidator
      implements IFieldLoadSaveValidator<TruncateTableItem> {
    final Random rand = new Random();

    @Override
    public TruncateTableItem getTestObject() {

      TruncateTableItem field =
          new TruncateTableItem(UUID.randomUUID().toString(), UUID.randomUUID().toString());

      return field;
    }

    @Override
    public boolean validateTestObject(TruncateTableItem testObject, Object actual) {

      if (!(actual instanceof TruncateTableItem)) {
        return false;
      }

      TruncateTableItem another = (TruncateTableItem) actual;
      return new EqualsBuilder()
          .append(testObject.getSchemaName(), another.getSchemaName())
          .append(testObject.getTableName(), another.getTableName())
          .isEquals();
    }
  }
}
