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
 */

package org.apache.hop.pipeline.transforms.drools;

import java.io.StringReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.kie.api.builder.Message;
import org.kie.api.builder.Results;
import org.kie.api.io.Resource;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.io.ResourceFactory;
import org.kie.internal.utils.KieHelper;

public class RulesExecutorData extends BaseTransformData implements ITransformData {
  private static final Class<?> PKG = RulesExecutor.class; // for i18n purposes

  private IRowMeta outputRowMeta;

  private KieHelper kieHelper;

  private Rules.Column[] columnList;

  private Map<String, Rules.Column> resultMap = new HashMap<>();

  private String ruleString;

  public String getRuleString() {
    return ruleString;
  }

  public void setRuleString(String ruleString) {
    this.ruleString = ruleString;
  }

  public String getRuleFilePath() {
    return ruleFilePath;
  }

  public void setRuleFilePath(String ruleFilePath) {
    this.ruleFilePath = ruleFilePath;
  }

  private String ruleFilePath;

  public void setOutputRowMeta(IRowMeta outputRowMeta) {
    this.outputRowMeta = outputRowMeta;
  }

  public IRowMeta getOutputRowMeta() {
    return outputRowMeta;
  }

  public void initializeRules() throws RuleValidationException {

    // To ensure the plugin classloader use for dependency resolution
    ClassLoader orig = Thread.currentThread().getContextClassLoader();
    ClassLoader loader = getClass().getClassLoader();
    Thread.currentThread().setContextClassLoader(loader);

    Resource ruleSet = null;

    if (ruleString != null) {
      ruleSet = ResourceFactory.newReaderResource(new StringReader(ruleString));
    } else {
      ruleSet = ResourceFactory.newFileResource(ruleFilePath);
    }

    kieHelper = new KieHelper();
    kieHelper.addResource(ruleSet, ResourceType.DRL);

    Results results1 = kieHelper.verify();
    if (results1.hasMessages(Message.Level.ERROR)) {
      throw new RuleValidationException(results1.getMessages());
    }

    // reset classloader back to original
    Thread.currentThread().setContextClassLoader(orig);
  }

  public void initializeColumns(IRowMeta inputRowMeta) {
    if (inputRowMeta == null) {
      BaseMessages.getString(PKG, "RulesData.InitializeColumns.InputRowMetaIsNull");
      return;
    }

    // Create objects for insertion into the rules engine
    List<IValueMeta> columns = inputRowMeta.getValueMetaList();

    // This array must 1-1 match the row[] fetched by getRow()
    columnList = new Rules.Column[columns.size()];

    for (int i = 0; i < columns.size(); i++) {
      IValueMeta column = columns.get(i);

      Rules.Column c = new Rules.Column(true);
      c.setName(column.getName());
      c.setType(column.getTypeDesc());
      c.setPayload(null);

      columnList[i] = c;
    }
  }

  public void loadRow(Object[] r) {
    for (int i = 0; i < columnList.length; i++) {
      columnList[i].setPayload(r[i]);
    }
    resultMap.clear();
  }

  public void execute() {
    // To ensure the plugin classloader use for dependency resolution
    ClassLoader orig = Thread.currentThread().getContextClassLoader();
    ClassLoader loader = getClass().getClassLoader();
    Thread.currentThread().setContextClassLoader(loader);

    if (kieHelper != null) {
      KieSession session = kieHelper.getKieContainer().newKieSession();

      for (Rules.Column column : columnList) {
        session.insert(column);
      }

      session.fireAllRules();

      Collection<Object> oList = fetchColumns(session);
      for (Object o : oList) {
        resultMap.put(((Rules.Column) o).getName(), (Rules.Column) o);
      }

      session.dispose();
    }

    Thread.currentThread().setContextClassLoader(orig);
  }

  protected Collection<Object> fetchColumns(KieSession session) {

    return (Collection<Object>)
        session.getObjects(
            o -> {
              if (o instanceof Rules.Column column && !column.isExternalSource()) {
                return true;
              }
              return false;
            });
  }

  /**
   * @param columnName Column.payload associated with the result, or null if not found
   * @return
   */
  public Object fetchResult(String columnName) {
    return resultMap.get(columnName);
  }

  public void shutdown() {
    // Do nothing
  }
}
