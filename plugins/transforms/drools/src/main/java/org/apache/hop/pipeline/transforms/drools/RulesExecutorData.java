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
import java.util.UUID;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.Message;
import org.kie.api.builder.ReleaseId;
import org.kie.api.builder.Results;
import org.kie.api.io.Resource;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.io.ResourceFactory;
import org.kie.internal.utils.KieHelper;

public class RulesExecutorData extends BaseTransformData implements ITransformData {
  private static final Class<?> PKG = RulesExecutor.class; // for i18n purposes

  private IRowMeta outputRowMeta;

  private KieBase kieBase;

  private Rules.Column[] columnList;

  private final Map<String, Rules.Column> resultMap = new HashMap<>();

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

  /**
   * Compiles the rules once. Every row then runs in a new session on the compiled rule base, so the
   * DRL is not parsed and compiled again for each row.
   *
   * @throws RuleValidationException when the rules do not compile
   */
  public void initializeRules() throws RuleValidationException {

    // To ensure the plugin classloader use for dependency resolution
    ClassLoader orig = Thread.currentThread().getContextClassLoader();
    ClassLoader loader = getClass().getClassLoader();
    Thread.currentThread().setContextClassLoader(loader);

    try {
      Resource ruleSet;
      if (ruleString != null) {
        ruleSet = ResourceFactory.newReaderResource(new StringReader(ruleString));
      } else {
        ruleSet = ResourceFactory.newFileResource(ruleFilePath);
      }

      // A release id of its own: the default one is shared by every rule set built in this JVM,
      // so another transform building its rules at the same time could replace ours.
      KieServices kieServices = KieServices.Factory.get();
      ReleaseId releaseId =
          kieServices.newReleaseId("org.apache.hop", "rules-executor-" + UUID.randomUUID(), "1.0");

      KieHelper kieHelper = new KieHelper().setClassLoader(loader).setReleaseId(releaseId);
      kieHelper.addResource(ruleSet, ResourceType.DRL);

      KieBuilder kieBuilder = kieServices.newKieBuilder(kieHelper.kfs, loader).buildAll();
      Results results = kieBuilder.getResults();
      if (results.hasMessages(Message.Level.ERROR)) {
        throw new RuleValidationException(results.getMessages());
      }

      kieBase = kieServices.newKieContainer(releaseId, loader).getKieBase();

      // The rule base is built: the module is not needed in the shared repository any more.
      kieServices.getRepository().removeKieModule(releaseId);
    } finally {
      // reset classloader back to original
      Thread.currentThread().setContextClassLoader(orig);
    }
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

    try {
      if (kieBase != null) {
        // A new session per row, so facts of one row never reach the next.
        // KieSession.close() delegates to dispose()
        try (KieSession session = kieBase.newKieSession()) {
          for (Rules.Column column : columnList) {
            session.insert(column);
          }

          session.fireAllRules();

          Collection<Object> oList = fetchColumns(session);
          for (Object o : oList) {
            resultMap.put(((Rules.Column) o).getName(), (Rules.Column) o);
          }
        }
      }
    } finally {
      Thread.currentThread().setContextClassLoader(orig);
    }
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
