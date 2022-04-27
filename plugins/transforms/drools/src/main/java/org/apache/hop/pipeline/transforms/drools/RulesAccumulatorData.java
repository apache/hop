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

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.kie.api.KieServices;
import org.kie.api.builder.*;
import org.kie.api.io.Resource;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.ObjectFilter;
import org.kie.internal.io.ResourceFactory;
import org.kie.internal.utils.KieHelper;

import java.io.StringReader;
import java.util.*;

public class RulesAccumulatorData extends BaseTransformData implements ITransformData {
  private static Class<?> PKG = RulesAccumulator.class; // for i18n purposes

  private IRowMeta outputRowMeta;
  private IRowMeta inputRowMeta;

  private List<Object[]> results;

  private String ruleString;

  private List<Rules.Row> rowList = new ArrayList<>();
  private List<Rules.Row> resultList = new ArrayList<>();

  private KieHelper kieHelper;

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

  public void initializeRules() {

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
      System.out.println(results1.getMessages());
      throw new RuntimeException(BaseMessages.getString(PKG, "RulesData.Error.CompileDRL"));
    }

   // reset classloader back to original
    Thread.currentThread().setContextClassLoader(orig);
  }

  public void initializeInput(IRowMeta _inputRowMeta) {
    if (_inputRowMeta == null) {
      BaseMessages.getString(PKG, "RulesData.InitializeColumns.InputRowMetaIsNull");
      return;
    }

    this.inputRowMeta = _inputRowMeta;
  }

  public void loadRow(Object[] r) throws Exception {
    // Store rows for processing
    Map<String, Object> columns = new Hashtable<>();
    for (String field : inputRowMeta.getFieldNames()) {
      columns.put(field, r[inputRowMeta.indexOfValue(field)]);
    }

    rowList.add(new Rules.Row(columns, true));
  }

  public List<Rules.Row> getResultRows() {
    return resultList;
  }

  public void execute() {

    // To ensure the plugin classloader use for dependency resolution
    ClassLoader orig = Thread.currentThread().getContextClassLoader();
    ClassLoader loader = getClass().getClassLoader();
    Thread.currentThread().setContextClassLoader(loader);

    if (kieHelper != null) {
      KieSession session = kieHelper.getKieContainer().newKieSession();
      for (Rules.Row fact : rowList) {
        session.insert(fact);
      }

      int fh = session.fireAllRules();

      Collection<Object> oList =
          (Collection<Object>)
              session.getObjects(
                  new ObjectFilter() {
                    @Override
                    public boolean accept(Object o) {
                      if (o instanceof Rules.Row && !((Rules.Row) o).isExternalSource()) {
                        return true;
                      }
                      return false;
                    }
                  });

      for (Object o : oList) {
        resultList.add((Rules.Row) o);
      }

      session.dispose();
    }

    Thread.currentThread().setContextClassLoader(orig);
  }

  /**
   * Get the list of rows generated by the Rules execution
   *
   * @return List of rows generated
   */
  public List<Object[]> fetchResults() {
    return results;
  }

  public void shutdown() {}
}
