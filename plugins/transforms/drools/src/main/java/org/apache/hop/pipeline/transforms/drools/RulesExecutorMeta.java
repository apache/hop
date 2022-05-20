
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

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.List;


@Transform(
        id = "RuleExecutor",
        image = "rules_exec.svg",
        name = "i18n::RulesExecutor.Name",
        description = "i18n::RulesExecutor.Description",
        categoryDescription = "i18n::Rules.Category",
        keywords = "i18n::RulesExecutor.keyword",
        documentationUrl = "/pipeline/transforms/rulesexecutor.html")
public class RulesExecutorMeta
        extends BaseTransformMeta<RulesExecutor, RulesExecutorData> {
  private static Class<?> PKG = Rules.class; // for i18n purposes

  // Contain storage keys in single location to cut down on save/load bugs

  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<RuleResultItem> ruleResultColumns = new ArrayList<>();

  @HopMetadataProperty(key="rule-file")
  private String ruleFile;

  @HopMetadataProperty(key="rule-definition")
  private String ruleDefinition;

  private boolean keepInputFields = true;

  public List<RuleResultItem> getRuleResultColumns() {
    return ruleResultColumns;
  }

  public void setRuleResultColumns( List<RuleResultItem> ruleResultColumns ) {
    this.ruleResultColumns = ruleResultColumns;
  }

  public void setRuleFile( String ruleFile ) {
    this.ruleFile = ruleFile;
  }

  public String getRuleFile() {
    return ruleFile;
  }

  public void setRuleDefinition( String ruleDefinition ) {
    this.ruleDefinition = ruleDefinition;
  }

  public String getRuleDefinition() {
    return ruleDefinition;
  }

  public boolean isKeepInputFields() {
    return keepInputFields;
  }

  public void setKeepInputFields( boolean keepInputFields ) {
    this.keepInputFields = keepInputFields;
  }


  @Override
  public void setDefault() {
  }

  @Override
  public void getFields(
          IRowMeta inputRowMeta,
          String name,
          IRowMeta[] info,
          TransformMeta nextTransform,
          IVariables variables,
          IHopMetadataProvider metadataProvider)
          throws HopTransformException {

    if (!keepInputFields) {
      inputRowMeta.clear();
    }
    try {
    if (ruleResultColumns != null) {
      for (int i = 0; i < ruleResultColumns.size(); i++) {
        int type = ValueMetaFactory.getIdForValueMeta( ruleResultColumns.get(i).getType() ) ;
        IValueMeta vm = ValueMetaFactory.createValueMeta( ruleResultColumns.get(i).getName(), type );

        vm.setOrigin(name);
        inputRowMeta.addValueMeta(vm);
      }
    }
    } catch (HopPluginException e) {
      throw new HopTransformException(
              "Unable to get rule result columns");
    }
  }

  public String[] getExpectedResultList() {
    String[] result = new String[ruleResultColumns.size()];

    for ( int i = 0; i < ruleResultColumns.size(); i++ ) {
      result[i] = ruleResultColumns.get( i ).getName();
    }

    return result;
  }

}
