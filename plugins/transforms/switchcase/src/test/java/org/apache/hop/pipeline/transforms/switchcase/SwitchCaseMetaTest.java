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

package org.apache.hop.pipeline.transforms.switchcase;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidatorFactory;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** @author nhudak */
public class SwitchCaseMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  LoadSaveTester<SwitchCaseMeta> loadSaveTester;

  public SwitchCaseMetaTest() {
    // SwitchCaseMeta bean-like attributes
    List<String> attributes =
        Arrays.asList(
            "fieldName",
            "usingContains",
            "caseValueFormat",
            "caseValueDecimal", /* "caseValueType",*/
            "caseValueGroup",
            "defaultTargetTransformName",
            "caseTargets");

    // Non-standard getters & setters
    Map<String, String> getterMap = new HashMap<>();
    // getterMap.put( "isContains", "isContains" );

    Map<String, String> setterMap = new HashMap<>();
    // setterMap.put( "isContains", "setContains" );

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    this.loadSaveTester =
        new LoadSaveTester<>(
            SwitchCaseMeta.class,
            attributes,
            getterMap,
            setterMap,
            attrValidatorMap,
            typeValidatorMap);

    IFieldLoadSaveValidatorFactory validatorFactory =
        loadSaveTester.getFieldLoadSaveValidatorFactory();

    IFieldLoadSaveValidator<SwitchCaseTarget> targetValidator =
        new IFieldLoadSaveValidator<SwitchCaseTarget>() {
          @Override
          public SwitchCaseTarget getTestObject() {
            return new SwitchCaseTarget() {
              {
                setCaseValue(UUID.randomUUID().toString());
                setCaseTargetTransformName(UUID.randomUUID().toString());
              }
            };
          }

          @Override
          public boolean validateTestObject(SwitchCaseTarget testObject, Object actual) {
            return testObject.getCaseValue().equals(((SwitchCaseTarget) actual).getCaseValue())
                && testObject
                    .getCaseTargetTransformName()
                    .equals(((SwitchCaseTarget) actual).getCaseTargetTransformName());
          }
        };

    validatorFactory.registerValidator(
        validatorFactory.getName(SwitchCaseTarget.class), targetValidator);
    validatorFactory.registerValidator(
        validatorFactory.getName(List.class, SwitchCaseTarget.class),
        new ListLoadSaveValidator<>(targetValidator));
  }
}
