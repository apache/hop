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

package org.apache.hop.workflow.action.loadsave;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.base.LoadSaveBase;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.transforms.loadsave.initializer.ActionInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.workflow.action.IAction;

public class LoadSaveTester<T extends IAction> extends LoadSaveBase<T> {

  public LoadSaveTester(
      Class<T> clazz,
      List<String> attributes,
      Map<String, String> getterMap,
      Map<String, String> setterMap,
      Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap,
      Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorTypeMap,
      ActionInitializer<T> actionInitializer)
      throws HopException {
    super(
        clazz,
        attributes,
        getterMap,
        setterMap,
        fieldLoadSaveValidatorAttributeMap,
        fieldLoadSaveValidatorTypeMap);
  }

  public LoadSaveTester(
      Class<T> clazz,
      List<String> attributes,
      Map<String, String> getterMap,
      Map<String, String> setterMap,
      Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap,
      Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorTypeMap)
      throws HopException {
    this(
        clazz,
        attributes,
        getterMap,
        setterMap,
        fieldLoadSaveValidatorAttributeMap,
        fieldLoadSaveValidatorTypeMap,
        null);
  }

  public LoadSaveTester(
      Class<T> clazz,
      List<String> attributes,
      Map<String, String> getterMap,
      Map<String, String> setterMap)
      throws HopException {
    this(clazz, attributes, getterMap, setterMap, new HashMap<>(), new HashMap<>());
  }

  @Override
  protected void validateLoadedMeta(
      List<String> attributes,
      Map<String, IFieldLoadSaveValidator<?>> validatorMap,
      T metaSaved,
      T metaLoaded) {
    super.validateLoadedMeta(attributes, validatorMap, metaSaved, metaLoaded);
  }

  public void testSerialization() throws HopException {
    testXmlRoundTrip();
    testClone();
  }

  public void testXmlRoundTrip() throws HopException {
    T metaToSave = createMeta();
    if (initializer != null) {
      initializer.modify(metaToSave);
    }
    Map<String, IFieldLoadSaveValidator<?>> validatorMap =
        createValidatorMapAndInvokeSetters(attributes, metaToSave);
    T metaLoaded = createMeta();
    String xml = "<transform>" + metaToSave.getXml() + "</transform>";
    InputStream is = new ByteArrayInputStream(xml.getBytes());
    IVariables variables = new Variables();
    metaLoaded.loadXml(
        XmlHandler.getSubNode(XmlHandler.loadXmlFile(is, null, false, false), "transform"),
        metadataProvider,
        variables);
    validateLoadedMeta(attributes, validatorMap, metaToSave, metaLoaded);
  }

  protected void testClone() {
    T metaToSave = createMeta();
    if (initializer != null) {
      initializer.modify(metaToSave);
    }
    Map<String, IFieldLoadSaveValidator<?>> validatorMap =
        createValidatorMapAndInvokeSetters(attributes, metaToSave);

    T metaLoaded = (T) metaToSave.clone();
    validateLoadedMeta(attributes, validatorMap, metaToSave, metaLoaded);
  }
}
