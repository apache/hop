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

package org.apache.hop.pipeline.transforms.loadsave;

import org.apache.hop.base.LoadSaveBase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.getter.IGetter;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.setter.ISetter;
import org.apache.hop.pipeline.transforms.loadsave.validator.DatabaseMetaLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidatorFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotSame;

public class LoadSaveTester<T extends ITransformMeta> extends LoadSaveBase<T> {

  public LoadSaveTester(
      Class<T> clazz,
      List<String> attributes,
      Map<String, String> getterMap,
      Map<String, String> setterMap,
      Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap,
      Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorTypeMap,
      IInitializer<T> metaInitializerIFace) throws HopException {
    super(
        clazz,
        attributes,
        getterMap,
        setterMap,
        fieldLoadSaveValidatorAttributeMap,
        fieldLoadSaveValidatorTypeMap,
        metaInitializerIFace);
  }

  public LoadSaveTester(
      Class<T> clazz,
      List<String> attributes,
      Map<String, String> getterMap,
      Map<String, String> setterMap,
      Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap,
      Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorTypeMap) throws HopException {
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
      Map<String, String> setterMap) throws HopException {
    this(
        clazz,
        attributes,
        getterMap,
        setterMap,
        new HashMap<>(),
        new HashMap<>());
  }

  public LoadSaveTester(Class<T> clazz, List<String> attributes) throws HopException {
    this(clazz, attributes, new HashMap<>(), new HashMap<>());
  }

  public LoadSaveTester(Class<T> clazz) throws HopException {
    this(clazz, new ArrayList<>());
  }

  public IFieldLoadSaveValidatorFactory getFieldLoadSaveValidatorFactory() {
    return fieldLoadSaveValidatorFactory;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Map<String, IFieldLoadSaveValidator<?>> createValidatorMapAndInvokeSetters(
      List<String> attributes, T metaToSave) {
    Map<String, IFieldLoadSaveValidator<?>> validatorMap = new HashMap<>();
    for (String attribute : attributes) {
      IGetter<?> getter = manipulator.getGetter(attribute);
      @SuppressWarnings("rawtypes")
      ISetter setter = manipulator.getSetter(attribute);
      IFieldLoadSaveValidator<?> validator = fieldLoadSaveValidatorFactory.createValidator(getter);
      try {
        Object testValue = validator.getTestObject();
        setter.set(metaToSave, testValue);
        if (validator instanceof DatabaseMetaLoadSaveValidator) {
          addDatabase((DatabaseMeta) testValue);
        }
      } catch (Exception e) {
        throw new RuntimeException("Unable to invoke setter for " + attribute, e);
      }
      validatorMap.put(attribute, validator);
    }
    return validatorMap;
  }

  public void testSerialization() throws HopException {
    testXmlRoundTrip();
    testClone();
  }

  @SuppressWarnings({"deprecation", "unchecked"})
  protected void testClone() throws HopException {
    T metaToSave = createMeta();
    if (initializer != null) {
      initializer.modify(metaToSave);
    }
    Map<String, IFieldLoadSaveValidator<?>> validatorMap =
        createValidatorMapAndInvokeSetters(attributes, metaToSave);

    T metaLoaded = (T) metaToSave.clone();
    assertNotSame(metaToSave, metaLoaded);
    validateLoadedMeta(attributes, validatorMap, metaToSave, metaLoaded);
  }

  /**
   * @throws HopException
   * @deprecated the {@link #testSerialization()} method should be used instead, as additional tests
   *     may be added in the future to cover other topics related to transform serialization
   */
  @Deprecated(since="2.0")
  // TODO Change method visibility to protected
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
    metaLoaded.loadXml(
        XmlHandler.getSubNode(XmlHandler.loadXmlFile(is, null, false, false), "transform"),
        metadataProvider);
    validateLoadedMeta(attributes, validatorMap, metaToSave, metaLoaded);

    // TODO Remove after method visibility changed, it should be called in testSerialization
    testClone();
  }
}
