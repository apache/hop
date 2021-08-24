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

package org.apache.hop.pipeline.transforms.checksum;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.EnumLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class CheckSumMetaTest implements IInitializer<CheckSumMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify(CheckSumMeta someMeta) {
    // someMeta.allocate( 7 );
  }

  @Test
  public void testConstants() {
    assertEquals("CRC32", CheckSumMeta.CheckSumType.CRC32.getCode());
    assertEquals("ADLER32", CheckSumMeta.CheckSumType.ADLER32.getCode());
    assertEquals("MD5", CheckSumMeta.CheckSumType.MD5.getCode());
    assertEquals("SHA-1", CheckSumMeta.CheckSumType.SHA1.getCode());
    assertEquals("SHA-256", CheckSumMeta.CheckSumType.SHA256.getCode());
    assertEquals("SHA-384", CheckSumMeta.CheckSumType.SHA384.getCode());
    assertEquals("SHA-512", CheckSumMeta.CheckSumType.SHA512.getCode());
  }

  @Test
  public void testSerialization() throws HopException {
    List<String> attributes =
        Arrays.asList("fields", "resultFieldName", "checkSumType", "resultType");

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();

    ListLoadSaveValidator<Field> listLoadSaveValidator =
        new ListLoadSaveValidator<>(new CheckSumFieldLoadSaveValidator());

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put("fields", listLoadSaveValidator);
    attrValidatorMap.put(
        "checkSumType",
        new EnumLoadSaveValidator<CheckSumMeta.CheckSumType>(CheckSumMeta.CheckSumType.class));
    attrValidatorMap.put(
        "resultType",
        new EnumLoadSaveValidator<CheckSumMeta.ResultType>(CheckSumMeta.ResultType.class));

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    LoadSaveTester<CheckSumMeta> loadSaveTester =
        new LoadSaveTester<>(
            CheckSumMeta.class,
            attributes,
            getterMap,
            setterMap,
            attrValidatorMap,
            typeValidatorMap,
            this);

    loadSaveTester.testSerialization();
  }

  public static class CheckSumFieldLoadSaveValidator implements IFieldLoadSaveValidator<Field> {
    @Override
    public Field getTestObject() {
      return new Field(UUID.randomUUID().toString());
    }

    @Override
    public boolean validateTestObject(Field testObject, Object actual) {
      return testObject.equals(actual);
    }
  }
}
