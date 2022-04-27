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

package org.apache.hop.pipeline.transforms.httppost;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidatorFactory;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class HttpPostMetaTest {
  @Before
  public void testLoadSaveRoundTrip() throws HopException {
    HopEnvironment.init();
  }

  @Test
  public void testSerialization() throws Exception {
    LoadSaveTester<HttpPostMeta> tester = new LoadSaveTester<>(HttpPostMeta.class);
    IFieldLoadSaveValidatorFactory factory = tester.getFieldLoadSaveValidatorFactory();
    factory.registerValidator(
            HttpPostMeta.class.getDeclaredField("lookupFields").getGenericType().toString(),
            new ListLoadSaveValidator<>(new HttpPostLookupFieldValidator())
    );
    factory.registerValidator(
            HttpPostMeta.class.getDeclaredField("resultFields").getGenericType().toString(),
            new ListLoadSaveValidator<>(new HttpPostResultFieldValidator())
    );

    tester.testSerialization();
  }

  @Test
  public void setDefault() {
    HttpPostMeta meta = new HttpPostMeta();
    assertNull(meta.getEncoding());

    meta.setDefault();
    assertEquals("UTF-8", meta.getEncoding());
  }

  public static final class HttpPostLookupFieldValidator implements IFieldLoadSaveValidator<HttpPostLookupField> {

    @Override
    public HttpPostLookupField getTestObject() {
      HttpPostLookupField field = new HttpPostLookupField();
      field.getArgumentField().addAll(Arrays.asList(
              new HttpPostArgumentField(UUID.randomUUID().toString(), UUID.randomUUID().toString(), new Random().nextBoolean()),
              new HttpPostArgumentField(UUID.randomUUID().toString(), UUID.randomUUID().toString(), new Random().nextBoolean()),
              new HttpPostArgumentField(UUID.randomUUID().toString(), UUID.randomUUID().toString(), new Random().nextBoolean())
      ));
      field.getQueryField().addAll(Arrays.asList(
              new HttpPostQuery(UUID.randomUUID().toString(), UUID.randomUUID().toString()),
              new HttpPostQuery(UUID.randomUUID().toString(), UUID.randomUUID().toString())
              ));
      return field;
    }

    @Override
    public boolean validateTestObject(HttpPostLookupField testObject, Object actual) {
      if (!(actual instanceof HttpPostLookupField)){
        return false;
      }
      HttpPostLookupField actualObject = (HttpPostLookupField) actual;

      // Check the argument fields...
      //
      if (testObject.getArgumentField().size()!=actualObject.getArgumentField().size()) {
        return false;
      }
      for (int i=0;i<testObject.getArgumentField().size();i++) {
        HttpPostArgumentField testField = testObject.getArgumentField().get(i);
        HttpPostArgumentField actualField = actualObject.getArgumentField().get(i);
        if (!testField.equals(actualField)) {
          return false;
        }
      }

      // Check the query fields...
      //
      if (testObject.getQueryField().size()!=actualObject.getQueryField().size()) {
        return false;
      }
      for (int i=0;i<testObject.getQueryField().size();i++) {
        HttpPostQuery testField = testObject.getQueryField().get(i);
        HttpPostQuery actualField = actualObject.getQueryField().get(i);
        if (!testField.equals(actualField)) {
          return false;
        }
      }

      return true;
    }
  }

  public static final class HttpPostResultFieldValidator implements IFieldLoadSaveValidator<HttpPostResultField> {

    @Override
    public HttpPostResultField getTestObject() {
      return new HttpPostResultField(UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString());
    }

    @Override
    public boolean validateTestObject(HttpPostResultField testObject, Object actual) {
      return testObject.equals(actual);
    }
  }
}
