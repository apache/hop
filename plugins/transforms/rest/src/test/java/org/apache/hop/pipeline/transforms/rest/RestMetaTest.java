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

package org.apache.hop.pipeline.transforms.rest;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidatorFactory;
import org.apache.hop.pipeline.transforms.loadsave.validator.ObjectValidator;
import org.apache.hop.pipeline.transforms.rest.fields.HeaderField;
import org.apache.hop.pipeline.transforms.rest.fields.MatrixParameterField;
import org.apache.hop.pipeline.transforms.rest.fields.ParameterField;
import org.apache.hop.pipeline.transforms.rest.fields.ResultField;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class RestMetaTest implements IInitializer<ITransformMeta> {

  LoadSaveTester loadSaveTester;
  Class<RestMeta> testMetaClass = RestMeta.class;

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void beforeClass() throws HopException {
    PluginRegistry.addPluginType(TwoWayPasswordEncoderPluginType.getInstance());
    PluginRegistry.init();
    String passwordEncoderPluginID =
        Const.NVL(EnvUtil.getSystemProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN), "Hop");
    Encr.init(passwordEncoderPluginID);
  }

  @Test
  void testLoadSaveRoundTrip() throws HopException {
    List<String> attributes =
        Arrays.asList(
            "applicationType",
            "url",
            "urlInField",
            "urlField",
            "proxyHost",
            "proxyPort",
            "httpLogin",
            "httpPassword",
            "preemptive",
            "bodyField",
            "method",
            "dynamicMethod",
            "methodFieldName",
            "trustStoreFile",
            "trustStorePassword",
            "connectionTimeout",
            "readTimeout",
            "ignoreSsl",
            "headerFields",
            "parameterFields",
            "matrixParameterFields",
            "resultField"
            //            "headerField",
            //            "headerName",
            //            "parameterField",
            //            "parameterName",
            //            "matrixParameterField",
            //            "matrixParameterName",
            //            "fieldName",
            //            "resultCodeFieldName",
            //            "responseTimeFieldName",
            //            "responseHeaderFieldName"
            );

    Map<String, String> getterMap = new HashMap<>();
    getterMap.put("applicationType", "getApplicationType");
    getterMap.put("url", "getUrl");
    getterMap.put("urlInField", "isUrlInField");
    getterMap.put("urlField", "getUrlField");
    getterMap.put("proxyHost", "getProxyHost");
    getterMap.put("proxyPort", "getProxyPort");
    getterMap.put("httpLogin", "getHttpLogin");
    getterMap.put("httpPassword", "getHttpPassword");
    getterMap.put("preemptive", "isPreemptive");
    getterMap.put("bodyField", "getBodyField");
    getterMap.put("method", "getMethod");
    getterMap.put("dynamicMethod", "isDynamicMethod");
    getterMap.put("methodFieldName", "getMethodFieldName");
    getterMap.put("trustStoreFile", "getTrustStoreFile");
    getterMap.put("trustStorePassword", "getTrustStorePassword");
    getterMap.put("connectionTimeout", "getConnectionTimeout");
    getterMap.put("readTimeout", "getReadTimeout");
    getterMap.put("ignoreSsl", "isIgnoreSsl");
    getterMap.put("headerFields", "getHeaderFields");
    getterMap.put("parameterFields", "getParameterFields");
    getterMap.put("matrixParameterFields", "getMatrixParameterFields");
    getterMap.put("resultField", "getResultField");

    Map<String, String> setterMap = new HashMap<>();
    setterMap.put("applicationType", "setApplicationType");
    setterMap.put("url", "setUrl");
    setterMap.put("urlInField", "setUrlInField");
    setterMap.put("urlField", "setUrlField");
    setterMap.put("proxyHost", "setProxyHost");
    setterMap.put("proxyPort", "setProxyPort");
    setterMap.put("httpLogin", "setHttpLogin");
    setterMap.put("httpPassword", "setHttpPassword");
    setterMap.put("preemptive", "setPreemptive");
    setterMap.put("bodyField", "setBodyField");
    setterMap.put("method", "setMethod");
    setterMap.put("dynamicMethod", "setDynamicMethod");
    setterMap.put("methodFieldName", "setMethodFieldName");
    setterMap.put("trustStoreFile", "setTrustStoreFile");
    setterMap.put("trustStorePassword", "setTrustStorePassword");
    setterMap.put("connectionTimeout", "setConnectionTimeout");
    setterMap.put("readTimeout", "setReadTimeout");
    setterMap.put("ignoreSsl", "setIgnoreSsl");
    setterMap.put("headerFields", "setHeaderFields");
    setterMap.put("parameterFields", "setParameterFields");
    setterMap.put("matrixParameterFields", "setMatrixParameterFields");
    setterMap.put("resultField", "setResultField");

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
        new LoadSaveTester(
            testMetaClass,
            attributes,
            getterMap,
            setterMap,
            attrValidatorMap,
            typeValidatorMap,
            this);

    IFieldLoadSaveValidatorFactory validatorFactory =
        loadSaveTester.getFieldLoadSaveValidatorFactory();

    validatorFactory.registerValidator(
        validatorFactory.getName(ResultField.class),
        new ObjectValidator<>(
            validatorFactory,
            ResultField.class,
            Arrays.asList("fieldName", "code", "responseTime", "responseHeader"),
            new HashMap<String, String>() {
              {
                put("fieldname", "getFieldName");
                put("code", "getCode");
                put("responseTime", "getResponseTime");
                put("responseHeader", "getResponseHeader");
              }
            },
            new HashMap<String, String>() {
              {
                put("fieldname", "setFieldName");
                put("code", "setCode");
                put("responseTime", "setResponseTime");
                put("responseHeader", "setResponseHeader");
              }
            }));

    validatorFactory.registerValidator(
        validatorFactory.getName(HeaderField.class),
        new ObjectValidator<>(
            validatorFactory,
            HeaderField.class,
            Arrays.asList("name", "headerField"),
            new HashMap<String, String>() {
              {
                put("name", "getName");
                put("headerField", "getHeaderField");
              }
            },
            new HashMap<String, String>() {
              {
                put("bame", "setName");
                put("headerField", "setHeaderField");
              }
            }));

    validatorFactory.registerValidator(
        validatorFactory.getName(ParameterField.class),
        new ObjectValidator<>(
            validatorFactory,
            ParameterField.class,
            Arrays.asList("name", "headerField"),
            new HashMap<String, String>() {
              {
                put("name", "getName");
                put("headerField", "getHeaderField");
              }
            },
            new HashMap<String, String>() {
              {
                put("bame", "setName");
                put("headerField", "setHeaderField");
              }
            }));

    validatorFactory.registerValidator(
        validatorFactory.getName(MatrixParameterField.class),
        new ObjectValidator<>(
            validatorFactory,
            MatrixParameterField.class,
            Arrays.asList("name", "headerField"),
            new HashMap<String, String>() {
              {
                put("name", "getName");
                put("headerField", "getHeaderField");
              }
            },
            new HashMap<String, String>() {
              {
                put("bame", "setName");
                put("headerField", "setHeaderField");
              }
            }));

    //    Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap = new
    // HashMap<>();
    //
    //    // Arrays need to be consistent length
    //    IFieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
    //        new ArrayLoadSaveValidator<>(new StringLoadSaveValidator(), 25);
    //    fieldLoadSaveValidatorAttributeMap.put("headerField", stringArrayLoadSaveValidator);
    //    fieldLoadSaveValidatorAttributeMap.put("headerName", stringArrayLoadSaveValidator);
    //    fieldLoadSaveValidatorAttributeMap.put("parameterField", stringArrayLoadSaveValidator);
    //    fieldLoadSaveValidatorAttributeMap.put("parameterName", stringArrayLoadSaveValidator);
    //    fieldLoadSaveValidatorAttributeMap.put("matrixParameterField",
    // stringArrayLoadSaveValidator);
    //    fieldLoadSaveValidatorAttributeMap.put("matrixParameterName",
    // stringArrayLoadSaveValidator);
    //
    //    LoadSaveTester<RestMeta> loadSaveTester =
    //        new LoadSaveTester<>(
    //            RestMeta.class,
    //            attributes,
    //            new HashMap<>(),
    //            new HashMap<>(),
    //            fieldLoadSaveValidatorAttributeMap,
    //            new HashMap<>());

    //    loadSaveTester.testSerialization();
  }

  @Test
  void testTransformChecks() {
    RestMeta meta = new RestMeta();
    List<ICheckResult> remarks = new ArrayList<>();
    PipelineMeta pipelineMeta = new PipelineMeta();
    TransformMeta transform = new TransformMeta();
    IRowMeta prev = new RowMeta();
    IRowMeta info = new RowMeta();
    String[] input = new String[0];
    String[] output = new String[0];
    IVariables variables = new Variables();
    IHopMetadataProvider metadataProvider = null;

    // In a default configuration, it's expected that some errors will occur.
    // For this, we'll grab a baseline count of the number of errors
    // as the error count should decrease as we change configuration settings to proper values.
    remarks.clear();
    meta.check(
        remarks, pipelineMeta, transform, prev, input, output, info, variables, metadataProvider);
    final int errorsDefault = getCheckResultErrorCount(remarks);
    assertTrue(errorsDefault > 0);

    // Setting the transform to read the URL from a field should fix one of the check() errors
    meta.setUrlInField(true);
    meta.setUrlField("urlField");
    prev.addValueMeta(new ValueMetaString("urlField"));
    remarks.clear();
    meta.check(
        remarks, pipelineMeta, transform, prev, input, output, info, variables, metadataProvider);
    int errorsCurrent = getCheckResultErrorCount(remarks);
    assertTrue(errorsDefault > errorsCurrent);
  }

  private static int getCheckResultErrorCount(List<ICheckResult> remarks) {
    return remarks.stream()
        .filter(p -> p.getType() == ICheckResult.TYPE_RESULT_ERROR)
        .collect(Collectors.toList())
        .size();
  }

  @Test
  void testEntityEnclosingMethods() {
    assertTrue(RestMeta.isActiveBody(RestMeta.HTTP_METHOD_POST));
    assertTrue(RestMeta.isActiveBody(RestMeta.HTTP_METHOD_PUT));
    assertTrue(RestMeta.isActiveBody(RestMeta.HTTP_METHOD_PATCH));

    assertFalse(RestMeta.isActiveBody(RestMeta.HTTP_METHOD_GET));
    assertFalse(RestMeta.isActiveBody(RestMeta.HTTP_METHOD_DELETE));
    assertFalse(RestMeta.isActiveBody(RestMeta.HTTP_METHOD_HEAD));
    assertFalse(RestMeta.isActiveBody(RestMeta.HTTP_METHOD_OPTIONS));
  }

  @Override
  public void modify(ITransformMeta someMeta) {
    if (someMeta instanceof RestMeta) {
      ((RestMeta) someMeta).getHeaderFields().clear();
      ((RestMeta) someMeta).getParameterFields().clear();
      ((RestMeta) someMeta).getMatrixParameterFields().clear();

      ((RestMeta) someMeta)
          .getHeaderFields()
          .addAll(
              Arrays.asList(
                  new HeaderField("field1", "name1"),
                  new HeaderField("field2", "name2"),
                  new HeaderField("field3", "name3"),
                  new HeaderField("field4", "name4"),
                  new HeaderField("field1", "name5")));

      ((RestMeta) someMeta)
          .getMatrixParameterFields()
          .addAll(
              Arrays.asList(
                  new MatrixParameterField("field1", "name1"),
                  new MatrixParameterField("field2", "name2"),
                  new MatrixParameterField("field3", "name3"),
                  new MatrixParameterField("field4", "name4"),
                  new MatrixParameterField("field5", "name5")));

      ((RestMeta) someMeta)
          .getParameterFields()
          .addAll(
              Arrays.asList(
                  new ParameterField("field1", "name1"),
                  new ParameterField("field2", "name2"),
                  new ParameterField("field3", "name3"),
                  new ParameterField("field4", "name4"),
                  new ParameterField("field5", "name5")));
      ((RestMeta) someMeta).getResultField().setFieldName("fieldname");
      ((RestMeta) someMeta).getResultField().setResponseHeader("headerfield");
    }
  }
}
