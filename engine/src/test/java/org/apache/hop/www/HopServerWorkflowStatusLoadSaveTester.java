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

package org.apache.hop.www;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.base.LoadSaveBase;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;

public class HopServerWorkflowStatusLoadSaveTester extends LoadSaveBase<HopServerWorkflowStatus> {

  public HopServerWorkflowStatusLoadSaveTester(
      Class<HopServerWorkflowStatus> clazz, List<String> commonAttributes) throws HopException {
    super(clazz, commonAttributes);
  }

  public HopServerWorkflowStatusLoadSaveTester(
      Class<HopServerWorkflowStatus> clazz,
      List<String> attributes,
      Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap)
      throws HopException {
    super(
        clazz,
        attributes,
        new HashMap<>(),
        new HashMap<>(),
        fieldLoadSaveValidatorAttributeMap,
        new HashMap<>());
  }

  public void testSerialization() throws HopException {
    testXmlRoundTrip();
  }

  protected void testXmlRoundTrip() throws HopException {
    HopServerWorkflowStatus metaToSave = createMeta();
    Map<String, IFieldLoadSaveValidator<?>> validatorMap =
        createValidatorMapAndInvokeSetters(attributes, metaToSave);

    String xml = metaToSave.getXml();
    HopServerWorkflowStatus metaLoaded = HopServerWorkflowStatus.fromXml(xml);
    validateLoadedMeta(attributes, validatorMap, metaToSave, metaLoaded);
  }
}
