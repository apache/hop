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

package org.apache.hop.pipeline;

import org.apache.hop.base.LoadSaveBase;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class TransformLoadSaveTester<T extends ITransformMeta> extends LoadSaveBase<T> {

  public TransformLoadSaveTester( Class<T> clazz, List<String> commonAttributes, List<String> xmlAttributes,
                                  Map<String, String> getterMap, Map<String, String> setterMap,
                                  Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap,
                                  Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorTypeMap ) {
    super( clazz, commonAttributes, xmlAttributes, getterMap, setterMap,
      fieldLoadSaveValidatorAttributeMap, fieldLoadSaveValidatorTypeMap );
  }

  public TransformLoadSaveTester( Class<T> clazz, List<String> commonAttributes ) {
    super( clazz, commonAttributes );
  }

  public void testSerialization() throws HopException {
    testXmlRoundTrip();
    testClone();
  }

  public void testXmlRoundTrip() throws HopException {
    T metaToSave = createMeta();
    Map<String, IFieldLoadSaveValidator<?>> validatorMap =
      createValidatorMapAndInvokeSetters( xmlAttributes, metaToSave );
    T metaLoaded = createMeta();
    String xml = "<transform>" + metaToSave.getXml() + "</transform>";
    InputStream is = new ByteArrayInputStream( xml.getBytes() );
    metaLoaded.loadXml( XmlHandler.getSubNode( XmlHandler.loadXmlFile( is, null, false, false ), "transform" ), metadataProvider );
    validateLoadedMeta( xmlAttributes, validatorMap, metaToSave, metaLoaded );
  }


  protected void testClone() {
    T metaToSave = createMeta();
    Map<String, IFieldLoadSaveValidator<?>> validatorMap =
      createValidatorMapAndInvokeSetters( xmlAttributes, metaToSave );

    @SuppressWarnings( "unchecked" )
    T metaLoaded = (T) metaToSave.clone();
    validateLoadedMeta( xmlAttributes, validatorMap, metaToSave, metaLoaded );
  }
}
