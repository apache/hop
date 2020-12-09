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

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * @author Andrey Khayrutdinov
 */
public abstract class WorkflowActionLoadSaveTestSupport<T extends IAction> {

  private LoadSaveTester<T> tester;

  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Before
  public void setUp() throws Exception {
    List<String> commonAttributes = listCommonAttributes();
    List<String> xmlAttributes = listXmlAttributes();
    Map<String, String> getters = createGettersMap();
    Map<String, String> setters = createSettersMap();
    Map<String, IFieldLoadSaveValidator<?>> attributeValidators = createAttributeValidatorsMap();
    Map<String, IFieldLoadSaveValidator<?>> typeValidators = createTypeValidatorsMap();

    assertTrue( !commonAttributes.isEmpty() || !xmlAttributes.isEmpty() );

    tester = new LoadSaveTester<>( getActionClass(), commonAttributes, xmlAttributes, getters, setters,
      attributeValidators, typeValidators );
  }

  @Test
  public void testSerialization() throws HopException {
    tester.testSerialization();
  }

  protected abstract Class<T> getActionClass();

  protected abstract List<String> listCommonAttributes();


  protected List<String> listXmlAttributes() {
    return Collections.emptyList();
  }

  protected Map<String, String> createGettersMap() {
    return Collections.emptyMap();
  }

  protected Map<String, String> createSettersMap() {
    return Collections.emptyMap();
  }

  protected Map<String, IFieldLoadSaveValidator<?>> createAttributeValidatorsMap() {
    return Collections.emptyMap();
  }

  protected Map<String, IFieldLoadSaveValidator<?>> createTypeValidatorsMap() {
    return Collections.emptyMap();
  }


  @SuppressWarnings( "unchecked" )
  protected static <T1, T2> Map<T1, T2> toMap( Object... pairs ) {
    Map<T1, T2> result = new HashMap<>( pairs.length );
    for ( int i = 0; i < pairs.length; i += 2 ) {
      T1 key = (T1) pairs[ i ];
      T2 value = (T2) pairs[ i + 1 ];
      result.put( key, value );
    }
    return result;
  }
}
