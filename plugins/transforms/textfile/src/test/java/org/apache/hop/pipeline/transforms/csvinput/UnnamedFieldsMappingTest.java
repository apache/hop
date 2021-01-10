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

package org.apache.hop.pipeline.transforms.csvinput;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UnnamedFieldsMappingTest {

  private UnnamedFieldsMapping fieldsMapping;

  @Before
  public void before() {
    fieldsMapping = new UnnamedFieldsMapping( 2 );
  }

  @Test
  public void fieldMetaIndex() {
    assertEquals( 1, fieldsMapping.fieldMetaIndex( 1 ) );
  }

  @Test
  public void fieldMetaIndexWithUnexistingField() {
    assertEquals( IFieldsMapping.FIELD_DOES_NOT_EXIST, fieldsMapping.fieldMetaIndex( 2 ) );
  }

  @Test
  public void size() {
    assertEquals( 2, fieldsMapping.size() );
  }

  @Test
  public void mapping() {
    UnnamedFieldsMapping mapping = UnnamedFieldsMapping.mapping( 2 );
    assertEquals( 1, mapping.fieldMetaIndex( 1 ) );
  }

}
