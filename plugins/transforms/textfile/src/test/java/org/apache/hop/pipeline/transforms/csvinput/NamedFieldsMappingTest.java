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

public class NamedFieldsMappingTest {

  private NamedFieldsMapping fieldsMapping;

  @Before
  public void before() {
    fieldsMapping = new NamedFieldsMapping( new int[] { 3, 4 } );
  }

  @Test
  public void fieldMetaIndex() {
    assertEquals( 3, fieldsMapping.fieldMetaIndex( 0 ) );
  }

  @Test
  public void fieldMetaIndexWithUnexistingField() {
    assertEquals( IFieldsMapping.FIELD_DOES_NOT_EXIST, fieldsMapping.fieldMetaIndex( 4 ) );
  }

  @Test
  public void size() {
    assertEquals( 2, fieldsMapping.size() );
  }

  @Test
  public void mapping() {
    NamedFieldsMapping mapping =
      NamedFieldsMapping.mapping( new String[] { "FIRST", "SECOND", "THIRD" }, new String[] { "SECOND", "THIRD" } );
    assertEquals( 0, mapping.fieldMetaIndex( 1 ) );
  }

  @Test
  public void mappingWithNonUniqueColumnNames() {
    NamedFieldsMapping mapping =
      NamedFieldsMapping.mapping( new String[] { "Object", "Test", "Object" }, new String[] { "Object", "Test",
        "Object" } );
    assertEquals( 0, mapping.fieldMetaIndex( 0 ) );
    assertEquals( 2, mapping.fieldMetaIndex( 2 ) );
  }

  @Test
  public void fieldMetaIndexWithUnexistingField_nonUniqueColumnNames() {
    NamedFieldsMapping mapping =
      NamedFieldsMapping.mapping( new String[] { "Object", "Test", "Object" }, new String[] { "Object", "Test" } );
    assertEquals( IFieldsMapping.FIELD_DOES_NOT_EXIST, mapping.fieldMetaIndex( 2 ) );
  }

  @Test
  public void mappingWithNonMatchingColumnNames() {
    NamedFieldsMapping mapping =
      NamedFieldsMapping.mapping( new String[] { "One", "Two", "Three" }, new String[] { "A", "B", "C" } );
    assertEquals( 0, mapping.fieldMetaIndex( 0 ) ); // One -> A
    assertEquals( 1, mapping.fieldMetaIndex( 1 ) ); // Two -> B
    assertEquals( 2, mapping.fieldMetaIndex( 2 ) ); // Three -> C

    mapping =
      NamedFieldsMapping.mapping( new String[] { "A", "B", "A" }, new String[] { "A", "A" } );
    assertEquals( 0, mapping.fieldMetaIndex( 0 ) ); // A -> A
    assertEquals( IFieldsMapping.FIELD_DOES_NOT_EXIST, mapping.fieldMetaIndex( 1 ) ); // B -> undefined
    assertEquals( 1, mapping.fieldMetaIndex( 2 ) ); // A -> A

    mapping =
      NamedFieldsMapping.mapping( new String[] { "A", "B", "A" }, new String[] { "A", "X", "B", "Z" } );
    assertEquals( 0, mapping.fieldMetaIndex( 0 ) ); // A -> A
    assertEquals( 2, mapping.fieldMetaIndex( 1 ) ); // B -> B
    assertEquals( 1, mapping.fieldMetaIndex( 2 ) ); // A -> X
    assertEquals( IFieldsMapping.FIELD_DOES_NOT_EXIST, mapping.fieldMetaIndex( 3 ) ); // undefined -> undefined

    mapping =
      NamedFieldsMapping.mapping( new String[] { "A", "B", "A" }, new String[] { "A", "A", "C" } );
    assertEquals( 0, mapping.fieldMetaIndex( 0 ) ); // A -> A
    assertEquals( 2, mapping.fieldMetaIndex( 1 ) ); // B -> C
    assertEquals( 1, mapping.fieldMetaIndex( 2 ) ); // A -> A

    mapping =
      NamedFieldsMapping.mapping(
        new String[] { "A", "B", "C", "D", "E" },
        new String[] { "X", "C", "Y", "B" } );
    assertEquals( 0, mapping.fieldMetaIndex( 0 ) ); // A -> X
    assertEquals( 3, mapping.fieldMetaIndex( 1 ) ); // B -> B
    assertEquals( 1, mapping.fieldMetaIndex( 2 ) ); // C -> C
    assertEquals( 2, mapping.fieldMetaIndex( 3 ) ); // D -> Y
    assertEquals( IFieldsMapping.FIELD_DOES_NOT_EXIST, mapping.fieldMetaIndex( 4 ) ); // E -> undefined
  }

}
