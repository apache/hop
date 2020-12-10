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

package org.apache.hop.pipeline.transforms.checksum;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IntLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class CheckSumMetaTest implements IInitializer<CheckSumMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( CheckSumMeta someMeta ) {
    someMeta.allocate( 7 );  }

  @Test
  public void testConstants() {
    assertEquals( "CRC32", CheckSumMeta.TYPE_CRC32 );
    assertEquals( "CRC32", CheckSumMeta.checksumtypeCodes[ 0 ] );
    assertEquals( "ADLER32", CheckSumMeta.TYPE_ADLER32 );
    assertEquals( "ADLER32", CheckSumMeta.checksumtypeCodes[ 1 ] );
    assertEquals( "MD5", CheckSumMeta.TYPE_MD5 );
    assertEquals( "MD5", CheckSumMeta.checksumtypeCodes[ 2 ] );
    assertEquals( "SHA-1", CheckSumMeta.TYPE_SHA1 );
    assertEquals( "SHA-1", CheckSumMeta.checksumtypeCodes[ 3 ] );
    assertEquals( "SHA-256", CheckSumMeta.TYPE_SHA256 );
    assertEquals( "SHA-256", CheckSumMeta.checksumtypeCodes[ 4 ] );
    assertEquals( "SHA-384", CheckSumMeta.TYPE_SHA384 );
    assertEquals( "SHA-384", CheckSumMeta.checksumtypeCodes[ 5 ] );
    assertEquals( "SHA-512", CheckSumMeta.TYPE_SHA512 );
    assertEquals( "SHA-512", CheckSumMeta.checksumtypeCodes[ 6 ] );
    assertEquals( CheckSumMeta.checksumtypeCodes.length, CheckSumMeta.checksumtypeDescs.length );
  }

  @Test
  public void testSerialization() throws HopException {
    List<String> attributes =
      Arrays.asList( "FieldName", "ResultFieldName", "CheckSumType", "ResultType" );

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();
    getterMap.put( "CheckSumType", "getTypeByDesc" );

    IFieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), 5 );

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "FieldName", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "CheckSumType", new IntLoadSaveValidator( CheckSumMeta.checksumtypeCodes.length ) );
    attrValidatorMap.put( "ResultType", new IntLoadSaveValidator( CheckSumMeta.resultTypeCode.length ) );

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    LoadSaveTester<CheckSumMeta> loadSaveTester =
      new LoadSaveTester<>( CheckSumMeta.class, attributes, getterMap, setterMap,
        attrValidatorMap, typeValidatorMap, this );

    loadSaveTester.testSerialization();
  }
}
