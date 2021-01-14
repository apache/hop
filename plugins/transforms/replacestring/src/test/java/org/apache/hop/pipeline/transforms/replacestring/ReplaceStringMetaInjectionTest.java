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

package org.apache.hop.pipeline.transforms.replacestring;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Created by bmorrise on 3/21/16.
 */
public class ReplaceStringMetaInjectionTest extends BaseMetadataInjectionTest<ReplaceStringMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setup() throws Exception {
    setup( new ReplaceStringMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "FIELD_IN_STREAM", () -> meta.getFieldInStream()[ 0 ] );
    check( "FIELD_OUT_STREAM", () -> meta.getFieldOutStream()[ 0 ] );
    check( "USE_REGEX", () -> meta.getUseRegEx()[ 0 ] );
    check( "REPLACE_STRING", () -> meta.getReplaceString()[ 0 ] );
    check( "REPLACE_BY", () -> meta.getReplaceByString()[ 0 ] );
    check( "EMPTY_STRING", () -> meta.isSetEmptyString()[ 0 ] );
    check( "REPLACE_WITH_FIELD", () -> meta.getFieldReplaceByString()[ 0 ] );
    check( "REPLACE_WHOLE_WORD", () -> meta.getWholeWord()[ 0 ] );
    check( "CASE_SENSITIVE", () -> meta.getCaseSensitive()[ 0 ] );
    check( "IS_UNICODE", () -> meta.isUnicode()[ 0 ] );
  }
}
