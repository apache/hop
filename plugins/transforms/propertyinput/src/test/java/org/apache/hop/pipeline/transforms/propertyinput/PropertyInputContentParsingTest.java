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
package org.apache.hop.pipeline.transforms.propertyinput;

import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.ClassRule;
import org.junit.Test;

public class PropertyInputContentParsingTest extends BasePropertyParsingTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Test
  public void testDefaultOptions() throws Exception {
    init( "default.properties" );

    PropertyInputField f1 = new PropertyInputField( "f1" );
    f1.setColumn( PropertyInputField.COLUMN_KEY );
    PropertyInputField f2 = new PropertyInputField( "f2" );
    f2.setColumn( PropertyInputField.COLUMN_VALUE );
    setFields( f1, f2 );

    process();

    check( new Object[][] { { "f1", "d1" }, { "f2", "d2" } } );
  }
}
