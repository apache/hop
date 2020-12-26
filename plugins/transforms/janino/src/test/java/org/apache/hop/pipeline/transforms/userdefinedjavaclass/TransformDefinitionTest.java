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

package org.apache.hop.pipeline.transforms.userdefinedjavaclass;

import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * User: Dzmitry Stsiapanau Date: 2/6/14 Time: 2:29 PM
 */
public class TransformDefinitionTest {
  @Test
  public void testClone() throws Exception {
    try {
      TransformDefinition transformDefinition = new TransformDefinition( "tag", "transformName", null, "" );
      transformDefinition.clone();
    } catch ( NullPointerException npe ) {
      fail( "Null value is not handled" );
    }
  }

}
