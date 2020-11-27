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

package org.apache.hop.pipeline.transforms.metainject;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class MetaInjectMigrationTest {
  @Test
  public void test70() {
    Map<TargetTransformAttribute, SourceTransformField> targetSourceMapping = new HashMap<>();
    TargetTransformAttribute target = new TargetTransformAttribute( "transform", "SCHENAMENAMEFIELD", true );
    SourceTransformField source = new SourceTransformField( "transform", "field" );
    targetSourceMapping.put( target, source );

    MetaInjectMigration.migrateFrom70( targetSourceMapping );

    assertEquals( 1, targetSourceMapping.size() );
    TargetTransformAttribute target2 = targetSourceMapping.keySet().iterator().next();
    assertEquals( "SCHEMANAMEFIELD", target2.getAttributeKey() );
    assertEquals( target.getTransformName(), target2.getTransformName() );
    assertEquals( target.isDetail(), target2.isDetail() );

    assertEquals( source, targetSourceMapping.get( target2 ) );
  }
}
