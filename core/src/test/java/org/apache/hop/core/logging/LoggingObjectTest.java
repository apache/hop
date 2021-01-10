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


package org.apache.hop.core.logging;

import junit.framework.Assert;
import org.junit.Test;

public class LoggingObjectTest {

  @Test
  public void testEquals() throws Exception {
    ILoggingObject parent = new LoggingObject( new SimpleLoggingObject( "parent", LoggingObjectType.WORKFLOW, null ) );

    LoggingObject loggingObject1 = new LoggingObject( "test" );
    loggingObject1.setFilename( "fileName" );
    loggingObject1.setParent( parent );
    loggingObject1.setObjectName( "job1" );


    LoggingObject loggingObject2 = new LoggingObject( "test" );
    loggingObject2.setFilename( "fileName" );
    loggingObject2.setParent( parent );
    loggingObject2.setObjectName( "job2" );

    Assert.assertFalse( loggingObject1.equals( loggingObject2 ) );
  }

}
