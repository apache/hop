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
package org.apache.hop.pipeline.transforms.mqtt.subscriber;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.junit.Test;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class MQTTSubscriberMetaTest {

@Test public void testRoundTrips()
      throws HopException, NoSuchMethodException, SecurityException {
    Map<String, String> getterMap = new HashMap<String, String>();
    getterMap.put( "CA_FILE", "getSSLCaFile" );
    getterMap.put( "CERT_FILE", "getSSLCertFile" );
    getterMap.put( "KEY_FILE", "getSSLKeyFile" );
    getterMap.put( "KEY_FILE_PASS", "getSSLKeyFilePass" );
    getterMap.put( "KEEP_ALIVE", "getKeepAliveInterval" );
    getterMap.put( "READ_OBJECTS", "getAllowReadMessageOfTypeObject" );

    Map<String, String> setterMap = new HashMap<String, String>();
    setterMap.put( "CA_FILE", "setSSLCaFile" );
    setterMap.put( "CERT_FILE", "setSSLCertFile" );
    setterMap.put( "KEY_FILE", "setSSLKeyFile" );
    setterMap.put( "KEY_FILE_PASS", "setSSLKeyFilePass" );
    setterMap.put( "KEEP_ALIVE", "setKeepAliveInterval" );
    getterMap.put( "READ_OBJECTS", "setAllowReadMessageOfTypeObject" );

    Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap = new HashMap<>();
    Map<String, IFieldLoadSaveValidator<?>>
        fieldLoadSaveValidatorTypeMap =
        new HashMap<String, IFieldLoadSaveValidator<?>>();

    LoadSaveTester
        tester =
        new LoadSaveTester( MQTTSubscriberMeta.class,
            Arrays.<String>asList( "broker", "topics", "message_type", "client_id", "timeout", "qo_s",
                "execute_for_duration", "requires_auth", "password", "username" ), getterMap, setterMap,
            fieldLoadSaveValidatorAttributeMap, fieldLoadSaveValidatorTypeMap );

    tester.testSerialization();
  }
}
