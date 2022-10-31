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

package org.apache.hop.mongo.wrapper;

import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.MongoProp;
import org.apache.hop.mongo.MongoProperties;
import org.apache.hop.mongo.MongoUtilLogger;
import org.apache.hop.mongo.Util;

import java.lang.reflect.Proxy;

/**
 * MongoClientWrapperFactory is used to instantiate MongoClientWrapper objects appropriate for given
 * configuration properties, i.e. using the correct authentication mechanism, server info, and
 * MongoConfigurationOptions.
 */
public class MongoClientWrapperFactory {

  /**
   * @param props The MongoProperties to use for connection initialization
   * @param log MongoUtilLogger implementation used for all log output
   * @return MongoClientWrapper
   * @throws MongoDbException
   */
  public static org.apache.hop.mongo.wrapper.MongoClientWrapper createMongoClientWrapper(
      MongoProperties props, MongoUtilLogger log) throws MongoDbException {
    if (props.useKerberos()) {
      return initKerberosProxy(new KerberosMongoClientWrapper(props, log));
    } else if (!Util.isEmpty(props.get(MongoProp.USERNAME))
        || !Util.isEmpty(props.get(MongoProp.PASSWORD))
        || !Util.isEmpty(props.get(MongoProp.AUTH_DATABASE))) {
      return new org.apache.hop.mongo.wrapper.UsernamePasswordMongoClientWrapper(props, log);
    }
    // default
    return new org.apache.hop.mongo.wrapper.NoAuthMongoClientWrapper(props, log);
  }

  private static org.apache.hop.mongo.wrapper.MongoClientWrapper initKerberosProxy(
      KerberosMongoClientWrapper wrapper) {
    return (org.apache.hop.mongo.wrapper.MongoClientWrapper)
        Proxy.newProxyInstance(
            wrapper.getClass().getClassLoader(),
            new Class<?>[] {org.apache.hop.mongo.wrapper.MongoClientWrapper.class},
            new org.apache.hop.mongo.wrapper.KerberosInvocationHandler(
                wrapper.getAuthContext(), wrapper));
  }
}
