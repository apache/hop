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

import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.mongo.MongoUtilLogger;

/** Created by bryan on 8/7/14. */
public class HopMongoUtilLogger implements MongoUtilLogger {
  private final ILogChannel iLogChannel;

  public HopMongoUtilLogger(ILogChannel iLogChannel) {
    this.iLogChannel = iLogChannel;
  }

  @Override
  public void debug(String s) {
    if (iLogChannel != null) {
      iLogChannel.logDebug(s);
    }
  }

  @Override
  public void info(String s) {
    if (iLogChannel != null) {
      iLogChannel.logBasic(s);
    }
  }

  @Override
  public void warn(String s, Throwable throwable) {
    if (iLogChannel != null) {
      iLogChannel.logError(s, throwable);
    }
  }

  @Override
  public void error(String s, Throwable throwable) {
    if (iLogChannel != null) {
      iLogChannel.logError(s, throwable);
    }
  }

  @Override
  public boolean isDebugEnabled() {
    if (iLogChannel != null) {
      return iLogChannel.isDebug();
    } else {
      return false;
    }
  }
}
