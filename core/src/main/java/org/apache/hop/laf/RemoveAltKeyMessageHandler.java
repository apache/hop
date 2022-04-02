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

package org.apache.hop.laf;

import org.apache.hop.i18n.IMessageHandler;
import org.apache.hop.i18n.LafMessageHandler;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

class RemoveAltKeyMessageHandler implements IMessageHandler {
  private final IMessageHandler defMessageHandler;
  private final Pattern altKeyPattern = Pattern.compile("\\(&[A-Z]\\)(\\.{3})?$");

  public RemoveAltKeyMessageHandler() {
    this(new LafMessageHandler());
  }

  RemoveAltKeyMessageHandler(IMessageHandler messageHandler) {
    this.defMessageHandler = messageHandler;
  }

  @Override
  public String getString(String key) {
    return trimAltKey(defMessageHandler.getString(key));
  }

  @Override
  public String getString(String packageName, String key) {
    return trimAltKey(defMessageHandler.getString(packageName, key));
  }

  @Override
  public String getString(String packageName, String key, String... parameters) {
    return trimAltKey(defMessageHandler.getString(packageName, key, parameters));
  }

  @Override
  public String getString(
      String packageName, String key, Class<?> resourceClass, String... parameters) {
    return trimAltKey(defMessageHandler.getString(packageName, key, resourceClass, parameters));
  }

  private String trimAltKey(String value) {
    Matcher matcher = altKeyPattern.matcher(value);
    if (matcher.find()) {
      value = value.substring(0, matcher.start());
      if (matcher.group(1) != null){
          value += matcher.group(1);
      }
    }
    return value;
  }
}
