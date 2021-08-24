/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.core.gui.plugin.callback;

import java.lang.reflect.Method;

public class GuiCallbackMethod {
  private String callbackId;
  private Class<?> singletonClass;
  private Method callbackMethod;

  public GuiCallbackMethod(String callbackId, Class<?> singletonClass, Method callbackMethod) {
    this.callbackId = callbackId;
    this.singletonClass = singletonClass;
    this.callbackMethod = callbackMethod;
  }

  public void execute() {
    try {
      Method getInstanceMethod = singletonClass.getMethod("getInstance");
      Object singleton = getInstanceMethod.invoke(null);
      callbackMethod.invoke(singleton);
    } catch (Exception e) {
      throw new RuntimeException(
          "Error calling callback method with ID "
              + callbackId
              + " in class "
              + singletonClass.getName()
              + " with method "
              + callbackMethod.getName(),
          e);
    }
  }

  /**
   * Gets callbackId
   *
   * @return value of callbackId
   */
  public String getCallbackId() {
    return callbackId;
  }

  /** @param callbackId The callbackId to set */
  public void setCallbackId(String callbackId) {
    this.callbackId = callbackId;
  }

  /**
   * Gets singletonClass
   *
   * @return value of singletonClass
   */
  public Class<?> getSingletonClass() {
    return singletonClass;
  }

  /** @param singletonClass The singletonClass to set */
  public void setSingletonClass(Class<?> singletonClass) {
    this.singletonClass = singletonClass;
  }

  /**
   * Gets callbackMethod
   *
   * @return value of callbackMethod
   */
  public Method getCallbackMethod() {
    return callbackMethod;
  }

  /** @param callbackMethod The callbackMethod to set */
  public void setCallbackMethod(Method callbackMethod) {
    this.callbackMethod = callbackMethod;
  }
}
