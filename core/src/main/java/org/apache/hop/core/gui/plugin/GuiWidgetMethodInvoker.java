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
package org.apache.hop.core.gui.plugin;

import java.lang.reflect.Method;

/**
 * Invokes a {@code @GuiWidgetElement} BUTTON method against the live editor object.
 *
 * <p>The GUI registry keeps the {@link Method} captured when {@code GuiPluginType} scanned the
 * class. That scan can use a different {@link ClassLoader} than the metadata serializer, so
 * instantiating the scanned class and casting the editor object to it throws {@link
 * ClassCastException} even though both classes have the same name. When the source object's class
 * has the same name, this invoker looks the method up on that class and calls it there.
 */
public final class GuiWidgetMethodInvoker {

  private GuiWidgetMethodInvoker() {}

  /**
   * Invoke {@code scannedMethod} so that the method body and the {@code sourceObject} argument
   * share a class loader whenever they are the same class by name.
   *
   * @param scannedMethod the method stored in the GUI registry
   * @param sourceObject the live editor / metadata object, passed as the method argument
   * @throws Exception if construction or invocation fails
   */
  public static void invoke(Method scannedMethod, Object sourceObject) throws Exception {
    Resolved resolved = resolve(scannedMethod, sourceObject);
    resolved.method.invoke(resolved.target, sourceObject);
  }

  static Resolved resolve(Method scannedMethod, Object sourceObject) throws Exception {
    Class<?> scannedClass = scannedMethod.getDeclaringClass();
    if (sourceObject != null && scannedClass.isInstance(sourceObject)) {
      return new Resolved(scannedMethod, sourceObject);
    }
    if (sourceObject != null && sourceObject.getClass().getName().equals(scannedClass.getName())) {
      return new Resolved(findOnSource(scannedMethod, sourceObject.getClass()), sourceObject);
    }
    return new Resolved(scannedMethod, scannedClass.getDeclaredConstructor().newInstance());
  }

  private static Method findOnSource(Method scannedMethod, Class<?> sourceClass)
      throws NoSuchMethodException {
    Class<?>[] scannedParams = scannedMethod.getParameterTypes();
    for (Method candidate : sourceClass.getMethods()) {
      if (!candidate.getName().equals(scannedMethod.getName())) {
        continue;
      }
      Class<?>[] candidateParams = candidate.getParameterTypes();
      if (sameParameterNames(scannedParams, candidateParams)) {
        return candidate;
      }
    }
    throw new NoSuchMethodException(
        sourceClass.getName() + "." + scannedMethod.getName() + " with matching parameters");
  }

  private static boolean sameParameterNames(Class<?>[] left, Class<?>[] right) {
    if (left.length != right.length) {
      return false;
    }
    for (int i = 0; i < left.length; i++) {
      if (!left[i].getName().equals(right[i].getName())) {
        return false;
      }
    }
    return true;
  }

  static final class Resolved {
    final Method method;
    final Object target;

    Resolved(Method method, Object target) {
      this.method = method;
      this.target = target;
    }
  }
}
