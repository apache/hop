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

package org.apache.hop.ui.hopgui;

import java.text.MessageFormat;

public class ImplementationLoader {
  public static Object newInstance(final Class<?> type) {
    String name = type.getName();
    Object result = null;
    try {
      // This loads the implementation of the class type requested
      // For example, for class Canvas it creates a new instance of CanvasImpl,
      // HopGui.class gives new HopGuiImpl() and so on.
      //
      result = type.getClassLoader().loadClass(name + "Impl").getConstructor().newInstance();
    } catch (Throwable throwable) {
      String txt = "Could not load implementation for {0}";
      String msg = MessageFormat.format(txt, name);
      throw new RuntimeException(msg, throwable);
    }
    return result;
  }
}
