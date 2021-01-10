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

package org.apache.hop.i18n;

import java.util.Locale;

/**
 * Standard Message handler that takes a root package, plus key and resolves that into one/more resultant messages. This
 * IHandler is used by all message types to enable flexible look and feel as well as i18n to be implemented in variable
 * ways.
 *
 * @author dhushon
 */
public abstract class AbstractMessageHandler implements IMessageHandler {

  /**
   * forced override to allow singleton instantiation through dynamic class loader
   *
   * @return IMessageHandler
   * @see org.apache.hop.i18n.GlobalMessages for sample
   */
  public static synchronized IMessageHandler getInstance() {
    return null;
  }

  /**
   * forced override, concrete implementations must provide implementation
   *
   * @return Locale
   */
  public static synchronized Locale getLocale() {
    return null;
  }

  /**
   * forced override, concrete implementations must provide implementation
   *
   * @param newLocale
   */
  public static synchronized void setLocale( Locale newLocale ) {
  }

}
