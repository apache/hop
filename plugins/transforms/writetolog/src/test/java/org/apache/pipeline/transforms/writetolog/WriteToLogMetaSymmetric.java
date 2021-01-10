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
package org.apache.pipeline.transforms.writetolog;

import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.pipeline.transforms.writetolog.WriteToLogMeta;

public class WriteToLogMetaSymmetric extends WriteToLogMeta {

  /**
   * This class is here because of the asymmetry in WriteToLogMeta
   * with respect to the getter and setter for the "loglevel" variable.
   * The only getter for the variable actually returns a LogLevel object,
   * and the setter expects an int. The underlying storage is a String.
   * This asymmetry causes issues with test harnesses using reflection.
   * <p>
   * MB - 5/2016
   */
  public WriteToLogMetaSymmetric() {
    super();
  }

  public String getLogLevelString() {
    LogLevel lvl = super.getLogLevelByDesc();
    if ( lvl == null ) {
      lvl = LogLevel.BASIC;
    }
    return WriteToLogMeta.logLevelCodes[ lvl.getLevel() ];
  }

  public void setLogLevelString( String value ) {
    LogLevel lvl = LogLevel.getLogLevelForCode( value );
    super.setLogLevel( lvl.getLevel() );
  }

}
