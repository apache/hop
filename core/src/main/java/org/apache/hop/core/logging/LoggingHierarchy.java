/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.core.logging;

/**
 * The logging hierarchy of a pipeline or workflow
 *
 * @author matt
 */
public class LoggingHierarchy {
  private String rootChannelId; // from the xform or workflow
  private ILoggingObject loggingObject;

  /**
   * @return the rootChannelId
   */
  public String getRootChannelId() {
    return rootChannelId;
  }

  /**
   * @param rootChannelId the rootChannelId to set
   */
  public void setRootChannelId( String rootChannelId ) {
    this.rootChannelId = rootChannelId;
  }


  /**
   * @return the loggingObject
   */
  public ILoggingObject getLoggingObject() {
    return loggingObject;
  }

  /**
   * @param loggingObject the loggingObject to set
   */
  public void setLoggingObject( ILoggingObject loggingObject ) {
    this.loggingObject = loggingObject;
  }

  /**
   * @param rootChannelId
   * @param loggingObject
   */
  public LoggingHierarchy( String rootChannelId, ILoggingObject loggingObject ) {
    this.rootChannelId = rootChannelId;
    this.loggingObject = loggingObject;
  }

}
