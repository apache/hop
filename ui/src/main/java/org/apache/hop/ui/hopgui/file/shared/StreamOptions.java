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

package org.apache.hop.ui.hopgui.file.shared;

import org.apache.hop.core.gui.Point;
import org.apache.hop.pipeline.transform.errorhandling.IStream;

import java.util.List;

public class StreamOptions {
  private List<IStream> options;
  private Point location;

  /**
   * @param options
   * @param location
   */
  public StreamOptions( List<IStream> options, Point location ) {
    this.options = options;
    this.location = location;
  }

  /**
   * @return the options
   */
  public List<IStream> getOptions() {
    return options;
  }

  /**
   * @param options the options to set
   */
  public void setOptions( List<IStream> options ) {
    this.options = options;
  }

  /**
   * @return the location
   */
  public Point getLocation() {
    return location;
  }

  /**
   * @param location the location to set
   */
  public void setLocation( Point location ) {
    this.location = location;
  }

}
