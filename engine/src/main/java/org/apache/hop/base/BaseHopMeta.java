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
package org.apache.hop.base;

import org.apache.hop.core.xml.IXml;


/**
 * This class defines a base hop from one action copy to another, or from one transform to another.
 *
 * @author Alexander Buloichik
 */
public abstract class BaseHopMeta<T> implements IXml {
  public static final String XML_TAG = "hop";

  public boolean split = false;
  protected T from, to;
  protected boolean enabled;
  protected boolean changed;
  private boolean errorHop;

  public BaseHopMeta() {
  }

  public BaseHopMeta( boolean split, T from, T to, boolean enabled, boolean changed, boolean errorHop ) {
    this.split = split;
    this.from = from;
    this.to = to;
    this.enabled = enabled;
    this.changed = changed;
    this.errorHop = errorHop;
  }

  public void setChanged() {
    setChanged( true );
  }

  public void setChanged( boolean ch ) {
    changed = ch;
  }

  public boolean hasChanged() {
    return changed;
  }

  public void setEnabled() {
    setEnabled( true );
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled( boolean en ) {
    if ( enabled != en ) {
      setChanged();
      enabled = en;
    }
  }

  public boolean isErrorHop() {
    return errorHop;
  }

  public void setErrorHop( boolean errorHop ) {
    this.errorHop = errorHop;
  }

  /**
   * Gets split
   *
   * @return value of split
   */
  public boolean isSplit() {
    return split;
  }

  /**
   * @param split The split to set
   */
  public void setSplit( boolean split ) {
    this.split = split;
  }
}
