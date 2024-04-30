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

package org.apache.hop.core.gui;

import java.util.Objects;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** A point but with double precision */
public class DPoint {

  public DPoint() {
    this(0.0, 0.0);
  }

  public DPoint(double x, double y) {
    this.x = x;
    this.y = y;
  }

  public DPoint(DPoint p) {
    this.x = p.x;
    this.y = p.y;
  }

  public DPoint(Point p) {
    this.x = p.x;
    this.y = p.y;
  }

  @HopMetadataProperty(key = "x")
  public double x;

  @HopMetadataProperty(key = "y")
  public double y;

  public void multiply(double factor) {
    x = x * factor;
    y = y * factor;
  }

  public Point toPoint() {
    return new Point((int) Math.round(x), (int) Math.round(y));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DPoint point = (DPoint) o;
    return x == point.x && y == point.y;
  }

  @Override
  public int hashCode() {
    return Objects.hash(x, y);
  }

  @Override
  public String toString() {
    return "DPoint(" + x + "," + y + ")";
  }

  /**
   * Gets x
   *
   * @return value of x
   */
  public double getX() {
    return x;
  }

  /**
   * Sets x
   *
   * @param x value of x
   */
  public void setX(double x) {
    this.x = x;
  }

  /**
   * Gets y
   *
   * @return value of y
   */
  public double getY() {
    return y;
  }

  /**
   * Sets y
   *
   * @param y value of y
   */
  public void setY(double y) {
    this.y = y;
  }
}
