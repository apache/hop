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

public class Rectangle {
  public int x;
  public int y;
  public int width;
  public int height;

  /**
   * @param x
   * @param y
   * @param width
   * @param height
   */
  public Rectangle( int x, int y, int width, int height ) {
    this.x = x;
    this.y = y;
    this.width = width;
    this.height = height;
  }

  public Rectangle(Rectangle r) {
    this.x = r.x;
    this.y = r.y;
    this.width = r.width;
    this.height = r.height;
  }

  @Override public String toString() {
    return "Rectangle(" + x + "," + y + "-" + width + "x" + height +')';
  }

  public boolean contains( int x2, int y2 ) {
    return x2 >= x && x2 <= x + width && y2 >= y && y2 <= y + height;
  }

  public int getCentreX() {
    return x+width/2;
  }

  public int getCentreY() {
    return y+height/2;
  }

  public double distance(Rectangle r) {
    double deltaX = getCentreX()-r.getCentreX();
    double deltaY = getCentreY()-r.getCentreY();
    return Math.sqrt( deltaX*deltaX + deltaY*deltaY );
  }

  public boolean isEmpty() {
    return width==0 && height==0;
  }
}
