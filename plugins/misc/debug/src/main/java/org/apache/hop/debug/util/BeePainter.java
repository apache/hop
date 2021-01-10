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

package org.apache.hop.debug.util;

import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.gui.Rectangle;
import org.apache.hop.core.svg.SvgFile;

public class BeePainter {

  public Rectangle drawBee( IGc gc, int x, int y, int iconSize, ClassLoader classLoader ) throws Exception {
    int imageWidth = 16;
    int imageHeight = 16;
    int locationX = x + iconSize;
    int locationY = y + iconSize - imageHeight - 5; // -5 to prevent us from hitting the left bottom circle of the icon

    gc.drawImage( new SvgFile( "bee.svg", classLoader ), locationX, locationY, imageWidth, imageHeight,  gc.getMagnification(), 0 );
    return new Rectangle( locationX, locationY, imageWidth, imageHeight );
  }
}
