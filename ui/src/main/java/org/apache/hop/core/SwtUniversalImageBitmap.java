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

package org.apache.hop.core;

import org.eclipse.swt.graphics.Device;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Transform;

public class SwtUniversalImageBitmap extends SwtUniversalImage {
  private final Image bitmap;
  private final double zoomFactor;

  public SwtUniversalImageBitmap( Image bitmap, double zoomFactor ) {
    this.bitmap = bitmap;
    this.zoomFactor = zoomFactor;
  }

  @Override
  public synchronized void dispose() {
    super.dispose();
    if ( !bitmap.isDisposed() ) {
      bitmap.dispose();
    }
  }

  @Override
  protected Image renderSimple( Device device ) {
    return bitmap;
  }

  @Override
  protected Image renderSimple( Device device, int width, int height ) {
    return renderRotated( device, width, height, 0d );
  }

  @Override
  protected Image renderRotated( Device device, int width, int height, double angleRadians ) {
    Image result = new Image( device, width * 2, height * 2 );

    GC gc = new GC( result );

    int bw = bitmap.getBounds().width;
    int bh = bitmap.getBounds().height;
    Transform affineTransform = new Transform( device );
    affineTransform.translate( width, height );
    affineTransform.rotate( (float) Math.toDegrees( angleRadians ) );
    affineTransform.scale( (float) zoomFactor * width / bw, (float) zoomFactor * height / bh );
    gc.setTransform( affineTransform );

    gc.drawImage( bitmap, 0, 0, bw, bh, -bw / 2, -bh / 2, bw, bh );

    gc.dispose();

    return result;
  }
}
