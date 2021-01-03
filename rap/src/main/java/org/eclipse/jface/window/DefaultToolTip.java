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

package org.eclipse.jface.window;

import org.apache.hop.ui.core.widget.CheckBoxToolTip;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.widgets.Control;

public class DefaultToolTip extends CheckBoxToolTip {

  public DefaultToolTip( Control control ) {
    super( control );
  }

  public DefaultToolTip( Control control, int style, boolean manualActivation ) {
    super( control );
  }

  public void setBackgroundColor(Color color) {
    // TODO Auto-generated method stub

  }

  public void setForegroundColor(Color color) {
    // TODO Auto-generated method stub

  }
}
