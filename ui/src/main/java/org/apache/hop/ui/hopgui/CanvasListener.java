/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.ui.hopgui;

import org.eclipse.swt.widgets.Listener;

public class CanvasListener {
  private static final ISingletonProvider PROVIDER;
  static {
    PROVIDER = (ISingletonProvider) ImplementationLoader.newInstance( CanvasListener.class );
  }
  public static final Listener getInstance() {
    return (Listener) PROVIDER.getInstanceInternal();
  }
}
