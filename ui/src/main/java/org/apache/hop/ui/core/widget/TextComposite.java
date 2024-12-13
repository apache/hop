/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.core.widget;

import java.util.List;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.widgets.Composite;

public abstract class TextComposite extends Composite {
  /**
   * Constructs a new instance of this class given its parent and a style value describing its
   * behavior and appearance.
   *
   * <p>The style value is either one of the style constants defined in class <code>SWT</code> which
   * is applicable to instances of this class, or must be built by <em>bitwise OR</em>'ing together
   * (that is, using the <code>int</code> "|" operator) two or more of those <code>SWT</code> style
   * constants. The class description lists the style constants that are applicable to the class.
   * Style bits are also inherited from superclasses.
   *
   * @param parent a widget which will be the parent of the new instance (cannot be null)
   * @param style the style of widget to construct
   * @throws IllegalArgumentException
   */
  public TextComposite(Composite parent, int style) {
    super(parent, style);
  }

  public abstract void addModifyListener(ModifyListener lsMod);

  public abstract void addLineStyleListener();

  public abstract void addLineStyleListener(List<String> keywords);

  public void addLineStyleListener(String scriptEngine) {
    throw new UnsupportedOperationException("Cannot specify a script engine");
  }

  public abstract int getLineNumber();

  public abstract int getColumnNumber();

  public abstract String getText();

  public abstract void setText(String text);

  public abstract String getSelectionText();

  public abstract int getCaretOffset();

  public abstract void insert(String strInsert);

  public abstract void setSelection(int iStart, int length);

  public abstract int getSelectionCount();
}
