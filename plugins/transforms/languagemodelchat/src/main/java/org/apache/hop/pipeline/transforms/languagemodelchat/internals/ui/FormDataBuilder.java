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

package org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui;

import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Control;

public class FormDataBuilder {

  public static final class Builder {
    private Control control;
    private int margin;
    private FormAttachment left;
    private FormAttachment right;

    private Builder() {}

    public static Builder buildFormData() {
      return new Builder();
    }

    public Builder control(Control control) {
      this.control = control;
      return this;
    }

    public Builder margin(int margin) {
      this.margin = margin;
      return this;
    }

    public Builder left(int numerator, int offset) {
      this.left = new FormAttachment(numerator, offset);
      return this;
    }

    public Builder right(int numerator, int offset) {
      this.right = new FormAttachment(numerator, offset);
      return this;
    }

    public FormData build() {
      FormData e = new FormData();
      e.left = this.left;
      e.right = this.right;
      e.top = new FormAttachment(control, margin);
      return e;
    }
  }
}
