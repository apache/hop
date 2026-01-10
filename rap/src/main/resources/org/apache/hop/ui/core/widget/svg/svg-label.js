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
 *
 */

const handleEvent = function (event) {
    const widget = event.widget;

    const props = widget.getData('props');
    const id = props.id;

    let img = document.getElementById(id);

    if (img===null) {
        return;
    }

    switch (event.type) {
        case SWT.MouseDown:
            // Handled by regular SWT listener
            break;
        case SWT.Hide:
            // The widget is hidden so changing anything doesn't matter.
            break;
        case SWT.Show:
            // We always get one call here, so we can set the background transparent and adjust the style in general.
            //
            img.style.background = 'transparent';
            img.style.opacity = '1.0';
            img.parentElement.style.background = 'transparent';
            img.parentElement.parentElement.style.background = 'transparent';
            break;
        case SWT.MouseEnter:
            // Try to make the background a bit gray
            //
            if (props.enabled) {
                img.style.background = 'rgb(200, 200, 200)';
            }
            break;
        case SWT.MouseExit:
            // Set the background back to transparent.
            //
            if (props.enabled) {
                img.style.background = 'transparent';
            }
            break;
    }
}