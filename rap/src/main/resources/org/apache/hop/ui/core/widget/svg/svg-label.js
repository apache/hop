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

function findImg(widget, id) {
    var img = document.getElementById(id);
    if (img === null && widget.$el && widget.$el[0]) {
        img = widget.$el[0].querySelector('img');
    }
    return img;
}

/** Apply transparent background (and optionally opacity) so toolbar has no white box. */
function applyShowStylesToImg(img, opacity) {
    if (!img) return;
    img.style.background = 'transparent';
    if (opacity != null) {
        img.style.opacity = opacity;
    }
    if (img.parentElement) {
        img.parentElement.style.background = 'transparent';
    }
    if (img.parentElement && img.parentElement.parentElement) {
        img.parentElement.parentElement.style.background = 'transparent';
    }
}

/** Only set transparent background; do not touch opacity (preserves enabled/disabled state). */
function applyTransparentBackgroundOnly(img) {
    if (!img) return;
    img.style.background = 'transparent';
    if (img.parentElement) {
        img.parentElement.style.background = 'transparent';
    }
    if (img.parentElement && img.parentElement.parentElement) {
        img.parentElement.parentElement.style.background = 'transparent';
    }
}

/**
 * Find the toolbar container and set transparent background on every img so the last item
 * is never missed. We do not set opacity here so we don't overwrite setEnabled (opacity 0.3).
 */
function styleAllToolbarImgsFrom(img) {
    if (!img) return;
    var node = img.parentElement;
    var toolbar = null;
    while (node) {
        if (node.children && node.children.length > 2) {
            toolbar = node;
            break;
        }
        node = node.parentElement;
    }
    if (!toolbar) return;
    for (var i = 0; i < toolbar.children.length; i++) {
        var child = toolbar.children[i];
        var im = child.querySelector ? child.querySelector('img') : null;
        if (im) {
            applyTransparentBackgroundOnly(im);
        }
    }
}

const handleEvent = function (event) {
    const widget = event.widget;

    const props = widget.getData('props');
    if (!props || props.id == null) {
        return;
    }
    const id = props.id;

    const img = findImg(widget, id);
    if (img === null) {
        return;
    }

    switch (event.type) {
        case SWT.MouseDown:
            break;
        case SWT.Hide:
            break;
        case SWT.Show:
            applyShowStylesToImg(img, props.enabled ? '1.0' : '0.3');
            styleAllToolbarImgsFrom(img);
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