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
//# sourceURL=dnd-cursor-passthrough.js

/**
 * Keeps RAP's drag-and-drop cursor icon from catching the pointer it follows.
 *
 * During a drag the RAP client draws the operation icon (move / copy / no-drop) as a small
 * absolutely positioned widget 5 px right of and 15 px below the pointer, on top of everything
 * else. Its element is an ordinary div, so a quick downward move lands the pointer inside it:
 * the drop target is then resolved from the icon, which has none, and the client tells the
 * server the drag left the folder or canvas it was over. Tab drags towards the bottom of a
 * folder lost their drop that way (RAP 4.7.0 / 4.8.0, unchanged upstream).
 *
 * The icon is feedback, not a target: let the pointer pass through it.
 */
(function () {
    "use strict";

    if (typeof rwt === "undefined" || !rwt.event || !rwt.event.DragAndDropHandler) {
        return;
    }
    var handler = rwt.event.DragAndDropHandler.getInstance();
    var renderCursor = handler._renderCursor;
    if (typeof renderCursor !== "function" || renderCursor.hopPassesPointer) {
        return;
    }
    handler._renderCursor = function () {
        renderCursor.apply(this, arguments);
        var cursor = this.__cursor;
        var element = cursor && cursor.getElement ? cursor.getElement() : null;
        if (element && element.style.pointerEvents !== "none") {
            element.style.pointerEvents = "none";
        }
    };
    handler._renderCursor.hopPassesPointer = true;
})();
