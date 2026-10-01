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

//# sourceURL=gc-pending-images.js

/**
 * Stops a disposed RAP canvas from throwing when an image it was still loading arrives.
 *
 * RAP's rwt.widgets.GC draws images asynchronously: when drawImage is asked for an image the
 * browser has not loaded yet, drawing pauses and an onload handler finishes it later. A new
 * draw() clears those handlers, but destruct() does not (RAP 4.7.0 and 4.8.0, unchanged
 * upstream since 2022): it just nulls the 2D context. So a canvas that is disposed while one of
 * its images is in flight - a context dialog closed the moment it appears, before every action
 * icon has arrived - later runs onload against a dead GC and logs
 * "Uncaught TypeError: Cannot read properties of null (reading 'save')".
 *
 * Nothing is lost for the user (the widget is gone), but it is an uncaught error on every
 * quick close, and the selenium tests rightly fail on any browser console error. Drop the
 * pending handlers before the destructor runs, which is what draw() already does.
 */
(function () {
    "use strict";

    if (typeof rwt === "undefined" || !rwt.widgets || !rwt.widgets.GC) {
        return;
    }
    var GC = rwt.widgets.GC;
    // rwt.qx.Class stores "destruct" here, and rwt.qx.Object.dispose calls it per class.
    var destruct = GC.$$destructor;
    if (!destruct || destruct.hopDropsPendingImages) {
        return;
    }
    GC.$$destructor = function () {
        if (typeof this._cleanPendingImages === "function") {
            this._cleanPendingImages();
        }
        destruct.call(this);
    };
    GC.$$destructor.hopDropsPendingImages = true;
})();
