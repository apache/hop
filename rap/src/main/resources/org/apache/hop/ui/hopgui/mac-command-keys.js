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

/**
 * Map Mac Command key to Ctrl for RAP ACTIVE_KEYS matching.
 * RAP only recognizes CTRL in key sequences; when the user presses Cmd+S on Mac,
 * the browser sends metaKey: true. We intercept in capture phase and dispatch
 * a synthetic event with ctrlKey: true so RAP matches "CTRL+S" and sends it to the server.
 * For Cmd+C/V/X/A we only leave them to the browser when focus is in a text field;
 * otherwise we remap so the application can handle copy/paste/cut/select-all.
 */
(function() {
  'use strict';

  function isMac() {
    var ua = navigator.userAgent || '';
    if (/iPhone|iPad|iPod/.test(ua)) return false;
    return /Mac|Macintosh/i.test(ua) || navigator.platform === 'MacIntel';
  }

  if (!isMac()) return;

  function isTextField(el) {
    if (!el || !el.tagName) return false;
    var tag = (el.tagName || '').toLowerCase();
    if (tag === 'input' || tag === 'textarea') return true;
    return el.isContentEditable === true;
  }

  var copyPasteKeys = { 'c': 1, 'v': 1, 'x': 1, 'a': 1 };

  document.addEventListener('keydown', function(event) {
    if (!event.metaKey || event.ctrlKey) return;
    var key = (event.key || '').toLowerCase();
    if (copyPasteKeys[key] && isTextField(event.target)) return;

    event.preventDefault();
    event.stopImmediatePropagation();

    var opts = {
      key: event.key,
      code: event.code,
      keyCode: event.keyCode,
      which: event.which,
      ctrlKey: true,
      metaKey: false,
      shiftKey: event.shiftKey,
      altKey: event.altKey,
      bubbles: true,
      cancelable: true
    };
    var synthetic = new KeyboardEvent('keydown', opts);
    event.target.dispatchEvent(synthetic);
  }, true);
})();
