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
 * Select all text on Ctrl/Cmd+A.
 *
 * The same chord selects the graph, so it is in RAP CANCEL_KEYS and the browser never selects the
 * field. This runs in the capture phase, before that cancel, and leaves the event running so RAP
 * records the new selection. Monaco selects inside its own model and is left alone.
 */
(function () {
  'use strict';

  function isTextField(el) {
    if (!el || !el.tagName) {
      return false;
    }
    var tag = el.tagName.toLowerCase();
    if (tag === 'textarea') {
      return true;
    }
    if (tag !== 'input') {
      return false;
    }
    var type = (el.type || 'text').toLowerCase();
    if (type === 'hidden' || type === 'checkbox' || type === 'radio'
        || type === 'button' || type === 'submit' || type === 'file'
        || type === 'number' || type === 'range' || type === 'color') {
      return false;
    }
    return typeof el.selectionStart === 'number' && typeof el.selectionEnd === 'number';
  }

  function inMonaco(el) {
    return el.closest && el.closest('.monaco-editor');
  }

  document.addEventListener('keydown', function (event) {
    if (!(event.ctrlKey || event.metaKey) || event.altKey || event.shiftKey) {
      return;
    }
    var key = (event.key || '').toLowerCase();
    if (key !== 'a') {
      return;
    }
    var el = event.target;
    if (!isTextField(el) || inMonaco(el)) {
      return;
    }
    var length = (el.value || '').length;
    try {
      el.setSelectionRange(0, length);
    } catch (e) {
      try {
        el.select();
      } catch (ignored) {
        // The control does not expose a selection. Leave the key to the widget.
      }
    }
  }, true);
})();
