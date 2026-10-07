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
 * Copy or cut the current line when a text field has no selection.
 *
 * Runs in the capture phase, during the key gesture, because a clipboard write started from the
 * server round trip is rejected by the browser. Monaco does this itself (emptySelectionClipboard).
 * Word movement is left to the browser: those chords are not in RAP CANCEL_KEYS.
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
    if (type === 'password' || type === 'hidden' || type === 'checkbox' || type === 'radio'
        || type === 'button' || type === 'submit' || type === 'file' || type === 'number') {
      return false;
    }
    return typeof el.selectionStart === 'number' && typeof el.selectionEnd === 'number';
  }

  function inMonaco(el) {
    return el.closest && el.closest('.monaco-editor');
  }

  /** Half-open [start, end) of the line containing caret. Matches TextLineClipboard.lineRange. */
  function lineRange(text, caret) {
    var length = text.length;
    if (caret < 0) {
      caret = 0;
    } else if (caret > length) {
      caret = length;
    }
    if (caret > 0 && caret < length && text.charAt(caret - 1) === '\r' && text.charAt(caret) === '\n') {
      caret--;
    }
    var start = caret;
    while (start > 0) {
      var previous = text.charAt(start - 1);
      if (previous === '\n' || previous === '\r') {
        break;
      }
      start--;
    }
    var end = caret;
    while (end < length) {
      var current = text.charAt(end);
      if (current === '\r') {
        end++;
        if (end < length && text.charAt(end) === '\n') {
          end++;
        }
        break;
      }
      if (current === '\n') {
        end++;
        break;
      }
      end++;
    }
    return { start: start, end: end };
  }

  document.addEventListener('keydown', function (event) {
    if (!(event.ctrlKey || event.metaKey) || event.altKey || event.shiftKey) {
      return;
    }
    var key = (event.key || '').toLowerCase();
    if (key !== 'c' && key !== 'x') {
      return;
    }
    var el = event.target;
    if (!isTextField(el) || inMonaco(el)) {
      return;
    }
    if (el.selectionStart !== el.selectionEnd) {
      return;
    }
    if (key === 'x' && (el.readOnly || el.disabled)) {
      return;
    }

    var caret = el.selectionStart;
    var before = el.value || '';
    var range = lineRange(before, caret);
    event.preventDefault();
    event.stopImmediatePropagation();

    var command = key === 'x' ? 'cut' : 'copy';
    el.setSelectionRange(range.start, range.end);
    var copied = false;
    try {
      copied = document.execCommand(command);
    } catch (e) {
      copied = false;
    }
    if (command === 'copy' || !copied) {
      el.setSelectionRange(caret, caret);
      return;
    }
    // RAP syncs the text widget from the input event. execCommand usually fires one; fire one
    // ourselves as well so a browser that does not still updates the server.
    if (el.value !== before) {
      el.dispatchEvent(new Event('input', { bubbles: true }));
    }
  }, true);
})();
