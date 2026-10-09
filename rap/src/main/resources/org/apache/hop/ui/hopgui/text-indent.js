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
 * Tab and Shift+Tab indent the selected lines of a multi-line text field.
 *
 * The line rules match org.apache.hop.ui.core.widget.TextIndent. A line that already starts with a
 * tab, or HOP_TEXT_TAB_SIZE 0, gains a tab; otherwise the indent is window.hopTextTabSize spaces
 * (default 2). Monaco indents itself. Esc then Tab moves focus instead of indenting. This runs in
 * the capture phase and stops the indent key, so the browser does not move focus and the server
 * does not indent again.
 */
(function () {
  'use strict';

  var DEFAULT_SIZE = 2;
  var MAX_SIZE = 32;
  var focusExit = null;

  function tabSize() {
    var size = window.hopTextTabSize;
    if (typeof size !== 'number' || !isFinite(size)) {
      return DEFAULT_SIZE;
    }
    size = Math.floor(size);
    if (size === 0) {
      return 0;
    }
    if (size < 1 || size > MAX_SIZE) {
      return DEFAULT_SIZE;
    }
    return size;
  }

  function isTextArea(el) {
    return !!(el && el.tagName && el.tagName.toLowerCase() === 'textarea'
        && typeof el.selectionStart === 'number' && typeof el.selectionEnd === 'number');
  }

  function inMonaco(el) {
    return el.closest && el.closest('.monaco-editor');
  }

  function isBreak(character) {
    return character === '\n' || character === '\r';
  }

  function clamp(offset, length) {
    if (offset < 0) {
      return 0;
    }
    if (offset > length) {
      return length;
    }
    return offset;
  }

  function normalize(text, offset) {
    if (offset > 0 && offset < text.length
        && text.charAt(offset - 1) === '\r' && text.charAt(offset) === '\n') {
      return offset - 1;
    }
    return offset;
  }

  function lineStart(text, offset) {
    var index = normalize(text, clamp(offset, text.length));
    while (index > 0 && !isBreak(text.charAt(index - 1))) {
      index--;
    }
    return index;
  }

  function lineContentEnd(text, offset) {
    var index = normalize(text, clamp(offset, text.length));
    if (index < text.length && isBreak(text.charAt(index))) {
      return index;
    }
    while (index < text.length && !isBreak(text.charAt(index))) {
      index++;
    }
    return index;
  }

  function isAtNextLine(text, offset) {
    if (offset <= 0 || offset > text.length) {
      return false;
    }
    if (offset < text.length && text.charAt(offset - 1) === '\r' && text.charAt(offset) === '\n') {
      return false;
    }
    return isBreak(text.charAt(offset - 1));
  }

  function includedEnd(text, from, to) {
    if (to > from && isAtNextLine(text, to)) {
      return lineContentEnd(text, to - 1);
    }
    return lineContentEnd(text, to);
  }

  function breakEnd(text, offset) {
    if (offset >= text.length) {
      return offset;
    }
    var current = text.charAt(offset);
    if (current === '\r') {
      if (offset + 1 < text.length && text.charAt(offset + 1) === '\n') {
        return offset + 2;
      }
      return offset + 1;
    }
    if (current === '\n') {
      return offset + 1;
    }
    return offset;
  }

  function spaces(count) {
    var pad = '';
    for (var i = 0; i < count; i++) {
      pad += ' ';
    }
    return pad;
  }

  function indentLine(line, size) {
    if (size === 0 || (line.length > 0 && line.charAt(0) === '\t')) {
      return '\t' + line;
    }
    return spaces(size) + line;
  }

  function outdentLine(line, size) {
    if (line.length > 0 && line.charAt(0) === '\t') {
      return line.substring(1);
    }
    var removed = 0;
    var limit = Math.min(size, line.length);
    while (removed < limit && line.charAt(removed) === ' ') {
      removed++;
    }
    return line.substring(removed);
  }

  function mapPoint(already, pos, start, end, newStart, shift, outdent) {
    if (already >= 0 || pos < start || pos > end) {
      return already;
    }
    var relative = pos - start;
    var mapped = outdent ? Math.max(0, relative - shift) : relative + shift;
    return newStart + mapped;
  }

  function mapBreak(already, pos, start, end, newStart) {
    if (already >= 0 || pos <= start || pos > end) {
      return already;
    }
    return newStart + (pos - start);
  }

  /** Same result shape as TextIndent.edit, applied to the whole value. */
  function indentText(text, anchor, caret, size, outdent) {
    if (text == null) {
      text = '';
    }
    if (size < 0) {
      size = DEFAULT_SIZE;
    }
    var length = text.length;
    var from = clamp(Math.min(anchor, caret), length);
    var to = clamp(Math.max(anchor, caret), length);
    var blockStart = lineStart(text, from);
    var blockEnd = includedEnd(text, from, to);
    var replacement = '';
    var newFrom = -1;
    var newTo = -1;
    var cursor = blockStart;
    while (true) {
      var contentEnd = lineContentEnd(text, cursor);
      if (contentEnd > blockEnd) {
        contentEnd = blockEnd;
      }
      var line = text.substring(cursor, contentEnd);
      var changed = outdent ? outdentLine(line, size) : indentLine(line, size);
      var shift = Math.abs(changed.length - line.length);
      var lineNew = blockStart + replacement.length;
      newFrom = mapPoint(newFrom, from, cursor, contentEnd, lineNew, shift, outdent);
      newTo = mapPoint(newTo, to, cursor, contentEnd, lineNew, shift, outdent);
      replacement += changed;
      cursor = contentEnd;
      if (cursor >= blockEnd) {
        break;
      }
      var nextBreak = breakEnd(text, cursor);
      if (nextBreak > blockEnd) {
        nextBreak = blockEnd;
      }
      var breakNew = blockStart + replacement.length;
      newFrom = mapBreak(newFrom, from, cursor, nextBreak, breakNew);
      newTo = mapBreak(newTo, to, cursor, nextBreak, breakNew);
      replacement += text.substring(cursor, nextBreak);
      cursor = nextBreak;
      if (cursor >= blockEnd) {
        break;
      }
    }
    var delta = replacement.length - (blockEnd - blockStart);
    if (newFrom < 0) {
      newFrom = from + delta;
    }
    if (newTo < 0) {
      newTo = to + delta;
    }
    var next = text.substring(0, blockStart) + replacement + text.substring(blockEnd);
    return { text: next, selectionStart: newFrom, selectionEnd: newTo };
  }

  function canIndentElement(el) {
    return isTextArea(el) && !inMonaco(el) && !el.readOnly && !el.disabled;
  }

  document.addEventListener('keydown', function (event) {
    var key = event.key || '';
    var el = event.target;
    var escape = key === 'Escape' || key === 'Esc' || event.keyCode === 27;
    if (escape && !event.ctrlKey && !event.metaKey && !event.altKey && !event.shiftKey) {
      focusExit = canIndentElement(el) ? el : null;
      return;
    }
    var tab = key === 'Tab' || event.keyCode === 9;
    if (tab && focusExit && focusExit === el && !event.ctrlKey && !event.metaKey && !event.altKey) {
      focusExit = null;
      return;
    }
    focusExit = null;
    if (event.ctrlKey || event.metaKey || event.altKey) {
      return;
    }
    if (!tab) {
      return;
    }
    if (!canIndentElement(el)) {
      return;
    }
    var before = el.value || '';
    var start = el.selectionStart;
    var end = el.selectionEnd;
    var next = indentText(before, start, end, tabSize(), !!event.shiftKey);
    event.preventDefault();
    event.stopImmediatePropagation();
    if (next.text === before && next.selectionStart === start && next.selectionEnd === end) {
      return;
    }
    el.value = next.text;
    try {
      el.setSelectionRange(next.selectionStart, next.selectionEnd);
    } catch (e) {
      // The control does not expose a selection. The text is still indented.
    }
    // RAP syncs the text widget from the input event.
    el.dispatchEvent(new Event('input', { bubbles: true }));
  }, true);
})();
