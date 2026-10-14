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

package org.apache.hop.openformula.parser;

import java.util.ArrayList;
import java.util.List;

public class FormulaLexer {
  private final String input;
  private final int length;
  private int pos = 0;
  private int line = 1;
  private int col = 1;

  public FormulaLexer(String input) {
    this.input = (input != null) ? input : "";
    this.length = this.input.length();
  }

  public List<Token> tokenize() throws ParseException {
    List<Token> tokens = new ArrayList<>();
    Token token;
    do {
      token = nextToken();
      tokens.add(token);
    } while (token.type() != TokenType.EOF);
    return tokens;
  }

  public Token nextToken() throws ParseException {
    skipWhitespaceAndComments();
    if (pos >= length) {
      return new Token(TokenType.EOF, "", line, col);
    }

    int startLine = line;
    int startCol = col;
    char c = peek();

    // String literal
    if (c == '"') {
      return readString(startLine, startCol);
    }

    // Column lookup: [ ... ]
    if (c == '[') {
      return readColumnLookup(startLine, startCol);
    }

    // Operators and delimiters
    if (c == '<') {
      advance();
      if (peek() == '>') {
        advance();
        return new Token(TokenType.NOT_EQUAL, "<>", startLine, startCol);
      }
      if (peek() == '=') {
        advance();
        return new Token(TokenType.LESS_EQUAL, "<=", startLine, startCol);
      }
      return new Token(TokenType.LESS, "<", startLine, startCol);
    }

    if (c == '>') {
      advance();
      if (peek() == '=') {
        advance();
        return new Token(TokenType.GREATER_EQUAL, ">=", startLine, startCol);
      }
      return new Token(TokenType.GREATER, ">", startLine, startCol);
    }

    if (c == '=') {
      advance();
      return new Token(TokenType.EQUAL, "=", startLine, startCol);
    }
    if (c == '+') {
      advance();
      return new Token(TokenType.PLUS, "+", startLine, startCol);
    }
    if (c == '-') {
      advance();
      return new Token(TokenType.MINUS, "-", startLine, startCol);
    }
    if (c == '*') {
      advance();
      return new Token(TokenType.STAR, "*", startLine, startCol);
    }
    if (c == '/') {
      advance();
      return new Token(TokenType.SLASH, "/", startLine, startCol);
    }
    if (c == '^') {
      advance();
      return new Token(TokenType.CARET, "^", startLine, startCol);
    }
    if (c == '&') {
      advance();
      return new Token(TokenType.AMPERSAND, "&", startLine, startCol);
    }
    if (c == '%') {
      advance();
      return new Token(TokenType.PERCENT, "%", startLine, startCol);
    }
    if (c == '(') {
      advance();
      return new Token(TokenType.LPAREN, "(", startLine, startCol);
    }
    if (c == ')') {
      advance();
      return new Token(TokenType.RPAREN, ")", startLine, startCol);
    }
    if (c == '{') {
      advance();
      return new Token(TokenType.LBRACE, "{", startLine, startCol);
    }
    if (c == '}') {
      advance();
      return new Token(TokenType.RBRACE, "}", startLine, startCol);
    }
    if (c == ';') {
      advance();
      return new Token(TokenType.SEMICOLON, ";", startLine, startCol);
    }
    if (c == '|') {
      advance();
      return new Token(TokenType.PIPE, "|", startLine, startCol);
    }
    if (c == ',') {
      advance();
      return new Token(TokenType.COMMA, ",", startLine, startCol);
    }

    // Number literal (starts with digit or '.' followed by digit)
    if (Character.isDigit(c)
        || (c == '.' && pos + 1 < length && Character.isDigit(input.charAt(pos + 1)))) {
      return readNumber(startLine, startCol);
    }

    // Identifier or keyword
    if (Character.isLetter(c) || c == '_' || c == '$' || c == '.') {
      return readIdentifier(startLine, startCol);
    }

    throw new ParseException("Unexpected character: '" + c + "'", startLine, startCol);
  }

  private void skipWhitespaceAndComments() {
    while (pos < length) {
      char c = peek();
      if (Character.isWhitespace(c)) {
        advance();
      } else if (c == '-' && pos + 1 < length && input.charAt(pos + 1) == '-') {
        // Comment until end of line
        while (pos < length && peek() != '\n' && peek() != '\r') {
          advance();
        }
      } else {
        break;
      }
    }
  }

  private Token readString(int startLine, int startCol) throws ParseException {
    advance(); // Consume opening quote
    StringBuilder sb = new StringBuilder();
    while (pos < length) {
      char c = peek();
      if (c == '"') {
        advance();
        if (pos < length && peek() == '"') {
          // Escaped quote ""
          sb.append('"');
          advance();
        } else {
          // Closing quote
          return new Token(TokenType.STRING_LITERAL, sb.toString(), startLine, startCol);
        }
      } else {
        sb.append(c);
        advance();
      }
    }
    throw new ParseException("Unterminated string literal", startLine, startCol);
  }

  private Token readColumnLookup(int startLine, int startCol) throws ParseException {
    advance(); // Consume '['
    StringBuilder sb = new StringBuilder();
    boolean quoted = false;

    if (pos < length && peek() == '"') {
      quoted = true;
      advance();
    }

    while (pos < length) {
      char c = peek();
      if (quoted) {
        if (c == '"') {
          advance();
          if (pos < length && peek() == '"') {
            sb.append('"');
            advance();
          } else {
            // End of quoted content, expect ']'
            while (pos < length && Character.isWhitespace(peek())) {
              advance();
            }
            if (pos < length && peek() == ']') {
              advance();
              return new Token(TokenType.COLUMN_LOOKUP, sb.toString(), startLine, startCol);
            }
            throw new ParseException("Expected ']' after quoted reference", line, col);
          }
        } else {
          sb.append(c);
          advance();
        }
      } else {
        if (c == ']') {
          advance();
          return new Token(TokenType.COLUMN_LOOKUP, sb.toString().trim(), startLine, startCol);
        }
        sb.append(c);
        advance();
      }
    }
    throw new ParseException("Unterminated column lookup reference '['", startLine, startCol);
  }

  private Token readNumber(int startLine, int startCol) {
    StringBuilder sb = new StringBuilder();
    boolean hasDot = false;
    boolean hasExp = false;

    while (pos < length) {
      char c = peek();
      if (Character.isDigit(c)) {
        sb.append(c);
        advance();
      } else if (c == '.' && !hasDot && !hasExp) {
        hasDot = true;
        sb.append(c);
        advance();
      } else if ((c == 'e' || c == 'E') && !hasExp) {
        hasExp = true;
        sb.append(c);
        advance();
        if (pos < length && (peek() == '+' || peek() == '-')) {
          sb.append(peek());
          advance();
        }
      } else {
        break;
      }
    }
    return new Token(TokenType.NUMERIC_LITERAL, sb.toString(), startLine, startCol);
  }

  private Token readIdentifier(int startLine, int startCol) {
    StringBuilder sb = new StringBuilder();
    while (pos < length) {
      char c = peek();
      if (Character.isLetterOrDigit(c) || c == '_' || c == '$' || c == '.') {
        sb.append(c);
        advance();
      } else {
        break;
      }
    }
    String text = sb.toString();
    if ("null".equalsIgnoreCase(text)) {
      return new Token(TokenType.NULL_LITERAL, text, startLine, startCol);
    }
    return new Token(TokenType.IDENTIFIER, text, startLine, startCol);
  }

  private char peek() {
    return input.charAt(pos);
  }

  private void advance() {
    if (pos < length) {
      char c = input.charAt(pos);
      pos++;
      if (c == '\n') {
        line++;
        col = 1;
      } else {
        col++;
      }
    }
  }
}
