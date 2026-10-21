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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.openformula.ast.ArrayExpr;
import org.apache.hop.openformula.ast.BinaryExpr;
import org.apache.hop.openformula.ast.Expression;
import org.apache.hop.openformula.ast.FunctionCallExpr;
import org.apache.hop.openformula.ast.LiteralExpr;
import org.apache.hop.openformula.ast.ReferenceExpr;
import org.apache.hop.openformula.ast.UnaryExpr;
import org.apache.hop.openformula.typing.TypedValue;

/** Pratt recursive-descent parser with precedence climbing for OpenFormula expressions. */
public class FormulaParser {
  private final List<Token> tokens;
  private int current = 0;

  public FormulaParser(List<Token> tokens) {
    this.tokens = (tokens != null) ? tokens : List.of();
  }

  public Expression parse() throws ParseException {
    if (peek().type() == TokenType.EOF) {
      return new LiteralExpr(TypedValue.NULL);
    }
    // Ignore leading '=' if present
    if (peek().type() == TokenType.EQUAL) {
      advance();
    }
    if (peek().type() == TokenType.EOF) {
      return new LiteralExpr(TypedValue.NULL);
    }

    Expression expr = parseExpression(0);
    if (peek().type() != TokenType.EOF) {
      Token unconsumed = peek();
      throw new ParseException(
          "Unexpected token after expression: " + unconsumed.text(),
          unconsumed.line(),
          unconsumed.column());
    }
    return expr;
  }

  private Expression parseExpression(int minPrecedence) throws ParseException {
    Expression left = parsePrimary();

    // Handle postfix operators (e.g. %)
    while (peek().type() == TokenType.PERCENT) {
      advance();
      left = new UnaryExpr(UnaryExpr.Operator.PERCENT, left);
    }

    while (true) {
      Token opToken = peek();
      int prec = getInfixPrecedence(opToken.type());
      if (prec < minPrecedence || prec <= 0) {
        break;
      }

      advance(); // Consume operator
      BinaryExpr.Operator op = mapBinaryOperator(opToken.type());
      boolean rightAssoc = (opToken.type() == TokenType.CARET);
      int nextMinPrec = rightAssoc ? prec : prec + 1;

      Expression right = parseExpression(nextMinPrec);
      left = new BinaryExpr(op, left, right);
    }

    return left;
  }

  private Expression parsePrimary() throws ParseException {
    Token token = peek();

    // Unary prefix operators (+, -)
    if (token.type() == TokenType.PLUS) {
      advance();
      Expression operand = parsePrimary();
      return new UnaryExpr(UnaryExpr.Operator.PLUS, operand);
    }
    if (token.type() == TokenType.MINUS) {
      advance();
      Expression operand = parsePrimary();
      return new UnaryExpr(UnaryExpr.Operator.MINUS, operand);
    }

    if (token.type() == TokenType.NUMERIC_LITERAL) {
      advance();
      return new LiteralExpr(TypedValue.ofNumber(new BigDecimal(token.text())));
    }

    if (token.type() == TokenType.STRING_LITERAL) {
      advance();
      return new LiteralExpr(TypedValue.ofText(token.text()));
    }

    if (token.type() == TokenType.NULL_LITERAL) {
      advance();
      return new LiteralExpr(TypedValue.NULL);
    }

    if (token.type() == TokenType.COLUMN_LOOKUP) {
      advance();
      return new ReferenceExpr(token.text());
    }

    if (token.type() == TokenType.LPAREN) {
      advance();
      Expression expr = parseExpression(0);
      consume(TokenType.RPAREN, "Expected ')' after parenthesized expression");
      return expr;
    }

    if (token.type() == TokenType.LBRACE) {
      return parseArrayLiteral();
    }

    if (token.type() == TokenType.IDENTIFIER) {
      advance();
      if (peek().type() == TokenType.LPAREN) {
        return parseFunctionCall(token.text());
      }
      // Bare identifier: treat as context reference
      return new ReferenceExpr(token.text());
    }

    throw new ParseException("Unexpected token: " + token.text(), token.line(), token.column());
  }

  private Expression parseFunctionCall(String name) throws ParseException {
    consume(TokenType.LPAREN, "Expected '(' after function name");
    List<Expression> args = new ArrayList<>();

    if (peek().type() != TokenType.RPAREN) {
      while (true) {
        if (peek().type() == TokenType.SEMICOLON) {
          // Empty argument, e.g. fn(; ...)
          args.add(new LiteralExpr(TypedValue.NULL));
        } else if (peek().type() == TokenType.RPAREN) {
          // Trailing semicolon before ')' e.g. fn(a;)
          args.add(new LiteralExpr(TypedValue.NULL));
          break;
        } else {
          args.add(parseExpression(0));
        }

        if (peek().type() == TokenType.SEMICOLON) {
          advance();
          // If right after semicolon is ')', there is a trailing empty argument
          if (peek().type() == TokenType.RPAREN) {
            args.add(new LiteralExpr(TypedValue.NULL));
            break;
          }
        } else {
          break;
        }
      }
    }

    consume(TokenType.RPAREN, "Expected ')' after function arguments");
    return new FunctionCallExpr(name, args);
  }

  private Expression parseArrayLiteral() throws ParseException {
    consume(TokenType.LBRACE, "Expected '{' to start array literal");
    List<List<Expression>> rows = new ArrayList<>();

    if (peek().type() != TokenType.RBRACE) {
      List<Expression> currentRow = new ArrayList<>();
      rows.add(currentRow);

      while (true) {
        if (peek().type() == TokenType.PIPE) {
          advance();
          currentRow = new ArrayList<>();
          rows.add(currentRow);
        } else if (peek().type() == TokenType.SEMICOLON) {
          advance();
        } else if (peek().type() == TokenType.RBRACE) {
          break;
        } else {
          currentRow.add(parseExpression(0));
          if (peek().type() == TokenType.SEMICOLON) {
            advance();
          } else if (peek().type() == TokenType.PIPE) {
            advance();
            currentRow = new ArrayList<>();
            rows.add(currentRow);
          } else {
            break;
          }
        }
      }
    }

    consume(TokenType.RBRACE, "Expected '}' to end array literal");
    return new ArrayExpr(rows);
  }

  private static int getInfixPrecedence(TokenType type) {
    return switch (type) {
      case CARET -> 50;
      case STAR, SLASH -> 40;
      case PLUS, MINUS -> 30;
      case AMPERSAND -> 20;
      case EQUAL, NOT_EQUAL, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL -> 10;
      default -> 0;
    };
  }

  private static BinaryExpr.Operator mapBinaryOperator(TokenType type) {
    return switch (type) {
      case PLUS -> BinaryExpr.Operator.ADD;
      case MINUS -> BinaryExpr.Operator.SUBTRACT;
      case STAR -> BinaryExpr.Operator.MULTIPLY;
      case SLASH -> BinaryExpr.Operator.DIVIDE;
      case CARET -> BinaryExpr.Operator.POWER;
      case AMPERSAND -> BinaryExpr.Operator.CONCAT;
      case EQUAL -> BinaryExpr.Operator.EQUAL;
      case NOT_EQUAL -> BinaryExpr.Operator.NOT_EQUAL;
      case LESS -> BinaryExpr.Operator.LESS;
      case LESS_EQUAL -> BinaryExpr.Operator.LESS_EQUAL;
      case GREATER -> BinaryExpr.Operator.GREATER;
      case GREATER_EQUAL -> BinaryExpr.Operator.GREATER_EQUAL;
      default -> throw new IllegalArgumentException("Not a binary operator: " + type);
    };
  }

  private Token peek() {
    if (current < tokens.size()) {
      return tokens.get(current);
    }
    return new Token(TokenType.EOF, "", -1, -1);
  }

  private Token advance() {
    if (current < tokens.size()) {
      return tokens.get(current++);
    }
    return peek();
  }

  private void consume(TokenType expected, String errorMessage) throws ParseException {
    if (peek().type() == expected) {
      advance();
    } else {
      Token actual = peek();
      throw new ParseException(
          errorMessage + " but found '" + actual.text() + "'", actual.line(), actual.column());
    }
  }
}
