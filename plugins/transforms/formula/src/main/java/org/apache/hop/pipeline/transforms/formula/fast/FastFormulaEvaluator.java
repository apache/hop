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
 */
package org.apache.hop.pipeline.transforms.formula.fast;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The pure-Java compiler for the Formula transform fast path.
 *
 * <p>A formula that only uses the supported subset (field references, numbers, strings, booleans,
 * the {@code + - * /} and {@code &} binary operators, comparisons, and the Excel functions {@code
 * IF}, {@code AND}, {@code OR}, {@code NOT}, {@code ABS}, {@code ISBLANK}, {@code ISNA}, {@code
 * LEN} and {@code TRIM}) is parsed once into a tree of {@link Node}s. Every row then just runs the
 * tree against the field values, which is orders of magnitude faster than going through a POI
 * workbook and worksheet.
 *
 * <p>Anything outside that subset makes {@link #parse} throw, which {@link FastFormulaCompiler}
 * turns into "not eligible for the fast path"; the transform then falls back to the regular POI
 * evaluation.
 */
final class FastFormulaEvaluator {

  private FastFormulaEvaluator() {}

  /**
   * Parses a formula into an executable tree.
   *
   * @param expression the variable-resolved formula
   * @param fieldIndex maps a field name to the position it takes in the {@code args} array handed
   *     to {@link Node#eval(Object[])}
   * @return the root node
   * @throws UnsupportedFormulaException when the formula uses a construct outside the fast path
   *     subset, meaning it is not eligible
   */
  static Node parse(String expression, Map<String, Integer> fieldIndex) {
    return new Parser(expression, fieldIndex).parseExpression();
  }

  abstract static class Node {
    abstract Object eval(Object[] args);
  }

  /** A literal number, string or boolean. */
  private static final class LiteralNode extends Node {
    private final Object value;

    private LiteralNode(Object value) {
      this.value = value;
    }

    @Override
    Object eval(Object[] args) {
      return value;
    }
  }

  /** A reference to one of the row fields, read from the position it was bound to. */
  static final class FieldNode extends Node {
    private final int index;

    FieldNode(int index) {
      this.index = index;
    }

    @Override
    Object eval(Object[] args) {
      return args[index];
    }
  }

  /** A unary operation ({@code -} or {@code !}). */
  private static final class UnaryNode extends Node {
    private final Node operand;
    private final boolean negate;

    private UnaryNode(Node operand, boolean negate) {
      this.operand = operand;
      this.negate = negate;
    }

    @Override
    Object eval(Object[] args) {
      Object value = operand.eval(args);
      if (negate) {
        return -toNumber(value);
      }
      return !toBoolean(value);
    }
  }

  private enum BinaryOp {
    ADD,
    SUBTRACT,
    MULTIPLY,
    DIVIDE,
    CONCAT,
    AND,
    OR,
    EQUAL,
    NOT_EQUAL,
    GREATER,
    GREATER_OR_EQUAL,
    LESS,
    LESS_OR_EQUAL
  }

  /** A binary operation between two operand nodes. */
  private static final class BinaryNode extends Node {
    private final Node left;
    private final Node right;
    private final BinaryOp op;

    private BinaryNode(Node left, Node right, BinaryOp op) {
      this.left = left;
      this.right = right;
      this.op = op;
    }

    @Override
    Object eval(Object[] args) {
      switch (op) {
        case ADD:
          return toNumber(left.eval(args)) + toNumber(right.eval(args));
        case SUBTRACT:
          return toNumber(left.eval(args)) - toNumber(right.eval(args));
        case MULTIPLY:
          return toNumber(left.eval(args)) * toNumber(right.eval(args));
        case DIVIDE:
          double divisor = toNumber(right.eval(args));
          if (divisor == 0.0d) {
            throw new ArithmeticException("Division by zero (#DIV/0!)");
          }
          return toNumber(left.eval(args)) / divisor;
        case CONCAT:
          Object concatLeft = left.eval(args);
          Object concatRight = right.eval(args);
          if (concatLeft == FastFormulaCompiler.NA || concatRight == FastFormulaCompiler.NA) {
            // Excel propagates an error cell through concatenation instead of rendering it.
            return FastFormulaCompiler.NA;
          }
          return TextValue.of(concatLeft) + TextValue.of(concatRight);
        case AND:
          return toBoolean(left.eval(args)) && toBoolean(right.eval(args));
        case OR:
          return toBoolean(left.eval(args)) || toBoolean(right.eval(args));
        case EQUAL:
          return compareEqual(left.eval(args), right.eval(args));
        case NOT_EQUAL:
          return !compareEqual(left.eval(args), right.eval(args));
        case GREATER:
          return compareOrdered(left.eval(args), right.eval(args)) > 0;
        case GREATER_OR_EQUAL:
          return compareOrdered(left.eval(args), right.eval(args)) >= 0;
        case LESS:
          return compareOrdered(left.eval(args), right.eval(args)) < 0;
        case LESS_OR_EQUAL:
          return compareOrdered(left.eval(args), right.eval(args)) <= 0;
        default:
          throw new UnsupportedFormulaException("Unsupported operator " + op);
      }
    }
  }

  /** A call to one of the supported Excel functions. */
  private static final class FunctionNode extends Node {
    private final String name;
    private final List<Node> arguments;

    private FunctionNode(String name, List<Node> arguments) {
      this.name = name;
      this.arguments = arguments;
    }

    @Override
    Object eval(Object[] args) {
      switch (name) {
        case "IF":
          // Excel allows a 2-argument IF: the (omitted) false branch evaluates to FALSE.
          if (arguments.size() != 2 && arguments.size() != 3) {
            throw new UnsupportedFormulaException("IF requires 2 or 3 arguments");
          }
          return toBoolean(arguments.get(0).eval(args))
              ? arguments.get(1).eval(args)
              : (arguments.size() == 3 ? arguments.get(2).eval(args) : Boolean.FALSE);
        case "AND":
          for (Node argument : arguments) {
            if (!toBoolean(argument.eval(args))) {
              return Boolean.FALSE;
            }
          }
          return Boolean.TRUE;
        case "OR":
          for (Node argument : arguments) {
            if (toBoolean(argument.eval(args))) {
              return Boolean.TRUE;
            }
          }
          return Boolean.FALSE;
        case "NOT":
          if (arguments.size() != 1) {
            throw new UnsupportedFormulaException("NOT requires 1 argument");
          }
          return !toBoolean(arguments.get(0).eval(args));
        case "ABS":
          if (arguments.size() != 1) {
            throw new UnsupportedFormulaException("ABS requires 1 argument");
          }
          return Math.abs(toNumber(arguments.get(0).eval(args)));
        case "ISBLANK":
          if (arguments.size() != 1) {
            throw new UnsupportedFormulaException("ISBLANK requires 1 argument");
          }
          return isBlank(arguments.get(0).eval(args));
        case "ISNA":
          if (arguments.size() != 1) {
            throw new UnsupportedFormulaException("ISNA requires 1 argument");
          }
          return arguments.get(0).eval(args) == FastFormulaCompiler.NA;
        case "LEN":
          if (arguments.size() != 1) {
            throw new UnsupportedFormulaException("LEN requires 1 argument");
          }
          return (double) TextValue.of(arguments.get(0).eval(args)).length();
        case "TRIM":
          if (arguments.size() != 1) {
            throw new UnsupportedFormulaException("TRIM requires 1 argument");
          }
          return trim(arguments.get(0).eval(args));
        default:
          throw new UnsupportedFormulaException("Unsupported function " + name);
      }
    }
  }

  private static boolean isBlank(Object value) {
    // A null field bound without the #N/A option is a blank cell; a field bound as #N/A is an
    // error cell, which is not blank.
    return value == null;
  }

  /**
   * Excel TRIM: trims leading/trailing whitespace and collapses any run of whitespace to a single
   * space. Uses a single consistent whitespace definition so tabs/newlines are handled the same way
   * whether the text contains a space or not.
   */
  private static String trim(Object value) {
    String text = TextValue.of(value);
    StringBuilder out = new StringBuilder(text.length());
    boolean lastWasSpace = true;
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (Character.isWhitespace(c)) {
        if (!lastWasSpace) {
          out.append(' ');
        }
        lastWasSpace = true;
      } else {
        out.append(c);
        lastWasSpace = false;
      }
    }
    if (out.length() > 0 && out.charAt(out.length() - 1) == ' ') {
      out.deleteCharAt(out.length() - 1);
    }
    return out.toString();
  }

  private static boolean compareEqual(Object left, Object right) {
    boolean leftBoolean = left instanceof Boolean;
    boolean rightBoolean = right instanceof Boolean;
    if (leftBoolean || rightBoolean) {
      // Excel and POI treat booleans as strictly distinct from numbers and strings: TRUE is not
      // equal to 1, and FALSE is not equal to 0 or "Y".
      if (leftBoolean != rightBoolean) {
        return false;
      }
      return (Boolean) left == (Boolean) right;
    }
    boolean leftNumber = left instanceof Number;
    boolean rightNumber = right instanceof Number;
    if (leftNumber || rightNumber) {
      // Excel and POI never compare a number with a string as equal, even when they look the same.
      if (leftNumber != rightNumber) {
        return false;
      }
      return toNumber(left) == toNumber(right);
    }
    return TextValue.of(left).equalsIgnoreCase(TextValue.of(right));
  }

  private static int compareOrdered(Object left, Object right) {
    if (left instanceof Number && right instanceof Number) {
      return Double.compare(toNumber(left), toNumber(right));
    }
    // Excel compares text case-insensitively, so follow that for mixed and text comparisons.
    return TextValue.of(left).compareToIgnoreCase(TextValue.of(right));
  }

  /** The values that are coerced to a boolean, mirroring Excel's truthiness. */
  static boolean toBoolean(Object value) {
    if (value instanceof Boolean booleanValue) {
      return booleanValue;
    }
    if (value instanceof Number number) {
      return number.doubleValue() != 0.0d;
    }
    if (value == null) {
      return false;
    }
    throw new UnsupportedFormulaException("Cannot use " + value + " as a boolean");
  }

  /** The numeric value of an operand, throwing when it can not be treated as a number. */
  private static double toNumber(Object value) {
    if (value instanceof Number number) {
      return number.doubleValue();
    }
    if (value == null) {
      // In Excel and POI a blank/null cell in an arithmetic expression is treated as 0.
      return 0.0d;
    }
    if (value == FastFormulaCompiler.NA) {
      throw new UnsupportedFormulaException("Cannot use " + value + " as a number");
    }
    if (value instanceof String string) {
      try {
        return Double.parseDouble(string.trim());
      } catch (NumberFormatException e) {
        throw new UnsupportedFormulaException("Cannot use \"" + string + "\" as a number");
      }
    }
    throw new UnsupportedFormulaException("Cannot use " + value + " as a number");
  }

  /** Formats a number the way Excel shows it in a text concatenation. */
  static String formatNumber(Object value) {
    if (value instanceof Number number) {
      double d = number.doubleValue();
      if (d == Math.rint(d) && !Double.isInfinite(d) && Math.abs(d) <= Long.MAX_VALUE) {
        return Long.toString((long) d);
      }
      return Double.toString(d);
    }
    return String.valueOf(value);
  }

  /** Text rendering for concatenation, mirroring Excel's coercion of numeric and boolean values. */
  private static final class TextValue {
    private static String of(Object value) {
      if (value == null) {
        return "";
      }
      if (value == FastFormulaCompiler.NA) {
        // An error cell must not be rendered as a Java object hash; report it as an error so the
        // expression can not silently produce garbage like "prefix-java.lang.Object@4f023fd2".
        throw new UnsupportedFormulaException("Cannot render #N/A as text");
      }
      if (value instanceof Boolean booleanValue) {
        return booleanValue ? "TRUE" : "FALSE";
      }
      if (value instanceof Number number) {
        return formatNumber(number);
      }
      return String.valueOf(value);
    }
  }

  /** A simple precedence-climbing parser over a char cursor. */
  private static final class Parser {
    private final String expression;
    private final Map<String, Integer> fieldIndex;
    private int pos;

    private Parser(String expression, Map<String, Integer> fieldIndex) {
      this.expression = expression;
      this.fieldIndex = fieldIndex;
    }

    Node parseExpression() {
      Node node = parseOr();
      skipWhitespace();
      if (pos < expression.length()) {
        throw new UnsupportedFormulaException(
            "Unexpected trailing content at position " + pos + ": " + expression.substring(pos));
      }
      return node;
    }

    private Node parseOr() {
      Node node = parseAnd();
      while (true) {
        skipWhitespace();
        if (!(pos < expression.length() && expression.startsWith("||", pos))) {
          return node;
        }
        pos += 2;
        Node right = parseAnd();
        node = new BinaryNode(node, right, BinaryOp.OR);
      }
    }

    private Node parseAnd() {
      Node node = parseComparison();
      while (true) {
        skipWhitespace();
        if (!(pos < expression.length() && expression.startsWith("&&", pos))) {
          return node;
        }
        pos += 2;
        Node right = parseComparison();
        node = new BinaryNode(node, right, BinaryOp.AND);
      }
    }

    private Node parseComparison() {
      Node node = parseConcat();
      skipWhitespace();
      while (pos < expression.length()) {
        BinaryOp op = matchComparison();
        if (op == null) {
          break;
        }
        Node right = parseConcat();
        node = new BinaryNode(node, right, op);
        skipWhitespace();
      }
      return node;
    }

    private Node parseConcat() {
      Node node = parseAdditive();
      while (true) {
        skipWhitespace();
        if (!(pos < expression.length()
            && expression.charAt(pos) == '&'
            && (pos + 1 >= expression.length() || expression.charAt(pos + 1) != '&'))) {
          return node;
        }
        pos++;
        Node right = parseAdditive();
        node = new BinaryNode(node, right, BinaryOp.CONCAT);
      }
    }

    private Node parseAdditive() {
      Node node = parseMultiplicative();
      skipWhitespace();
      while (pos < expression.length()) {
        char c = expression.charAt(pos);
        if (c == '+') {
          pos++;
          node = new BinaryNode(node, parseMultiplicative(), BinaryOp.ADD);
        } else if (c == '-') {
          pos++;
          node = new BinaryNode(node, parseMultiplicative(), BinaryOp.SUBTRACT);
        } else {
          break;
        }
        skipWhitespace();
      }
      return node;
    }

    private Node parseMultiplicative() {
      Node node = parseUnary();
      skipWhitespace();
      while (pos < expression.length()) {
        char c = expression.charAt(pos);
        if (c == '*') {
          pos++;
          node = new BinaryNode(node, parseUnary(), BinaryOp.MULTIPLY);
        } else if (c == '/') {
          pos++;
          node = new BinaryNode(node, parseUnary(), BinaryOp.DIVIDE);
        } else {
          break;
        }
        skipWhitespace();
      }
      return node;
    }

    private Node parseUnary() {
      skipWhitespace();
      if (pos < expression.length() && expression.charAt(pos) == '-') {
        pos++;
        return new UnaryNode(parseUnary(), true);
      }
      if (pos < expression.length() && expression.charAt(pos) == '!') {
        pos++;
        return new UnaryNode(parseUnary(), false);
      }
      return parseAtom();
    }

    private Node parseAtom() {
      skipWhitespace();
      if (pos >= expression.length()) {
        throw new UnsupportedFormulaException("Unexpected end of formula");
      }
      char c = expression.charAt(pos);
      if (c == '(') {
        pos++;
        Node node = parseOr();
        skipWhitespace();
        expect(')');
        return node;
      }
      if (c == '[') {
        return parseField();
      }
      if (c == '"' || c == '\'') {
        return new LiteralNode(parseString(c));
      }
      if (c == '-' || (c >= '0' && c <= '9')) {
        return new LiteralNode(parseNumber());
      }
      if (Character.isLetter(c) || c == '_') {
        return parseIdentifier();
      }
      throw new UnsupportedFormulaException("Unexpected character '" + c + "' at position " + pos);
    }

    private Node parseField() {
      int start = pos + 1;
      int end = expression.indexOf(']', start);
      if (end < 0) {
        throw new UnsupportedFormulaException("Unclosed field reference at position " + pos);
      }
      String name = expression.substring(start, end);
      pos = end + 1;
      Integer index = fieldIndex.get(name);
      if (index == null) {
        throw new UnsupportedFormulaException("Unknown field [" + name + "]");
      }
      return new FieldNode(index);
    }

    private Node parseIdentifier() {
      int start = pos;
      while (pos < expression.length()) {
        char c = expression.charAt(pos);
        if (Character.isLetterOrDigit(c) || c == '_') {
          pos++;
        } else {
          break;
        }
      }
      String word = expression.substring(start, pos);
      String upper = word.toUpperCase();
      skipWhitespace();
      if (pos < expression.length() && expression.charAt(pos) == '(') {
        return parseFunction(upper);
      }
      if ("TRUE".equals(upper)) {
        return new LiteralNode(Boolean.TRUE);
      }
      if ("FALSE".equals(upper)) {
        return new LiteralNode(Boolean.FALSE);
      }
      throw new UnsupportedFormulaException("Unknown identifier " + word);
    }

    private Node parseFunction(String name) {
      if (!SUPPORTED_FUNCTIONS.contains(name)) {
        throw new UnsupportedFormulaException("Unsupported function " + name);
      }
      expect('(');
      List<Node> arguments = new ArrayList<>();
      skipWhitespace();
      if (pos < expression.length() && expression.charAt(pos) == ')') {
        pos++;
        return new FunctionNode(name, arguments);
      }
      while (true) {
        arguments.add(parseOr());
        skipWhitespace();
        if (pos < expression.length() && expression.charAt(pos) == ',') {
          pos++;
          skipWhitespace();
          continue;
        }
        expect(')');
        break;
      }
      return new FunctionNode(name, arguments);
    }

    private double parseNumber() {
      int start = pos;
      boolean decimal = false;
      boolean exponent = false;
      while (pos < expression.length()) {
        char c = expression.charAt(pos);
        if (c >= '0' && c <= '9') {
          pos++;
        } else if (c == '.') {
          if (decimal || exponent) {
            break;
          }
          decimal = true;
          pos++;
        } else if ((c == 'e' || c == 'E')) {
          if (exponent) {
            break;
          }
          exponent = true;
          pos++;
          if (pos < expression.length()
              && (expression.charAt(pos) == '+' || expression.charAt(pos) == '-')) {
            pos++;
          }
        } else {
          break;
        }
      }
      if (pos == start) {
        throw new UnsupportedFormulaException("Invalid number at position " + pos);
      }
      return Double.parseDouble(expression.substring(start, pos));
    }

    private String parseString(char quote) {
      pos++;
      StringBuilder out = new StringBuilder();
      while (pos < expression.length()) {
        char c = expression.charAt(pos);
        if (c == quote) {
          if (pos + 1 < expression.length() && expression.charAt(pos + 1) == quote) {
            out.append(quote);
            pos += 2;
            continue;
          }
          pos++;
          return out.toString();
        }
        out.append(c);
        pos++;
      }
      throw new UnsupportedFormulaException("Unclosed string literal");
    }

    private BinaryOp matchComparison() {
      char c = expression.charAt(pos);
      switch (c) {
        case '=':
          pos++;
          return BinaryOp.EQUAL;
        case '>':
          pos++;
          if (pos < expression.length() && expression.charAt(pos) == '=') {
            pos++;
            return BinaryOp.GREATER_OR_EQUAL;
          }
          return BinaryOp.GREATER;
        case '<':
          pos++;
          if (pos < expression.length() && expression.charAt(pos) == '=') {
            pos++;
            return BinaryOp.LESS_OR_EQUAL;
          }
          if (pos < expression.length() && expression.charAt(pos) == '>') {
            pos++;
            return BinaryOp.NOT_EQUAL;
          }
          return BinaryOp.LESS;
        default:
          return null;
      }
    }

    private void expect(char expected) {
      skipWhitespace();
      if (pos >= expression.length() || expression.charAt(pos) != expected) {
        throw new UnsupportedFormulaException("Expected '" + expected + "' at position " + pos);
      }
      pos++;
    }

    private void skipWhitespace() {
      while (pos < expression.length() && Character.isWhitespace(expression.charAt(pos))) {
        pos++;
      }
    }
  }

  private static final List<String> SUPPORTED_FUNCTIONS =
      List.of("IF", "AND", "OR", "NOT", "ABS", "ISBLANK", "ISNA", "LEN", "TRIM");

  /** Signals that a formula can not be evaluated by the fast path. */
  static final class UnsupportedFormulaException extends RuntimeException {
    UnsupportedFormulaException(String message) {
      super(message);
    }
  }
}
