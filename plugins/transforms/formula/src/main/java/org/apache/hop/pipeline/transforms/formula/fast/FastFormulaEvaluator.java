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
 * <p>Values behave the way POI evaluates them: a blank cell is 0 in arithmetic and FALSE in a
 * boolean context, booleans are numeric (TRUE is 1) and rank above text above numbers in ordered
 * comparisons, and TRIM only trims characters up to the ASCII space and collapses ASCII spaces.
 *
 * <p>An {@code #N/A} error operand, produced by the "Set Null to #N/A" option, propagates through
 * every operation and function except {@code ISNA}, which is the only way to test for it.
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

  /** A unary minus. */
  private static final class UnaryNode extends Node {
    private final Node operand;

    private UnaryNode(Node operand) {
      this.operand = operand;
    }

    @Override
    Object eval(Object[] args) {
      Object value = operand.eval(args);
      if (value == FastFormulaCompiler.NA) {
        return FastFormulaCompiler.NA;
      }
      return -toNumber(value);
    }
  }

  private enum BinaryOp {
    ADD,
    SUBTRACT,
    MULTIPLY,
    DIVIDE,
    CONCAT,
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
      Object left = this.left.eval(args);
      Object right = this.right.eval(args);
      if (left == FastFormulaCompiler.NA || right == FastFormulaCompiler.NA) {
        // Excel and POI propagate an error operand through every operation instead of evaluating
        // it, so the NA sentinel short-circuits the whole expression.
        return FastFormulaCompiler.NA;
      }
      switch (op) {
        case ADD:
          return toNumber(left) + toNumber(right);
        case SUBTRACT:
          return toNumber(left) - toNumber(right);
        case MULTIPLY:
          return toNumber(left) * toNumber(right);
        case DIVIDE:
          {
            double divisor = toNumber(right);
            if (divisor == 0.0d) {
              throw new ArithmeticException("Division by zero (#DIV/0!)");
            }
            return toNumber(left) / divisor;
          }
        case CONCAT:
          return TextValue.of(left) + TextValue.of(right);
        case EQUAL:
          return compareEqual(left, right);
        case NOT_EQUAL:
          return !compareEqual(left, right);
        case GREATER:
          return compareOrdered(left, right) > 0;
        case GREATER_OR_EQUAL:
          return compareOrdered(left, right) >= 0;
        case LESS:
          return compareOrdered(left, right) < 0;
        case LESS_OR_EQUAL:
          return compareOrdered(left, right) <= 0;
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
          Object condition = arguments.get(0).eval(args);
          if (condition == FastFormulaCompiler.NA) {
            return FastFormulaCompiler.NA;
          }
          // Only the taken branch is evaluated: an error in the other one stays invisible.
          return toBoolean(condition)
              ? arguments.get(1).eval(args)
              : (arguments.size() == 3 ? arguments.get(2).eval(args) : Boolean.FALSE);
        case "AND":
          boolean andResult = true;
          for (Node argument : arguments) {
            Object value = argument.eval(args);
            if (value == FastFormulaCompiler.NA) {
              return FastFormulaCompiler.NA;
            }
            andResult &= toBoolean(value);
          }
          return andResult;
        case "OR":
          boolean orResult = false;
          for (Node argument : arguments) {
            Object value = argument.eval(args);
            if (value == FastFormulaCompiler.NA) {
              return FastFormulaCompiler.NA;
            }
            orResult |= toBoolean(value);
          }
          return orResult;
        case "NOT":
          if (arguments.size() != 1) {
            throw new UnsupportedFormulaException("NOT requires 1 argument");
          }
          Object operand = arguments.get(0).eval(args);
          if (operand == FastFormulaCompiler.NA) {
            return FastFormulaCompiler.NA;
          }
          return !toBoolean(operand);
        case "ABS":
          if (arguments.size() != 1) {
            throw new UnsupportedFormulaException("ABS requires 1 argument");
          }
          Object absOperand = arguments.get(0).eval(args);
          if (absOperand == FastFormulaCompiler.NA) {
            return FastFormulaCompiler.NA;
          }
          return Math.abs(toNumber(absOperand));
        case "ISBLANK":
          if (arguments.size() != 1) {
            throw new UnsupportedFormulaException("ISBLANK requires 1 argument");
          }
          // An error cell is not blank, so the NA sentinel flows through unchanged.
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
          Object lenOperand = arguments.get(0).eval(args);
          if (lenOperand == FastFormulaCompiler.NA) {
            return FastFormulaCompiler.NA;
          }
          return (double) TextValue.of(lenOperand).length();
        case "TRIM":
          if (arguments.size() != 1) {
            throw new UnsupportedFormulaException("TRIM requires 1 argument");
          }
          Object trimOperand = arguments.get(0).eval(args);
          if (trimOperand == FastFormulaCompiler.NA) {
            return FastFormulaCompiler.NA;
          }
          return trim(trimOperand);
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
   * Excel TRIM as POI implements it ({@code arg.trim().replaceAll(" +", " ")}): characters up to
   * and including the ASCII space are stripped at both ends and runs of ASCII spaces collapse to a
   * single space. Any other whitespace, like a tab, is only stripped at the ends and kept as-is in
   * the middle.
   */
  private static String trim(Object value) {
    String text = TextValue.of(value);
    int start = 0;
    int end = text.length();
    while (start < end && text.charAt(start) <= ' ') {
      start++;
    }
    while (end > start && text.charAt(end - 1) <= ' ') {
      end--;
    }
    StringBuilder out = new StringBuilder(end - start);
    boolean lastWasSpace = true;
    for (int i = start; i < end; i++) {
      char c = text.charAt(i);
      if (c == ' ') {
        if (!lastWasSpace) {
          out.append(' ');
        }
        lastWasSpace = true;
      } else {
        out.append(c);
        lastWasSpace = false;
      }
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

  /**
   * Orders two operands the way Excel and POI do: booleans sort above text, which sorts above
   * numbers, and no operand is coerced across those types by stringifying it. A blank operand
   * compares as the other operand's type: 0 against a number, empty text against text and FALSE
   * against a boolean.
   */
  private static int compareOrdered(Object left, Object right) {
    if (left == null || right == null) {
      if (left == null && right == null) {
        return 0;
      }
      Object other = left != null ? left : right;
      boolean blankIsLeft = left == null;
      int comparison;
      if (other instanceof Boolean) {
        comparison = (Boolean) other ? -1 : 0;
      } else if (other instanceof Number) {
        comparison = Double.compare(0.0d, ((Number) other).doubleValue());
      } else {
        comparison = ((String) other).isEmpty() ? 0 : -1;
      }
      return blankIsLeft ? comparison : -comparison;
    }
    if (left instanceof Boolean leftBoolean) {
      if (right instanceof Boolean) {
        return leftBoolean.compareTo((Boolean) right);
      }
      // Booleans rank above text and numbers.
      return 1;
    }
    if (right instanceof Boolean) {
      return -1;
    }
    if (left instanceof String leftText) {
      if (right instanceof String) {
        // Excel compares text case-insensitively.
        return leftText.compareToIgnoreCase((String) right);
      }
      // Text ranks above numbers.
      return 1;
    }
    if (right instanceof String) {
      return -1;
    }
    return Double.compare(toNumber(left), toNumber(right));
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
    if (value instanceof Boolean booleanValue) {
      // POI BoolEval is numeric: TRUE is 1 and FALSE is 0, so arithmetic on booleans works.
      return booleanValue ? 1.0d : 0.0d;
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
      Node node = parseComparison();
      skipWhitespace();
      if (pos < expression.length()) {
        throw new UnsupportedFormulaException(
            "Unexpected trailing content at position " + pos + ": " + expression.substring(pos));
      }
      return node;
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
        if (!(pos < expression.length() && expression.charAt(pos) == '&')) {
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
        return new UnaryNode(parseUnary());
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
        Node node = parseComparison();
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
        arguments.add(parseComparison());
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
