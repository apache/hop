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

package org.apache.hop.openformula;

import java.io.Serializable;
import java.util.List;
import org.apache.hop.openformula.ast.Expression;
import org.apache.hop.openformula.parser.FormulaLexer;
import org.apache.hop.openformula.parser.FormulaParser;
import org.apache.hop.openformula.parser.ParseException;
import org.apache.hop.openformula.parser.Token;
import org.apache.hop.openformula.typing.TypedValue;

/** Compiled OpenFormula expression ready for evaluation against a {@link FormulaContext}. */
public class Formula implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String formulaText;
  private final Expression rootExpression;
  private FormulaContext context;

  public Formula(String formulaText) throws ParseException {
    this.formulaText = formulaText;
    FormulaLexer lexer = new FormulaLexer(formulaText);
    List<Token> tokens = lexer.tokenize();
    FormulaParser parser = new FormulaParser(tokens);
    this.rootExpression = parser.parse();
  }

  public static Formula compile(String formulaText) throws ParseException {
    return new Formula(formulaText);
  }

  public void initialize(FormulaContext context) {
    this.context = context;
  }

  public Object evaluate() throws EvaluationException {
    try {
      return evaluateTyped().value();
    } catch (EvaluationException e) {
      return e.getErrorValue();
    }
  }

  public TypedValue evaluateTyped() throws EvaluationException {
    if (context == null) {
      context = new DefaultFormulaContext();
    }
    return rootExpression.evaluate(context);
  }

  public Object evaluate(FormulaContext evalContext) throws EvaluationException {
    this.context = evalContext;
    return evaluate();
  }

  public Expression getRootReference() {
    return rootExpression;
  }

  public Expression getRootExpression() {
    return rootExpression;
  }

  public String getFormulaText() {
    return formulaText;
  }
}
