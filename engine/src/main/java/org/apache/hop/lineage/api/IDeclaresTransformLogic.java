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

package org.apache.hop.lineage.api;

import java.util.List;
import org.apache.hop.core.variables.IVariables;

/**
 * A transform that can tell the logic it applies to the fields it calculates, such as a Formula or
 * a Calculator, for lineage sinks to show next to the transform. SQL a transform runs is not
 * declared here: it reaches the sinks with the tables it reads and writes.
 */
public interface IDeclaresTransformLogic {

  /**
   * The logic behind one field.
   *
   * @param field the field the expression calculates
   * @param language what the expression is written in, such as {@code formula} or {@code
   *     calculator}
   * @param expression the expression as the transform shows it
   */
  record FieldLogic(String field, String language, String expression) {}

  /**
   * The logic of the fields this transform calculates, in the order it calculates them. Empty when
   * it calculates none.
   *
   * @param variables the variables of the transform
   */
  List<FieldLogic> transformLogic(IVariables variables);
}
