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

package org.apache.hop.core.fatjar;

import org.apache.hop.core.variables.Variable;

/** A class with an org.apache.hop annotation on a field only: it goes into a fat jar's index. */
public class IndexSampleVariables {
  @Variable(value = "", description = "Sample for the fat jar index test")
  public static final String INDEX_SAMPLE_VARIABLE = "INDEX_SAMPLE_VARIABLE";
}
