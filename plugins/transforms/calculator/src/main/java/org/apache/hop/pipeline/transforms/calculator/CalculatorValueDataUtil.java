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

package org.apache.hop.pipeline.transforms.calculator;

import com.wcohen.ss.NeedlemanWunsch;

class CalculatorValueDataUtil {

    private CalculatorValueDataUtil() {
    }

    /**
     * NeedlemanWunsch distance is a measure of the similarity between two strings, which we will refer to as the source
     * string (s) and the target string (t). The distance is the number of deletions, insertions, or substitutions
     * required to transform s into t.
     */
    public static Long getNeedlemanWunschDistance(Object dataA, Object dataB ) {
        if ( dataA == null || dataB == null ) {
            return null;
        }
        return (long) new NeedlemanWunsch().score( dataA.toString(), dataB.toString() );
    }


}
