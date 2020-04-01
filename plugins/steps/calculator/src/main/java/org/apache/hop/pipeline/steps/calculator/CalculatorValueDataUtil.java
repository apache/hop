package org.apache.hop.pipeline.steps.calculator;

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
