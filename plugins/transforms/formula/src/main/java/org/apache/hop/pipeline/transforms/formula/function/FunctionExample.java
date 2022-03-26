package org.apache.hop.pipeline.transforms.formula.function;

import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Node;

public class FunctionExample {
    public static final String XML_TAG = "example";

    private String expression;
    private String result;
    private String level;
    private String comment;

    public FunctionExample( String expression, String result, String level, String comment ) {
        this.expression = expression;
        this.result = result;
        this.level = level;
        this.comment = comment;
    }

    public FunctionExample( Node node ) {
        this.expression = XmlHandler.getTagValue( node, "expression" );
        this.result = XmlHandler.getTagValue( node, "result" );
        this.level = XmlHandler.getTagValue( node, "level" );
        this.comment = XmlHandler.getTagValue( node, "comment" );
    }

    /**
     * @return the expression
     */
    public String getExpression() {
        return expression;
    }

    /**
     * @param expression
     *          the expression to set
     */
    public void setExpression( String expression ) {
        this.expression = expression;
    }

    /**
     * @return the result
     */
    public String getResult() {
        return result;
    }

    /**
     * @param result
     *          the result to set
     */
    public void setResult( String result ) {
        this.result = result;
    }

    /**
     * @return the level
     */
    public String getLevel() {
        return level;
    }

    /**
     * @param level
     *          the level to set
     */
    public void setLevel( String level ) {
        this.level = level;
    }

    /**
     * @return the comment
     */
    public String getComment() {
        return comment;
    }

    /**
     * @param comment
     *          the comment to set
     */
    public void setComment( String comment ) {
        this.comment = comment;
    }

}
