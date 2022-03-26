package org.apache.hop.pipeline.transforms.formula;

import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class Formula extends BaseTransform<FormulaMeta, FormulaData>
        implements ITransform<FormulaMeta, FormulaData> {
    private static final Class<?> PKG = Formula.class; // For Translator

    /**
     * This is the base transform that forms that basis for all transforms. You can derive from this
     * class to implement your own transforms.
     *
     * @param transformMeta The TransformMeta object to run.
     * @param meta
     * @param data          the data object to store temporary data, database connections, caches, result sets,
     *                      hashtables etc.
     * @param copyNr        The copynumber for this transform.
     * @param pipelineMeta  The PipelineMeta of which the transform transformMeta is part of.
     * @param pipeline      The (running) pipeline to obtain information shared among the transforms.
     */
    public Formula(TransformMeta transformMeta, FormulaMeta meta, FormulaData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline) {
        super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);//TODO
    }
}
