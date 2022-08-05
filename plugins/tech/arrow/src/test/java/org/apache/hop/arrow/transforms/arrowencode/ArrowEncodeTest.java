package org.apache.hop.arrow.transforms.arrowencode;

import org.apache.arrow.vector.ValueVector;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

public class ArrowEncodeTest {
    private static TransformMockHelper<ArrowEncodeMeta, ArrowEncodeData> amh;

    @Before
    public void setup() {
        amh = new TransformMockHelper<>("ArrowEncode", ArrowEncodeMeta.class, ArrowEncodeData.class);
        when(amh.logChannelFactory.create(any(), any(ILoggingObject.class)))
                .thenReturn(amh.iLogChannel);
        when(amh.pipeline.isRunning()).thenReturn(true);
    }

    @After
    public void cleanUp() {
        amh.cleanUp();
    }

    @Test
    public void testEncodingSingleBatch() throws HopException {
        ArrowEncodeMeta meta = new ArrowEncodeMeta();
        meta.setSourceFields(List.of(
                new SourceField("age", "age_out"),
                new SourceField("name", "name_out")
        ));
        ArrowEncodeData data = new ArrowEncodeData();

        ArrowEncode transform =
                new ArrowEncode(
                        amh.transformMeta,
                        meta,
                        data,
                        0,
                        amh.pipelineMeta,
                        amh.pipeline
                );
        transform.init();
        transform.addRowSetToInputRowSets(mockInputRowSet());

        IRowSet outputRowSet = new QueueRowSet();
        transform.addRowSetToOutputRowSets(outputRowSet);

        // We are testing 2 rows. Third call should be the end.
        //
        Assert.assertTrue(transform.processRow());
        Assert.assertTrue(transform.processRow());
        Assert.assertFalse("Should be done processing rows", transform.processRow());
        Assert.assertTrue(outputRowSet.isDone());

        Object[] row = outputRowSet.getRow();
        Assert.assertNotNull(row);
        int index = outputRowSet.getRowMeta().indexOfValue("arrow");
        Assert.assertTrue("Should have a non-zero index", index > 0);

        List<ValueVector> vectors = (List<ValueVector>) row[index];
        Assert.assertEquals("Should have 2 vectors", 2, vectors.size());

        Assert.assertEquals("First vector should be renamed age_out",
                vectors.get(0).getName(), "age_out");
        Assert.assertEquals("First vector should be renamed name_out",
                vectors.get(1).getName(), "name_out");

        try {
            vectors.forEach(ValueVector::close);
        } catch (Exception e) {
            Assert.fail("Failed to release vectors");
        }
    }

    private IRowSet mockInputRowSet() {
        IRowMeta inputRowMeta = new RowMeta();

        inputRowMeta.addValueMeta(0, new ValueMetaInteger("age"));
        inputRowMeta.addValueMeta(1, new ValueMetaString("name"));

        IRowSet inputRowSet = amh.getMockInputRowSet(new Object[][] {{42L, "Forty Two"}, {10L, "Ten"}});
        doReturn(inputRowMeta).when(inputRowSet).getRowMeta();

        return inputRowSet;
    }
}
