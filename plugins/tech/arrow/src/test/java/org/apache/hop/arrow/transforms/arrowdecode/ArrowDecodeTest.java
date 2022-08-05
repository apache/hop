package org.apache.hop.arrow.transforms.arrowdecode;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaArrowVector;
import org.apache.hop.core.util.ArrowBufferAllocator;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.*;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

public class ArrowDecodeTest {
    private static TransformMockHelper<ArrowDecodeMeta, ArrowDecodeData> amh;

    @ClassRule
    public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

    @Before
    public void setup() throws HopException {
        amh = new TransformMockHelper<>("ArrowDecode", ArrowDecodeMeta.class, ArrowDecodeData.class);
        when(amh.logChannelFactory.create(any(), any(ILoggingObject.class)))
                .thenReturn(amh.iLogChannel);
        when(amh.pipeline.isRunning()).thenReturn(true);

        HopEnvironment.init();
        PluginRegistry.init();
    }

    @After
    public void cleanUp() {
        amh.cleanUp();
    }

    @Test
    public void testDecodingSingleBatch() throws HopException {
        ArrowDecodeMeta meta = new ArrowDecodeMeta();
        meta.setSourceFieldName("arrow");
        meta.setTargetFields(List.of(
                new TargetField("age", "age_out", "Integer", "", "", ""),
                new TargetField("name", "name_out", "String", "", "", "")
        ));
        ArrowDecodeData data = new ArrowDecodeData();

        ArrowDecode transform =
                new ArrowDecode(
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

        // We are testing a single row. First invocation should transform. Second ends.
        //
        Assert.assertTrue(transform.processRow());
        Assert.assertFalse("Should be done processing rows", transform.processRow());
        Assert.assertTrue(outputRowSet.isDone());
    }

    private IRowSet mockInputRowSet() {
        IRowMeta inputRowMeta = new RowMeta();

        // Create some test Arrow vectors
        BigIntVector ageVector = new BigIntVector("age", ArrowBufferAllocator.rootAllocator());
        ageVector.allocateNew(2);
        ageVector.set(0, 42);
        ageVector.set(1, 10);
        ageVector.setValueCount(2);

        VarCharVector nameVector = new VarCharVector("name", ArrowBufferAllocator.rootAllocator());
        nameVector.allocateNew(2);
        nameVector.set(0, "Forty Two".getBytes(StandardCharsets.UTF_8));
        nameVector.set(1, "Ten".getBytes(StandardCharsets.UTF_8));
        nameVector.setValueCount(2);

        inputRowMeta.addValueMeta(0, new ValueMetaArrowVector("arrow"));

        IRowSet inputRowSet = amh.getMockInputRowSet(
                new Object[][] {{List.of(ageVector, nameVector)}});
        doReturn(inputRowMeta).when(inputRowSet).getRowMeta();

        return inputRowSet;
    }
}
