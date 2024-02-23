package org.apache.hop.pipeline.transforms.aws.sqs;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

public class SqsReaderData extends BaseTransformData implements ITransformData {

    public IRowMeta outputRowMeta;
    public AwsSqsReader aws_sqs;
    public String realMessageIDFieldName;
    public String realMessageBodyFieldName;
    public String realReceiptHandleFieldName;
    public String realBodyMD5FieldName;
    public String realSQSQueue;
    public Integer realMaxMessages;
    public String realSNSMessageFieldName;

    public SqsReaderData()
    {
        super();
    }
}
