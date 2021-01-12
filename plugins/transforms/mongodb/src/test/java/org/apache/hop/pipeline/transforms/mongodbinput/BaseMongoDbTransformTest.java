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

package org.apache.hop.pipeline.transforms.mongodbinput;

import com.mongodb.Cursor;
import com.mongodb.DBObject;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILogChannelFactory;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.MongoProperties;
import org.apache.hop.mongo.MongoUtilLogger;
import org.apache.hop.mongo.wrapper.MongoClientWrapper;
import org.apache.hop.mongo.wrapper.MongoWrapperClientFactory;
import org.apache.hop.mongo.wrapper.MongoWrapperUtil;
import org.apache.hop.mongo.wrapper.collection.MongoCollectionWrapper;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/** Common mock setup for MongoDbOutputTest and MongoDbInput */
public class BaseMongoDbTransformTest {

  @Mock protected TransformMeta transformMeta;
  @Mock protected PipelineMeta pipelineMeta;
  protected Pipeline pipeline = spy(new LocalPipelineEngine() );
  @Mock protected ILogChannel mockLog;
  @Mock protected MongoWrapperClientFactory mongoClientWrapperFactory;
  @Mock protected MongoClientWrapper mongoClientWrapper;
  @Mock protected MongoCollectionWrapper mongoCollectionWrapper;
  @Mock protected ILogChannelFactory logChannelFactory;
  @Mock protected Cursor cursor;
  @Captor protected ArgumentCaptor<String> stringCaptor;
  @Captor protected ArgumentCaptor<DBObject> dbObjectCaptor;
  @Captor protected ArgumentCaptor<Throwable> throwableCaptor;

  protected RowMeta rowMeta = new RowMeta();
  protected Object[] rowData;

  @Before
  public void before() throws MongoDbException {
    MockitoAnnotations.initMocks(this);
    MongoWrapperUtil.setMongoWrapperClientFactory(mongoClientWrapperFactory);
    when(mongoClientWrapperFactory.createMongoClientWrapper(
            any(MongoProperties.class), any(MongoUtilLogger.class)))
        .thenReturn(mongoClientWrapper);

    when(transformMeta.getName()).thenReturn("transformMetaName");
    when(pipelineMeta.findTransform(anyString())).thenReturn(transformMeta);
    when(logChannelFactory.create(any(BaseTransform.class), any(Pipeline.class)))
        .thenReturn(mockLog);
    HopLogStore.setLogChannelFactory(logChannelFactory);
  }
}
