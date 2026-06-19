#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import sys
import xml.dom.minidom

from py4j.java_gateway import JavaGateway

gateway = JavaGateway()
hop = gateway.entry_point.getPyHop()
vars = hop.getVariables()

# Build the pipeline metadata.
# We simply want to generate 100M empty rows and send them to a dummy transform.
#
pipelineMeta = hop.newPipelineMeta()
pipelineMeta.setName("generate-rows-test")

generate = hop.newTransformMeta("generate", "RowGenerator")
generateTransform = generate.getTransform()
generateTransform.setRowLimit("100000000")

dummy = hop.newTransformMeta("dummy", "Dummy")

pipelineMeta.addTransform(generate)
pipelineMeta.addTransform(dummy)
pipelineMeta.addPipelineHop(hop.newPipelineHopMeta(generate, dummy))

# Now we can execute this pipeline.
# The "local" pipeline run configuration is picked up from the project metadata.
# This project is specified with the "--project" option when you run "hop python".
#
pipeline = hop.newPipeline(pipelineMeta, "local", "Basic")

# Execute this pipeline
#
pipeline.execute()
print("Execution of the pipeline has started.")

# Get the status of the engine
#
print("Status: "+pipeline.getStatusDescription())

# Wait until it's finished
#
pipeline.waitUntilFinished()

# Evaluate the result of the pip# Get the logging from this execution
#
pipelineLog = hop.getLogging(pipeline.getLogChannelId())
print("The logging of the pipeline:")
print("----------------------------")
print(pipelineLog)

# Get the logging of a specific transform copy.
#
generateLog = hop.getLogging( pipeline.getTransform("generate", 0).getLogChannelId() )
print("The logging of transform 'generate':")
print("-------------------------------------")
print(generateLog)

# Evaluate the result of the pipeline.
# This object contains result rows, result files, metrics, and so on.
#
result = pipeline.getResult()

# Were there errors?
#
if result.getNrErrors() != 0:
  print("Pipeline had errors!")

