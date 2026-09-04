#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Build a valid /hop/addPipeline request body (PipelineConfiguration XML).
#
#   ./make-payload.sh <pipeline.hpl> [out.xml]
#
# The servlet expects:
#   <pipeline_configuration>
#     <pipeline>...</pipeline>                          <- pipeline meta (a .hpl body)
#     <pipeline_execution_configuration>...</...>       <- run settings
#     <metastore_json>base64(gzip(json))</metastore_json>  <- metadata, incl. run configuration
#   </pipeline_configuration>
#
# metastore_json mirrors Hop's HttpUtil.encodeBase64ZippedString: base64(gzip(utf8)).
set -uo pipefail

HPL="${1:-}"
OUT="${2:-payload.xml}"

if [[ -z "${HPL}" || ! -f "${HPL}" ]]; then
  echo "usage: make-payload.sh <pipeline.hpl> [out.xml]" >&2
  exit 1
fi

# Extract the <pipeline>...</pipeline> element from the .hpl (drops the XML declaration/licence).
pipeline_xml="$(awk '/<pipeline>/{f=1} f{print} /<\/pipeline>/{exit}' "${HPL}")"
if [[ -z "${pipeline_xml}" ]]; then
  echo "no <pipeline> element found in ${HPL}" >&2
  exit 1
fi

# Ship a 'local' run configuration inline so the target needs no pre-existing metadata.
metadata='{"pipeline-run-configuration":[{"engineRunConfiguration":{"Local":{"feedback_size":"50000","sample_size":"100","sample_type_in_gui":"Last","rowset_size":"10000","safe_mode":false,"show_feedback":false,"topo_sort":false,"gather_metrics":false}},"name":"local","configurationVariables":[],"description":"","dataProfile":"","defaultSelection":true}]}'

# base64(gzip(json)) - the encoding Hop's SerializableMetadataProvider reads back.
metastore="$(printf '%s' "${metadata}" | gzip -c | base64 | tr -d '\n')"

exec_cfg='<pipeline_execution_configuration>
<pass_export>N</pass_export>
<parameters/>
<variables/>
<log_level>Basic</log_level>
<log_file>N</log_file>
<clear_log>Y</clear_log>
<run_configuration>local</run_configuration>
<gather_metrics>N</gather_metrics>
</pipeline_execution_configuration>'

{
  echo "<pipeline_configuration>"
  echo "${pipeline_xml}"
  echo "${exec_cfg}"
  echo "<metastore_json>${metastore}</metastore_json>"
  echo "</pipeline_configuration>"
} >"${OUT}"

name="$(printf '%s' "${pipeline_xml}" | sed -n 's:.*<name>\(.*\)</name>.*:\1:p' | head -1)"
echo "${OUT} written ($(wc -c <"${OUT}") bytes), pipeline name: ${name:-?}"
