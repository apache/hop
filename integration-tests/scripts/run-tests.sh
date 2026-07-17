#!/bin/bash

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
#

CURRENT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
PROJECT_NAME="$1"

echo "The Project Name = ${PROJECT_NAME}"

if [ -z "${HOP_LOCATION}" ]; then
  HOP_LOCATION=/opt/hop
fi

if [ -z "${SUREFIRE_REPORT}" ]; then
  SUREFIRE_REPORT="true"
fi

# When false/unset (default), run all main-*.hwf tests in a single hop-run JVM via
# run-project-tests.hpl. Set HOP_IT_PER_TEST_JVM=true to restore the classic isolation model
# (one hop-run process per main workflow).
if [ -z "${HOP_IT_PER_TEST_JVM}" ]; then
  HOP_IT_PER_TEST_JVM="false"
fi

# Install any JDBC drivers required by this test set, using the Hop driver-download CLI.
# Driven by HOP_DRIVERS_DOWNLOAD (comma separated driver ids, each optionally with a version, e.g.
# "vertica,mysql:9.2.0"), set per test in the integration-tests-*.yaml compose files. Restricted
# (Category X) drivers need HOP_DRIVERS_ACCEPT_LICENSE=true (the default here for the test run).
if [ -n "${HOP_DRIVERS_DOWNLOAD}" ]; then
  ACCEPT_FLAG=""
  case "${HOP_DRIVERS_ACCEPT_LICENSE:-true}" in
  true | TRUE | True | Y | y | yes | YES | 1) ACCEPT_FLAG="--accept-license" ;;
  *) ;;
  esac
  for DRIVER_SPEC in ${HOP_DRIVERS_DOWNLOAD//,/ }; do
    DRIVER_ID="${DRIVER_SPEC%%:*}"
    VERSION_FLAG=""
    if [ "${DRIVER_SPEC}" != "${DRIVER_ID}" ]; then
      VERSION_FLAG="--driver-version=${DRIVER_SPEC#*:}"
    fi
    echo "Installing JDBC driver for the tests: ${DRIVER_SPEC}"
    # shellcheck disable=SC2086
    if ! bash "${HOP_LOCATION}/hop" driver install "${DRIVER_ID}" ${VERSION_FLAG} ${ACCEPT_FLAG}; then
      echo "ERROR: failed to install JDBC driver '${DRIVER_SPEC}'"
      exit 1
    fi
  done
fi

# Ensure surefire-reports directory exists and is writable
mkdir -p "${CURRENT_DIR}"/../surefire-reports/
chmod 777 "${CURRENT_DIR}"/../surefire-reports/ 2>/dev/null || true

# Get kafka parameters
if [ -z "${BOOTSTRAP_SERVERS}" ]; then
  BOOTSTRAP_SERVERS=kafka:9092
fi
# Best-effort diagnostics when running the kafka project (helps Jenkins triage)
if [ "${PROJECT_NAME}" = "kafka" ] || [ "$(basename "${PROJECT_NAME}" 2>/dev/null)" = "kafka" ]; then
  echo "Kafka IT: BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS}"
  if command -v getent >/dev/null 2>&1; then
    echo "Kafka IT: getent hosts kafka => $(getent hosts kafka 2>&1 || true)"
  fi
  if command -v nc >/dev/null 2>&1; then
    nc -z -w 2 kafka 9092 && echo "Kafka IT: kafka:9092 is reachable" || echo "Kafka IT: kafka:9092 is NOT reachable yet"
  fi
fi

# Get database parameters
if [ -z "${POSTGRES_HOST}" ]; then
  POSTGRES_HOST=postgres
fi

if [ -z "${POSTGRES_DATABASE}" ]; then
  POSTGRES_DATABASE=hop_database
fi

if [ -z "${POSTGRES_PORT}" ]; then
  POSTGRES_PORT=5432
fi

if [ -z "${POSTGRES_USER}" ]; then
  POSTGRES_USER=hop_user
fi

if [ -z "${POSTGRES_PASSWORD}" ]; then
  POSTGRES_PASSWORD=hop_password
fi

# SSH tunnel parameters (for PostgreSQL-via-SSH integration tests)
if [ -z "${SSH_TUNNEL_HOST}" ]; then
  SSH_TUNNEL_HOST=ssh
fi
if [ -z "${SSH_TUNNEL_PORT}" ]; then
  SSH_TUNNEL_PORT=22
fi
if [ -z "${SSH_TUNNEL_USER}" ]; then
  SSH_TUNNEL_USER=hop
fi
if [ -z "${SSH_TUNNEL_PASSWORD}" ]; then
  SSH_TUNNEL_PASSWORD=hop_ssh_password
fi

if [ -z "${PROJECT_NAME}" ]; then
  PROJECT_NAME="*"
fi

# Optional filter for main*.hwf workflows (substring or glob against basename).
# Comma-separated list is supported. Examples:
#   TEST_FILTER=0077-merge-rows
#   TEST_FILTER='*0077*','*0078*'
#   TEST_FILTER=main-0077-merge-rows.hwf
if [ -z "${TEST_FILTER}" ]; then
  TEST_FILTER=""
fi

# When the GCP service-account key is missing or is the IT dummy file, skip Google Sheets
# workflows (they need a real JSON key; ASF Jenkins provides credentials id gcp-access-hop).
if [ -z "${SKIP_GOOGLE_SHEETS}" ]; then
  SKIP_GOOGLE_SHEETS="false"
fi

#set global variables
SPACER="==========================================="

# Return 0 if the workflow file should run under the current TEST_FILTER.
should_run_workflow() {
  local file="$1"
  local base
  base=$(basename "$file")

  if [ "${SKIP_GOOGLE_SHEETS}" = "true" ]; then
    case "${base}" in
    *google-sheet* | *google-sheets*)
      echo "Skipping ${base} (SKIP_GOOGLE_SHEETS=true: no valid GCP service-account JSON)"
      return 1
      ;;
    esac
  fi

  if [ -z "${TEST_FILTER}" ]; then
    return 0
  fi

  local old_ifs=$IFS
  IFS=','
  local pattern
  # shellcheck disable=SC2086
  for pattern in ${TEST_FILTER}; do
    # trim whitespace
    pattern="${pattern#"${pattern%%[![:space:]]*}"}"
    pattern="${pattern%"${pattern##*[![:space:]]}"}"
    [ -z "${pattern}" ] && continue

    # No glob meta-characters: treat as basename substring
    if [[ "${pattern}" != *[\*\?[]* ]]; then
      case "${base}" in
      *"${pattern}"*)
        IFS=$old_ifs
        return 0
        ;;
      esac
    else
      # Glob match against basename
      case "${base}" in
      ${pattern})
        IFS=$old_ifs
        return 0
        ;;
      esac
    fi
  done
  IFS=$old_ifs
  return 1
}

# Set up a temporary folder
export TMP_FOLDER=/tmp/hop-it-$$
rm -rf "${TMP_FOLDER}"
mkdir -p "${TMP_FOLDER}"

#cleanup Temp
export TMP_TESTCASES="${TMP_FOLDER}"/testcases.xml
rm -f "${TMP_TESTCASES}"

# Set up auditing
# Start with a new blank slate every time
# This means it's not needed to delete a project
#
export HOP_AUDIT_FOLDER="${TMP_FOLDER}"/audit
rm -rf "${HOP_AUDIT_FOLDER}"
mkdir -p "${HOP_AUDIT_FOLDER}"

# Store current HOP_CONFIG_FOLDER
TMP_CONFIG_FOLDER="${HOP_CONFIG_FOLDER}"

SUREFIRE_DIR="$(cd "${CURRENT_DIR}/../surefire-reports" && pwd)"
RUNNER_PIPELINE="${CURRENT_DIR}/run-project-tests.hpl"

# Shared hop-run parameters for both single-JVM and per-test modes (run configuration added per project)
HOP_RUN_COMMON_ARGS=(
  -e "dev"
  -p "POSTGRES_HOST=${POSTGRES_HOST}"
  -p "POSTGRES_DATABASE=${POSTGRES_DATABASE}"
  -p "POSTGRES_PORT=${POSTGRES_PORT}"
  -p "POSTGRES_USER=${POSTGRES_USER}"
  -p "POSTGRES_PASSWORD=${POSTGRES_PASSWORD}"
  -p "SSH_TUNNEL_HOST=${SSH_TUNNEL_HOST}"
  -p "SSH_TUNNEL_PORT=${SSH_TUNNEL_PORT}"
  -p "SSH_TUNNEL_USER=${SSH_TUNNEL_USER}"
  -p "SSH_TUNNEL_PASSWORD=${SSH_TUNNEL_PASSWORD}"
  -p "BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS}"
)

#Loop over project folders
for d in "${CURRENT_DIR}"/../${PROJECT_NAME}/; do
  #cleanup project testcases
  rm -f "${TMP_TESTCASES}"

  if [[ "$d" != *"scripts/" ]] && [[ "$d" != *"surefire-reports/" ]] && [[ "$d" != *"hopweb/" ]]; then

    # If there is a file called disabled.txt the project is disabled
    #
    if [ ! -f "$d/disabled.txt" ]; then

      #set test variables
      start_time=$SECONDS
      test_counter=0
      errors_counter=0
      skipped_counter=0
      failures_counter=0

      PROJECT_NAME=$(basename "$d")

      echo ${SPACER}
      echo "Starting Tests in project: ${PROJECT_NAME}"
      echo ${SPACER}

      # Create New Project
      export HOP_CONFIG_FOLDER="$d"

      # Project output/ is often written by pipelines (CSV, Excel/ODS temp files, etc.).
      # On ASF Jenkins the container UID matches the agent workspace owner (Jenkinsfile.daily
      # passes id -u / id -g), so writes succeed by ownership. When UIDs differ, output/ is
      # pre-created and chmod'd world-writable by run-tests-docker.sh on the host; here we
      # only best-effort reinforce that (mkdir/chmod may no-op if not owner).
      mkdir -p "$d/output" 2>/dev/null || true
      if [ -d "$d/output" ]; then
        chmod 777 "$d/output" 2>/dev/null || true
      fi

      # Default pipeline run configuration name used by hop-run and the suite runner.
      # Beam projects name their Beam engine "local" and keep a native Local engine as "hop-local".
      # The single-JVM suite driver (run-project-tests.hpl) must never run under Beam.
      PIPELINE_RUN_CONFIG="local"
      SUITE_RUN_CONFIG="local"
      if [ -f "$d/metadata/pipeline-run-configuration/hop-local.json" ]; then
        SUITE_RUN_CONFIG="hop-local"
      fi

      # Prefer single-JVM suite runner when available (unless isolation mode is requested).
      # TEST_FILTER is only applied on the classic per-workflow path, so force that mode when set.
      if [ "${HOP_IT_PER_TEST_JVM}" != "true" ] && [ -z "${TEST_FILTER}" ] && [ -f "${RUNNER_PIPELINE}" ]; then

        echo ${SPACER}
        echo "Running project tests in single JVM via run-project-tests.hpl (run config: ${SUITE_RUN_CONFIG})"
        echo ${SPACER}

        start_time_test=$SECONDS

        $HOP_LOCATION/hop-run.sh \
          -r "${SUITE_RUN_CONFIG}" \
          "${HOP_RUN_COMMON_ARGS[@]}" \
          -p "PROJECT_NAME=${PROJECT_NAME}" \
          -p "IT_SUREFIRE_DIR=${SUREFIRE_DIR}" \
          -f "${RUNNER_PIPELINE}" > >(tee /tmp/test_output) 2> >(tee /tmp/test_output_err >&1)

        exit_code=${PIPESTATUS[0]}
        test_duration=$((SECONDS - start_time_test))
        total_duration=$((SECONDS - start_time))

        if (($exit_code >= 1)); then
          errors_counter=1
          failures_counter=1
          echo "${PROJECT_NAME}" >>"${CURRENT_DIR}"/../surefire-reports/failed_tests
        else
          echo "${PROJECT_NAME}" >>"${CURRENT_DIR}"/../surefire-reports/passed_tests
        fi

        echo ${SPACER}
        echo "Project suite result"
        echo ${SPACER}
        echo "Duration: $test_duration"
        echo "Exit Code: $exit_code"

        # Surefire XML is written by the Surefire Report Output transform.
        # If the transform never ran (startup failure), write a minimal failure suite.
        if [ "${SUREFIRE_REPORT}" = "true" ]; then
          if [ ! -f "${SUREFIRE_DIR}/surefile_${PROJECT_NAME}.xml" ]; then
            echo "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" >"${SUREFIRE_DIR}/surefile_${PROJECT_NAME}.xml"
            echo "<testsuite xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:noNamespaceSchemaLocation=\"https://maven.apache.org/surefire/maven-surefire-plugin/xsd/surefire-test-report-3.0.xsd\" version=\"3.0\" name=\"${PROJECT_NAME}\" time=\"$total_duration\" tests=\"1\" errors=\"1\" skipped=\"0\" failures=\"0\">" >>"${SUREFIRE_DIR}/surefile_${PROJECT_NAME}.xml"
            echo "<testcase name=\"suite_startup\" time=\"$test_duration\"><failure type=\"suite_startup\"></failure><system-out><![CDATA[" >>"${SUREFIRE_DIR}/surefile_${PROJECT_NAME}.xml"
            cat /tmp/test_output >>"${SUREFIRE_DIR}/surefile_${PROJECT_NAME}.xml"
            echo "]]></system-out><system-err><![CDATA[" >>"${SUREFIRE_DIR}/surefile_${PROJECT_NAME}.xml"
            cat /tmp/test_output_err >>"${SUREFIRE_DIR}/surefile_${PROJECT_NAME}.xml"
            echo "]]></system-err></testcase></testsuite>" >>"${SUREFIRE_DIR}/surefile_${PROJECT_NAME}.xml"
          fi
        fi

      else

        # Classic path: one hop-run JVM per main-*.hwf
        #
        if [ -n "${TEST_FILTER}" ]; then
          echo "TEST_FILTER is set: ${TEST_FILTER}"
        fi

        find "$d" -name 'main*.hwf' | sort | while read -r f; do

          if ! should_run_workflow "$f"; then
            continue
          fi

          #cleanup temp files
          rm -f /tmp/test_output
          rm -f /tmp/test_output_err

          #set file and test name
          hop_file="$(realpath "$f")"
          test_name=$(basename "$f")
          test_name=${test_name//'main_'/}
          test_name=${test_name//'main-'/}
          test_name=${test_name//'.hwf'/}

          #Starting Test
          echo ${SPACER}
          echo "Starting Test: $test_name"
          echo ${SPACER}

          #Start time test
          start_time_test=$SECONDS

          #Run Test (use project pipeline run config, e.g. Beam "local")
          $HOP_LOCATION/hop-run.sh \
            -r "${PIPELINE_RUN_CONFIG}" \
            "${HOP_RUN_COMMON_ARGS[@]}" \
            -f "$hop_file" > >(tee /tmp/test_output) 2> >(tee /tmp/test_output_err >&1)

          #Capture exit code
          exit_code=${PIPESTATUS[0]}

          #Test time duration
          test_duration=$((SECONDS - start_time_test))

          if (($exit_code >= 1)); then
            #Write single line to overview file
            echo "$test_name" >>"${CURRENT_DIR}"/../surefire-reports/failed_tests
            #Create surefire xml failure
            echo "<testcase name=\"$test_name\" time=\"$test_duration\">" >>${TMP_TESTCASES}
            echo "<failure type=\"$test_name\"></failure>" >>${TMP_TESTCASES}
            echo "<system-out>" >>${TMP_TESTCASES}
            echo "<![CDATA[" >>${TMP_TESTCASES}
            cat /tmp/test_output >>${TMP_TESTCASES}
            echo "]]>" >>${TMP_TESTCASES}
            echo "</system-out>" >>${TMP_TESTCASES}
            echo "<system-err>" >>${TMP_TESTCASES}
            echo "<![CDATA[" >>${TMP_TESTCASES}
            cat /tmp/test_output_err >>${TMP_TESTCASES}
            echo "]]>" >>${TMP_TESTCASES}
            echo "</system-err>" >>${TMP_TESTCASES}
            echo "</testcase>" >>${TMP_TESTCASES}

          else
            #Write single line to overview file
            echo "$test_name" >>"${CURRENT_DIR}"/../surefire-reports/passed_tests
            #Create surefire xml success
            echo "<testcase name=\"$test_name\" time=\"$test_duration\">" >>${TMP_TESTCASES}
            echo "<system-out>" >>${TMP_TESTCASES}
            echo "<![CDATA[" >>${TMP_TESTCASES}
            cat /tmp/test_output >>${TMP_TESTCASES}
            echo "]]>" >>${TMP_TESTCASES}
            echo "</system-out>" >>${TMP_TESTCASES}
            echo "</testcase>" >>${TMP_TESTCASES}
          fi

          #Print results to console
          echo ${SPACER}
          echo "Test Result"
          echo ${SPACER}
          echo "Test duration: $test_duration"
          echo "Test Exit Code: $exit_code"

        done

        total_duration=$((SECONDS - start_time))

        #create final report
        if [ "${SUREFIRE_REPORT}" = "true" ]; then

          # Count testcases written (subshell-safe)
          if [ -f "${TMP_TESTCASES}" ]; then
            test_counter=$(grep -c '<testcase ' "${TMP_TESTCASES}" || true)
            failures_counter=$(grep -c '<failure ' "${TMP_TESTCASES}" || true)
            errors_counter=${failures_counter}
          fi

          echo "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" >"${CURRENT_DIR}"/../surefire-reports/surefile_${PROJECT_NAME}.xml
          echo "<testsuite xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:noNamespaceSchemaLocation=\"https://maven.apache.org/surefire/maven-surefire-plugin/xsd/surefire-test-report-3.0.xsd\" version=\"3.0\" name=\"${PROJECT_NAME}\" time=\"$total_duration\" tests=\"$test_counter\" errors=\"$errors_counter\" skipped=\"$skipped_counter\" failures=\"$failures_counter\">" >>"${CURRENT_DIR}"/../surefire-reports/surefile_${PROJECT_NAME}.xml
          if [ -f "${TMP_TESTCASES}" ]; then
            cat ${TMP_TESTCASES} >>"${CURRENT_DIR}"/../surefire-reports/surefile_${PROJECT_NAME}.xml
          fi
          echo "</testsuite>" >>"${CURRENT_DIR}"/../surefire-reports/surefile_${PROJECT_NAME}.xml

        fi
      fi
    fi
  fi
done

# Cleanup config and audit folders
#
rm -rf "${HOP_AUDIT_FOLDER}"
rm -rf "${TMP_FOLDER}"

# Set back to old config folder
export HOP_CONFIG_FOLDER="${TMP_CONFIG_FOLDER}"
