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

# Hard cap (in seconds) on a single hop-run invocation. Without it, one workflow that never
# returns blocks the whole nightly and says nothing about why: ASF Jenkins builds #2269 and #2270
# both sat inside the sftp project until the run was killed hours later, with the console log
# ending mid-workflow. When the cap is hit the watchdog first sends SIGQUIT to the JVM, which
# writes a full thread dump to the test output (the test image ships a JRE, so jstack/jcmd are not
# available), and only then kills it, so the next hang lands in the log with a stack trace.
# Set HOP_IT_TIMEOUT=0 to disable the watchdog.
if [ -z "${HOP_IT_TIMEOUT}" ]; then
  HOP_IT_TIMEOUT=3600
fi
case "${HOP_IT_TIMEOUT}" in
'' | *[!0-9]*)
  echo "WARNING: ignoring non-numeric HOP_IT_TIMEOUT='${HOP_IT_TIMEOUT}', using 3600"
  HOP_IT_TIMEOUT=3600
  ;;
*) ;;
esac

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

# Double-check inside the container: the host may have a valid key while the file that actually
# reaches the pipelines is still the placeholder (bad mount, stale image, ...). Deciding here, on
# the file the transforms open, turns that into a clean skip instead of a Google credentials error.
GCP_KEY_IN_CONTAINER="${GOOGLE_APPLICATION_CREDENTIALS:-/tmp/google-key-apache-hop-it.json}"
if [ "${SKIP_GOOGLE_SHEETS}" != "true" ]; then
  if [ ! -s "${GCP_KEY_IN_CONTAINER}" ] \
    || ! grep -qE '"type"[[:space:]]*:[[:space:]]*"service_account"' "${GCP_KEY_IN_CONTAINER}" 2>/dev/null; then
    echo "WARNING: ${GCP_KEY_IN_CONTAINER} is not a service-account JSON key even though the host"
    echo "         reported a valid one (check the GCP_KEY_HOST_PATH mount in integration-tests-base.yaml)."
    echo "         Skipping the Google Sheets integration tests."
    SKIP_GOOGLE_SHEETS="true"
  fi
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

# Emit one "STATUS<TAB>NAME<TAB>TIME" line per <testcase> in a surefire XML report.
# The single-JVM suite runner records each test's outcome in that report rather than in this
# script's loop, so it is the only place the per-test breakdown still exists.
# Parsed with python3 (installed in the IT image) rather than grep/awk: every testcase embeds
# its full workflow log in a CDATA section, and that log text can contain anything a line-based
# parser would mistake for markup.
parse_surefire_testcases() {
  python3 - "$1" <<'PYTHON_PARSE_SUREFIRE' 2>/dev/null
import sys
import xml.etree.ElementTree as ET

try:
    root = ET.parse(sys.argv[1]).getroot()
except Exception:
    sys.exit(1)

for testcase in root.iter("testcase"):
    if testcase.find("failure") is not None:
        status = "FAIL"
    elif testcase.find("error") is not None:
        status = "ERROR"
    elif testcase.find("skipped") is not None:
        status = "SKIP"
    else:
        status = "PASS"
    print("%s\t%s\t%s" % (status, testcase.get("name", ""), testcase.get("time", "")))
PYTHON_PARSE_SUREFIRE
}

# Run hop-run.sh with the usual tee redirection, bounded by HOP_IT_TIMEOUT (see above).
# Returns hop-run's own exit code, or 124 when the watchdog had to kill a stuck run.
run_hop_with_watchdog() {
  if [ "${HOP_IT_TIMEOUT}" -eq 0 ]; then
    $HOP_LOCATION/hop-run.sh "$@" > >(tee /tmp/test_output) 2> >(tee /tmp/test_output_err >&1)
    return $?
  fi

  $HOP_LOCATION/hop-run.sh "$@" > >(tee /tmp/test_output) 2> >(tee /tmp/test_output_err >&1) &
  local runner_pid=$!
  local waited=0

  while kill -0 "${runner_pid}" 2>/dev/null; do
    if [ "${waited}" -ge "${HOP_IT_TIMEOUT}" ]; then
      echo "${SPACER}"
      echo "ERROR: hop-run has not finished after ${HOP_IT_TIMEOUT}s, assuming it is stuck."
      echo "Sending SIGQUIT to the JVM for a thread dump, then terminating it."
      echo "${SPACER}"

      # hop-run.sh does not exec java, so the JVM is a child of the shell we started.
      local jvm_pid
      jvm_pid=$(pgrep -P "${runner_pid}" java | head -n1)
      if [ -z "${jvm_pid}" ]; then
        jvm_pid=$(pgrep -f 'org.apache.hop.run.HopRun' | head -n1)
      fi
      if [ -n "${jvm_pid}" ]; then
        # SIGQUIT makes the JVM print every thread's stack on its stdout, which tee captures.
        # Twice: two dumps a few seconds apart show whether anything is moving at all.
        kill -QUIT "${jvm_pid}" 2>/dev/null || true
        sleep 15
        kill -QUIT "${jvm_pid}" 2>/dev/null || true
        sleep 10
        kill -TERM "${jvm_pid}" 2>/dev/null || true
        sleep 5
        kill -KILL "${jvm_pid}" 2>/dev/null || true
      else
        echo "WARNING: could not find the hop-run JVM process, killing the wrapper only"
      fi

      kill -TERM "${runner_pid}" 2>/dev/null || true
      wait "${runner_pid}" 2>/dev/null

      # The surefire report is built from these files, so say there why the run has no result.
      {
        echo ""
        echo "ERROR: hop-run was killed by the integration-test watchdog after ${HOP_IT_TIMEOUT}s."
        echo "The JVM thread dump above (SIGQUIT) shows where the run was stuck."
      } >>/tmp/test_output
      return 124
    fi
    sleep 5
    waited=$((waited + 5))
  done

  wait "${runner_pid}"
  return $?
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

    # If there is a file called disabled.txt the project is disabled, unless the run explicitly
    # opted in with INCLUDE_DISABLED=true (see run-tests-docker.sh).
    #
    if [ ! -f "$d/disabled.txt" ] \
      || [ "${INCLUDE_DISABLED:-false}" = "true" ] \
      || [[ ",${INCLUDE_DISABLED:-}," == *",$(basename "$d"),"* ]]; then

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

      # Project output/ and files/ are written by pipelines (CSV, Excel/ODS, HTTP downloads,
      # MDI JSON, parquet, etc.). MDI also writes *-injected.hpl into the project root.
      # On ASF Jenkins the container UID matches the agent workspace owner (Jenkinsfile.daily
      # passes id -u / id -g), so writes succeed by ownership. When UIDs differ, dirs are
      # pre-created and chmod'd world-writable by run-tests-docker.sh on the host; here we
      # only best-effort reinforce that (mkdir/chmod may no-op if not owner).
      chmod a+rwx "$d" 2>/dev/null || true
      mkdir -p "$d/output" "$d/files" 2>/dev/null || true
      if [ -d "$d/output" ]; then
        chmod 777 "$d/output" 2>/dev/null || true
      fi
      if [ -d "$d/files" ]; then
        chmod 777 "$d/files" 2>/dev/null || true
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
      # TEST_FILTER and SKIP_GOOGLE_SHEETS are only applied on the classic per-workflow path
      # (should_run_workflow), so force that mode when either is set for this project.
      USE_SUITE_RUNNER=true
      if [ "${HOP_IT_PER_TEST_JVM}" = "true" ] || [ -n "${TEST_FILTER}" ]; then
        USE_SUITE_RUNNER=false
      fi
      if [ "${SKIP_GOOGLE_SHEETS}" = "true" ] && [ "${PROJECT_NAME}" = "spreadsheet" ]; then
        USE_SUITE_RUNNER=false
        echo "SKIP_GOOGLE_SHEETS=true: using classic per-workflow runner so Google Sheets tests are skipped"
      fi
      if [ "${USE_SUITE_RUNNER}" = "true" ] && [ -f "${RUNNER_PIPELINE}" ]; then

        echo ${SPACER}
        echo "Running project tests in single JVM via run-project-tests.hpl (run config: ${SUITE_RUN_CONFIG})"
        echo ${SPACER}

        start_time_test=$SECONDS

        run_hop_with_watchdog \
          -r "${SUITE_RUN_CONFIG}" \
          "${HOP_RUN_COMMON_ARGS[@]}" \
          -p "PROJECT_NAME=${PROJECT_NAME}" \
          -p "IT_SUREFIRE_DIR=${SUREFIRE_DIR}" \
          -f "${RUNNER_PIPELINE}"

        exit_code=$?
        test_duration=$((SECONDS - start_time_test))
        total_duration=$((SECONDS - start_time))

        if (($exit_code >= 1)); then
          errors_counter=1
          failures_counter=1
        fi

        echo ${SPACER}
        echo "Project suite result"
        echo ${SPACER}
        echo "Duration: $test_duration"
        echo "Exit Code: $exit_code"

        # A watchdog kill must always turn the build red. The suite may already have written a
        # report for the workflows it did finish, so add a separate failing suite rather than
        # overwriting those results.
        if [ "${exit_code}" -eq 124 ] && [ "${SUREFIRE_REPORT}" = "true" ]; then
          TIMEOUT_REPORT="${SUREFIRE_DIR}/surefile_${PROJECT_NAME}_timeout.xml"
          {
            echo "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            echo "<testsuite xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:noNamespaceSchemaLocation=\"https://maven.apache.org/surefire/maven-surefire-plugin/xsd/surefire-test-report-3.0.xsd\" version=\"3.0\" name=\"${PROJECT_NAME}_timeout\" time=\"$total_duration\" tests=\"1\" errors=\"1\" skipped=\"0\" failures=\"0\">"
            echo "<testcase name=\"suite_timeout\" time=\"$test_duration\"><failure type=\"suite_timeout\">hop-run did not finish within ${HOP_IT_TIMEOUT}s</failure><system-out><![CDATA["
            cat /tmp/test_output
            echo "]]></system-out><system-err><![CDATA["
            cat /tmp/test_output_err
            echo "]]></system-err></testcase></testsuite>"
          } >"${TIMEOUT_REPORT}"
        fi

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

        # Every main*.hwf ran inside one JVM, so this loop never saw the individual tests.
        # Replay the per-test outcomes from the surefire report the suite just wrote, so the
        # console keeps its per-test breakdown and the passed_tests/failed_tests overview files
        # (printed at the end of run-tests-docker.sh) list test names rather than one project
        # name. Runs after the fallback report above on purpose: a suite that died on startup
        # then shows up here as a failed "suite_startup" test instead of vanishing.
        SUITE_RESULTS="${TMP_FOLDER}/suite-results-${PROJECT_NAME}.tsv"
        : >"${SUITE_RESULTS}"
        if [ -f "${SUREFIRE_DIR}/surefile_${PROJECT_NAME}.xml" ]; then
          parse_surefire_testcases "${SUREFIRE_DIR}/surefile_${PROJECT_NAME}.xml" \
            >"${SUITE_RESULTS}" || : >"${SUITE_RESULTS}"
        fi

        if [ -s "${SUITE_RESULTS}" ]; then
          suite_passed=0
          suite_failed=0
          suite_skipped=0

          echo ${SPACER}
          echo "Test results: ${PROJECT_NAME}"
          echo ${SPACER}

          while IFS=$'\t' read -r tc_status tc_name tc_time; do
            [ -z "${tc_name}" ] && continue
            case "${tc_status}" in
            PASS)
              suite_passed=$((suite_passed + 1))
              echo -e "\033[1;32mPASSED \033[0m ${tc_name} (${tc_time}s)"
              echo "${tc_name}" >>"${CURRENT_DIR}"/../surefire-reports/passed_tests
              ;;
            SKIP)
              suite_skipped=$((suite_skipped + 1))
              echo -e "\033[1;93mSKIPPED\033[0m ${tc_name}"
              ;;
            *)
              suite_failed=$((suite_failed + 1))
              echo -e "\033[1;91mFAILED \033[0m ${tc_name} (${tc_time}s)"
              echo "${tc_name}" >>"${CURRENT_DIR}"/../surefire-reports/failed_tests
              ;;
            esac
          done <"${SUITE_RESULTS}"

          echo ${SPACER}
          echo "${PROJECT_NAME}: ${suite_passed} passed, ${suite_failed} failed, ${suite_skipped} skipped"

          # A non-zero hop-run exit that no testcase accounts for (e.g. the suite aborted after
          # the report was written) must still surface as a failure rather than an all-green list.
          if (($exit_code >= 1)) && ((suite_failed == 0)); then
            echo "${PROJECT_NAME} (suite exited ${exit_code})" >>"${CURRENT_DIR}"/../surefire-reports/failed_tests
          fi
        else
          # No parseable report at all: fall back to a single project-level entry.
          if (($exit_code >= 1)); then
            echo "${PROJECT_NAME}" >>"${CURRENT_DIR}"/../surefire-reports/failed_tests
          else
            echo "${PROJECT_NAME}" >>"${CURRENT_DIR}"/../surefire-reports/passed_tests
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
          run_hop_with_watchdog \
            -r "${PIPELINE_RUN_CONFIG}" \
            "${HOP_RUN_COMMON_ARGS[@]}" \
            -f "$hop_file"

          #Capture exit code
          exit_code=$?

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
