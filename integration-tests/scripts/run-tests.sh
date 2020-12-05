#!/bin/bash

if [ -z "${HOP_LOCATION}" ]; then
    HOP_LOCATION=/opt/hop
fi

#set start variables
start_time=$SECONDS
test_counter=0
errors_counter=0
skipped_counter=0
failures_counter=0
spacer="==========================================="

#cleanup surefire report
/dev/null > /tmp/testcases

for d in ../*/ ; do
    if [[ "$d" != *"scripts/" ]]; then

        test_name=$(basename $d)

        echo $spacer
        echo "Starting Test: $test_name"
        echo $spacer

        #Increment timer and set test start time
        test_counter=$((test_counter+1))
        
        #Delete project first
        $HOP_LOCATION/hop-conf.sh -pd -p $test_name

        #Create New Project
        $HOP_LOCATION/hop-conf.sh -pc -p $test_name -ph "$(readlink -f $d)"

        #Find main hwf/ TODO: add hpl support when result is returned correctly
        HOP_FILE="$(readlink -f $d/main.hwf)"

        #Start time test
        start_time_test=$SECONDS

        #Run Test
        $HOP_LOCATION/hop-run.sh -j $test_name -r "local" -f $HOP_FILE > >(tee /tmp/test_output) 2> >(tee /tmp/test_output_err >&1)

        #Capture exit code
        exit_code=${PIPESTATUS[0]}

        #Test time duration
        test_duration=$(( SECONDS - start_time_test ))

        if (( $exit_code >= 1 )) ; 
        then
            errors_counter=$((errors_counter+1))
            failures_counter=$((failures_counter+1))
            #Create surefire xml failure
            echo "<testcase name=\"$test_name\" time=\"$test_duration\">" >> /tmp/testcases
            echo "<failure type=\"$test_name\"></failure>" >> /tmp/testcases
            echo "<system-out>" >> /tmp/testcases
            cat /tmp/test_output >> /tmp/testcases
            echo "</system-out>" >> /tmp/testcases
            echo "<system-err>" >> /tmp/testcases
            cat /tmp/test_output_err >> /tmp/testcases
            echo "</system-err>" >> /tmp/testcases
            echo "</testcase>" >> /tmp/testcases

        else
            #Create surefire xml success
            echo "<testcase name=\"$test_name\" time=\"$test_duration\">" >> /tmp/testcases
            echo "<system-out>" >> /tmp/testcases
            cat /tmp/test_output >> /tmp/testcases
            echo "</system-out>" >> /tmp/testcases
            echo "</testcase>" >> /tmp/testcases
        fi

        #Print results to console
        echo $spacer
        echo "Test Result"
        echo $spacer
        echo "Test duration: $test_duration"
        echo "Test Exit Code: $exit_code"

    fi
done

total_duration=$(( SECONDS - start_time ))

#Print End results
echo $spacer
echo "Final Report"
echo $spacer
echo "Number of Tests: $test_counter"
echo "Total errors: $errors_counter"
echo "Total faliures: $failures_counter"
echo "Total duration: $total_duration"

#create final report
echo "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" >> /tmp/surefire_report.xml
echo "<testsuite xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:noNamespaceSchemaLocation=\"https://maven.apache.org/surefire/maven-surefire-plugin/xsd/surefire-test-report-3.0.xsd\" version=\"3.0\" name=\"Hop Integration Tests\" time=\"$total_duration\" tests=\"$test_counter\" errors=\"$errors_counter\" skipped=\"$skipped_counter\" failures=\"$failures_counter\">" >> /tmp/surefire_report.xml
cat /tmp/testcases >> /tmp/surefire_report.xml
echo "</testsuite>" >> /tmp/surefire_report.xml
