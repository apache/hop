#!/bin/bash

current_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

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


for d in $current_dir/../*/ ; do
    if [[ "$d" != *"scripts/" ]]; then
        echo "Starting Test: $(basename $d)"

        #Delete project first
        $HOP_LOCATION/hop-conf.sh -pd -p $(basename $d)

        #Create New Project
        $HOP_LOCATION/hop-conf.sh -pc -p $(basename $d) -ph "$(readlink -f $d)"

        #Find main hpl/hwf
        HOP_FILE="$(readlink -f $d/main*)"

        #Run Test
        $HOP_LOCATION/hop-run.sh -j $(basename $d) -r "local" -f $HOP_FILE

        #echo Exit code
        echo $?
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

#Copy final report back
mkdir -p $current_dir/../surefire-reports/
cp /tmp/surefire_report.xml $current_dir/../surefire-reports/report.xml
