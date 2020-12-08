#!/bin/bash

current_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

if [ -z "${HOP_LOCATION}" ]; then
    HOP_LOCATION=/opt/hop
fi

#set global variables
spacer="==========================================="

#cleanup Temp
rm -f /tmp/testcases
rm -rf $current_dir/../surefire-reports
mkdir -p $current_dir/../surefire-reports/

#Loop over project folders
for d in $current_dir/../*/ ; do
    if [[ "$d" != *"scripts/" ]] && [[ "$d" != *"surefire-reports/" ]]; then

        #set test variables
        start_time=$SECONDS
        test_counter=0
        errors_counter=0
        skipped_counter=0
        failures_counter=0

        project_name=$(basename $d)

        echo $spacer
        echo "Starting Tests in project: $project_name"
        echo $spacer

        #Increment timer and set test start time
        test_counter=$((test_counter+1))
        
        #Delete project first
        $HOP_LOCATION/hop-conf.sh -pd -p $project_name

        #Create New Project
        $HOP_LOCATION/hop-conf.sh -pc -p $project_name -ph "$(readlink -f $d)"

        #Find main hwf files TODO: add hpl support when result is returned correctly
        for f in $d/main_*.hwf ; do

            #cleanup temp files
            rm -f /tmp/test_output
            rm -f /tmp/test_output_err

            #set file and test name
            hop_file="$(readlink -f $f)"
            test_name=$(basename $f)
            test_name=${test_name//'main_'/}
            test_name=${test_name//'.hwf'/}

            #Starting Test
            echo $spacer
            echo "Starting Test: $test_name"
            echo $spacer

            #Start time test
            start_time_test=$SECONDS

            #Run Test
            $HOP_LOCATION/hop-run.sh -j $project_name -r "local" -f $hop_file > >(tee /tmp/test_output) 2> >(tee /tmp/test_output_err >&1)

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
                echo "<![CDATA["  >> /tmp/testcases
                cat /tmp/test_output >> /tmp/testcases
                echo "]]>"  >> /tmp/testcases
                echo "</system-out>" >> /tmp/testcases
                echo "<system-err>" >> /tmp/testcases
                echo "<![CDATA["  >> /tmp/testcases
                cat /tmp/test_output_err >> /tmp/testcases
                echo "]]>"  >> /tmp/testcases
                echo "</system-err>" >> /tmp/testcases
                echo "</testcase>" >> /tmp/testcases

            else
                #Create surefire xml success
                echo "<testcase name=\"$test_name\" time=\"$test_duration\">" >> /tmp/testcases
                echo "<system-out>" >> /tmp/testcases
                echo "<![CDATA["  >> /tmp/testcases
                cat /tmp/test_output >> /tmp/testcases
                echo "]]>"  >> /tmp/testcases
                echo "</system-out>" >> /tmp/testcases
                echo "</testcase>" >> /tmp/testcases
            fi

            #Print results to console
            echo $spacer
            echo "Test Result"
            echo $spacer
            echo "Test duration: $test_duration"
            echo "Test Exit Code: $exit_code"

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
        echo "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" > $current_dir/../surefire-reports/surefile_$project_name.xml
        echo "<testsuite xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:noNamespaceSchemaLocation=\"https://maven.apache.org/surefire/maven-surefire-plugin/xsd/surefire-test-report-3.0.xsd\" version=\"3.0\" name=\"$project_name\" time=\"$total_duration\" tests=\"$test_counter\" errors=\"$errors_counter\" skipped=\"$skipped_counter\" failures=\"$failures_counter\">" >> $current_dir/../surefire-reports/surefile_$project_name.xml
        cat /tmp/testcases >> $current_dir/../surefire-reports/surefile_$project_name.xml
        echo "</testsuite>" >> $current_dir/../surefire-reports/surefile_$project_name.xml

    fi
done