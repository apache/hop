#!/bin/bash


if [ -z "${HOP_LOCATION}" ]; then
    HOP_LOCATION=/opt/hop
fi

for d in ../*/ ; do
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
