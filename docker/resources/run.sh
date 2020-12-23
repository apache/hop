#!/bin/bash


#
# When stopping the running hop-server container with 'docker stop' it ended up with an timeout
# and an exitcode > 0 because signals are not catched correctly.
# It was not possible to end the container gracefully with Ctrl-C when it was started without -d 
# option which might be annoying on a gratefull exit. Therefore this script ( run.sh ) 
# that catches signals coming from the docker host is introduced.
#

log() {
    echo `date '+%Y/%m/%d %H:%M:%S'`" - ${1}"
}

#
#   catch all signals that come from outside the container
#   to be able to exit gracefully
#
trapper() {
    "$@" &
    pid="$!"
    log "Running the entrypoint script with PID ${pid}"
    trap "log 'Stopping entrypoint script with $pid'; kill -SIGTERM $pid" SIGINT SIGTERM

    while kill -0 $pid > /dev/null 2>&1; do
        wait
    done
}

trapper /opt/project-hop/load-and-execute.sh $@
