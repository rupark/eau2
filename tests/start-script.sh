#!/bin/bash

# Enable background tasks
set -m

# Read args
if [ "$#" -lt 2 ]; then
	    echo "Usage: $1 <file-to-run> <num-instances> <args passed to code...>"
	        exit 1
	fi

	example="$1"
	numinst="$2"
	shift
	shift
	rest="$@"

	# Start rendezvous server
	# CHANGEME: Replace the following with whatever you need to start your rendezvous server
	# Mine starts the program at bin/rendezvous with the address argument our implementation needs
	bin/rendezvous 127.0.0.1 &>/dev/null &

	# Also capture the PID of the server
	rendpid=$!

	# Create an array of application PIDs
	pids=()

	# Run each application on a different port
	for i in $(seq 0 $(($numinst - 1))); do
		    # CHANGEME: Also replace the following line with your implementation's command to start applications
		        # here, i run racket with the provided example file with parameters for the node ID ($i),
			    # the total nodes ($numinst), the address and port (13337+$i) and $rest for any additional arguments needed
			        racket "$example" -n $i -t $numinst -r 127.0.0.1 -l $((13337 + $i)) $rest &

				    # Capture PIDs in the array
				        pids+=($!)
				done

				# Make sure we kill all the processes on CTRL-C
				function myhandler {
					    for pid in "${pids[@]}"; do
						            kill $pid
							            wait $pid
								        done

									    kill $rendpid
									        wait $rendpid
									}

									trap myhandler INT

									# Wait for all processes to complete
									for pid in "${pids[@]}"; do
										    wait $pid
									    done

									    # Kill rendezvous server
									    kill $rendpid
									    wait $rendpid

									    # Indicate success
									    exit 0
