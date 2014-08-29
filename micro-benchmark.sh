#!/bin/bash

#USERNAME="ubuntu"
#USER_ROOT="/home/$USERNAME/crdtdb-git/"
#RIAK_ROOT="/home/$USERNAME/riak/"

USERNAME="balegas"
USER_ROOT="/Users/$USERNAME/"
INDIGO_ROOT=$USER_ROOT"workspace/java/swiftcloud-indigo/"

REGION_NAME=(
	"US-EAST"
	"EUROPE"
	)

INDIGOS=(
	"tcp://*/36001/US-EAST"
	"tcp://*/36001/EUROPE"
	)

#Pass all of these
SEQUENCERS=(
	"tcp://*/31001/US-EAST"
	"tcp://*/31002/EUROPE"
	)
					
#Pass all of these? or just the others?
SERVERS=(
	"tcp://*/32001/US-EAST"
	"tcp://*/32002/EUROPE"
	)

SERVER_MACHINES=(
	"localhost"
	)

CLIENT_MACHINES=(
	"localhost"
	)

TABLE="table"
N_KEYS=(1 100 1000)
N_REGIONS=(1)
N_THREADS=(1 10 20 30)
MODE=("-indigo" "-weak" "-strong")
SHEPARD="localhost"
DISTRIBUTION="uniform"
INIT_VAL=1000
#<Clients> #<Command>
ssh_command() {
	hosts=($1)
	for h in ${hosts[@]}; do
		OIFS=$IFS
		IFS=':'			
		tokens=($h)
		client=${tokens[0]}
		echo "client  " $client
		ssh -t $USERNAME@$client $2
		IFS=$OIFS
	done
}

kill_all() {
#	cmd="rm -fr crdtdb/results/*"
	cmd="killall java"
	ssh_command "$1" "$cmd"
	echo "All clients have stopped"
}

#Process options
while getopts "c:d:n:r:t:v:k" optname
  do
    case "$optname" in
		"c")
			case $OPTARG in
				'strong')
					MODE=( "-strong")
				;;
				'weak') 
					MODE=( "-weak")
				;;
				'indigo') 
					MODE=( "-indigo")
				;;
			esac
		;;
		"d")
			case $OPTARG in
				'zipf')
					DISTRIBUTION="zipf"
				;;
				'uniform') 
					DISTRIBUTION="uniform"
				;;
			esac
		;;
		"r")
			N_REGIONS=($OPTARG)
		;;
		"n")
			N_KEYS=($OPTARG)
		;;
		"t")
			N_THREADS=($OPTARG)
		;;
		"v")
			INIT_VAL=($OPTARG)
		;;
		"k")
			kill_all "`echo ${SERVER_MACHINES[@]}`"
			kill_all "`echo ${CLIENT_MACHINES[@]}`"
			exit
		;;
		"?")
			echo "Unknown option $OPTARG"
		;;
		":")
			echo "No argument value for option $OPTARG"
		;;
		*)
			# Should not occur
			echo "Unknown error while processing options"
		;;
	esac
	done

LOG="-Djava.util.logging.config.file="$INDIGO_ROOT"TrueIndigo/stuff/benchmarks.properties"
CMD="java -classpath "$INDIGO_ROOT"bin/:"$INDIGO_ROOT"TrueIndigo/lib/* "$LOG" indigo.application.benchmark.MicroBenchmark"

echo "####################################################"
echo "####################################################"
echo "####################################################"

for m in "${MODE[@]}"
do
	:
	for i in "${N_REGIONS[@]}"
	do
		:
		for j in "${N_THREADS[@]}"
		do
			:
			for k in "${N_KEYS[@]}"
			do
				:
				echo $j" THREADS"
				echo $i" REGIONS"
				echo $k" KEYS"
				echo $m" MODE"
				echo $DISTRIBUTION" DISTRIBUTION"
				OUTPUT_DIR=$USER_ROOT"results"$m"-k"$k"-r"$i"-t"$j"/"
				makeDir="mkdir -p $OUTPUT_DIR "

				sequencers=(${SEQUENCERS[@]:0:$i})
				servers=(${SERVERS[@]:0:$i})
				server_machines=(${SERVER_MACHINES[@]:0:$i})
				echo "SERVERS "$servers
				ri=0;
				for h in ${server_machines[@]}; do
					cmd=$CMD" -startDC -siteId "${REGION_NAME[$((ri))]}" -sequencers "${sequencers[@]}" -servers "${servers[@]}" "$MODE
					ri=`expr $ri + 1`
					echo "Start DC "$h "CMD" $cmd
					ssh $USERNAME@$h "nohup "$cmd " > dc_console.log" &
					sleep 2 
					
				done

				master=${CLIENT_MACHINES[0]}
				cmd=$CMD" -init -siteId "${REGION_NAME[0]}" -nKeys "$k" -table "$TABLE" "$MODE" -initValue "$INIT_VAL
				echo "Init data "$master" CMD "$cmd
				ssh $USERNAME@$master $cmd

				indigos=(${INDIGOS[@]:0:$i})
				client_machines=(${CLIENT_MACHINES[@]:0:$i})
				ri=0;
				for h in ${client_machines[@]}; do
					cmd=$makeDir"; "$CMD" -run -siteId "${REGION_NAME[$((ri))]}" -nKeys "$k" -threads "$j" -srvAddress "${indigos[$((ri))]}" -table "$TABLE" "$MODE" -results_dir "$OUTPUT_DIR" -initValue "$INIT_VAL
					ri=`expr $ri + 1`
					echo "Run client "$h" CMD "$cmd
					ssh $USERNAME@$h $cmd
				done

				sleep 10
				kill_all "`echo ${SERVER_MACHINES[@]}`"

			done
		done
	done
done

echo "Finish"