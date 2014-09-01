#!/bin/bash

#USERNAME="balegas"
#INDIGO_ROOT="/Users/$USERNAME/swiftcloud_deployment/"
#SOURCE_ROOT="/Users/$USERNAME/workspace/java/swiftcloud-indigo/"

USERNAME="ec2-user"
INDIGO_ROOT="/home/$USERNAME/"
SOURCE_ROOT="/Users/balegas/workspace/java/swiftcloud-indigo/"

REGION_NAME=(
	"US-EAST"
	)

INDIGOS=(
	"tcp://ec2-54-164-140-77.compute-1.amazonaws.com:36001/US-EAST"
	)

#Pass all of these
SEQUENCERS=(
	"tcp://ec2-54-164-140-77.compute-1.amazonaws.com:31001/US-EAST"
	)
					
#Pass all of these? or just the others?
SERVERS=(
	"tcp://ec2-54-164-140-77.compute-1.amazonaws.com:32001/US-EAST"
	)

SERVER_MACHINES=(
	"ec2-54-164-140-77.compute-1.amazonaws.com"
	)

CLIENT_MACHINES=(
	"ec2-54-164-80-239.compute-1.amazonaws.com"
	)

SHEPARD_URL="tcp://ec2-54-164-140-77.compute-1.amazonaws.com:29876/"
TABLE="table"
N_KEYS=(1 100 1000)
N_REGIONS=(1)
N_THREADS=(1 10 20 30)
MODE=("-indigo")
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

rsync_source() {
	servers=("$@")
	cmd="prsync -r "		
	for h in ${servers[@]}; do
		cmd=$cmd" -H "$USERNAME"@"$h" "
	done
	cmd1=$cmd" "$SOURCE_ROOT"TrueIndigo/swiftcloud.jar "$INDIGO_ROOT
	$cmd1
	cmd2=$cmd" "$SOURCE_ROOT"TrueIndigo/stuff "$INDIGO_ROOT
	$cmd2
}

get_results() {
	servers=("$@")
	cmd="rsync -r "		
	for h in ${servers[@]}; do
		cmd=$cmd" "$USERNAME"@"$h":results* "$SOURCE_ROOT"../indigo_results/"
		$cmd
	done
}

#Process options
while getopts "abc:d:n:r:t:v:k" optname
  do
    case "$optname" in
		"a")
			rsync_source "${SERVER_MACHINES[@]}"
			rsync_source "${CLIENT_MACHINES[@]}"
			exit
		;;
		"b")
			get_results "${CLIENT_MACHINES[@]}"
			exit
		;;
		
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

CLASSPATH="-classpath "$INDIGO_ROOT"swiftcloud.jar"
LOG="-Djava.util.logging.config.file="$INDIGO_ROOT"stuff/benchmarks.properties"
CMD="java "$CLASSPATH" "$LOG" indigo.application.benchmark.MicroBenchmark"
SHEPARD="java "$CLASSPATH" "$LOG" sys.shepard.PatientShepard"
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
				echo $INIT_VAL" INIT VALUE"
				OUTPUT_DIR=$INDIGO_ROOT"results"$m"-k"$k"-r"$i"-t"$j"-v"$INIT_VAL"-"$DISTRIBUTION"/"
				makeDir="mkdir -p $OUTPUT_DIR"

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

				master=${SERVER_MACHINES[0]}
				cmd=$makeDir" & "$makeDir"init & "$CMD" -init -siteId "${REGION_NAME[0]}" -nKeys "$k" -table "$TABLE" "$MODE" -initValue "$INIT_VAL" -results_dir "$OUTPUT_DIR"init"
				echo "Init data "$master" CMD "$cmd
				ssh $USERNAME@$master "nohup "$cmd
				ssh $USERNAME@$master "nohup "$SHEPARD" -url "$SHEPARD_URL" -count "$i &
				
				sleep 1

				indigos=(${INDIGOS[@]:0:$i})
				client_machines=(${CLIENT_MACHINES[@]:0:$i})
				ri=0;
				for h in ${client_machines[@]}; do
					cmd=$makeDir" ; "$CMD" -run -siteId "${REGION_NAME[$((ri))]}" -nKeys "$k" -threads "$j" -srvAddress "${indigos[$((ri))]}" -table "$TABLE" "$MODE" -results_dir "$OUTPUT_DIR" -initValue "$INIT_VAL" -shepard "$SHEPARD_URL
					ri=`expr $ri + 1`
					echo "Run client "$h" CMD "$cmd
					ssh $USERNAME@$h "nohup "$cmd" > client_console.log" &
				done

				sleep 120
				kill_all "`echo ${SERVER_MACHINES[@]}`"
				kill_all "`echo ${CLIENT_MACHINES[@]}`"

				
				#Generate results
				ri=0;
				RUN_STATS="java $CLASSPATH evaluation.StatisticsUtils"
				CDF="-cdf 0 1000 10"
				for h in ${client_machines[@]}; do
					
					cdf_dir=$OUTPUT_DIR"CDF/"
					makeDir="mkdir -p "$cdf_dir
					output_cdf=$cdf_dir"remote_indigo_results_"${REGION_NAME[$((ri))]}".dat"
					
					awk="awk -F '\t'  '{print \$4}' "$OUTPUT_DIR"remote_indigo_results_"${REGION_NAME[$((ri))]}".log"
					cmd="$awk | $RUN_STATS $CDF"
					ri=`expr $ri + 1`
					echo "Generate results "$h" CMD "$cmd" to "$output_cdf
					ssh $USERNAME@$h "$makeDir ; $cmd > $output_cdf"
				done

			done
		done
	done
done

echo "Finish"