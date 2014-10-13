#!/bin/bash

USERNAME="ec2-user"
INDIGO_ROOT="/home/$USERNAME/"
SOURCE_ROOT="/Users/balegas/workspace/java/swiftcloud-indigo/"


REGION_NAME=(
	"STRONG"
	"US-WEST"
	"EUROPE"
	)

INDIGOS=(
	"tcp://ec2-54-172-124-85.compute-1.amazonaws.com:36001/STRONG"
	"tcp://ec2-54-172-124-85.compute-1.amazonaws.com:36001/STRONG"
	"tcp://ec2-54-172-124-85.compute-1.amazonaws.com:36001/STRONG"
	)

#Pass all of these
SEQUENCERS=(
	"tcp://ec2-54-172-124-69.compute-1.amazonaws.com:31001/STRONG"
	)
					
#Pass all of these? or just the others?
SERVERS=(
	"tcp://ec2-54-172-124-85.compute-1.amazonaws.com:32001/US-EAST"
	"tcp://ec2-54-183-210-161.us-west-1.compute.amazonaws.com:32001/US-WEST"
	"tcp://ec2-54-171-60-129.eu-west-1.compute.amazonaws.com:32001/EUROPE"
	)

SEQUENCER_MACHINES=(
	"ec2-54-172-124-69.compute-1.amazonaws.com"
	)

SERVER_MACHINES=(
	"ec2-54-172-124-85.compute-1.amazonaws.com"
	"ec2-54-183-210-161.us-west-1.compute.amazonaws.com"
	"ec2-54-171-60-129.eu-west-1.compute.amazonaws.com"
	)

CLIENT_MACHINES=(
	"ec2-54-172-124-74.compute-1.amazonaws.com"
	"ec2-54-165-186-208.compute-1.amazonaws.com"
	"ec2-54-86-68-148.compute-1.amazonaws.com"
	)

SHEPARD_URL="tcp://ec2-54-172-124-85.compute-1.amazonaws.com:29876/"

	
#LOCAL OVERRIDE
#USERNAME="balegas"
#INDIGO_ROOT="/Users/$USERNAME/swiftcloud_deployment/"
#SOURCE_ROOT="/Users/$USERNAME/workspace/java/swiftcloud-indigo/"

#REGION_NAME=( "LOCAL" )
#INDIGOS=( "tcp://*:36001/LOCAL" )
#SEQUENCERS=( "tcp://*:31001/LOCAL" )
#SERVERS=( "tcp://*:32001/LOCAL" )
#SERVER_MACHINES=("localhost")
#CLIENT_MACHINES=("localhost")
#SHEPARD_URL="tcp://*:29876/"


TABLE="table"
#N_KEYS=(1 10 100 1000 10000)
N_KEYS=(1000)
#N_REGIONS=(1)
N_REGIONS=(3)
#N_THREADS=(60)
N_THREADS=(1 5 10 15 20 25 30 40 50 60 70 80)
MODE=("-indigo")
DISTRIBUTION="uniform"
INIT_VAL=2999999
N_READS=0
N_WRITES=1

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
    ant -buildfile $SOURCE_ROOT/TrueIndigo/balegas-jar-build.xml 
	cmd1=$cmd" "$SOURCE_ROOT"TrueIndigo/swiftcloud.jar "$INDIGO_ROOT
	$cmd1
	cmd2=$cmd" "$SOURCE_ROOT"TrueIndigo/stuff "$INDIGO_ROOT
	$cmd2
}

get_results() {
	servers=("$@")
	CMD="rsync -r "		
	for h in ${servers[@]}; do
		cmd=$CMD" "$USERNAME"@"$h":results* "$SOURCE_ROOT"../indigo_results/"
		$cmd
	done
}

#function join { local IFS="$1"; shift; echo "$*"; }

#Process options
while getopts "abc:d:n:r:t:v:kR:W:" optname
  do
    case "$optname" in
		"a")
			rsync_source "${SERVER_MACHINES[@]}"
			rsync_source "${CLIENT_MACHINES[@]}"
			rsync_source "${SEQUENCER_MACHINES[@]}"
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
		"R")
			N_READS=($OPTARG)
		;;
		"W")
			N_WRITES=($OPTARG)
		;;
		"k")
			kill_all "`echo ${SEQUENCER_MACHINES[@]}`"
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
				OUTPUT_DIR=$INDIGO_ROOT"new-results-strong-R"$N_READS"-W"$N_WRITES"-k"$k"-r"$i"-t"$j"-v"$INIT_VAL"-"$DISTRIBUTION"/"
				makeDir="mkdir -p $OUTPUT_DIR"

				sequencer_machines=(${SEQUENCER_MACHINES[@]:0:$i})
				sequencers=${SEQUENCERS[@]:0:$i}
				servers=(${SERVERS[@]:0:$i})
				
				ri=0;
				for h in ${sequencer_machines[@]}; do
					cmd=$CMD" -startSequencer -siteId ${REGION_NAME[0]} -master STRONG -sequencers "${SEQUENCERS[0]}" -server "${servers[$((ri))]}" "$m
					echo "Start Sequencer "$h "CMD" $cmd
					ssh $USERNAME@$h "nohup "$cmd " 2>&1 | tee dc_sequencer_console.log" &
					ri=`expr $ri + 1`
				done
				
				sleep 5

				server_machines=(${SERVER_MACHINES[@]:0:$i})
				ri=0;
				for h in ${server_machines[@]}; do
					allservers=${servers[@]}
					delete=(${servers[$((ri))]})
					s=( "${allservers[@]/$delete}" )
					echo "Servers to connect "${s[@]}
					cmd=$CMD" -startServer -siteId STRONG -master STRONG -sequencerUrl "${SEQUENCERS[0]}" -servers "${s[@]}" "$m" -strong"
					echo "Start Server "$h "CMD" $cmd
					ssh $USERNAME@$h "nohup "$cmd " 2>&1 | tee dc_server_console.log" &
					ri=`expr $ri + 1`
				done
				
				sleep 10

				master=${SERVER_MACHINES[0]}
				cmd=$makeDir" & "$makeDir"init & "$CMD" -init -siteId STRONG -master STRONG -nKeys "$k" -table "$TABLE" "$m" -initValue "$INIT_VAL" -results_dir "$OUTPUT_DIR"init"
				echo "Init data "$master" CMD "$cmd
				ssh $USERNAME@$master "nohup "$cmd
				echo "Start shepard "$SHEPARD" -url "$SHEPARD_URL" -count "$i
				ssh $USERNAME@$master "nohup "$SHEPARD" -url "$SHEPARD_URL" -count "$i &
				
				sleep 10

				indigos=(${INDIGOS[@]:0:$i})
				client_machines=(${CLIENT_MACHINES[@]:0:$i})
				ri=0;
				for h in ${client_machines[@]}; do
					cmd=$makeDir" ; "$CMD" -run -siteId STRONG -master STRONG -nKeys "$k" -threads "$j" -srvAddress "${indigos[0]}" -table "$TABLE" "$m" -results_dir "$OUTPUT_DIR" -initValue "$INIT_VAL" -fileNameSuffix _"${REGION_NAME[$((ri))]}" -fakeCS "${servers[$((ri))]}" -nReads "$N_READS" -nWrites "$N_WRITES
					ri=`expr $ri + 1`
					echo "Run client "$h" CMD "$cmd
					ssh $USERNAME@$h "nohup "$cmd" 2>&1 | tee client_console.log" &
				done

				sleep 120

				kill_all "`echo ${CLIENT_MACHINES[@]}`"
				kill_all "`echo ${SERVER_MACHINES[@]}`"
				kill_all "`echo ${SEQUENCER_MACHINES[@]}`"

				
				#Generate results
				ri=0;
				RUN_STATS="java $CLASSPATH evaluation.StatisticsUtils"
				CDF="-cdf 0 1000 5"
				TPSL="-tpsl"
				cdf_dir=$OUTPUT_DIR"CDF/"
				tpsl_dir=$OUTPUT_DIR"TPSL/"
				makeDir="mkdir -p "$cdf_dir" ; mkdir -p "$tpsl_dir
				
				for h in ${client_machines[@]}; do
					output_cdf=$cdf_dir"micro_benchmark_results_"${REGION_NAME[$((ri))]}".dat"
					output_tpsl=$tpsl_dir"micro_benchmark_results_"${REGION_NAME[$((ri))]}".dat"

					awk="awk -F '\t'  '{print \$2\" \"\$4}' "$OUTPUT_DIR"micro_benchmark_results_STRONG_"${REGION_NAME[$((ri))]}".log"
					cmd="$awk | $RUN_STATS $CDF"
					echo "Generate RemoteIndigo CDF "$h" CMD "$cmd" to "$output_cdf
					ssh $USERNAME@$h "$makeDir ; $cmd > $output_cdf"

					awk="awk -F '\t'  '{print \$2\" \"\$4}' "$OUTPUT_DIR"micro_benchmark_results_STRONG_"${REGION_NAME[$((ri))]}".log"
					cmd="$awk | $RUN_STATS $TPSL"
					echo "Generate results "$h" CMD "$cmd" to "$output_tpsl
					ssh $USERNAME@$h "$cmd > $output_tpsl"

					ri=`expr $ri + 1`

				done

			done
		done
	done
done

echo "Finish"