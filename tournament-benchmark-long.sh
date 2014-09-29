#!/bin/bash
USERNAME="ec2-user"
INDIGO_ROOT="/home/$USERNAME/"
SOURCE_ROOT="/Users/balegas/workspace/java/swiftcloud-indigo/"

REGION_NAME=(
	"US-EAST"
	"US-WEST"
	"EUROPE"
	)

INDIGOS=(
	"tcp://ec2-54-209-207-71.compute-1.amazonaws.com:36001/US-EAST"
	"tcp://ec2-54-193-102-146.us-west-1.compute.amazonaws.com:36001/US-WEST"
	"tcp://ec2-54-72-74-191.eu-west-1.compute.amazonaws.com:36001/EUROPE"
	)

#Pass all of these
SEQUENCERS=(
	"tcp://ec2-54-210-11-138.compute-1.amazonaws.com:31001/US-EAST"
	"tcp://ec2-54-193-102-149.us-west-1.compute.amazonaws.com:31001/US-WEST"
	"tcp://ec2-54-77-144-231.eu-west-1.compute.amazonaws.com:31001/EUROPE"
	)
					
#Pass all of these? or just the others?
SERVERS=(
	"tcp://ec2-54-209-207-71.compute-1.amazonaws.com:32001/US-EAST"
	"tcp://ec2-54-193-102-146.us-west-1.compute.amazonaws.com:32001/US-WEST"
	"tcp://ec2-54-72-74-191.eu-west-1.compute.amazonaws.com:32001/EUROPE"
	)

SEQUENCER_MACHINES=(
	"ec2-54-210-11-138.compute-1.amazonaws.com"
	"ec2-54-193-102-149.us-west-1.compute.amazonaws.com"
	"ec2-54-77-144-231.eu-west-1.compute.amazonaws.com"
	)

SERVER_MACHINES=(
	"ec2-54-209-207-71.compute-1.amazonaws.com"
	"ec2-54-193-102-146.us-west-1.compute.amazonaws.com"
	"ec2-54-72-74-191.eu-west-1.compute.amazonaws.com"
	)

CLIENT_MACHINES=(
	"ec2-54-209-166-112.compute-1.amazonaws.com"
	"ec2-54-193-69-65.us-west-1.compute.amazonaws.com"
	"ec2-54-77-172-0.eu-west-1.compute.amazonaws.com"
	)

SHEPARD_URL="tcp://ec2-54-209-207-71.compute-1.amazonaws.com:29876/"

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


CONFIG=("indigo-tournament-l90.props")
N_REGIONS=(3)
N_THREADS=(1)
MODE=("-indigo")

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
	cmd3=$cmd" "$SOURCE_ROOT"configs/ "$INDIGO_ROOT
	$cmd3
}

get_results() {
	servers=("$@")
	CMD="rsync -r "		
	for h in ${servers[@]}; do
		cmd=$CMD" "$USERNAME"@"$h":long_results_tournament* "$SOURCE_ROOT"../indigo_results/"
		$cmd
	done
}

#function join { local IFS="$1"; shift; echo "$*"; }

#Process options
while getopts "abc:d:n:r:t:v:k" optname
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
			CONFIG=($OPTARG)
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
			kill_all "`echo ${SEQUENCER_MACHINES[@]}`"
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

CLASSPATH="-classpath "$INDIGO_ROOT"swiftcloud.jar -Xms2G -Xmx4G"
LOG="-Djava.util.logging.config.file="$INDIGO_ROOT"stuff/benchmarks.properties"
CMD_SRV="java "$CLASSPATH" "$LOG" indigo.application.benchmark.MicroBenchmark"
CMD_CLT="java "$CLASSPATH" "$LOG" indigo.application.tournament.TournamentServiceBenchmark"
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
			for k in "${CONFIG[@]}"
			do
				:
				echo $j" THREADS"
				echo $i" REGIONS"
				echo $k" CONFIG"
				echo $m" MODE"
				echo $INIT_VAL" INIT VALUE"
				OUTPUT_DIR=$INDIGO_ROOT"long_results_tournament"$m"-c-"$k"-r"$i"-t"$j"/"
				makeDir="mkdir -p $OUTPUT_DIR"

				sequencer_machines=(${SEQUENCER_MACHINES[@]:0:$i})
				sequencers=${SEQUENCERS[@]:0:$i}
				servers=(${SERVERS[@]:0:$i})
				
				ri=0;
				for h in ${sequencer_machines[@]}; do
					cmd=$CMD_SRV" -startSequencer -siteId "${REGION_NAME[$((ri))]}" -master "${REGION_NAME[0]}" -sequencers "$sequencers" -server "${servers[$((ri))]}" "$m
					echo "Start Sequencer "$h "CMD" $cmd
					ssh $USERNAME@$h "nohup "$cmd " 2>&1 | tee dc_sequencer_console.log" &
					ri=`expr $ri + 1`
				done
				
				sleep 5

				server_machines=(${SERVER_MACHINES[@]:0:$i})
				ri=0;
				for h in ${server_machines[@]}; do
					cmd=$CMD_SRV" -startServer -siteId "${REGION_NAME[$((ri))]}" -master "${REGION_NAME[0]}" -sequencerUrl "${SEQUENCERS[$((ri))]}" -servers "${servers[@]}" "$m
					echo "Start Server "$h "CMD" $cmd
					ssh $USERNAME@$h "nohup "$cmd " 2>&1 | tee dc_server_console.log" &
					ri=`expr $ri + 1`
				done
				
				sleep 10

				master=${SERVER_MACHINES[0]}
				cmd=$makeDir" & "$makeDir"init & "$CMD_CLT" -init -siteId "${REGION_NAME[0]}" -master "${REGION_NAME[0]}" -config "$k" -results_dir "$OUTPUT_DIR"init "$m
				echo "Init data "$master" CMD "$cmd
				ssh $USERNAME@$master "nohup "$cmd
				echo "Start shepard "$SHEPARD" -url "$SHEPARD_URL" -count "$i
				ssh $USERNAME@$master "nohup "$SHEPARD" -url "$SHEPARD_URL" -count "$i &
				
				sleep 30

				indigos=(${INDIGOS[@]:0:$i})
				client_machines=(${CLIENT_MACHINES[@]:0:$i})
				ri=0;
				for h in ${client_machines[@]}; do
					site=`expr $ri + 1`
					cmd=$makeDir" ; "$CMD_CLT" -run -siteId "${REGION_NAME[$((ri))]}" -site "$site" -master "${REGION_NAME[0]}" -config "$k" -threads "$j" -srvAddress "${indigos[$((ri))]}" -results_dir "$OUTPUT_DIR" -shepard "$SHEPARD_URL" "$m
					ri=`expr $ri + 1`
					echo "Run client "$h" CMD "$cmd
					ssh $USERNAME@$h "nohup "$cmd" 2>&1 | tee client_console.log" &
				done

				sleep 420

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
					output_cdf=$cdf_dir"tournament_results_"${REGION_NAME[$((ri))]}".dat"
					output_tpsl=$tpsl_dir"tournament_results_"${REGION_NAME[$((ri))]}".dat"

					awk="awk -F '\t'  '{print \$2\" \"\$4}' "$OUTPUT_DIR"tournament_results_"${REGION_NAME[$((ri))]}".log"
					cmd="$awk | $RUN_STATS $CDF"
					echo "Generate RemoteIndigo CDF "$h" CMD "$cmd" to "$output_cdf
					ssh $USERNAME@$h "$makeDir ; $cmd > $output_cdf"

					awk="awk -F '\t'  '{print \$2\" \"\$4}' "$OUTPUT_DIR"tournament_results_"${REGION_NAME[$((ri))]}".log"
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