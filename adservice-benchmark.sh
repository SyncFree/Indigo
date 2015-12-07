#!/bin/bash
source util.bash

#Process options
while getopts "abc:d:kl:n:p:r:t:v" optname
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
		
		"d")
			DEPLOYMENT=$OPTARG
			source $DEPLOYMENT
		;;
		"k")
			kill_all "`echo ${SERVER_MACHINES[@]}`"
			kill_all "`echo ${CLIENT_MACHINES[@]}`"
			kill_all "`echo ${SEQUENCER_MACHINES[@]}`"
			exit
		;;
		"l")
			case $OPTARG in
				'debug')
					LOG_LEVEL="debug-log.properties"
				;;
				'normal') 
					LOG_LEVEL="default-log.properties"
				;;
			esac
		;;
		"p")
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
	
	
if [ -z ${DEPLOYMENT} ]
 then 
	echo "Deployment file not set. Please use -d DEPLOYMENT_FILENAME"
	set -e
	exit
else
	echo "Loaded config file "$DEPLOYMENT
fi

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
				mode=$m
				m="-"$m
				echo "THREADS "$j
				echo "REGIONS "$i
				echo "CONFIG "$k
				echo "MODE "$m
				OUTPUT_DIR=$RESULTS_ROOT""$EXPERIMENT_NAME_PREFIX"-c-"$k"-r"$i"-t"$j"/"
				makeDir="mkdir -p $OUTPUT_DIR"

				sequencer_machines=(${SEQUENCER_MACHINES[@]:0:$i})
				sequencers=${SEQUENCERS[@]:0:$i}
				servers=(${SERVERS[@]:0:$i})
				sequencer_ports=(${SEQUENCER_PORTS[@]:0:$i})
				
				ri=0;
				for h in ${sequencer_machines[@]}; do
					CLASSPATH="-classpath "${INDIGO_ROOT[$((ri))]}"swiftcloud.jar"
					LOG="-Djava.util.logging.config.file="${INDIGO_ROOT[$((ri))]}"configs/"$LOG_LEVEL
					CMD_SRV="java "$CLASSPATH" "$LOG" "$SERVER_RUNTIME
					cmd=$CMD_SRV" -startSequencer -siteId "${REGION_NAME[$((ri))]}" -master "${REGION_NAME[0]}" -sequencers "$sequencers" -server "${servers[$((ri))]}" "$m" -seqPort "${SEQUENCER_PORTS[$((ri))]}
					echo "Start Sequencer "$h "CMD" $cmd
					ssh $USERNAME@$h "nohup "$cmd " 2>&1 | tee dc_sequencer_console.log" &
					ri=`expr $ri + 1`
				done
				
				sleep 5

				server_machines=(${SERVER_MACHINES[@]:0:$i})
				ri=0;
				for h in ${server_machines[@]}; do
					CLASSPATH="-classpath "${INDIGO_ROOT[$((ri))]}"swiftcloud.jar"
					LOG="-Djava.util.logging.config.file="${INDIGO_ROOT[$((ri))]}"configs/"$LOG_LEVEL
					CMD_SRV="java "$CLASSPATH" "$LOG" "$SERVER_RUNTIME
					cmd=$CMD_SRV" -startServer -siteId "${REGION_NAME[$((ri))]}" -master "${REGION_NAME[0]}" -sequencerUrl "${SEQUENCERS[$((ri))]}" -servers "${servers[@]}" "$m" -srvPortForSeq "${SER_SEQ_PORTS[$((ri))]}" -dhtPort "${DHT_PORT[$((ri))]}" -pubSubPort "${PUB_SUB_PORT[$((ri))]}" -indigoPort "${INDIGO_PORTS[$((ri))]}" -srvPort "${SER_PORTS[$((ri))]}
					
					echo "Start Server "$h "CMD" $cmd
					ssh $USERNAME@$h "nohup "$cmd " 2>&1 | tee dc_server_console.log" &
					ri=`expr $ri + 1`
				done
				
				sleep 10

				master=${SERVER_MACHINES[0]}
				CLASSPATH="-classpath "${INDIGO_ROOT[$((0))]}"swiftcloud.jar"
				LOG="-Djava.util.logging.config.file="${INDIGO_ROOT[$((0))]}"configs/"$LOG_LEVEL
				CMD_CLT="java "$CLASSPATH" "$LOG" "$APP
				config_dir=${INDIGO_ROOT[$((0))]}"/configs/"$k
				cmd=$makeDir" & "$makeDir"init & "$CMD_CLT" -init -siteId "${REGION_NAME[0]}" -master "${REGION_NAME[0]}" -nKeys "$k" -table "$TABLE" -initValue "$INIT_VAL" -results_dir "$OUTPUT_DIR"init "$m
				SHEPARD="java "$CLASSPATH" "$LOG" "$SHEPARD
				ssh $USERNAME@${SERVER_MACHINES[0]} "nohup "$SHEPARD" -url "$SHEPARD_URL" -count "$i &
				echo "Init data "$master" CMD "$cmd
				ssh $USERNAME@$master "nohup "$cmd
				
				sleep 10

				indigos=(${INDIGOS[@]:0:$i})
				client_machines=(${CLIENT_MACHINES[@]:0:$i})
				ri=0;
				for h in ${client_machines[@]}; do
					site=`expr $ri + 1`
					CLASSPATH="-classpath "${INDIGO_ROOT[$((ri))]}"swiftcloud.jar"
					LOG="-Djava.util.logging.config.file="${INDIGO_ROOT[$((ri))]}"configs/"$LOG_LEVEL
					CMD_CLT="java "$CLASSPATH" "$LOG" "$APP
					config_dir=${INDIGO_ROOT[$((ri))]}"/configs/"$k
					cmd=$makeDir" ; "$CMD_CLT" -run -siteId "${REGION_NAME[$((ri))]}" -site "$site" -master "${REGION_NAME[0]}" -nKeys "$k" -threads "$j" -initValue "$INIT_VAL" -table "$TABLE" -srvAddress "${indigos[$((ri))]}" -results_dir "$OUTPUT_DIR" "$m" -shepard "$SHEPARD_URL
					ri=`expr $ri + 1`
					echo "Run client "$h" CMD "$cmd
					ssh $USERNAME@$h "nohup "$cmd" 2>&1 | tee client_console.log" &
				done

				sleep $DURATION

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

echo "####################################################"
echo "####################################################"
echo "####################################################"