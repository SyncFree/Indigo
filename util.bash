
#<Clients> #<Command>
ssh_command() {
	hosts=($1)
	for h in ${hosts[@]}; do
		OIFS=$IFS
		IFS=':'			
		tokens=($h)
		client=${tokens[0]}
		echo "ssh command to " $client" "$2
		ssh -t $USERNAME@$client $2
		IFS=$OIFS
	done
}

kill_all() {
	cmd="killall java"
	ssh_command "$1" "$cmd"
	echo "All clients have stopped"
}

rsync_source() {
	servers=("$@")
	ant -buildfile $SOURCE_ROOT/TrueIndigo/balegas-jar-build.xml 
	ri=0
	for h in ${servers[@]}; do
		cmd="prsync -r -H "$USERNAME"@"$h" "
		cmd1=$cmd" "$SOURCE_ROOT"TrueIndigo/swiftcloud.jar "${INDIGO_ROOT[$((ri))]}
		$cmd1
		cmd2=$cmd" "$SOURCE_ROOT"TrueIndigo/stuff "${INDIGO_ROOT[$((ri))]}
		$cmd2
		cmd3=$cmd" "$SOURCE_ROOT"configs/ "${INDIGO_ROOT[$((ri))]}"configs/"
		$cmd3
		ri=`expr $ri + 1`
	done
}

get_results() {
	servers=("$@")
	CMD="rsync -r "		
	for h in ${servers[@]}; do
		cmd=$CMD" "$USERNAME"@"$h":RESULTS* "$SOURCE_ROOT
		$cmd
	done
}
