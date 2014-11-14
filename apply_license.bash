files=( $(find $1 -name "*java"))
for f in ${files[@]}; do 
	cat .license > newfile
	cat $f >> newfile
	cp newfile $f
done
