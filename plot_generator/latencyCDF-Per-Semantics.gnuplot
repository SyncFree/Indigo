#set encoding utf8;
#set label 1 "XXX - scout at DC" font "Helvetica,16" at 1,950;
set terminal postscript size 10.0, 7.0 enhanced monochrome dashed font "Helvetica,24" linewidth 1;
set xlabel "Latency [ ms ]";
set ylabel "Cumulative Ocurrences [ % ]";
set mxtics;
set mytics;
set xr [0:100];
set yr [0:100];

set key right bottom;
#set grid xtics ytics lt 30 lt 30;
set label;
set clip points;
#set lmargin at screen 0.11;
#set rmargin at screen 0.99;
#set bmargin at screen 0.05;
#set tmargin at screen 0.9999;

set pointinterval 0.1;
set decimalsign locale;

plot  indigo using 1:($2*100) title "Indigo" with linespoints pointinterval 0.1 lw 4 ps 2 pt 7, \
	  weak using 1:($2*100) title "Causal" with linespoints pointinterval 0.1 lw 4 ps 2 pt 3

