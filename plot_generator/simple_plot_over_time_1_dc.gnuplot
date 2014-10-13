#set encoding utf8;
#set label 1 "XXX - scout at DC" font "Helvetica,16" at 1,950;
set terminal postscript size 10.0, 7.0 enhanced monochrome dashed font "Helvetica,28" linewidth 1;
set xlabel "Time [seconds]" font ",40";
set ylabel "Latency [ms]" font ",40";

set mxtics;
set mytics;
set yr [1:300];

set key left top;
#set grid xtics ytics lt 30 lt 30;
set label;
#set clip points;
#set lmargin at screen 0.11;
#set rmargin at screen 0.99;
#set bmargin at screen 0.05;
#set tmargin at screen 0.9999;

set pointinterval 100;
stats iusw using 1:2
start_e=STATS_min_x

set xr [*:(STATS_max_x-STATS_min_x)/1000000000];



plot iusw every ::90 using ($1-start_e)/1000000000:2 title "US-WEST" lt 1;



