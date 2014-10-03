#set encoding utf8;
#set label 1 "XXX - scout at DC" font "Helvetica,16" at 1,950;
set terminal postscript size 10.0, 7.0 enhanced monochrome dashed font "Helvetica,24" linewidth 1;
set xlabel "Time [seconds]";
set ylabel "Latency [ms]";
set y2label "Site resources [units]";

set mxtics;
set mytics;
set xr [*:*];
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
stats iuse using 1:2
start_e=STATS_min_x
stats iusw using 1:2
start_w=STATS_min_x
stats ieur using 1:2
start_eur=STATS_min_x

set y2tics;
set y2range [1:1500];


plot iuse using ($1-start_e)/1000000000:2 title "US-EAST" lt 1 axes x1y1,\
iusw using ($1-start_w)/1000000000:2 title "US-WEST" ps 1 pt 8 axes x1y1,\
ieur using ($1-start_eur)/1000000000:2 title "EUROPE" ps 1 pt 10 axes x1y1,\
ruse using ($1-start_e)/1000000000:2 title "" w l lt 1 lc rgb "red" axes x1y2,\
rusw using ($1-start_w)/1000000000:2 title "" w l lt 1 lc rgb "green" axes x1y2,\
reur using ($1-start_eur)/1000000000:2 title "" w l lt 1 lc rgb "blue" axes x1y2;



