#set encoding utf8;
#set label 1 "XXX - scout at DC" font "Helvetica,16" at 1,950;

if (exists("xmax")) set xr [0:xmax];

set terminal postscript size 10.0, 7.0 enhanced monochrome dashed font "Helvetica,20" linewidth 1;
set xlabel "Latency [ ms ]";
set ylabel "Cumulative Ocurrences [ % ]";
set mxtics;
set mytics;
set yr [0:*];

set key right bottom;
#set grid xtics ytics lt 30 lt 30;
set label;
set clip points;
#set lmargin at screen 0.11;
#set rmargin at screen 0.99;
#set bmargin at screen 0.05;
#set tmargin at screen 0.9999;

set pointinterval 1;
set decimalsign locale;

plot    iuse using 1:($2*100) title "Indigo [US-E]" with lines lt 1 lc rgb '#0000FF',\
        iusw using 1:($2*100) title "Indigo [US-W]" with lines lt 1 lc rgb '#00FF00',\
        ieur using 1:($2*100) title "Indigo [EUR]"  with lines lt 1 lc rgb '#FF0000',\
        wuse using 1:($2*100) title "Causal [US-E]" with lines lt 2 lc rgb '#0000FF',\
        wusw using 1:($2*100) title "Causal [US-W]" with lines lt 2 lc rgb '#00FF00',\
        weur using 1:($2*100) title "Causal [EUR]"  with lines lt 2 lc rgb '#FF0000',;

#plot    iuse using 1:($2*100) title "Indigo [US-E]" with lines lt 1 lc rgb '#000000',\
#        iusw using 1:($2*100) title "Indigo [US-W]" with lines lt 1 lc rgb '#0F0F0F',\
#        ieur using 1:($2*100) title "Indigo [EUR]"  with lines lt 1 lc rgb '#444444',\
#        wuse using 1:($2*100) title "Causal [US-E]" with lines lt 2 lc rgb '#000000',\
#        wusw using 1:($2*100) title "Causal [US-W]" with lines lt 2 lc rgb '#0F0F0F',\
#        weur using 1:($2*100) title "Causal [EUR]"  with lines lt 2 lc rgb '#444444',;





