set term postscript noenhanced eps font "Helvetica,28"
set output "histoerrorbartest.eps"
set boxwidth 0.9
set style fill solid 1.00 border 0
set style histogram errorbars gap 2 lw 1
set style data histograms
set xtics rotate by -20
set bars 0.5
set yrange [*:*] 
#set ytics 0,300,2000;
set ylabel "Latency [ms]" font ",40"
set xtic font ",22"
set key left top

plot 'tournament-indigo.dat' using 2:4:5:xtic(1) every ::1 ti "Indigo" lc rgb "red",\
     'tournament-redblue.dat' using 2:4:5:xtic(1) every ::1 ti "Red-Blue" lc rgb "green",\
     'tournament-strong.dat' using 2:4:5:xtic(1) every ::1 ti "Strong" lc rgb "blue";




    
