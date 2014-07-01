#!/usr/bin/env groovy -classpath .:scripts/groovy:lib/core/ssj.jar
package swift.stats

import static swift.stats.GnuPlot.*
import umontreal.iro.lecuyer.stat.Tally

Stats<Double> stats = new Stats<Double>();

SURROGATES = 1
File baseDir = new File("/tmp/dat/use");

	baseDir.eachFile{ f->
		if( f.name.endsWith(".txt")) {

			println f.absolutePath
			
			String[] ftok = f.absolutePath.split('-');
			if( ftok.length < 11 )
			return
						
			int scouts = Double.valueOf( ftok[8] );
			int threads = Double.valueOf( ftok[10]);
			int surrogates = Double.valueOf( ftok[6]);
			
			if( surrogates != SURROGATES )
				return
				
			println scouts + "   " + threads + "  " + surrogates
			
			f.eachLine { l ->
				
				String[] tok = l.split('\t');
				
				if( tok.length != 6 )
					return
									
				double a = Double.valueOf(tok[1]), 
						b = Double.valueOf(tok[2]), 
						c = Double.valueOf(tok[3]), 
						d = Double.valueOf(tok[4]),
						e = Double.valueOf(tok[5])
						
				
				if( l.charAt(0) == '+' ) {
					stats.series("#updates", "" + (scouts*threads)).add(a, e)
					return
				} 
				if( l.charAt(0) == '*' ) {
					println a + "   " + e			
					stats.series("staleness", "" + (scouts*threads)).add(a*0.001, e)
					return
				}
			}						
		}
	}


def maxX = -1;

plots = [:]
for( def Series i : stats.series("#updates") ) {
	println i.name
	
	data = []
	
	i.size().times() {
	
		double x = i.xValue(it), y = i.yValue(it)
		data << String.format("%.000001f\t%.000001f", x, y)
		
		maxX = Math.max(x, maxX)
		
	}
    plots[i.name +" Clients"] = data
}

println maxX
maxX = 10

def gnuplot = [
    '#set encoding utf8',
    '#set label 1 "SwiftSocial - scout at DC" font "Helvetica,16" at 1,950',
    'set terminal postscript size 10.0, 7.0 monochrome dashed font "Helvetica,28" linewidth 1',
    //                   'set terminal aqua dashed',
    'set xlabel "staleness [ seconds ]"',
    'set ylabel "Cumulative Ocurrences [ % ]"',
    'set mxtics',
    'set mytics',
    'set yr [95.0:100.0]',
    'set xr [0:'+ maxX +']',
    'set pointinterval 20',
    'set key right bottom',
    'set grid xtics ytics lt 30 lt 30',
    'set label',
    'set clip points',
    'set lmargin at screen 0.11',
    'set rmargin at screen 0.99',
    'set bmargin at screen 0.05',
    'set tmargin at screen 0.9999',
]

String outputFile = "/tmp/middleware/staleness_updatesCDF"

//Sort by key length, then alphabetically
def keySorter = { String a, b ->
    int l = Integer.signum( a.length() - b.length() )
    l == 0 ? a.compareTo(b) : l
}


GnuPlot.doGraph( outputFile, gnuplot, plots, { k, v ->
    String lw = k.toString().contains("---") ? 1: 3
    String.format('title "%s" with lines lw %s', k, lw)
    //    String.format('notitle  with lines lw %s', lw)
}, keySorter )

