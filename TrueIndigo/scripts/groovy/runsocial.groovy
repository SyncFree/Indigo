#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

import static lib.Tools.*
import static lib.Topology.*

import lib.Topology
import lib.SwiftBase
import lib.SwiftSocial

def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(0);
})

ShepardAddr = 'peeramidion.irisa.fr'


PlanetLab = [
    'planetlab-3.imperial.ac.uk',
    'planetlab-4.imperial.ac.uk',
    'planetlab1.xeno.cl.cam.ac.uk',
    'planetlab2.xeno.cl.cam.ac.uk',
]

// Optional argument - limit of scouts number
if (args.length < 2) {
	err.println "missing args..."
    System.exit(1)
}


PerDCClientNodesLimit = Integer.valueOf(args[0])
Threads = Integer.valueOf(args[1])

DC_1 = DC([PlanetLab[0]], [PlanetLab[1]])
DC_2 = DC([PlanetLab[2]], [PlanetLab[3]])

Scouts1 = SGroup( PlanetLab[2..3], DC_1 )
Scouts2 = SGroup( PlanetLab[0..1], DC_2 )

Scouts = ( Topology.scouts() ).unique()

Duration = 180
InterCmdDelay = 25
SwiftSocial_Props = "swiftsocial-test.props"

//DbSize = 200*Scouts.size()*Threads
DbSize = 50000
props = SwiftBase.genPropsFile(['swiftsocial.numUsers':DbSize.toString()], SwiftSocial.DEFAULT_PROPS)

AllMachines = ( Topology.allMachines() + ShepardAddr).unique()

Version = getGitCommitId()

println getBinding().getVariables()

dumpTo(AllMachines, "/tmp/nodes.txt")

pnuke(AllMachines, "java", 60)

//System.exit(0)


println "==== BUILDING JAR for version " + Version + "..."
sh("ant -buildfile smd-jar-build.xml").waitFor()
deployTo(AllMachines, "swiftcloud.jar")
deployTo(AllMachines, "src-app/swift/application/social/" + SwiftSocial_Props, SwiftSocial_Props)
deployTo(AllMachines, "stuff/logging.properties", "logging.properties")

def shep = SwiftSocial.runShepard( ShepardAddr, Duration, "Released" )

println "==== LAUNCHING SEQUENCERS"
Topology.datacenters.each { datacenter ->
    datacenter.deploySequencers("256m" )
}

Sleep(10)
println "==== LAUNCHING SURROGATES"
Topology.datacenters.each { datacenter ->
    datacenter.deploySurrogates(ShepardAddr, "512m")
}

println "==== WAITING A BIT BEFORE INITIALIZING DB ===="
Sleep(10)

println "==== INITIALIZING DATABASE ===="
def INIT_DB_DC = Topology.datacenters[0].surrogates[0]
def INIT_DB_CLIENT = Topology.datacenters[0].sequencers[0]

SwiftSocial.initDB( INIT_DB_CLIENT, INIT_DB_DC, SwiftSocial_Props, "512m")

println "==== WAITING A BIT BEFORE STARTING SCOUTS ===="
Sleep(InterCmdDelay)

SwiftSocial.runScouts( Topology.scoutGroups, SwiftSocial_Props, ShepardAddr, Threads, "256m" )

println "==== WAITING FOR SHEPARD SIGNAL PRIOR TO COUNTDOWN ===="
shep.take()

Countdown( "Max. remaining time: ", Duration + InterCmdDelay)

pnuke(AllMachines, "java", 60)

def dstDir="results/swiftsocial/" + new Date().format('MMMdd-') +
        System.currentTimeMillis() + "-" + Version + "-" +
        String.format("DC-%s-SU-%s-SC-%s-TH-%s-USERS-%d", Topology.datacenters.size(), Topology.datacenters[0].surrogates.size(), Topology.totalScouts(), Threads, DbSize)

pslurp( Scouts, "scout-stdout.txt", dstDir, "scout-stdout.log", 300)
props.renameTo(new File(dstDir, SwiftSocial_Props))

exec([
    "/bin/bash",
    "-c",
    "wc " + dstDir + "/*/*"
]).waitFor()

System.exit(0)
