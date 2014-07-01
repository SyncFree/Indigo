#!/usr/bin/env groovy -classpath .:scripts/groovy:scripts/groovy/lib
package swift.deployment

import static swift.deployment.PlanetLab_3X.*
import static swift.deployment.Tools.*

def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(0);
})


Surrogates = [
    'ec2-54-228-122-250.eu-west-1.compute.amazonaws.com',
    'ec2-54-244-165-15.us-west-2.compute.amazonaws.com',
    'ec2-54-234-203-38.compute-1.amazonaws.com'
]

EndClients = (PlanetLab_NC + PlanetLab_NV + PlanetLab_EU).unique()

ShepardAddr = Surrogates.get(0);

def Threads = 1
def Duration = 900
def SwiftSocial_Props = "swiftsocial-test.props"

def Scouts = Surrogates

AllMachines = (Surrogates + Scouts + EndClients + ShepardAddr).unique()

dumpTo(AllMachines, "/tmp/nodes.txt")

pnuke(AllMachines, "java", 60)

boolean deployJar = true, deploy_logging = false, deploy_props = true

if( deployJar ) {
    println "==== BUILDING JAR..."
    sh("ant -buildfile smd-jar-build.xml").waitFor()
    deployTo(AllMachines, "swiftcloud.jar")
}

if( deploy_logging)
    deployTo(AllMachines, "stuff/all_logging.properties", "all_logging.properties")

if( deploy_props)
    deployTo(AllMachines, SwiftSocial_Props)


def shep = SwiftSocial.runShepard( ShepardAddr, Duration, "Released" )

SwiftSocial.runEachAsDatacentre(Surrogates, "256m", "1024m")

//println "==== WAITING A BIT BEFORE INITIALIZING DB ===="
//Sleep(10)
//
//println "==== INITIALIZING DATABASE ===="
//def INIT_DB_DC = Surrogates.get(0)
//def INIT_DB_CLIENT = Surrogates.get(0)
//
//SwiftSocial.initDB( INIT_DB_CLIENT, INIT_DB_DC, SwiftSocial_Props)


println "==== WAITING A BIT BEFORE STARTING SERVER SCOUTS ===="
Sleep(10)

int instances = Math.max(1, (int)EndClients.size() / 16)
SwiftSocial.runCS_ServerScouts(instances, Scouts, ["localhost"], SwiftSocial_Props, "1024m")

println "==== WAITING A BIT BEFORE STARTING ENDCLIENTS ===="
Sleep(10)

SwiftSocial.runCS_EndClients( instances, EndClients, Scouts, SwiftSocial_Props, ShepardAddr, Threads )


println "==== WAITING FOR SHEPARD SIGNAL PRIOR TO COUNTDOWN ===="
shep.take()

Countdown( "Remaining: ", Duration + 30)

pnuke(AllMachines, "java", 60)

def dstDir="results/swiftsocial/multi_cdf/dc-" + new Date().format('MMMdd-') + System.currentTimeMillis()
def dstFile = String.format("1pc-results-swiftsocial-DC-%s-SC-%s-TH-%s.log", Surrogates.size(), EndClients.size(), Threads)

pslurp( EndClients, "client-stdout.txt", dstDir, dstFile, 300)

exec([
    "/bin/bash",
    "-c",
    "wc " + dstDir + "/*/*"
]).waitFor()

System.exit(0)

