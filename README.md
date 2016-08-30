#bgpmon

##Overview

##Examples
To run from gradle use the 'execute' task passing arguments 'mainClass' and 'myArgs'

gradle -PmainClass='edu.colostate.netsec.BgpmonServer' execute
gradle -PmainClass='edu.colostate.netsec.BgpmonClient' -PmyArgs='open,cassandra,csuadmin,n3ts3c@dmin,129.82.138.74,129.82.138.75,--session_id=TEST' execute
gradle -PmainClass='edu.colostate.netsec.BgpmonClient' -PmyArgs='list,sessions' execute
gradle -PmainClass='edu.colostate.netsec.BgpmonClient' -PmyArgs='close,session' execute
gradle -PmainClass='edu.colostate.netsec.BgpmonClient' -PmyArgs='start,prefix-hijack,TEST,--file,/home/hamersaw/Downloads/prefix.txt' execute
gradle -PmainClass='edu.colostate.netsec.BgpmonClient' -PmyArgs='list,modules' execute
gradle -PmainClass='edu.colostate.netsec.BgpmonClient' -PmyArgs='write,mrt-file,filename' execute

##TODO
increase funcationality of prefix hijack module
fuzz username/passwords on cassandra session
