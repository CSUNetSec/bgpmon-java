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
prefix hijack module
    run on today and yesterday (data isn't being inserted quckly enough)
    log failed queries (cassandra gets overloaded)
    error handling - should execute() throw Exception?

write bgp
    write collector mac

don't let people see sessions that haven't created (username, password)

don't let a session close if something else is using it
