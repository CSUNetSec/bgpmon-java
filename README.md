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
    error handling - should execute() throw Exception?
    query csu_bgp_core.update_messages_by_time for additional information

write bgp
    parse advertised/withdrawn prefixes

don't let people see sessions that haven't created (username, password)
