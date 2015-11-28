#!/usr/bin/env python3

import sys, getopt, os
import subprocess
import psycopg2
import time

from urllib.request import urlopen

def main():
    
    pgdbname =  'wfs'
    pghost=     'localhost'
    pgport=     5432
    pguser=     'postgres'
    pgpassword= 'abc'

    host=    'geodata.nationaalgeoregister.nl'
    wfs=     'bag'
    feature= 'verblijfsobject'
    table=   'woning'

    srs=     'EPSG:28992'

    xmin= 135000
    xmax= 136000
    ymin= 455000
    ymax= 456000
    step= 500

    try:
        opts, args = getopt.getopt(sys.argv[1:],"w:f:t:x:X:y:Y:s:d:h:P:u:p:S:")
    except getopt.GetoptError:
        print('WalkWFS.py unknown argument')
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-w"):
            wfs = arg
        elif opt in ("-f"):
            feature = arg
        elif opt in ("-t"):
            table = arg
        elif opt in ("-x"):
            xmin = int(arg)
        elif opt in ("-X"):
            xmax = int(arg)
        elif opt in ("-y"):
            ymin = int(arg)
        elif opt in ("-Y"):
            ymax = int(arg)
        elif opt in ("-s"):
            step = int(arg)
        elif opt in ("-S"):
            srs = arg
        elif opt in ("-d"):
            pgdbname = arg
        elif opt in ("-h"):
            pghost = arg
        elif opt in ("-P"):
            pgport = int(arg)
        elif opt in ("-u"):
            pguser = arg
        elif opt in ("-p"):
            pgpassword = arg
        else:
            assert False, "unknown option"

    print(wfs, feature, table, xmin, xmax, ymin, ymax, step)
   
    #DB connection properties
    conn = psycopg2.connect(dbname = pgdbname, host= pghost, port= pgport, user = pguser, password= pgpassword)
    cur = conn.cursor()

    tf = 'temp-' + table + '.gml'

    for x in range(xmin, xmax, step):
        for y in range(ymin, ymax, step):
            
            q= 'http://' + host + '/' + wfs + '/wfs?&REQUEST=GetFeature&SERVICE=WFS&VERSION=1.1.0&TYPENAME=' + feature + '&BBOX=' \
                + str(x) + ',' + str(y) + ',' + str(x+step) + ',' + str(y+step) + '&SRSNAME=EPSG:28992&OUTPUTFORMAT=text%2Fxml%3B%20subtype%3Dgml%2F3.1.1'
            print(x, y, end=' ')
            for retry in range(1,5): # retry loop
                try:
                    with urlopen(q) as r:
                            d= r.read()
                    print(len(d), 'bytes')
                            
                    with open(tf, 'wb') as f:
                        f.write(d)
                        f.close()

                    subprocess.call(["ogr2ogr", "-append", "-skipfailures", "-progress", "-f", "PostgreSQL",
                        "PG:dbname='" + pgdbname + "' host='" + pghost + "' port='" + str(pgport) + "' user='" + pguser +"' password='" + pgpassword + "'",
                        "-t_srs", srs, tf, "-nln", table])
                    break
                except:
                    print("Unexpected error:", sys.exc_info()[0])
                    print("Retry: ", str(retry))
                    os.remove(tf)
                    time.sleep(3 * retry * retry)
                    
    # Remove duplicates
    cur.execute("DELETE FROM " + table + " USING " + table + " t2 " + \
                "WHERE " + table + ".gml_id = t2.gml_id AND " \
                + table + ".ogc_fid < t2.ogc_fid;")
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
