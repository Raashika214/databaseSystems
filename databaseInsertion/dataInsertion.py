#!/usr/bin/python
# query for traffic data

import psycopg2
from config import config
import csv
from os import listdir
from os.path import isfile, join


def connect(inputfolder):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        inputFolderName = inputfolder

        # reading all files in the input folder
        files = [f for f in listdir(inputFolderName) if isfile(join(inputFolderName, f))]
        for file in files:
            file = inputFolderName + "/" + file
            csv_file = open(file, encoding="cp932", errors="",
                            newline="")
            f = csv.reader(csv_file, delimiter=",", doublequote=True, lineterminator="\r\n", quotechar='"',
                           skipinitialspace=True)
            header = next(f)
            for row in f:
                for i in range(len(row)):

                    # filling missing values
                    if row[i] == '':
                        row[i] = '9999'

                # writing query
                query = 'insert into  traffic_data values(' + row[0] + ',\'' + row[1] + ' ' + row[
                    2] + ':00:00\'' + ',' + \
                        row[3] + ',' + row[4] + ',' + row[5] + ',' \
                        + row[6] + ',' + row[7] + ',' + row[8] + ',' + row[9] + ',' + row[10] + ',' + row[11] + ',' + \
                        row[12] + ',' + row[13] + ',' + row[14] + ',-1' + ',' + row[16] + ',' + row[17] + ',' + row[
                            18] + ")"

                # executing the query
                cur.execute(query)
                
        conn.commit()

        # close the communication with the PostgreSQL
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == '__main__':
    connect("01")
