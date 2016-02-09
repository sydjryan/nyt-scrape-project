import MySQLdb as DB
import csv

def create_tables(db, cursor):
    print "dropping data tables if necessary"
    drop_tables = ''' drop table if exists actors;
                        drop table if exists directors;
                        drop table if exists names_aka;
                        drop table if exists titles_aka;
                        drop table if exists titles'''
    for sql in drop_tables.split(';'):
        cursor.execute(sql)
    db.commit()

    print "creating data tables"
    create_tables = '''
        create table `actors` (person_id int(11),
                            movie_id int(11),
                            name varchar(255),
                            gender varchar(255));
        create table `directors` like `actors`;
        create table `names_aka` (person_id int(11), name varchar(255));
        create table `titles_aka` (movie_id int(11), alternate_title varchar(255));
        create table `titles` (movie_id int(11), title varchar(255), production_year int(11))
    '''

    for sql in create_tables.split(';'):
        cursor.execute(sql)
    db.commit()


def insert_data(db, cursor, path_to_data, table_name):
    print "inserting data for table:", table_name
    cursor.execute('''LOAD DATA local INFILE '''+"\'"+path_to_data+"\'"+'''
        into table '''+table_name+'''
        FIELDS TERMINATED By '\t'
        OPTIONALLY ENCLOSED BY '\"'
        IGNORE 1 LINES ''')
    db.commit()


