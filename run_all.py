# comprehensive script to import data, parse html, and match nyt articles to movies
# sydney ryan 01/29/16

import MySQLdb as DB
# import my scripts
import import_tables
import parse_html
import match_movies
import sys

# these are file locations for files containing actor data, director data, name_aka data,
# titles_aka data, titles data.
act_loc = sys.argv[1]
dir_loc = sys.argv[2]
nam_aka_loc = sys.argv[3]
tit_aka_loc = sys.argv[4]
tit_loc = sys.argv[5]
# final_output is the file you want the final results to be put in
final_output = sys.argv[6]

# connect to database (change details to connect to different db)
db = DB.connect(host='',
                user='',
                passwd='',
                db='')
cursor = db.cursor()

import_tables.create_tables(db, cursor)
import_tables.insert_data(db, cursor, act_loc, "actors")
import_tables.insert_data(db, cursor, dir_loc, "directors")
import_tables.insert_data(db, cursor, nam_aka_loc, "names_aka")
import_tables.insert_data(db, cursor, tit_aka_loc, "titles_aka")
import_tables.insert_data(db, cursor, tit_loc, "titles")

parse_html.parse_html(db, cursor)

match_movies.match_movies(db, cursor)
match_movies.create_result(db, cursor, final_output)

db.commit()
db.close()

print "script completed successfully :)"

