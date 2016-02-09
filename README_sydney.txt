README:

required packages:
    pandas, BeautifulSoup, MySQLdb, csv, os, re, nltk, sys

INSTRUCTIONS TO RUN:
make sure run_all.py contains correct data about connecting to your mysql db.
then run "run_all.py" with the following arguments (in order):
    act_loc -- file location for actors.tsv
    dir_loc -- file location for directors.tsv
    nam_aka_loc -- file location for name_AKA.tsv
    tit_aka_loc -- file location for titles_AKA.tsv
    tit_loc -- file location for titles.tsv
    final_output -- file location for final csv
example:
    python run_all.py "./actors.tsv" "./directors.tsv" "./names_AKA.tsv" "./titles_AKA.tsv" "./titles.tsv" "outfile.csv"


### breakdown of subfiles

* import_tables.py:
    imports provided data (actors.tsv, directors.tsv, etc) into the db

* parse_html.py:
    contains functions to parse the nyt html and grab important info. creates a table "nyt_data" containing
    this data.

* match_movies.py:
    calls mysql queries to match movies from the nyt data to film titles in the provided data. writes
    final output to a csv file with columns "file" (the path to the article) and "movie_id".





