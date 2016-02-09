import MySQLdb as DB
import csv
import os
import pandas as pd
from BeautifulSoup import BeautifulSoup
import re
import nltk
from sys import stdout

def parse_html(db, cursor):
    # create table for film data
    print "creating nyt_data"
    cursor.execute(''' drop table if exists nyt_data ''')
    cursor.execute(''' create table nyt_data (path varchar(255), movie_title varchar(255),
        text_body text, people_names text, years text)''')

    # initialize final dataframe
    nyt_data = pd.DataFrame()

    # i want to print progress, so we need to know how many files we're iterating over
    # (inefficient/redundant but low cost)
    ind = 1
    total_files = 0
    for root, dirs, files in os.walk("./nyt"):
        for file in files:
            if file.endswith('.html'):
                total_files += 1

    print "processing nyt data"
    # loop through the files in the directory
    for root, dirs, files in os.walk("./nyt"):
        for name in files:
            if name.endswith((".html")):
                stdout.write("\r%d/%d" % (ind, total_files))
                stdout.flush()
                df = {}
                # initialize beautifulsoup stuff
                path = root+"/"+name
                article = open(path, 'r')
                soup = BeautifulSoup(article)

                df['path'] = path

                # get the movie title first. in almost all cases it's either in a div with
                # id = movieTitle or in an itemprop tag
                div_title = soup.find('div', {'id':'movieTitle'})
                itemprop_title = soup.find(itemprop="name")

                # best source seems to be itemprop_title. however it's only available after 2008.
                # if 2007/2008, use the div_title
                # this method seems to correctly get 496/500 titles, the missing ones seem to be one-off
                # changes in formatting. is there a better way to get these remaining 4? hmmm
                if re.match(".*/200[78]/.*", path):
                    if div_title:
                        df['movie_title'] = re.sub("\(.*", "", div_title.text).strip()
                    else:
                        df['movie_title'] = None
                else:
                    if itemprop_title:
                        df['movie_title'] = itemprop_title.text
                    else:
                        df['movie_title'] = None

                # now look for names of people
                # grab the body of the story, which can be in two places depending when the article was written
                if re.match(".*/200[78]/.*", path):
                    text_body = soup.findAll('div', attrs={'class':'articleBody'})
                    if len(text_body) > 1:
                        text_body = [x.text for x in text_body]
                        text_body = ' '.join(text_body)
                        df['text_body'] = text_body
                    elif len(text_body) == 1:
                        text_body = text_body[0].text
                        df['text_body'] = text_body
                    else:
                        df['text_body'] = None
                else:
                    text_body = soup.findAll('div', {'id':'story-body'})
                    if len(text_body) > 1:
                        text_body = [x.text for x in text_body]
                        text_body = ' '.join(text_body)
                        df['text_body'] = text_body
                    elif len(text_body) == 1:
                        text_body = text_body[0].text
                        df['text_body'] = text_body
                    else:
                        df['text_body'] = None

                # use nltk to pull words that look like names
                # method borrowed from:
                # http://timmcnamara.co.nz/post/2650550090/extracting-names-with-6-lines-of-python-code
                # i chose this method over others (there are lots of ways to do this!) because it is
                # relatively short/simple and seemed to produce good results (not too much non-name stuff,
                # caught most of the names)
                director_names = ""
                if text_body:
                    for sent in nltk.sent_tokenize(text_body):
                        for chunk in nltk.ne_chunk(nltk.pos_tag(nltk.word_tokenize(sent))):
                            if hasattr(chunk, 'node'):
                                if chunk.node == "PERSON":
                                    director_names = director_names +" "+ ' '.join(c[0] for c in chunk.leaves())
                    df['people_names'] = director_names
                else:
                    df['people_names'] = None

                # pull out things that look like years
                # also include year that article was published
                if text_body:
                    years = re.findall("\d{4}", text_body)
                    years_string = " ".join(years) + " " + re.search("\d{4}", path).group(0)
                    df['years'] = years_string
                else:
                    df['years'] = None

                # put data for this page in our main dataframe
                nyt_data = nyt_data.append(df, ignore_index=True)
                ind += 1

    # put the main dataframe in the db
    stdout.write("\n")
    print "inserting data to db"
    nyt_data.to_sql(con=db, name='nyt_data', flavor='mysql', index=False, if_exists='append')
    db.commit()
















