import MySQLdb as DB
import csv

def match_movies(db, cursor):
    # add indices to tables
    print "adding indices"
    add_indices = '''# alter table actors add index(person_id), add index(movie_id), add index(`name`);
        # alter table directors add index(person_id), add index(movie_id), add index(`name`);
        # alter table names_aka add index(person_id), add index(`name`);
        # alter table titles add index(movie_id), add index(title);
        # alter table titles_aka add index(movie_id), add index(alternate_title);
        alter table nyt_data add id int primary key auto_increment first;
        alter table nyt_data add index(movie_title)'''

    for sql in add_indices.split(';'):
        cursor.execute(sql)
    db.commit()

    # prepare to match
    cursor.execute('''alter table nyt_data add column movie_id int(11);''')
    cursor.execute('''alter table nyt_data add column match_specs varchar(255);''')

    # match on unique title name (255 matches)
    cursor.execute('''
        update nyt_data d
        join (select d.id as did, d.movie_title, t.movie_id, t.title from nyt_data d
            join titles t on t.title = d.movie_title
            group by d.id having count(*) = 1) w on w.did = d.id
        set d.movie_id = w.movie_id, match_specs = "unique title"
        where d.movie_id is null;''')
    print "matches on unique title name:", cursor.rowcount
    db.commit()

    # match on unique alternate title name (37 matches)
    cursor.execute('''
        update nyt_data d
        join (select d.id as did, d.movie_title, t.movie_id, t.alternate_title from nyt_data d
            join titles_aka t on t.alternate_title = d.movie_title
            where d.movie_id is null
            group by d.id having count(*) = 1) w on w.did = d.id
        set d.movie_id = w.movie_id, match_specs = "unique alternate title"
        where d.movie_id is null;''')
    print "matches on unique alt title name:", cursor.rowcount
    db.commit()

    # match on partial name--if title is "Blah Blah: Blah Blah Blah" and we scraped "Blah Blah",
    # count as a match (4 matches)
    cursor.execute('''
        update nyt_data d
        join (select d.id as did, d.movie_title, t.title, t.movie_id from nyt_data d
            join titles t on substring_index(t.title, ":", 1) = d.movie_title or t.title = substring_index(d.movie_title, ":", 1)
            where d.movie_id is null
            group by d.id having count(*) = 1) w on w.did = d.id
        set d.movie_id = w.movie_id, d.match_specs = "unique partial title"
        where d.movie_id is null''')
    print "matches on unique partial names", cursor.rowcount
    db.commit()

    # match on title or partial title and year article was published (18 matches)
    cursor.execute('''
        update nyt_data d
        join (select d.id as did, d.movie_title, d.years, t.movie_id, t.title, t.production_year from nyt_data d
            join titles t on (t.title = d.movie_title or substring_index(t.title, ":", 1) = d.movie_title
                            or t.title = substring_index(d.movie_title, ":", 1))
                            and d.years like concat("%", t.production_year)
            where d.movie_id is null
            group by d.id having count(*) = 1) w on w.did = d.id
        set d.movie_id = w.movie_id, match_specs = "title and year published"
        where d.movie_id is null;''')
    print "matches on title and year published:", cursor.rowcount
    db.commit()

    # match on director first and last and title or alt title (4 matches)
    # director last name: substring_index(d.name, ",", 1)
    # director first name: substring_index(d.name, ",", -1)
    cursor.execute('''
        update nyt_data d
        join (select d.id as did, t.movie_id as tmovie_id, ta.movie_id as tamovie_id, t.title, d.movie_title, d.people_names, di.name, d.years, t.production_year from nyt_data d
            join directors di on d.people_names like concat("%", substring_index(di.name, ",", -1), " ", substring_index(di.name, ",", 1), " ", "%" ) or
                                d.people_names like concat("%", substring_index(di.name, ",", -1), " ", substring_index(di.name, ",", 1))
            left join titles t on (t.title = d.movie_title or substring_index(t.title, ":", 1) = d.movie_title
                            or t.title = substring_index(d.movie_title, ":", 1))
                            and t.movie_id = di.movie_id
            left join titles_aka ta on (ta.alternate_title = d.movie_title or substring_index(ta.alternate_title, ":", 1) = d.movie_title
                            or ta.alternate_title = substring_index(d.movie_title, ":", 1))
                            and ta.movie_id = di.movie_id
            where d.movie_id is null and (t.movie_id is not null or ta.movie_id is not null)
            group by d.id having count(*) = 1) w on d.id = w.did
        set d.match_specs = "director first and last",
            d.movie_id = case
            when w.tmovie_id is null then w.tamovie_id
            else w.tmovie_id
            end;''')
    print "matches on director f/l and title/alt title:", cursor.rowcount
    db.commit()

    # match on actor first and last and title/alt title (3 matches)
    cursor.execute('''
        update nyt_data d
        join (select d.id as did, t.movie_id as tmovie_id, ta.movie_id as tamovie_id, t.title, d.movie_title, d.people_names, a.name, d.years,
            t.production_year from nyt_data d
            join actors a on d.people_names like concat("%", substring_index(a.name, ",", -1), " ", substring_index(a.name, ",", 1), " ", "%" )
                        or d.people_names like concat("%", substring_index(a.name, ",", -1), " ", substring_index(a.name, ",", 1))
            left join titles t on (t.title = d.movie_title or substring_index(t.title, ":", 1) = d.movie_title
                            or t.title = substring_index(d.movie_title, ":", 1))
                            and t.movie_id = a.movie_id
            left join titles_aka ta on (ta.alternate_title = d.movie_title or substring_index(ta.alternate_title, ":", 1) = d.movie_title
                            or ta.alternate_title = substring_index(d.movie_title, ":", 1))
                            and ta.movie_id = a.movie_id
            where d.movie_id is null and (t.movie_id is not null or ta.movie_id is not null)
            group by d.id, t.movie_id , ta.movie_id  having count(*) = 1) w on w.did = d.id
        set d.match_specs = "actor first and last",
            d.movie_id = case
            when w.tmovie_id is null then w.tamovie_id
            else w.tmovie_id
            end;''')
    print "matches on actor f/l name and title/alt title:", cursor.rowcount
    db.commit()

    # match on alt names (1 match)
    cursor.execute('''
        update nyt_data d
        join (select d.id as did, t.movie_id as tmovie_id, ta.movie_id as tamovie_id, t.title, d.movie_title, d.people_names, a.name, d.years, t.production_year from nyt_data d
            join names_aka a on d.people_names like concat("%", substring_index(a.name, ",", -1), " ",
                                                            substring_index(a.name, ",", 1), " ", "%" ) or
                                d.people_names like concat("%", substring_index(a.name, ",", -1), " ",
                                                            substring_index(a.name, ",", 1))
            left join directors dir on dir.person_id = a.person_id
            left join actors act on act.person_id = a.person_id
            left join titles t on (t.title = d.movie_title or substring_index(t.title, ":", 1) = d.movie_title
                            or t.title = substring_index(d.movie_title, ":", 1))
                            and (t.movie_id = dir.movie_id or t.movie_id = act.movie_id)
            left join titles_aka ta on (ta.alternate_title = d.movie_title or substring_index(ta.alternate_title, ":", 1) = d.movie_title
                            or ta.alternate_title = substring_index(d.movie_title, ":", 1))
                            and (ta.movie_id = dir.movie_id or ta.movie_id = act.movie_id)
            where d.movie_id is null and (t.movie_id is not null or ta.movie_id is not null)
            group by d.id having count(*) = 1) w on d.id = w.did
        set d.match_specs = "alt names first and last",
            d.movie_id = case
            when w.tmovie_id is null then w.tamovie_id
            else w.tmovie_id
            end;''')
    print "matches on people alt names and title/alt title:", cursor.rowcount
    db.commit()

    # print how many we were able to match our of total
    cursor.execute(''' select count(*) from nyt_data where movie_id is not null''')
    print "total matches:", int(cursor.fetchone()[0])
    cursor.execute(''' select count(*) from nyt_data''')
    print "total movies:", int(cursor.fetchone()[0])

# create csv with results
def create_result(db, cursor, output_file):
    cursor.execute('''select path as file, movie_id from nyt_data where movie_id is not null''')
    data = cursor.fetchall()

    out_file = open(output_file,"wb")
    out = csv.writer(out_file)

    out.writerow(('file', 'movie_id'))
    for row in data:
        out.writerow(row)

    out_file.close()














