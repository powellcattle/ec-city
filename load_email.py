con = None
SQL_INSERT_CALF = "INSERT INTO cattle.calf(" \
                  "id," \
                  "sire_id," \
                  "dam_id," \
                  "real_dam_id," \
                  "sex," \
                  "breed," \
                  "coat_color_dna," \
                  "dob," \
                  "ear_tag," \
                  "birth_weight," \
                  "weaning_weight," \
                  "yearling_weight," \
                  "adj_birth_weight," \
                  "adj_weaning_weight," \
                  "adj_yearling_weight)" \
                  "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
SQL_DROP_CALF = "DROP TABLE IF EXISTS cattle.calf CASCADE"
SQL_CREATE_CALF = "CREATE TABLE cattle.calf(" \
                  "id integer NOT NULL," \
                  "sire_id integer," \
                  "dam_id integer," \
                  "real_dam_id integer," \
                  "sex character varying(10)," \
                  "breed character varying(20)," \
                  "coat_color_dna character varying(10)," \
                  "dob date," \
                  "ear_tag character varying(50) NOT NULL," \
                  "birth_weight integer," \
                  "weaning_weight integer," \
                  "yearling_weight integer," \
                  "adj_birth_weight integer," \
                  "adj_weaning_weight integer," \
                  "adj_yearling_weight integer," \
                  "CONSTRAINT calf_pkey PRIMARY KEY (id))"
try:

    # breeding_forms = load_animal_custom_fields("BREEDING FORM")
    color_DNAs = load_animal_custom_fields("COAT COLOR DNA")
    breeds = load_breeds()

    con = psycopg2.connect(database='rafter',
                           user='postgres',
                           password='postgres',
                           host='localhost')

    cur = con.cursor()
    cur.execute(SQL_DROP_CALF)
    con.commit()
    cur.execute(SQL_CREATE_CALF)
    con.commit()

    with open(_file_name, "rb") as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        next(reader, None)
        for row in reader:
            active = ec_util.to_upper_or_none(row[25])  # Z
            if active != "ACTIVE":
                continue
            animal_type = ec_util.to_upper_or_none(row[17])
            if animal_type != "CALF":
                continue

            id = ec_util.to_pos_int_or_none(row[0])  # A
            sire_id = ec_util.to_pos_int_or_none(row[27])  # AB
            dam_id = ec_util.to_pos_int_or_none(row[28])  # AC
            real_dam_id = ec_util.to_pos_int_or_none(row[30])  # AE
            # breed
            breed_key = ec_util.to_pos_int_or_none(row[20])  # U
            breed = ec_hashmap.get(breeds, breed_key)

            coat_color_dna = ec_hashmap.get(color_DNAs, id)
            if coat_color_dna:
                if "ED/ED" in coat_color_dna:
                    coat_color_dna = "ED/ED"
                elif "ED/E" in coat_color_dna:
                    coat_color_dna = "ED/E"
                elif "NOT TESTED" in coat_color_dna:
                    coat_color_dna = "NOT TESTED"
                else:
                    coat_color_dna = None

            ear_tag = ec_util.to_upper_or_none(row[3])  # D
            sex = ec_util.to_upper_or_none(row[18])
            dob_date = ec_util.to_upper_or_none(row[35])
            birth_weight = ec_util.to_pos_int_or_none(row[69])
            weaning_weight = ec_util.to_pos_int_or_none(row[70])
            yearling_weight = ec_util.to_pos_int_or_none(row[71])
            adj_birth_weight = ec_util.to_pos_int_or_none(row[72])
            adj_weaning_weight = ec_util.to_pos_int_or_none(row[73])
            adj_yearling_weight = ec_util.to_pos_int_or_none(row[74])

            seller_id = ec_util.to_pos_int_or_none(row[43])  # AR
            current_breeding_status = ec_util.to_upper_or_none(row[59])  # BH
            last_calving_date = ec_util.to_date_or_none(row[58])
            estimated_calving_date = ec_util.to_date_or_none(row[60])
            last_breeding_date = ec_util.to_date_or_none(row[58])
            contact_id = ec_util.to_pos_int_or_none(row[43])  # AR
            dob = ec_util.to_date_or_none(row[35])

            cur.execute(SQL_INSERT_CALF, (
                id,
                sire_id,
                dam_id,
                real_dam_id,
                sex,
                breed,
                coat_color_dna,
                dob_date,
                ear_tag,
                birth_weight,
                weaning_weight,
                yearling_weight,
                adj_birth_weight,
                adj_weaning_weight,
                adj_yearling_weight))
            con.commit()




except psycopg2.DatabaseError, e:
    print 'Error %s' % e
    sys.exit(1)

finally:
    if con:
        con.close()
    if csvfile:
        csvfile.close()
