# load required libraries
import datetime as dt
import json
import logging
import os
import pandas as pd
import psycopg2
import requests
from sqlalchemy import create_engine

# Create text file to record logs
logging.basicConfig(
    filename="logfile.txt",
    level=logging.DEBUG,
    format="%(asctime)s:%(levelname)s:%(message)s")


try:
    # get data in json using provided API
    response1 = requests.get(r"https://jsonplaceholder.typicode.com/posts")

except Exception as e:
    # check response of given API
    print("Exception occurs due to", e)

else:
    logging.debug(f"API responded with response: {response1.status_code}")
    # check content of API in bytes
    # print(response1.content)

    # check content of API in unicode
    # print(response1.text)

    # 2. Create Table in Database

    # Create Connection between Python and Database
    db_username = os.getenv("user")
    db_password = os.getenv("password")
    db_host = os.getenv("host")
    db_port = os.getenv("port")
    db_database = os.getenv("database")

    try:
        connection = psycopg2.connect(
            user=db_username,
            password=db_password,
            host=db_host,
            port=db_port,
            database=db_database,
        )

        cursor = connection.cursor()
        logging.debug(
            "Python and Database Connection Established Successfully"
        )
        print()

    except Exception as e:
        print("Exception Occurs due to: ", e)

    else:
        # create table in database
        cursor.execute("""drop table if exists assign1""")
        connection.commit()

        cursor.execute(
            """create table assign1
                        (userid           int,
                        id               int,
                        title   varchar(255),
                        body    varchar(255),
                        created         date,
                        updated         date,
                        deleted         date)
                        """
        )

        connection.commit()
        logging.debug("Table Created in Database")
        cursor.close()

        # 3. Filter API data for userId=2 and save the data in csv format
        try:
            # get data in json using provided API
            response2 = requests.get(
                r"https://jsonplaceholder.typicode.com/posts?userId=2"
            )

        except Exception as f:
            # check response of given API
            print("Exception occurs due to", f)

        else:
            logging.debug(
                f"Response of filtered API:{response2.status_code}"
            )

            # filter data for userId = 2 from API and load it in json format
            filtered_data = json.loads(response2.text)

            # read filtered data in Pandas DataFrame and Preprocess if required
            df = pd.DataFrame(filtered_data)
            print("API holds below data for userid = 2")
            print(df.head())
            print()

            # check column names
            # print(df.columns)

            # rename column names
            df.columns = [i.lower() for i in df.columns]
            # print(df.columns)

            # add created and updated column in dataframe
            df["created"] = df.apply(lambda _: "", axis=1)
            df["updated"] = df.apply(lambda _: "", axis=1)

            # add current date in created and updated column
            df["created"] = df["created"].apply(lambda x: dt.date.today())
            df["updated"] = df["updated"].apply(lambda x: dt.date.today())
            print("Preprocessed Data")
            print(df.head())
            print()

            # 4. save dataframe in csv format

            df.to_csv("userid2.csv", index=False)
            logging.debug("preprocessed data stored in csv file")

            # 5. Save Preprocessed DataFrame to Database

            # create connection between python and database using sqlalchemy
            engine = create_engine(
                "postgresql://"
                + db_username
                + ":"
                + db_password
                + "@"
                + db_host
                + ":"
                + db_port
                + "/"
                + db_database
            )

            # upload dataframe to database
            df.to_sql("assign1", con=engine, if_exists="append", index=False)
            logging.debug("Preprocessed Data uploaded to Database")

            # 6. Read data from database

            db_df = pd.read_sql("select * from assign1", engine)
            print("Database Data")
            print(db_df)
