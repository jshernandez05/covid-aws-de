import sys
sys.path.insert(0, '/glue/lib/installation')
keys = [k for k in sys.modules.keys() if 'boto' in k]
for k in keys:
    if 'boto' in k:
    del sys.modules[k]

import awscli
import s3transfer
import redshift_connector

# Estabish DWH Connection
conn = redshift_connector.connect(
   host='covid-de-dc.cuahqnlx3uts.us-west-2.redshift.amazonaws.com',
   database='covid_dw',
   user='dwuser',
   password='Pa$$w0rd123!',
)

conn.autocommit = True
cur = conn.cursor()

# Create DWH Tables
cur.execute("""
            CREATE TABLE "dim_date" (
                "date_id" INTEGER,
                "date" DATE NOT NULL,
                "day_name" VARCHAR(9) NOT NULL,
                "day_of_week" INTEGER NOT NULL,
                "day" INTEGER NOT NULL,
                "day_of_year" INTEGER NOT NULL,
                "month" INTEGER NOT NULL,
                "month_name" VARCHAR(10) NOT NULL,
                "week" INTEGER NOT NULL,
                "quarter" INTEGER NOT NULL,
                "year" INTEGER NOT NULL,
                "year_half" INTEGER NOT NULL,
                "is_weekend" BOOLEAN NOT NULL,
                PRIMARY KEY (date_id)
                )
                SORTKEY (date)
            """)

cur.execute("""
            CREATE TABLE "dim_hospital" (
                "hosp_sk" INTEGER IDENTITY(1, 1),
                "fips" VARCHAR(6) NOT NULL,
                "state_fips" VARCHAR(2) NOT NULL,
                "county_fips" VARCHAR(3) NOT NULL,
                "state_name" VARCHAR(30) NOT NULL,
                "county_name" VARCHAR(120),
                "hospital_name" TEXT NOT NULL,
                "hq_address" VARCHAR(150),
                "hq_city" VARCHAR(150),
                "hq_state" CHAR(2),
                "hq_zip_code" CHAR(5),
                "hospital_type" VARCHAR(150),
                "latitude" REAL,
                "longtitude" REAL,
                PRIMARY KEY (hosp_sk)
                )
                SORTKEY (state_name)
            """)
            
cur.execute("""
            CREATE TABLE "dim_region" (
                "region_SK" INTEGER IDENTITY(1,1),
                "fips" VARCHAR(6) NOT NULL,
                "state_fips" VARCHAR(2) NOT NULL,
                "county_fips" VARCHAR(3) NOT NULL,
                "state" VARCHAR(30) NOT NULL,
                "county" VARCHAR(120),
                "country" VARCHAR(20),
                "latitude" REAL,
                "longitude" REAL,
                PRIMARY KEY (region_SK)
                )
                SORTKEY (state)
            """)
            
cur.execute("""
            CREATE TABLE "fact_covid" (
                "date" INTEGER,
                "state_fips" VARCHAR(3) NOT NULL,
                "state" VARCHAR(30) NOT NULL,
                "positive" REAL,
                "positiveincrease" INTEGER,
                "negative" REAL,
                "death" REAL,
                "deathincrease" INTEGER,
                "recovered" REAL,
                "hospitalized" REAL,
                "hospitalizedcurrently" REAL,
                "hospitalizeddischarged" REAL,
                "hospitalizedcumulative" REAL,
                "hospitalizedincrease" INTEGER,
                "region_sk" INTEGER,
                "hosp_sk" INTEGER,
                PRIMARY KEY (date, state_fips),
                FOREIGN KEY (date) REFERENCES dim_date (date_id),
                FOREIGN KEY (region_sk) REFERENCES dim_region (region_sk),
                FOREIGN KEY (hosp_sk) REFERENCES dim_hospital (hosp_sk)
                )
                SORTKEY (date, state)
            """)
            
# Load data from S3 Bucket
cur.execute("""
            copy dim_date from 's3://hernanjs-covid19-de/output/dim_date.csv'
            credentials 'aws_iam_role=arn:aws:iam::672787581761:role/redshift-s3-access'
            region 'us-west-2'
            delimiter ','
            IGNOREHEADER 1
            COMPUPDATE OFF
        """)

cur.execute("""
            copy dim_region from 's3://hernanjs-covid19-de/output/dim_region.csv'
            credentials 'aws_iam_role=arn:aws:iam::672787581761:role/redshift-s3-access'
            region 'us-west-2'
            delimiter ','
            explicit_ids
            IGNOREHEADER 1
            COMPUPDATE OFF
        """)

cur.execute("""
            copy dim_hospital from 's3://hernanjs-covid19-de/output/dim_hospital.csv'
            credentials 'aws_iam_role=arn:aws:iam::672787581761:role/redshift-s3-access'
            region 'us-west-2'
            delimiter ','
            explicit_ids
            IGNOREHEADER 1
            COMPUPDATE OFF
        """)
        
cur.execute("""
            copy fact_covid from 's3://hernanjs-covid19-de/output/fact_covid.csv'
            credentials 'aws_iam_role=arn:aws:iam::672787581761:role/redshift-s3-access'
            region 'us-west-2'
            delimiter ','
            IGNOREHEADER 1
            COMPUPDATE OFF
        """)
        
# Create Views for Visualizations
# /* total by state positive, death, hospitalized */
cur.execute("""                         
            CREATE OR REPLACE VIEW state_totals (state, state_abv, total_positive_cases, total_deaths) AS
                SELECT dr.state as state_name, fc.state, SUM(positive) as positive_cases, 
                    SUM(death) as deaths, ROUND(AVG(hospitalizedcurrently), 0) avg_hospitalized
                FROM fact_covid fc
                    JOIN dim_region dr ON fc.region_sk = dr.region_sk
                GROUP BY dr.state, fc.state
                ORDER BY fc.state
        """)
        
# /* total US */            
cur.execute("""                         
            CREATE OR REPLACE VIEW us_totals (postive_cases, deaths) AS
                SELECT SUM(positive) as positive_cases, SUM(death) as deaths, 
                MIN(date) as From, MAX(date) as To
                FROM fact_covid
        """)
        
# /* Daily */
cur.execute("""                         
            CREATE OR REPLACE VIEW state_daily (
                date, state, state_abv, positive, pos_increase,
                negative, deaths, death_increase, recovered, hospitalized, hosp_currently,
                hosp_increase, lattitude, longitude) AS
                SELECT dd.date, dr.state as state_name, fc.state, fc.positive, fc.positiveincrease,
                        fc.negative, fc.death, fc.deathincrease, fc.recovered, fc.hospitalized, 
                        fc.hospitalizedcurrently, fc.hospitalizedincrease, 
                        min(dr.latitude) as latitude, min(dr.longitude) as longitude
                FROM fact_covid fc
                JOIN dim_date dd ON fc.date = dd.date_id
                JOIN dim_region dr ON fc.region_sk = dr.region_sk
                GROUP BY dd.date, dr.state , fc.state, fc.positive, fc.positiveincrease,
                        negative, death, deathincrease, recovered, hospitalized, hospitalizedcurrently,
                        hospitalizedincrease
                ORDER BY dd.date, fc.state
        """)
            
cur.close()
conn.close()