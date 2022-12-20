# 1 get articles from NewsAPI
# 2 save the information as JSON then upload to S3
# 3 load from s3 to snowflake
# 4 doing data analysis and SQL query based on the result
# 5 make everything into documentation

import requests
import boto3
import json
import snowflake.connector


class TovalaTools:

    def __init__(self):
        self.s3 = boto3.resource('s3')
        self.bucket_name = "tovala-de-coding-challenge"
        self.prefix = 'Ziyu-Huang'
        #self.file_name = self.prefix + '/NewsAPI_Tovala_' + time.strftime("%Y%m%d-%H%M%S") + '.json'
        self.file_name = self.prefix + '/NewsAPI_Tovala.json'
        print('S3 bucket initialization completed')

    def conn_Snowflake(self):

        snowflake.connector.paramstyle = 'qmark'
        self.sf_con = snowflake.connector.connect(

            account="jta05846",
            user="de_candidate",
            password="yummy_%$food",
            role="interview",
            warehouse="analyst_warehouse",
            database="interview"


        )
        self.sf_cur = self.sf_con.cursor()
        print('Successful Connection for snowflake')
    def sf_NewsAPI_creation(self):
        self.sf_cur.execute("use schema DE_LANDING")
        table_query = '''CREATE TABLE IF NOT EXISTS NewsAPI (
                          
                          source string,
                          author string,
                          title string ,
                          description string,
                          url string,
                          publishedAt timestamp_ntz
                          ); '''
        self.sf_cur.execute(table_query)
        print('NewsAPI table has been created successfully')

    def sf_load_NewsAPI(self):
        self.sf_cur.execute("use schema DE_LANDING")
        json_table_query = '''
                              CREATE TABLE IF NOT EXISTS  json_table (json variant);'''
        json_format_query = '''create or replace file format myjsonformat
                                type = 'JSON'
                                strip_outer_array = true;'''
        query_stage = '''create or replace stage my_s3_stage
                          file_format =myjsonformat
                          url = 's3://tovala-de-coding-challenge';'''

        query_load = '''copy into json_table
                          from s3://tovala-de-coding-challenge/Ziyu-Huang/NewsAPI_Tovala.json
                          file_format = "json"
                          on_error = 'skip_file';'''
        query_newsapi = '''
                        CREATE TABLE IF NOT EXISTS newsapi as 
                        select f.value:source::string as source,f.value:author::string as author, f.value:title::string as title, f.value:description::string as description,
                        f.value:url::string as url, f.value:publishedAt::timestamp_ntz as publishedAt
                        from json_table,table(flatten(json:articles)) f;'''
        self.sf_cur.execute(json_table_query)
        self.sf_cur.execute(json_format_query)
        self.sf_cur.execute(query_stage)
        self.sf_cur.execute(query_load)
        self.sf_cur.execute(query_newsapi)
        print('done with loading json file into snowflake')

    def sf_NewsAPI_deletion(self):
        self.sf_cur.execute("use schema DE_LANDING")
        table_query1 = '''drop table NewsAPI ; '''
        table_query2 = '''drop table json_table ; '''
        try:
            self.sf_cur.execute(table_query1)
            self.sf_cur.execute(table_query2)
            print('All the tables have been deleted successfully')
        except:
            print("There is no table existed to be dropped in the schema")
    def generate_NewsAPI_S3(self):
        query_keyword = 'Tovala OR meal kit OR smart oven&'
        search_start_date = '2022-12-19&'
        search_end_date = '2022-12-20&'

        url = ('https://newsapi.org/v2/everything?'
               'q='+query_keyword+
               'from='+search_start_date+
               'to='+search_end_date+
               'sortBy=popularity&'
               'apiKey=f629c70c4f524803a7e8ea12d6cfcdbb')

        response = requests.get(url)
        initial_json = response.json()
        second_json = initial_json["articles"] #100
        for each in second_json:
            each['source'] = each['source']['name']
            del each['content']
            del each['urlToImage']

        #delete useless info to save space
        dst_json = {'articles':second_json}

        #for i in range(len(second_json)):
            #dst_json.update({i+1:second_json[i] })

        self.s3object = self.s3.Object(self.bucket_name,self.file_name )
        self.s3object.put(Body=(bytes(json.dumps(dst_json).encode('UTF-8'))))
        print('API extracted data has been uploaded to S3 bucket successfully, bucket name is: ',self.bucket_name)

    def del_NewsAPI_S3(self):

        self.bucket = self.s3.Bucket(self.bucket_name)
        self.bucket.objects.filter(Prefix=self.prefix).delete()
        print('S3 folder for Ziyu-Huang has been deleted successfully')
#next method need to upload json file from s3 to snowflake

# method 1: create a table in snowflake
if __name__== "__main__":
    Tovala = TovalaTools()
    Tovala.generate_NewsAPI_S3()
    Tovala.conn_Snowflake()
    Tovala.sf_load_NewsAPI()
    print("Great! Time to check your data at S3 and snowflake!")

