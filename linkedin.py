# -*- coding: utf-8 -*-
"""
Created on Mon Aug 22 11:33:22 2022

@author: deep.gosar
"""
import pandas as pd
from linkedin_api import Linkedin
import time
from flask import Flask, jsonify
from flask_restful import reqparse, abort, Api, Resource, request
import json
from sqlalchemy import create_engine
import os

engine = create_engine('sqlite:///linked_in_users.db', echo = False) 
sqlite_connection = engine.connect()
#engine.execute("CREATE TABLE LinkedIn_Data( `index` INT,`First Name` VARCHAR(100), `Last Name` VARCHAR(100), Country VARCHAR(50), Headline VARCHAR(255), `Industry Type` VARCHAR(100), `Public ID` VARCHAR(100), `Email ID` VARCHAR(255));") 

app = Flask(__name__)
api = Api(app)

parser = reqparse.RequestParser()
parser.add_argument('user-id')
parser.add_argument('password')
parser.add_argument('profile-id')

class linked(Resource):
    def get(self):
        global user_id, password, profile_id
        args = parser.parse_args()
        user_id = args['user-id']
        password = args['password']
        profile_id = args['profile-id']




        linkedin_api = Linkedin(user_id, password)
        profile = linkedin_api.get_profile(profile_id)
        convert_to_list = list(profile.values())
        urn_id = convert_to_list[13] 
        urn_id = urn_id[18:]
        my_connections = linkedin_api.get_profile_connections(urn_id)
        my_list = list(profile.values())
        pub_id = pd.DataFrame(my_connections)['public_id'].tolist()
        test_var = engine.execute("SELECT `Public ID` FROM LinkedIn_Data WHERE `Email ID` = ? ;", user_id).fetchall()
        
        global t_pub_id, t_my_conn, conn_info
        t_pub_id = []
        t_my_conn = []
        conn_info = []
        count_ = len(test_var)
        global final_df
        final_df = pd.DataFrame()
        if count_ == 0: # 2nd Scenario
            var = engine.execute("SELECT * FROM LinkedIn_Data LIMIT 3;").fetchall()
            if len(var) > 1:
                print("Scenario 2 got executed")
                for i in range(10): 
                    t_pub_id.insert(i, pub_id[i])
                    t_my_conn.insert(i, my_connections[i])
                    conn_info.insert(i, linkedin_api.get_profile(t_pub_id[i])) 
                    print(f"Fetched profile's {i+1} of {len(my_connections)}")
                    time.sleep(0.1)
                    i = i + 1
                final_df = create_df(conn_info)
            if len(var) == 0: # 1st Scenario
                print("Scenario 1 got executed")
                for i in range(10): 
                    t_pub_id.insert(i, pub_id[i]) 
                    t_my_conn.insert(i, my_connections[i]) 
                    conn_info.insert(i, linkedin_api.get_profile(t_pub_id[i])) 
                    print(f"Fetched profile's {i+1} of {len(my_connections)}")
                    time.sleep(0.1)
                    i = i + 1
                final_df = create_df(conn_info)
        if count_ > 1: # 3rd Scenario
            var = engine.execute("SELECT * FROM LinkedIn_Data LIMIT 3;").fetchall()
            sql_tab_uni = pd.read_sql("SELECT `Public Id` FROM LinkedIn_Data", con = engine)
            uni = sql_tab_uni['Public ID'].tolist()
            res = list(set(pub_id) - set(uni))
            print("Scenario 3 got executed")
            if len(res) != 0:
                if len(var) > 1:
                    for i in range(10):
                        t_pub_id.insert(i, res[i]) 
                        t_my_conn.insert(i, my_connections[i]) 
                        conn_info.insert(i, linkedin_api.get_profile(t_pub_id[i])) 
                        print(f"Fetched profile's {i+1} of {len(my_connections)}")
                        time.sleep(0.1)
                        i = i + 1
                    final_df = create_df(conn_info)  
            else:
                final_df = pd.read_sql(f"SELECT * FROM LinkedIn_Data WHERE `Email ID` = '{user_id}' ;", con = engine)
            jsons = final_df.to_json(orient = 'records')
            js_1 = json.loads(jsons)
            return jsonify(js_1)

def create_df(conn_info_rec):
    test_ = pd.DataFrame()
    f_name = pd.DataFrame(conn_info_rec)['firstName'].tolist()
    test_['First Name'] = f_name
    l_name = pd.DataFrame(conn_info_rec)['lastName'].tolist()
    test_['Last Name'] = l_name
    country = pd.DataFrame(conn_info_rec)['geoCountryName'].tolist()
    test_['Country'] = country
    headline = pd.DataFrame(conn_info_rec)['headline'].tolist()
    test_['Headline'] = headline
    ind_name = pd.DataFrame(conn_info_rec)['industryName'].tolist()
    test_['Industry Type'] = ind_name
    test_['Public ID'] = t_pub_id
    email = []
    for i in range(10):
        email.insert(i ,user_id)
    test_['Email ID'] = email

    i = 0
    exp = []
    ext_exp = []
    exp = pd.DataFrame(conn_info)['experience'].tolist() 
    exp_of = []
    ext_exp = [items for sublist in exp for items in sublist]
    ext_dict_titles = [j["title"] for j in ext_exp]
    list_keys = []
    list_values = []
    for j in ext_exp:
        list_keys.insert(i, list(j.keys()))
        list_values.insert(i, list(j.values()))
        i = i + 1
    for i in range(len(exp)):
        exp_of.insert(i, len(exp))

    test_ = test_.reset_index(drop=True)
    test_.to_sql('LinkedIn_Data', con = engine, if_exists='append', chunksize=1000)
    return test_

api.add_resource(linked, '/linked')
#port = int(os.environ.get('PORT', 5000))
app.run()
