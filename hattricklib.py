from authlib.integrations.flask_client import OAuth
from flask import Flask, render_template, redirect, url_for, session, send_from_directory
import pandas as pd
import xml.etree.ElementTree as ET
import os
import json
from numpy import arange
from datetime import datetime

script_directory = os.path.dirname(os.path.abspath(__file__))
json_file = open(f'{script_directory}/settings.json')
SETTINGS = json.load(json_file)

def connect_hattrick():
    app = Flask(__name__, static_url_path = "/static", static_folder = "static")
    app.secret_key = os.urandom(24)

    oauth = OAuth()
    oauth.init_app(app)

    oauth.register(
        name='hattrick',
        client_id=SETTINGS['client_id'],
        client_secret=SETTINGS['client_secret'],
        request_token_url="https://chpp.hattrick.org/oauth/request_token.ashx",
        request_token_params=None,
        access_token_url="https://chpp.hattrick.org/oauth/access_token.ashx",
        access_token_params=None,
        authorize_url='https://chpp.hattrick.org/oauth/authorize.aspx',
        authorize_params=None,
        api_base_url="https://chpp.hattrick.org/chppxml.ashx",
        client_kwargs=None,
    )

    print(f'{datetime.now().strftime("%H:%M:%S")} - HT connection has been established.')
    return oauth, SETTINGS['token']


def get_fed_members(oauth, token, version=1.5, fedID=117291):
    params = {
            'file' : 'alliancedetails',
            'version' : version,
            'allianceID' : fedID,
            'actionType' : 'members'
        }

    resp = oauth.hattrick.get('', params=params, token=token)
    xml = resp.text.encode("latin-1").decode("utf-8")
    root = ET.fromstring(xml)

    members = {member.find('UserID').text : {'loginname' : member.find('Loginname').text} for member in root.find('.//Members')}

    print(f'{datetime.now().strftime("%H:%M:%S")} - HT fed members has been downloaded.')
    return members


def get_user_achievments_points(userID, oauth, token, version=1.2):
    params = {
            'file' : 'achievements',
            'version' : version,
            'userID' : userID
        }

    resp = oauth.hattrick.get('', params=params, token=token)
    xml = resp.text.encode("latin-1").decode("utf-8")
    root = ET.fromstring(xml)

    achievs = {f'ach_{k.find("AchievementTypeID").text}':int(k.find("Points").text) for k in root.findall('.//Achievement')}
    achievs['total'] = sum([int(p.text) for p in root.findall('AchievementList/Achievement/Points')])

    return achievs


def download_data(oauth, token):
    members = get_fed_members(oauth=oauth, token=SETTINGS['token'])

    print(f'{datetime.now().strftime("%H:%M:%S")} - HT achievements download has been started.')
    for id, _ in members.items():
        achiev_points = get_user_achievments_points(oauth=oauth, token=SETTINGS['token'], userID=id)
        members[id] |= achiev_points
    print(f'{datetime.now().strftime("%H:%M:%S")} - HT achievements has been downloaded.')

    df_members_achievs = pd.DataFrame().from_dict(members, orient='index').sort_values('total', ascending=False)


    #data clean
    df_members_achievs.fillna(0, inplace=True)

    #adjust 0 to single digits 
    df_members_achievs.columns = [col.replace('_', '_0') if len(col) == 5 else col for col in df_members_achievs.columns]

    df_members_achievs.sort_values('total', inplace=True, ascending=False)
    df_members_achievs['user_id'] = df_members_achievs.index
    df_members_achievs.set_index(arange(1, len(df_members_achievs) + 1), inplace=True)
    #df_members_achievs.index.name = '#'

    #achive columns sort
    ach_sorted = sorted([col for col in df_members_achievs.columns if 'ach_' in col])
    #df_members_achievs[ach_sorted].astype('int')
    ach_sorted.insert(0, 'total')
    ach_sorted.insert(0, 'loginname')
    ach_sorted.append('user_id')
    df_members_achievs = df_members_achievs[ach_sorted]

    df_members_achievs.to_excel(f'{script_directory}/Data/fed_achievments_{datetime.now().strftime("%y%m%d_%H%M")}.xlsx')
    df_members_achievs.to_excel(f'{script_directory}/Data/fed_achievments_current.xlsx')

    print(f'{datetime.now().strftime("%H:%M:%S")} - HT data extraction has been succesfully done.')
    return df_members_achievs