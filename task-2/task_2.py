"""
  Для запуска скрипта каждые 30 минут я бы использовал huey c Luigi, либо внутренний механизм 
  Airflow. В качестве простой реализации приведу просто строчку из crontab: 
  30 * * * * python3 task_2.py
"""
import datetime
import json
import sys
import os
import requests
from sqlalchemy import create_engine

DB_LOGIN = '...'
DB_PASSWORD = '...'
DB_HOST = '...'
DB_PORT = '...'
AMOCRM_SUBDOMAIN = '...'
AUTHORIZATION = '...'

DB_STRING = "postgres://{0}:{1}@{2}:{3}/postgres".format(
    DB_LOGIN,
    DB_PASSWORD,
    DB_HOST,
    DB_PORT)

# дата запуска этого скрипта для заголовка IF-MODIFIED-SINCE 
UTC_DATETIME = datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S UTC')


def get_leads(modified, last_modified=None):
    """Делает GET запрос и возвращает сделки из amoCRM. 
    Необходим корректный субдомен AMOCRM_SUBDOMAIN и access token AUTHORIZATION. 
    
    Аргументы:
    modified -- если True, то возвращает все модифицированные сделки с момента last_modified 
    last_modified -- дата в формате "D, d M Y H:i:s" для заголовка запроса к amoCRM 
    """

    url = "https://{}.amocrm.ru/api/v2/leads".format(AMOCRM_SUBDOMAIN)
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {}'.format(AUTHORIZATION),
        
    }
    if modified:
        headers.update('IF-MODIFIED-SINCE': last_modified)

    res = []
    try:
        response = requests.request("GET", url, headers=headers)
        print(response)
        res = json.loads(response.text)
    except Exception:
        sys.exit()

    return res


def save_current_timestamp_to_file():
    """Сохранить время запуска этого скрипта в текстовый файл."""
    with open("lasttimestamp.txt", 'w') as fp:
        fp.write(UTC_DATETIME)


def main():
    db = create_engine(DB_STRING)
    if os.path.isfile("lasttimestamp.txt"):
        with open("lasttimestamp.txt", 'r') as f:
            data = get_leads(True, f.read())
    else:
    # если файл "lasttimestamp.txt" еще не создан, то создать его 
        f = open("lasttimestamp.txt", "w+")
        f.close()
        data = get_leads(True)

    # здесь вместо SQLalchemy я бы использовал внутренние механизмы работы с БД
    # какого-нибудь Luigi или Airflow
    with db.connect():
        for lead in data['_embedded']['items']:
            db.execute('''
                insert into amo_leads (created_by,
                                       name,
                                       created_at,
                                       closest_task_at,
                                       group_id,
                                       responsible_user_id,
                                       id,
                                       is_deleted,
                                       status_id,
                                       closed_at,
                                       pipeline_id,
                                       account_id,
                                       loss_reason_id,
                                       sale,
                                       updated_at,
                                       updated_by,
                                       company)
                values (
                    {0},'{1}',{2},
                    {3},{4},{5},
                    {6},{7},{8},
                    {9},{10},{11},
                    {12},{13},{14},
                    {15},'{16}'
                ) on conflict do nothing;
                '''.format(lead['created_by'],              #0
                           lead['name'],                    #1
                           lead['created_at'],              #2
                           lead['closest_task_at'],         #3
                           lead['group_id'],                #4
                           lead['responsible_user_id'],     #5
                           lead['id'],                      #6
                           lead['is_deleted'],              #7
                           lead['status_id'],               #8
                           lead['closed_at'],               #9
                           lead['pipeline_id'],             #10
                           lead['account_id'],              #11
                           lead['loss_reason_id'],          #12
                           lead['sale'],                    #13
                           lead['updated_at'],              #14
                           lead['updated_by'],              #15
                           lead['company']['name']          #16
                          ))

    save_current_timestamp_to_file()


if __name__ == '__main__':
    main()



