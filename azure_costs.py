import requests
import sys
from requests_types import *
from azure.cli.core import get_default_cli
import json
import psycopg2

class CostManagment:
    def __init__(self, pwd) -> None:
        self.token = token
        self.start = start_date
        self.end = end_date
        pg_connection_dict = {
            'dbname': 'finops',
            'user': 'tech_finops',
            'password': pwd,
            'port': '5432',
            'host': 'server-soma-dev.postgres.database.azure.com'
        }
        self.connector = psycopg2.connect(**pg_connection_dict)
        self.cursor = self.connector.cursor()
        
    
        
    def get_url(self):
        txt="https://management.azure.com/subscriptions/1b4816b3-6221-4ef3-aa46-c1f0260426e0/providers/Microsoft.CostManagement/query?api-version=2023-11-01"
        return(txt)
    
    def get_test_url(self):
        txt="https://management.azure.com/subscriptions/1b4816b3-6221-4ef3-aa46-c1f0260426e0/resources?api-version=2021-04-01"
        return txt
    
    def request_api(self):
        cli = get_default_cli()
        cli.invoke(['account', 'get-access-token', '--resource=https://management.azure.com/', '--query', 'accessToken', '-o', 'tsv'])
        token = cli.result.result
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        url = self.get_url()      
        
        result_service_name_last_week = json.loads(requests.post(url, headers=headers, json=service_name_last_week).content)
        query_result_service_name_last_week = "INSERT INTO cost_managment.services_costs (service_name, source, day, month, year, cost, currency) VALUES "
        for result in result_service_name_last_week["properties"]["rows"]:
            query_result_service_name_last_week += f"('{result[2]}', 'Azure', {str(result[1])[6:]}, {str(result[1])[4:6]}, {str(result[1])[:4]}, {result[0]}, '{result[3]}'),"
        del result_service_name_last_week
        query_result_service_name_last_week = query_result_service_name_last_week[:-1] + ";"
        self.cursor.execute(query_result_service_name_last_week)
        
        result_ressource_last_week = json.loads(requests.post(url, headers=headers, json=ressource_last_week).content)
        query_cost_ressource_last_week = "INSERT INTO cost_managment.resources_costs (ressource_id, ressource_grp_name, ressource_name, source, day, month, year, cost, currency) VALUES "
        for result in result_ressource_last_week["properties"]["rows"]:
            query_cost_ressource_last_week += f"('{result[2]}', '{result[2].split('/')[4]}', '{result[2].split('/')[-1]}', 'Azure', {str(result[1])[6:]}, {str(result[1])[4:6]}, {str(result[1])[:4]}, {result[0]}, '{result[3]}'),"
        del result_ressource_last_week
        query_cost_ressource_last_week = query_cost_ressource_last_week[:-1] + ";"
        self.cursor.execute(query_cost_ressource_last_week)

        self.connector.commit()
        

if __name__ == "__main__":
    get_default_cli().invoke(['login', '-u', sys.argv[1], '-p', sys.argv[2]])
    billing = CostManagment(sys.argv[3])
    billing.request_api()