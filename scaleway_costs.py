from scaleway import Client
from scaleway.billing.v2beta1 import BillingV2Beta1API
from scaleway.account.v3 import AccountV3ProjectAPI
import json
import psycopg2
import datetime
import sys

SCW_ACCESS_KEY="XXXXXX"
SCW_SECRET_KEY="XXXXXX"
SCW_DEFAULT_ORGANIZATION_ID="XXXXXX"
SCW_DEFAULT_PROJECT_ID="XXXXXX"
pg_connection_dict = {
    'dbname': 'finops',
    'user': 'tech_finops',
    'password': sys.argv[1],
    'port': '5432',
    'host': 'server-soma-dev.postgres.database.azure.com'
}

def from_money_to_real_amount(money_object):
    value = str(money_object.units) + "." + str(money_object.nanos)[:3]
    return [value, str(money_object.currency_code)]

client = Client(
   access_key=SCW_ACCESS_KEY,
   secret_key=SCW_SECRET_KEY,
   default_project_id=SCW_DEFAULT_PROJECT_ID,
   default_region="fr-par",
   default_zone="fr-par-1",
)

account_api = AccountV3ProjectAPI(client)
billing_api = BillingV2Beta1API(client)
connector = psycopg2.connect(**pg_connection_dict)
cursor = connector.cursor()
today = datetime.datetime.now()
month = today.strftime("%m")
year = today.strftime("%Y")
new_ressource = False
new_service = False

projects = account_api.list_projects(organization_id=SCW_DEFAULT_ORGANIZATION_ID)
services = {}
for project in [x for x in projects.projects if x.name != "default"]:
    print(f"CONSOMMATION PROJET : {project.name}")
    consumptions = billing_api.list_consumptions(organization_id=project.organization_id, project_id=project.id).consumptions
    taxes = billing_api.list_taxes().taxes
        
    for tax in taxes:
        print(f"{tax.description} | {tax.total_tax_value} | {tax.currency}")
        
    query_cost_ressource_last_week = "INSERT INTO cost_managment.resources_costs (ressource_id, ressource_grp_name, ressource_name, source, day, month, year, cost, currency) VALUES "
    query_cost_service_name_last_week = "INSERT INTO cost_managment.services_costs (service_name, source, day, month, year, cost, currency) VALUES "
    for consumption in consumptions:
        conso_value = from_money_to_real_amount(consumption.value)
        query_check_resource = f"SELECT ressource_id FROM cost_managment.resources_costs WHERE ressource_id = '{consumption.sku}' AND month = {month} AND year = {year}"
        query_check_service = f"SELECT service_name FROM cost_managment.services_costs WHERE service_name = '{consumption.category_name}' AND month = {month} AND year = {year}"
        cursor.execute(query_check_resource)
        ressource_exists = cursor.fetchall()
        cursor.execute(query_check_service)
        service_exists = cursor.fetchall()
        if ressource_exists != []:
            updateRessourceQuery = f"UPDATE cost_managment.resources_costs SET cost = {conso_value[0]} WHERE ressource_id = '{consumption.sku}' AND month = {month} AND year = {year}"
            cursor.execute(updateRessourceQuery)
        elif service_exists != []:
            new_ressource = True
            query_cost_ressource_last_week += f"('{consumption.sku}', '{project.name}', '{consumption.resource_name}', 'Scaleway', NULL, {month}, {year}, {conso_value[0]}, '{conso_value[1]}'),"
            updateServiceQuery = f"UPDATE cost_managment.services_costs SET cost = {conso_value[0]} WHERE service_name = '{consumption.category_name}' AND month = {month} AND year = {year}"
            cursor.execute(updateServiceQuery)
        else:
            new_ressource = True
            new_service = True
            query_cost_ressource_last_week += f"('{consumption.sku}', '{project.name}', '{consumption.resource_name}', 'Scaleway', NULL, {month}, {year}, {conso_value[0]}, '{conso_value[1]}'),"
            if consumption.category_name not in services.keys():
                services[consumption.category_name] = [[float(conso_value[0])], conso_value[1]]
            else:
                services[consumption.category_name][0].append(float(conso_value[0]))
        

if new_service:
    for key, value in services.items():
        query_cost_service_name_last_week += f"('{key}', 'Scaleway', NULL, {month}, {year}, {sum(value[0])}, '{value[1]}'),"
        query_cost_service_name_last_week = query_cost_service_name_last_week[:-1] + ";"
        cursor.execute(query_cost_service_name_last_week)

if new_ressource:
    query_cost_ressource_last_week = query_cost_ressource_last_week[:-1] + ";"
    cursor.execute(query_cost_ressource_last_week)
    
connector.commit()