import time

import boto3
from botocore.exceptions import ClientError
import msal
import requests
import json
import logging
import csv

def get_secret(secret_name, region_name="eu-west-3"):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    return json.loads(get_secret_value_response["SecretString"])


def put_request_one_drive(upload_url, file_path):
    access_token = get_access_token_ms()
    headers = {
        'Content-Type': 'application/octet-stream',
        'Authorization': f'Bearer {access_token}',
    }
    with open(file_path, "rb") as f:
        data = f.read()

    response = requests.put(upload_url, data=data, headers=headers)
    return response.status_code, response.text


def download_from_one_drive(file_url, save_path, message, webhook):
    access_token = get_access_token_ms()

    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    response = requests.get(file_url, headers=headers)
    if response.status_code != 200:
        raise Exception('Failed to download template.')
    with open(save_path, 'wb') as f:
        f.write(response.content)
    logging.info('Template downloaded successfully.')


def get_sharepoint_sites_id(access_token, search_string=None):
    if search_string is None:
        sites_url = f"https://graph.microsoft.com/v1.0/sites"
    else:
        sites_url = f"https://graph.microsoft.com/v1.0/sites?search='{search_string}'"

    headers = {
        'Authorization': f'Bearer {access_token}',
    }
    sites_ids = []
    response = requests.get(sites_url, headers=headers)
    if response.status_code == 200:
        sites_data = response.json().get('value', [])
        for site in sites_data:
            if site['siteCollection']['hostname'] == 'dnascriptco.sharepoint.com':
                print(site)
                sites_ids.append(site['id'])
    return sites_ids


def get_drive_id(sites_ids, access_token):
    for site_id in sites_ids:
        folder_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/"
        headers = {
            'Authorization': f'Bearer {access_token}',
        }
        response = requests.get(folder_url, headers=headers)
        try:
            for drive in response.json()['value']:
                print(drive)
            print(response.status_code)
            print(response.text)
        except:
            print(response.text)


def get_req_graph_api(endpoint, access_token):
    headers = {
        'Authorization': f'Bearer {access_token}',
    }
    response = requests.get(endpoint, headers=headers)
    print(response.status_code)
    print(response.text)
    return response


def get_access_token_ms():
    AUTH_CONFIG = get_secret('data_consolidation/auth/onedrive', 'eu-west-3')
    app = msal.ConfidentialClientApplication(
        client_id=AUTH_CONFIG['client_id'],
        client_credential=AUTH_CONFIG['client_secret'],
        authority=AUTH_CONFIG['authority']
    )
    scope = ['https://graph.microsoft.com/.default']

    token = app.acquire_token_for_client(scopes=scope)
    return token['access_token']


def get_recently_modified_files_in_drive(drive_id, folder_id, folder_name, full_path, access_token, csv_writer, root_folder=False, retry=False):
    url = f" https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{folder_id}/children"

    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    params = {
        "$select": "id,name,folder,file,lastModifiedDateTime,createdDateTime",
        # "$filter": "lastModifiedDateTime ge 2023-08-01T00:00:00Z"
    }
    if root_folder:
        start_time = time.time()
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()
        for item in data['value']:
            if item.get('folder', None) is not None:
                if root_folder:
                    print(item['name'])
                    folder_run_time = time.time() - start_time
                    start_time = folder_run_time
                    print(folder_run_time)
                get_recently_modified_files_in_drive(drive_id, item['id'], item["name"], f"{full_path}/{item['name']}", access_token, csv_writer)
            elif item.get('file', None) is not None:
                if not item["name"].endswith(".xlsm"):
                    continue
                csv_writer.writerow([item["name"],  folder_name, full_path, item["createdDateTime"], item["lastModifiedDateTime"], response.status_code])
    elif response.status_code == 401 and not retry:
        access_token = get_access_token_ms()
        get_recently_modified_files_in_drive(drive_id, folder_id, folder_name, full_path, access_token, csv_writer,
                                             root_folder=root_folder, retry=True)
    else:
        csv_writer.writerow([None, None, None, response.status_code])
        print(f"Failed to fetch data. Status code: {response.status_code}")



if __name__ == '__main__':
    access_token = get_access_token_ms()
    get_sites_ids = get_sharepoint_sites_id(access_token, 'enzymes')
    # get_drive_id(get_sites_ids, access_token)
    # synthesis_site_id = ['dnascriptco.sharepoint.com,395efd2f-1204-4e3a-95f2-b64e01e717c0,bfd1a148-c1d8-4bcf-84f1-22bdb6ba9f6a']
    # syn_op_site_id = ['dnascriptco.sharepoint.com,09a12ca9-6d32-444d-85e7-b5f04a5c2db4']
    # ip_site_id = ['dnascriptco.sharepoint.com,3d4e2530-2b7d-4a3c-b317-40677bb140fd,01b09ba2-ec6a-4917-b0d3-203dce3b2b00']
    get_drive_id(get_sites_ids, access_token)
    drive_id_syn_op = "b!qSyhCTJtTUSF57XwSlwttGWwlRCj0Y1Ilwtezv5c-Ap57b045n9WSLfOJVOVpXw5"
    # drive_id_ip = "b!MCVOPX0rPEqzF0Bne7FA_aKbsAFq7BdJsNMgPc47KwAZ8HI1PFpGTqqGd9dh4mVi"
    # endpoint = f"https://graph.microsoft.com/v1.0/sites/root/drives/{drive_id_syn_op}/root/children"
    # response = get_req_graph_api(endpoint, access_token)
    # with open("recently_modified_files.csv", mode='w', newline='') as csv_file:
    #     csv_writer = csv.writer(csv_file)
    #     csv_writer.writerow(["File Name", "Folder Name", "Full Path", "Created Datetime", "Last Modified Time", "Status Code"])
    #     get_recently_modified_files_in_drive(drive_id_syn_op, "root", "", "", access_token, csv_writer, root_folder=True)
    # with open('output.json', 'w') as file:
    #     json.dump(response.json(), file, indent=4)  # The `indent` parameter adds indentation for readability
