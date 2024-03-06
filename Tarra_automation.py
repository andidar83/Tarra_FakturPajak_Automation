# -*- coding: utf-8 -*-
"""
Created on Fri Feb 16 10:37:39 2024

@author: NXP
"""

import time
import pandas as pd
from bs4 import BeautifulSoup
import requests
import json
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
import os
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
from tqdm import tqdm
import re
import zipfile
import shutil

base_url = "https://ninjaxpress.tarra.pajakku.com/"
endpoint = "api/v1/sign-in"  # Corrected endpoint for sign-in

# Payload data
payload = {
    "username": "id-fat-tax@ninjavan.co",
    "password": "your_password",
    "rememberMe": "true"
}


# Making the request
response = requests.post(base_url + endpoint, json=payload)

# Check the response
if response.status_code == 200:
    print("Login successful!")
    id_token = response.json().get('id_token')
    print("id_token = ",id_token)
    # Further processing of response if needed
else:
    print("Login failed. Status code:", response.status_code)





##################################################################################
headers = {
    "Authorization": "Bearer "+ str(id_token),
    "Host" : "ninjaxpress.tarra.pajakku.com"
}

response2 = requests.get("https://ninjaxpress.tarra.pajakku.com/api/v1/wps-mine", headers=headers)

# Check the response
if response2.status_code == 200:
    print(" ")
    print("get wajib pajak success")
    wp_id = response2.json()[0].get('id')
    print("wp_id = ",wp_id)
    # Further processing of response if needed
else:
    print(" ")
    print("get wajib pajak failed", response2.status_code)


###################################################################################
from datetime import datetime, timedelta

# Get current date and time
curr_datetime = datetime.now()

# Adjust start date by subtracting one day
start_datetime = curr_datetime - timedelta(days=1)
start_time = "18:01:00"  # Example: Keep the time at 18:01:00
start_datetime = datetime.combine(start_datetime, datetime.strptime(start_time, "%H:%M:%S").time())

# Adjust end date by setting it to the current date
end_datetime = curr_datetime
end_time = "18:00:00"  # Example: Keep the time at 18:00:00
end_datetime = datetime.combine(end_datetime, datetime.strptime(end_time, "%H:%M:%S").time())

# Subtract 7 hours from start and end datetimes
start_datetime -= timedelta(hours=7)
end_datetime -= timedelta(hours=7)

# Format start and end datetimes as strings
start_str = start_datetime.strftime('%Y-%m-%dT%H:%M:%S.000Z')
end_str = end_datetime.strftime('%Y-%m-%dT%H:%M:%S.000Z')

print("Adjusted Start Date:", start_str)
print("Adjusted End Date:", end_str)


payload2 = {
    'size': 10000,
    'type': 'FakturPageSearchDate',
    'contentKdTransaksi.in': '01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,-',
    'statusApprove.in': 'Belum Approve,Approval Sukses,Reject,Siap Approve,Siap Batal,Batal Sukses',
    'statusData.in': 'Normal,Normal-Pengganti,Diganti,Batal',
    'contentMasa.in': '1,2,3,4,5,6,7,8,9,10,11,12',
    'efakturUploadTanggalApproval.greaterThanOrEqual': start_str,
    'efakturUploadTanggalApproval.lessThanOrEqual': end_str,
    'sort': 'efakturUploadTanggalApproval,ASC',
    'sortBy': 'efakturUploadTanggalApproval'
}

headers2 = {
    "Authorization": "Bearer " + str(id_token)
}

response3 = requests.get("https://ninjaxpress.tarra.pajakku.com/api/v1/wps/" + str(wp_id) + "/faktur-keluaran-archives", params=payload2, headers=headers2)
# Check the response
if response3.status_code == 200:
    print(" ")
    print("get faktur pajak success")

    # Further processing of response if needed
else:
    print(" ")
    print("get faktur pajak failed", response3.status_code)

# dictfaktur = response3.json()[0]

df_faktur_approved = pd.DataFrame(response3.json())
print(df_faktur_approved.info())

df_efaktur_upload = pd.DataFrame(df_faktur_approved['efakturUpload'].tolist())
df_nofaktur = pd.DataFrame(df_faktur_approved['content'].tolist())
# print(df_nofaktur.info())
# Extract and convert the 'tanggalApproval' datetime string to datetime object
df_efaktur_upload['tanggalApproval'] = pd.to_datetime(df_efaktur_upload['tanggalApproval'])


# Merge the new DataFrame with the original DataFrame horizontally
df_faktur_approved = df_faktur_approved.join(df_efaktur_upload['tanggalApproval'])
df_faktur_approved = df_faktur_approved.join(df_nofaktur['nomorFaktur']) 
df_faktur_approved['tanggalApproval'] = df_faktur_approved['tanggalApproval'].dt.tz_localize(None)

# df_faktur_approved['nomorFaktur'] =df_faktur_approved['nomorFaktur'].astype(int)
# Write the DataFrame to Excel

# Drop the original "efakturUpload" column
# df_faktur_approved.to_excel('faktur_test.xlsx')

###################################################################################

filtered_df = df_faktur_approved
print(" ")
print('List id faktur within date range...')
print(len(filtered_df['id'].tolist()))
print(filtered_df['id'].tolist())


payload3 = {
    "branchId": [],
    "customOnly": True,
    "formatEfaktur": False,
    "ids": filtered_df['id'].tolist(),
    "masa": ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"],
    "note": "-",
    "pembetulan": 0,
    "searchSort": {"sortBy": "contentNomorFaktur", "direction": "desc"},
    "tahun": 2024,
    "type": "ExportSearchUUID",
    "withDetail": False,
    "wpId": wp_id,
    "wrap": "zip"
}


headers3 = {
    "Authorization": "Bearer " + str(id_token),
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Origin": "https://ninjaxpress.tarra.pajakku.com",
    "Content-Type": "application/json"
}


response4 = requests.post("https://ninjaxpress.tarra.pajakku.com/api/v1/wps/" + str(wp_id) + "/faktur-keluaran-archives-export", json=payload3, headers=headers3)


# Check the response
if response4.status_code == 200:
    print(" ")
    print("export pdf success")
    id_download = response4.json().get('id')
    print("id_download = ",id_download)
    # Further processing of response if needed
else:
    print(" ")
    print("export pdf failed", response4.status_code)
    print(response4.text)


######################################################################################

headers4 = {
    "Authorization": "Bearer " + str(id_token)
}

# Specify the URL for download
url = "https://ninjaxpress.tarra.pajakku.com/api/v1/wps/" + str(wp_id) + "/log-exports/"+str(id_download)+"/download"

# Set up variables for retry
max_retries = 3  # Maximum number of retry attempts
retry_count = 0  # Initialize retry counter

while True:
    try:
        # Make the request and get the response
        response = requests.get(url, headers=headers4, stream=True)

        # Check if the request was successful
        if response.status_code == 200:
            # Specify the path where you want to save the downloaded file
            filename = "downloaded_file.zip"
            
            # Open the file in binary write mode and write the content of the response to it
            with open(filename, "wb") as f:
                f.write(response.content)
            
            print("File downloaded successfully")
            break  # Exit the loop if download is successful
        elif response.status_code == 400 and "Proses Export Data sedang diproses" in response.text:
            # If the export process is still in progress, retry the download
            print("Export process is still in progress. Retrying...")
            retry_count += 1
            if retry_count >= max_retries:
                print("Max retry attempts reached. Exiting...")
                break  # Exit the loop if max retry attempts reached
            time.sleep(10)  # Wait for 10 seconds before retrying
        else:
            print("Failed to download the file. Status code:", response.status_code)
            print("Response content:", response.content)
            print("Response headers:", response.headers)
            break  # Exit the loop if request fails for other reasons
    except Exception as e:
        print("An error occurred:", e)
        break  # Exit the loop if an error occurs

#######################################################################################
print(" ")
print("Unzipping files...")

# Define paths
zip_file_path = r"C:\Users\NXP\Desktop\TARRA FIN\downloaded_file.zip"
extracted_folder_path = r"C:\Users\NXP\Desktop\TARRA FIN\extracted_faktur"


# Delete the existing extracted folder (if it exists)
if os.path.exists(extracted_folder_path):
    print('extracted folder exist')
    shutil.rmtree(extracted_folder_path)

# Extract PDF files from the downloaded zip file
with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
    zip_ref.extractall(extracted_folder_path)
print("unziping success")

########################################################################################
#RENAME FILES

print("Renaming_files...")


# Define the folder path containing the PDF files
folder_path = r'C:\Users\NXP\Desktop\TARRA FIN\extracted_faktur'

# Define the regular expression pattern to extract the desired part of the filename
# Iterate through the files in the folder
for filename in os.listdir(folder_path):
    if filename.endswith('.pdf'):
        print(f'Processing file: {filename}')  # Print out the filename for debugging
        # Split the filename by underscores
        parts = filename.split('_')
        if len(parts) >= 5:
            desired_name = parts[4] + '.pdf'
            # Check if the desired name is already present
            if desired_name != filename:
                try:
                    # Rename the file
                    os.rename(os.path.join(folder_path, filename), os.path.join(folder_path, desired_name))
                    print(f'Renamed {filename} to {desired_name}')
                except Exception as e:
                    print(f'Error renaming {filename}: {e}')  # Print out the error message
            else:
                print(f'Skipped {filename} as it already has the desired name format')
        else:
            print(f'Could not rename {filename}: Insufficient segments separated by underscores')


#########################################################################################
# #Upload to Gdrive
# print(" ")
# print("uploading to drive...")


# # Get the current date
# current_date = datetime.now().strftime("%Y-%m-%d")

# # Specify the parent folder ID (replace 'Faktur Pajak Folder ID' with the actual ID)
# parent_folder_id = 'your_parent_folder_id'

# # Authenticate with Google Drive API
# service_account_json_key = 'serv_acc.json'
# scope = ['https://www.googleapis.com/auth/drive']
# credentials = service_account.Credentials.from_service_account_file(
#     filename=service_account_json_key, 
#     scopes=scope
# )
# service = build('drive', 'v3', credentials=credentials)

# # Function to upload files to Google Drive folder with progress bar
# def upload_files_to_folder_with_progress(folder_id, file_paths):
#     for file_path in tqdm(file_paths, desc="Uploading files", unit="file"):
#         file_metadata = {
#             'name': os.path.basename(file_path),
#             'parents': [folder_id]
#         }
#         media = MediaFileUpload(file_path, resumable=True)
#         file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
#         tqdm.write(f"File '{os.path.basename(file_path)}' uploaded with ID: {file.get('id')}")

# # Get the list of PDF files in a directory
# pdf_files_dir = r'C:\Users\NXP\Desktop\TARRA FIN\extracted_faktur'
# pdf_files = [os.path.join(pdf_files_dir, f) for f in os.listdir(pdf_files_dir) if f.endswith('.pdf')]

# # Check if the folder already exists
# folder_search_query = f"name='{current_date}' and parents='{parent_folder_id}' and mimeType='application/vnd.google-apps.folder'"
# try:
#     existing_folders = service.files().list(q=folder_search_query, fields='files(id)').execute()
#     existing_folder_id = existing_folders.get('files', [])[0]['id'] if existing_folders.get('files') else None

#     # If the folder does not exist, create it and upload PDF files
#     if not existing_folder_id:
#         folder_metadata = {
#             'name': current_date,
#             'mimeType': 'application/vnd.google-apps.folder',
#             'parents': [parent_folder_id]
#         }
#         new_folder = service.files().create(body=folder_metadata, fields='id').execute()
#         tqdm.write(f"Folder created with name: {current_date}, ID: {new_folder.get('id')} inside 'Faktur Pajak' folder")
#         upload_files_to_folder_with_progress(new_folder.get('id'), pdf_files)
#     else:
#         tqdm.write(f"Folder with name '{current_date}' already exists inside 'Faktur Pajak' folder")
        
#         # Clear existing files in the folder
#         existing_files_query = f"'{existing_folder_id}' in parents"
#         existing_files = service.files().list(q=existing_files_query, fields='files(id)').execute()
#         existing_file_ids = [file['id'] for file in existing_files.get('files', [])]
#         for file_id in existing_file_ids:
#             service.files().delete(fileId=file_id).execute()
#         tqdm.write("Existing files cleared from the folder.")
        
#         # Upload PDF files to the existing folder
#         upload_files_to_folder_with_progress(existing_folder_id, pdf_files)
# except HttpError as e:
#     tqdm.write(f"An error occurred: {e}")


