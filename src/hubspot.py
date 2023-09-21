import requests
from constants import *
import db as db
import datetime


def ContactsHS(action,contacts):
    if action == "POST":
        url="https://api.hubapi.com/crm/v3/objects/contacts"
        hubspotrecords = []
        for contact in contacts:
            try:
                body_params = {"properties" :{"email": contact[0],"firstname": contact[1]}}
                response = requests.post(url,json=body_params,headers=header_params )
                if response.status_code in (201,200):
                    response_data = response.json()
                    hubspotrecords = [((contact[0]),response_data['id'])] + hubspotrecords
                else:
                    print(f"Request failed with status code: {response.status_code}")
                    print(f"Error message: {response.text}")

            except requests.exceptions.RequestException as e:
                print(f"An error occurred: {str(e)}")
        print("returns a list to be added to the DB")

        return hubspotrecords
    elif action == "DELETE":
        for contact in contacts:
                try:
                    url=f"https://api.hubapi.com/crm/v3/objects/Contacts/{contact[0]}"
                    response = requests.delete(url,headers=header_params )
                    if response.status_code in (201,200,204):
                        print(url)
                    else:
                        print(f"Request failed with status code: {response.status_code}")
                        print(f"Error message: {response.text}")
            
                except requests.exceptions.RequestException as e:
                    print(f"An error occurred: {str(e)}")
        print("Contacts deleted in hubspot")
        return []






def SerialsHS(action,serials=None,payload=None):
    if action == "POST":
        url="https://api.hubapi.com/crm/v3/objects/License/"
        hubspotrecords = []
        print("adding new serials to hubspot")
        for serial in serials:
            try:
                created = int(serial[2])
                date_obj = datetime.datetime.utcfromtimestamp(created)
                iso_date_string = date_obj.strftime('%Y-%m-%d')
                iso_date_string_long = date_obj.strftime('%Y-%m-%d %H:%M:%S')

                body_params = {"properties" :{"serial": serial[0],"email": serial[1],"status": "Canceled","created": iso_date_string}}
                response = requests.post(url,json=body_params,headers=header_params )
                if response.status_code in (201,200):
                    response_data = response.json()
                    hubspotrecords = [((serial[0]),response_data['id'],iso_date_string_long,serial[6])] + hubspotrecords
                else:
                    print(f"Request failed with status code: {response.status_code}")
                    print(f"Error message: {response.text}")
        
            except requests.exceptions.RequestException as e:
                print(f"An error occurred: {str(e)}")
        print(" new serials added hubspot")
        print("returns a list to be added to the DB")
        return hubspotrecords
    elif action =="PUT":
        print("Updating serials to hubspot")
        updatedSerials = []
        print(serials)
        for serial in serials:
            try:
                values = str(serial[3])
                url=f"https://api.hubapi.com/crm/v3/objects/License/{values}"
                created = int(serial[2])
                date_obj = datetime.datetime.utcfromtimestamp(created)
                iso_date_string = date_obj.strftime('%Y-%m-%d')
                body_params = {"properties" :{"serial": serial[0],"email": serial[1],"status": "Canceled","created": iso_date_string}}
                response = requests.patch(url,json=body_params,headers=header_params )
                if response.status_code in (201,200):
                    print(url)
                    updatedSerials= updatedSerials +[serial]
                else:
                    print(f"Request failed with status code: {response.status_code}")
                    print(f"Error message: {response.text}")
        
            except requests.exceptions.RequestException as e:
                print(f"An error occurred: {str(e)}")
        return updatedSerials
    elif action =="DELETE":
        if payload is None:
            print("Deleting serials from hubspot")
            updatedSerials = []
            for serial in serials:
                try:
                    url=f"https://api.hubapi.com/crm/v3/objects/License/{serial[0]}"
                    response = requests.delete(url,headers=header_params )
                    if response.status_code in (201,200,204):
                        print(url)
                    else:
                        print(f"Request failed with status code: {response.status_code}")
                        print(f"Error message: {response.text}")
            
                except requests.exceptions.RequestException as e:
                    print(f"An error occurred: {str(e)}")
            print("Serials deleted in hubspot")
            return []
    elif action == "ASSOCIATE":
        print("associating serials with contacts")
        list = []
        print(payload)
        for x,serial,y, contact in payload:
            try:
                url = f"https://api.hubapi.com/crm/v3/objects/License/{serial}/associations/contacts/{contact}/license_to_contact"
                response = requests.put(url,headers=header_params )
                if response.status_code in (201,200):
                    list = list+[serial]
                else:
                    print(f"Request failed with status code: {response.status_code}")
                    print(f"Error message: {response.text}")
        
            except requests.exceptions.RequestException as e:
                print(f"An error occurred: {str(e)}")
        print("Serial contact association complete")

        return list




 
