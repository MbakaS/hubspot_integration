""" This module contains all API funtions to Hubspot """
import os
import re
import datetime
from hubspot import HubSpot
from hubspot.crm.contacts import (BatchInputSimplePublicObjectId,
                    BatchInputSimplePublicObjectInputForCreate,SimplePublicObjectInput)
from hubspot.crm.contacts.exceptions import ApiException
from hubspot.crm.associations.v4 import BatchInputPublicDefaultAssociationMultiPost
from dotenv import load_dotenv, find_dotenv
import database as db # Import your database functions module

# Load environment variables from a .env file
load_dotenv(find_dotenv())
token = os.getenv('TOKEN')
api_key = os.getenv('API_KEY')


def create_contacts(contacts):
    """
    Create contacts in HubSpot.

    Args:
        contacts (list): A list of tuples containing contact information.
            Each tuple should have the format (email, firstname).

    Returns:
        list: A list of tuples with the original email and the contact HubSpot ID .
    """
    print("START: adding contacts to hubspot...")
    api_client = HubSpot(access_token=token)
    invalid_email_list = []
    conflict_contacts = []
    batch_with_invalid_contacts = []

    # List to store batches of contacts
    contact_batches = []
    # Iterate through the contacts and create batches
    for i in range(0, len(contacts), 100):
        batch = contacts[i:i + 100]
        contact_batches.append(batch)

    # Format emails within batches
    i=0
    for batch in contact_batches:
        i=i+1
        hubspot_records = []
        try:
            json = [{"properties": {"email": email}} for email, *_ in batch ]
            payload = BatchInputSimplePublicObjectId(json)
            api_response = api_client.crm.contacts.batch_api.create(
            batch_input_simple_public_object_input_for_create=payload)
            # Iterate over the results list and create (id, email) tuples for each item
            length = len(api_response.results)
            for n in range(length):
                hubspot_id = api_response.results[n].id # Get the 'results' list from the dictionary
                email = api_response.results[n].properties.get("email", "N/A")
                created = batch[n][1]
                hubspot_records = hubspot_records+[(email,hubspot_id,created,batch[n][2])]
            db.insert_contact_ids(hubspot_records)
            print(f"Contacts -> {(i/len(contact_batches))*100}%")
        except Exception as e:
            error = str(e)
            status_code = f"{error[1]}{error[2]}{error[3]}"
            if status_code == "400":
                email_pattern = r"""Email address (.*?) is invalid"""
                # Extracting the email address
                invalid_emails = re.findall(email_pattern, error)
                invalid_email_list = invalid_email_list+invalid_emails
                batch_with_invalid_contacts = batch_with_invalid_contacts+batch
            if status_code == "409":
                conflict_contacts = conflict_contacts+batch
            print(f"Exception when creating contacts Batch {i}, ERROR: {status_code},")
            continue
    print("END: Contacts Added...")
    return ([],conflict_contacts,invalid_email_list,batch_with_invalid_contacts)

def delete_contacts(contacts):
    """
    Delete batch contacts in HubSpot.

    Args:
        contact_ids (list): A list of contact HubSpot IDs (as integers) to be deleted.

    Returns:
        None: if the delete operation was successful, Error otherwise.
    """
    print("START: Deleting Contacts .....")
    api_client = HubSpot()
    api_client.access_token = token

    # List to store batches of contacts
    contact_batches = []
    # Iterate through the contacts and create batches
    for i in range(0, len(contacts), 100):
        batch = contacts[i:i + 100]
        contact_batches.append(batch)

    # Format emails within batches
    i=0
    for batch in contact_batches:
        i=i+1
        try:
            json = [{"id": contact[0]} for contact in batch ]
            # Create a BatchInputSimplePublicObjectId instance
            batch_input = BatchInputSimplePublicObjectId(inputs=json)

            # Use the batch input to delete contacts
            api_client.crm.contacts.batch_api.archive(batch_input)
            # Check if the delete operation was successful (status code 204)
        except Exception as e:
            error = str(e)
            print(error)
            print(f"{error[1]}{error[2]}{error[3]}")
    print("SUCCESS: Contacts Deleted ")
    return True

def create_serials(payload):
    """
    Create serials in HubSpot.
    Create assciations between serials and Contacts.
    Args:
        serials (list): A list of serial information where each item is a tuple
        containing serial details like (serial_number, email, created_timestamp).

    Returns:
        list: A list of tuples with serial information including the HubSpot ID and
        the creation date.
    """
    print("START: Adding new serials to HubSpot")
    api_client = HubSpot()
    api_client.access_token = token
    # Batch size
    batch_size = 100

    # List to store batches of contacts
    serial_batches = []
    serials = payload[0]
    contacts_dict = payload[1]
    # Iterate through the contacts and create batches
    for i in range(0, len(serials), batch_size):
        batch = serials[i:i + batch_size]
        serial_batches.append(batch)
    i=0
    for batch in serial_batches:
        contact_associations = []
        hubspot_records = []
        try:
            json = [{"properties": {"serial": serial[0],"email": serial[1],
                                "status": serial[6],
                                "created": serial[2]}} for serial in batch ]
            i=i+1
            payload = BatchInputSimplePublicObjectInputForCreate(json)
            api_response = api_client.crm.objects.batch_api.create(
                    object_type="License",
            batch_input_simple_public_object_input_for_create=payload)
            # Iterate over the results list and create (id, email) tuples for each item
            for n in range(len(json)):
                hubspot_id = api_response.results[n].id # Get the 'results' list from the dictionary
                serial = api_response.results[n].properties.get("serial", "N/A")
                created = batch[n-1][7]
                hubspot_records = hubspot_records+[(hubspot_id,serial,created)]
                # Append the serial information including HubSpot ID and creation date to the list
                contact = contacts_dict.get(api_response.results[n].properties.get("email", "N/A")
                                            , None)
                contact_associations = contact_associations +[
                    {"from":{"id":hubspot_id},"to":{"id":str(contact)}}]
            print(f"Serials -> {(i/len(serial_batches))*100}%")
        except Exception as e:
            error = str(e)
            print(error)
            print(f"{error[1]}{error[2]}{error[3]}")
            continue

        db.insert_serial_ids(hubspot_records)

        # create associations for the newly added serials
        associations = BatchInputPublicDefaultAssociationMultiPost(inputs=contact_associations)
        try:
            api_response = api_client.crm.associations.v4.batch_api.create_default(
                from_object_type="License", to_object_type="Contact",
                batch_input_public_default_association_multi_post=associations)
            if api_response.status != "COMPLETE":
                print("Error: Assoiciating contacts")
        except ApiException as error:
            print(f"Exception when calling batch_api->Associate: {error}/n")
    print("SUCCESS: Serials added to HubSpot")
    return hubspot_records

def update_serials(serials):
    """
    Update serials in HubSpot.

    Args:
        serials (list): A list of serial information where each item is a tuple
        containing serial details like (serial_number, email, created_timestamp).

    Returns:
        True
    """
    print("START: Updating serials to hubspot")
    api_client = HubSpot()
    api_client.access_token = token
    # Batch size
    batch_size = 100

    # List to store batches of contacts
    serial_batches = []
    # Iterate through the contacts and create batches
    for i in range(0, len(serials), batch_size):
        batch = serials[i:i + batch_size]
        serial_batches.append(batch)

    i=0
    for batch in serial_batches:
        try:
            json = [{"id":serial[8],"properties": {"serial": serial[0],"email": serial[1],
                                "status": serial[6],
                                "created": serial[2]}} for serial in batch ]
            payload = BatchInputSimplePublicObjectId(inputs=json)
            api_client.crm.objects.batch_api.update(
                 object_type="License",
            batch_input_simple_public_object_batch_input=payload)
            i=i+1
            print(f"Serials -> {(i/len(serial_batches))*100}%")
        except Exception as e:
            error = str(e)
            print(error)
            print(f"{error[1]}{error[2]}{error[3]}")
            continue
    print("SUCCESS: Serials updated on Hubspot")
    return True

def delete_serials(serials):
    """
    Delete serials in HubSpot.

    Args:
        serials (list): A list of serial HubSpot IDs to be deleted.

    Returns:
        bool: True if the delete operation was successful, False otherwise.
    """
    print("START: Deleting serials on Hubspot")
    api_client = HubSpot()
    api_client.access_token = token
    # Batch size
    batch_size = 100

    # List to store batches of serials
    serial_batches = []

    # Iterate through the serials and create batches
    for i in range(0, len(serials), batch_size):
        batch = serials[i:i + batch_size]
        serial_batches.append(batch)

    # Format emails within batches
    i=0
    for batch in serial_batches:
        try:
            json = [ {"id": serial[0]} for serial in batch]
            payload = BatchInputSimplePublicObjectId(inputs=json)
        # Delete a serial object in HubSpot
            api_client.crm.objects.batch_api.archive(
                batch_input_simple_public_object_id=payload,
                object_type="License"
            )
        except Exception as e:
            error = str(e)
            print(error)
            continue  
    print("SUCCESS: Batch Serials deleted from the database")
    return True

def create_workspaces(workspaces):
    """
    Create workspaces in HubSpot CRM.

    Args:
        workspaces (list): List of workspace data.

    Returns:
        list: List of tuples containing HubSpot ID, SketchID, workspace name, and email.
    """
    print("START: Adding Workspaces to HubSpot...")
    api_client = HubSpot(access_token=token)
    batch_size = 100
    workspace_batches = []
    error_batch = []
    hubspot_records = []

    for i in range(0, len(workspaces), batch_size):
        batch = workspaces[i:i + batch_size]
        workspace_batches.append(batch)

    i = 0
    for batch in workspace_batches:
        i += 1
        try:
            # Prepare JSON payload for workspace creation
            json = [{
                "properties": {
                    "sketchid": int(workspace[0]),
                    "name": workspace[1],
                    "email": workspace[4],
                    "domain": workspace[4],
                    "link_to_cloud_admin": f"https://cloudadmin.sketchsrv.com/team/{workspace[2]}",
                    "link_to_stripe": f"https://dashboard.stripe.com/customers/{workspace[3]}",
                    "status": workspace[8],
                    "renewal_date": datetime.datetime.utcfromtimestamp(int(
                                                            workspace[14])).strftime('%Y-%m-%d'),
                    "paid_seats": int(workspace[11]),
                    "plan": workspace[7],
                    "created": datetime.datetime.utcfromtimestamp(int(
                                                            workspace[6])).strftime('%Y-%m-%d')
                }
            } for workspace in batch]
            payload = BatchInputSimplePublicObjectInputForCreate(json)
            api_response = api_client.crm.objects.batch_api.create(
                object_type="Workspaces",
                batch_input_simple_public_object_input_for_create=payload
            )

            # Extract HubSpot IDs and workspace details from the response
            length = len(api_response.results)
            for n in range(length):
                hubspot_id = api_response.results[n].id
                workspace = api_response.results[n].properties.get("sketchid", "N/A")
                email = api_response.results[n].properties.get("email", "N/A")
                customer_id = batch[n][3]
                hubspot_records.append((hubspot_id, workspace, str(batch[n][15]),
                                        email,customer_id))
            print(f"Workspaces -> {(i/len(workspace_batches))*100}%")

        except Exception as e:
            error = str(e)
            print(f"Exception when creating Workspace batch {i}")
            print(error)
            error_batch.append(batch)
            continue

    print("Success: Workspaces Added...")
    return hubspot_records

def workspaces_associate(workspaces):
    """
    Associate workspaces with contacts in HubSpot CRM.

    Args:
        workspaces (list): List of workspace data.

    Returns:
        bool: True if associations are successful, False otherwise.
    """
    api_client = HubSpot(access_token=token)
    batch_size = 1000
    workspace_batches = []

    for i in range(0, len(workspaces), batch_size):
        batch = workspaces[i:i + batch_size]
        workspace_batches.append(batch)

    for batch in workspace_batches:
        try:
            # Prepare HubSpot associations for workspaces and contacts
            hubspot_associations = [{
                "from": {"id": workspace[0]},
                "to": {"id": workspace[5]}
            } for workspace in batch]
            # Create associations for the newly added workspaces
            associations = BatchInputPublicDefaultAssociationMultiPost(inputs=hubspot_associations)
            api_response = api_client.crm.associations.v4.batch_api.create_default(
                from_object_type="Workspaces",
                to_object_type="Contact",
                batch_input_public_default_association_multi_post=associations
            )

            if api_response.status != "COMPLETE":
                print("Error: creating workspace Associations")
            else:
                print(api_response.status)

        except ApiException as error:
            print(f"Exception when calling batch_api->Associate: {error}\n")
            print(f"Workspaces -> {(i / len(workspace_batches)) * 100}%")

    return True

def update_workspace(payload, hubspotid):
    """
    Update workspace details in HubSpot CRM.

    Args:
        payload (dict): A dictionary containing updated workspace details.
        hubspotid (int): HubSpot object ID of the workspace to be updated.

    Returns:
        None
    """
    api_client = HubSpot()
    api_client.access_token = token
    # Create a SimplePublicObjectInput instance
    object_input = SimplePublicObjectInput(properties=payload)

    try:
        # Update the workspace object in HubSpot
        api_client.crm.objects.basic_api.update(
            object_type="Workspaces",
            object_id=hubspotid,
            simple_public_object_input=object_input
        )

    except ApiException as error:
        print(f"Exception when updating Workspaces: {error}\n")

def delete_workspaces(payload):
    """
    Delete workspaces in HubSpot CRM.

    Args:
        payload (list): A list of workspace HubSpot IDs to be deleted.

    Returns:
        bool: True if the delete operation was successful, False otherwise.
    """
    print("START: Deleting workspaces on HubSpot")

    api_client = HubSpot()
    api_client.access_token = token
    workspaces = payload[0]
    batch_size = 100
    workspace_batches = []

    for i in range(0, len(workspaces), batch_size):
        batch = workspaces[i:i + batch_size]
        workspace_batches.append(batch)

    i = 0
    for batch in workspace_batches:
        try:
            # Prepare JSON payload for workspace deletion
            json = [{"id": workspace[0]} for workspace in batch]
            payload = BatchInputSimplePublicObjectId(inputs=json)

            # Delete workspace objects in HubSpot
            api_client.crm.objects.batch_api.archive(
                batch_input_simple_public_object_id=payload,
                object_type="Workspaces"
            )
        except Exception as e:
            error = str(e)
            print(error)
            continue

    print("SUCCESS: Deleted workspaces on HubSpot")
    return True

def create_memberships(payload):
    """
    Create new memberships in HubSpot CRM.

    Args:
        payload (tuple): A tuple containing a list of membership 
        data and dictionaries for contacts and workspaces mapping.

    Returns:
        list: A list of tuples containing HubSpot object IDs, corresponding sketch IDs, 
        and creation dates.
    """
    print("START: Adding new memberships to HubSpot")
    api_client = HubSpot(access_token=token)
    api_client.access_token = token
    batch_size = 100
    membership_batches = []
    error_batch = []
    memberships = payload[0]
    contacts_dict = payload[1]
    workspaces_dict = payload[2]
    for i in range(0, len(memberships), batch_size):
        batch = memberships[i:i + batch_size]
        membership_batches.append(batch)

    i = 0
    for batch in membership_batches:
        hubspot_records = []
        workspaces_associations = []
        contacts_associations = []
        try:
            json = [{"properties": {
                        "name": f"{membership[7]}_{membership[2]}",
                        "sketchid": membership[0],
                        "email": membership[7],
                        "created": membership[6].strftime('%Y-%m-%d'),
                        "role": membership[3],
                        "editor": membership[5],
                    }} for membership in batch]
            payload = BatchInputSimplePublicObjectInputForCreate(json)
            api_response = api_client.crm.objects.batch_api.create(
                object_type="Memberships",
                batch_input_simple_public_object_input_for_create=payload
            )

            i += 1
            response_length = api_response.results
            for n in range(len(response_length)):
                hubspot_id = api_response.results[n].id
                membership_id = api_response.results[n].properties.get("sketchid", "N/A")
                hubspot_records.append((hubspot_id, membership_id, str(batch[n - 1][6])))
                email = api_response.results[n].properties.get("email", "N/A")
                contact = contacts_dict.get(email, None)   
                name = (api_response.results[n].properties.get("name", "N/A")).split('_')
                organization = workspaces_dict.get(int(name[-1]))

                workspaces_associations.append({"from": {"id": hubspot_id}, 
                                                "to": {"id": str(organization)}})
                contacts_associations.append({"from": {"id": hubspot_id}, 
                                              "to": {"id": str(contact)}})

            # Create associations for the newly added memberships
            associations = BatchInputPublicDefaultAssociationMultiPost(inputs=contacts_associations)
            api_client.crm.associations.v4.batch_api.create_default(
                from_object_type="Memberships", to_object_type="Contact",
                batch_input_public_default_association_multi_post=associations
            )

            associations = BatchInputPublicDefaultAssociationMultiPost(
                inputs=workspaces_associations)
            api_client.crm.associations.v4.batch_api.create_default(
                from_object_type="Memberships", to_object_type="Workspaces",
                batch_input_public_default_association_multi_post=associations
            )

            db.insert_membership_ids(hubspot_records)
            print(f"Memberships -> {(i / len(membership_batches)) * 100}%")

        except Exception as e:
            error = str(e)
            print(f"Exception when creating Membership batch {i}")
            print(error)
            print(f"{error[1]}{error[2]}{error[3]}")
            error_batch.append(batch)
            continue

    print("SUCCESS: Memberships added to HubSpot")
    return True

def update_memberships(payload):
    """
    Update memberships in HubSpot CRM.

    Args:
        payload (list): A list of membership information where each item is a tuple
        containing membership details like (sketchid, email, created_timestamp).

    Returns:
        bool: True if the update operation was successful, False otherwise.
    """
    print("START: Updating memberships on Hubspot")
    api_client = HubSpot(access_token=token)
    api_client.access_token = token
    batch_size = 100
    memberships_batches = []

    for i in range(0, len(payload), batch_size):
        batch = payload[i:i + batch_size]
        memberships_batches.append(batch)

    i = 0
    for batch in memberships_batches:
        try:
            json = [{"id":membership[8],"properties": {
                        "name": f"{membership[7]}_{membership[2]}",
                        "sketchid": membership[0],
                        "email": membership[7],
                        "created": membership[6].strftime('%Y-%m-%d'),
                        "role": membership[3],
                        "editor": membership[5],
                    }} for membership in batch]
            payload = BatchInputSimplePublicObjectId(inputs=json)
            api_client.crm.objects.batch_api.update(
                 object_type="Memberships",
                 batch_input_simple_public_object_batch_input=payload
            )
            i += 1
            print(f"Memberships -> {(i / len(memberships_batches)) * 100}%")
        except Exception as e:
            error = str(e)
            print(error)
            print(f"{error[1]}{error[2]}{error[3]}")
            continue

    print("SUCCESS: Memberships updated on Hubspot")
    return True

def delete_memberships(payload):
    """
    Delete memberships in HubSpot CRM.

    Args:
        payload (list): A list of membership HubSpot IDs to be deleted.

    Returns:
        bool: True if the delete operation was successful, False otherwise.
    """
    print("START: Deleting memberships on Hubspot")

    api_client = HubSpot()
    api_client.access_token = token
    workspaces = payload[0]
    # Batch size
    batch_size = 100

    # List to store batches of serials
    workspace_batches = []

    # Iterate through the serials and create batches
    for i in range(0, len(workspaces), batch_size):
        batch = workspaces[i:i + batch_size]
        workspace_batches.append(batch)
    i=0
    for batch in workspace_batches:
        try:
            json = [ {"id": workspace[0]} for workspace in batch]
            payload = BatchInputSimplePublicObjectId(inputs=json)
        # Delete a serial object in HubSpot
            api_client.crm.objects.batch_api.archive(
                batch_input_simple_public_object_id=payload,
                object_type="Workspaces"
            )
        except Exception as e:
            error = str(e)
            print(error)
            continue
    print("SUCCESS: Deleted workspaces on Hubspot")
    return True
