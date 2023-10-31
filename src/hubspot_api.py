""" This module contains all API funtions to Hubspot """
import os
import re
from hubspot import HubSpot
from hubspot.crm.contacts import (BatchInputSimplePublicObjectId,
                                  BatchInputSimplePublicObjectInputForCreate)
from hubspot.crm.contacts.exceptions import ApiException
from hubspot.crm.associations.v4 import BatchInputPublicDefaultAssociationMultiPost
from dotenv import load_dotenv, find_dotenv

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
    hubspot_records = []
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
        try:
            json = [{"properties": {"email": email}} for email, *_ in batch ]
            payload = BatchInputSimplePublicObjectId(json)
            api_response = api_client.crm.contacts.batch_api.create(
            batch_input_simple_public_object_input_for_create=payload)
            # Iterate over the results list and create (id, email) tuples for each item
            for n in range(len(json)):
                hubspot_id = api_response.results[n].id # Get the 'results' list from the dictionary
                email = api_response.results[n].properties.get("email", "N/A")
                created = batch[n][1]
                hubspot_records = hubspot_records+[(email,hubspot_id,created,batch[n][2])]
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
    return (hubspot_records,conflict_contacts,invalid_email_list,batch_with_invalid_contacts)


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


def create_serials(serials):
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
    hubspot_records = []
    hubspot_associations = []
    # Batch size
    batch_size = 100

    # List to store batches of contacts
    serial_batches = []
    hubspot_associations = []
    # Iterate through the contacts and create batches
    for i in range(0, len(serials), batch_size):
        batch = serials[i:i + batch_size]
        serial_batches.append(batch)
    i=0
    for batch in serial_batches:
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
                created = batch[n][7]
                hubspot_records = hubspot_records+[(hubspot_id,serial,created)]
                # Append the serial information including HubSpot ID and creation date to the list
                hubspot_associations.append(
                {"from":{"id":api_response.results[n].id},"to":{"id":batch[n][8]}}

            )
            print(f"Serials -> {(i/len(serial_batches))*100}%")
        except Exception as e:
            error = str(e)
            print(error)
            print(f"{error[1]}{error[2]}{error[3]}")
            continue
    # create associations for the newly added serials
    associations = BatchInputPublicDefaultAssociationMultiPost(inputs=hubspot_associations)
    try:
        api_response = api_client.crm.associations.v4.batch_api.create_default(
            from_object_type="License", to_object_type="Contact",
            batch_input_public_default_association_multi_post=associations)
        if api_response.status == "COMPLETE":
            print("SUCCESS: All serials associated with contacts")
        else:
            print(api_response.status)
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
            json = [ {"id": serial} for serial in batch]
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
