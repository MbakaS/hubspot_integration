""" This module contains all API funtions to Hubspot """
import datetime
from hubspot import HubSpot
from hubspot.crm.contacts import SimplePublicObjectInputForCreate,BatchInputSimplePublicObjectId
from hubspot.crm.contacts.exceptions import ApiException
from hubspot.crm.objects import SimplePublicObjectInputForCreate
from hubspot.crm.associations.v4 import BatchInputPublicDefaultAssociationMultiPost
from constants import TOKEN
from  database import SerialsDB


def create_contacts(contacts):
    """
    Create contacts in HubSpot.

    Args:
        contacts (list): A list of tuples containing contact information.
            Each tuple should have the format (email, firstname).

    Returns:
        list: A list of tuples with the original email and the contact HubSpot ID .
    """

    api_client = HubSpot()
    api_client.access_token = TOKEN
    hubspot_records = []
    for contact in contacts:
        try:
            payload = SimplePublicObjectInputForCreate(
                properties={"email": contact}
            )
            api_response = api_client.crm.contacts.basic_api.create(
                simple_public_object_input_for_create=payload
            )
            print(api_response)
            hubspot_records = [(contact,api_response.id)]
        except ApiException as error:
            print(f"Exception when creating contact: {error}\n")

    print("Returns a list to be added to the DB")
    print(hubspot_records)
    return hubspot_records

def delete_contacts(contacts):
    """
    Delete contacts in HubSpot.

    Args:
        contacts (list): A list of contact Hubspot IDs to be deleted.

    Returns:
        list: True or False.
    """
    api_client = HubSpot()
    api_client.access_token = TOKEN

    for contact in contacts:
        try:
            api_client.crm.contacts.basic_api.archive(
                contact
            )

        except ApiException as error:
            print(f"Exception when creating contact: {error}\n")

    print("Contacts deleted in HubSpot")
    return []



def delete_batch_contacts(contacts):
    """
    Delete batch contacts in HubSpot.

    Args:
        contact_ids (list): A list of contact HubSpot IDs (as integers) to be deleted.

    Returns:
        None: if the delete operation was successful, Error otherwise.
    """
    api_client = HubSpot()
    api_client.access_token = TOKEN
    try:
        payload = []
        for contact in contacts:
            id= {"id": contact}
            payload.append(id)
        # Create a BatchInputSimplePublicObjectId instance
        batch_input = BatchInputSimplePublicObjectId(inputs=payload)

        # Use the batch input to delete contacts
        api_response = api_client.crm.contacts.batch_api.archive(batch_input)
        # Check if the delete operation was successful (status code 204)
        if api_response is not None:
            # Check if the delete operation was successful (status code 204)
            if api_response.status_code == 204:
                print("Contacts deleted in HubSpot")
                return True
            else:
                print(f"Failed to delete contacts. Status code: {api_response.status_code}")
        else:
            print("Contacts Deleted")    
    except ApiException as error:
        print(f"Exception when deleting contacts: {error}\n")
    return False


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
    print("Adding new serials to HubSpot")
    api_client = HubSpot()
    api_client.access_token = TOKEN
    hubspot_records = []

    for serial in serials:
        created = int(serial[2])
        date_obj = datetime.datetime.utcfromtimestamp(created)
        iso_date_string = date_obj.strftime('%Y-%m-%d')
        iso_date_string_long = date_obj.strftime('%Y-%m-%d %H:%M:%S')

        payload = {
            "serial": serial[0],
            "email": serial[1],
            "status": "Canceled",
            "created": iso_date_string
        }

        # Create a SimplePublicObjectInputForCreate instance
        object = SimplePublicObjectInputForCreate(properties=payload)

        try:
            # Create a serial object in HubSpot
            api_response = api_client.crm.objects.basic_api.create(
                object_type="License",
                simple_public_object_input_for_create=object
            )

            # Append the serial information including HubSpot ID and creation date to the list
            hubspot_records.append(
                {"from":{"id":api_response.id},"to":{"id":25784012}}

            )

        except ApiException as error:
            print(f"Exception when creating serial: {error}\n")
    # create associations for the newly added serials
    associations = BatchInputPublicDefaultAssociationMultiPost(inputs=hubspot_records)
    try:
        api_response = api_client.crm.associations.v4.batch_api.create_default(
            from_object_type="License", to_object_type="Contact",
            batch_input_public_default_association_multi_post=associations)
        if api_response.status == "COMPLETE":
            print(" All serials associated with contacts")
        else:
            print(api_response.status)
    except ApiException as error:
        print(f"Exception when calling batch_api->Associate: {error}/n")
    print("Successfully added serials to HubSpot")
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
    print("Updating serials ")
    api_client = HubSpot()
    api_client.access_token = TOKEN
    for serial in serials:
        created = int(serial[2])
        date_obj = datetime.datetime.utcfromtimestamp(created)
        iso_date_string = date_obj.strftime('%Y-%m-%d')
        iso_date_string_long = date_obj.strftime('%Y-%m-%d %H:%M:%S')

        payload = {
            "serial": serial[0],
            "email": serial[1],
            "status": "Active",
            "created": iso_date_string
        }

        # Create a SimplePublicObjectInputForCreate instance
        update_object = SimplePublicObjectInputForCreate(properties=payload)
        try:
            # update a serial object in HubSpot
            api_client.crm.objects.basic_api.update(
                object_id=serial[3],
                object_type="License",
                simple_public_object_input=update_object
            )
        except ApiException as error:
            print(f"Exception when creating serial: {error}\n")

    print("Successfully updated serials")
    return True


def delete_serials(serials):
    """
    Delete serials in HubSpot.

    Args:
        serials (list): A list of serial HubSpot IDs to be deleted.

    Returns:
        bool: True if the delete operation was successful, False otherwise.
    """
    print("Deleting serials")
    api_client = HubSpot()
    api_client.access_token = TOKEN

    for serial in serials:
        try:
            # Delete a serial object in HubSpot
            api_response = api_client.crm.objects.basic_api.archive(
                object_id=serial,
                object_type="License"
            )
        except ApiException as error:
            print(f"Exception when deleting serial: {error}\n")

    print("Successfully deleted serials")
    return True
