""" This module contains all API funtions to Hubspot """
import datetime
import os
import stripe
from hubspot import HubSpot
from hubspot.crm.contacts import SimplePublicObjectInputForCreate,BatchInputSimplePublicObjectId,BatchInputSimplePublicObjectInputForCreate
from hubspot.crm.contacts.exceptions import ApiException
from hubspot.crm.objects import SimplePublicObjectInputForCreate, ApiException,SimplePublicObjectInput
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
    for contact in contacts:
        try:
            payload = SimplePublicObjectInputForCreate(
                properties={
                    "email": contact[0]
                            }
            )
            api_response = api_client.crm.contacts.basic_api.create(
                simple_public_object_input_for_create=payload
            )

            hubspot_records = hubspot_records+[(contact[0],api_response.id,contact[1])]
        except ApiException as error:
            print(f"Exception when creating contact: {contact[0]}")
    print("END: Contacts Added...")
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
    api_client.access_token = token

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
    api_client.access_token = token
    try:
        payload = []
        for contact in contacts:
            id= {"id": contact[0]}
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
            print("SUCCESS: Contacts Deleted ")    
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
    print("START: Adding new serials to HubSpot")
    api_client = HubSpot()
    api_client.access_token = token
    hubspot_records = []
    hubspot_associations = []
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
            hubspot_associations.append(
                {"from":{"id":api_response.id},"to":{"id":serial[6]}}

            )

            hubspot_records=hubspot_records+[(api_response.id,serial[0],iso_date_string_long)]

        except ApiException as error:
            print(f"Exception when creating serial: {error}\n")
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
                object_id=serial[6],
                object_type="License",
                simple_public_object_input=update_object
            )
        except ApiException as error:
            print(f"Exception when creating serial: {error}\n")

    print("SUCCESS: Serials update on Hubspot")
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

    for serial in serials:
        try:
            # Delete a serial object in HubSpot
            api_response = api_client.crm.objects.basic_api.archive(
                object_id=serial,
                object_type="License"
            )
        except ApiException as error:
            print(f"Exception when deleting serial: {error}\n")

    print("SUCCESS: Deleted serials on Hubspot")
    return True


def create_memberships(memberships):
    """
    Create new memberships in HubSpot CRM.

    Args:
        memberships (list): A list of membership data.

    Returns:
        list: A list of tuples containing HubSpot object IDs, 
        corresponding sketch IDs, and creation dates.
    """
    print("START: Adding new memberships to HubSpot")
    api_client = HubSpot()
    api_client.access_token = token
    hubspot_ids = []
    contacts_associations = []
    for member in memberships:
        payload = {
            "name": f"{member[7]}_{member[2]}",
            "sketchid": member[0],
            "email": member[7],
            "created": member[6].strftime('%Y-%m-%d'),
            "role": member[3],
            "editor": member[5],
        }
        # Create a SimplePublicObjectInputForCreate instance
        object = SimplePublicObjectInputForCreate(properties=payload)

        try:
            # Create a membership object in HubSpot
            api_response = api_client.crm.objects.basic_api.create(
                object_type="Memberships",
                simple_public_object_input_for_create=object
            )
        except ApiException as error:
            print(f"Exception when creating Memberships: {error}\n")
        try:
            hubspot_ids.append((api_response.id, member[1], member[6],member[7]))
            api_client.crm.associations.v4.basic_api.create(
                object_type="Workspaces", object_id=member[8], to_object_type="Memberships", to_object_id=api_response.id, 
                association_spec=[{"associationCategory":"USER_DEFINED","associationTypeId":145}])

        except ApiException as error:
            print(f"Exception when calling batch_api->Associate: {error}/n")

        # Append the serial information including HubSpot ID and creation date to the list
        contacts_associations.append(
            {"from":{"id":api_response.id},"to":{"id":member[9]}}

        )
    associations = BatchInputPublicDefaultAssociationMultiPost(inputs=contacts_associations)
    try:
        api_response = api_client.crm.associations.v4.batch_api.create_default(
            from_object_type="Memberships", to_object_type="Contact",
            batch_input_public_default_association_multi_post=associations)
        if api_response.status == "COMPLETE":
            print("SUCCESS: All serials associated with contacts")
        else:
            print(api_response.status)
    except ApiException as error:
        print(f"Exception when calling batch_api->Associate: {error}/n")
    print("SUCCESS: Memberships added to HubSpot")
    return hubspot_ids

def update_memberships(memberships):
    """
    Update memberships in HubSpot CRM.

    Args:
        memberships (list): A list of membership data.

    Returns:
        list: A list of tuples containing HubSpot object IDs, 
        corresponding sketch IDs, and creation dates.
    """
    print("START: Updating memberships to HubSpot")
    api_client = HubSpot()
    api_client.access_token = token
    for member in memberships:
        payload = {
            "name": f"{member[7]}_{member[2]}",
            "sketchid": member[1],
            "email": f"new{member[7]}",
            "created": member[6].strftime('%Y-%m-%d'),
            "role": member[3],
            "editor": str(member[5]),
        }

        # Create a SimplePublicObjectInputForCreate instance
        update_object = SimplePublicObjectInputForCreate(properties=payload)

        try:
            # Create a membership object in HubSpot
            api_client.crm.objects.basic_api.update(
                object_type="Memberships",
                object_id=member[8],
                simple_public_object_input=update_object
            )
        except ApiException as error:
            print(f"Exception when updating Memberships: {error}\n")

    return True


def delete_memberships(memberships):
    """
    Delete Memberships in HubSpot.

    Args:
        membrships (list): A list of membership HubSpot IDs to be deleted.

    Returns:
        bool: True if the delete operation was successful, False otherwise.
    """
    print("START: Deleting Memberships on Hubspot")
    api_client = HubSpot()
    api_client.access_token = token

    for member in memberships:
        try:
            # Delete a membership object in HubSpot
            api_response = api_client.crm.objects.basic_api.archive(
                object_id=member,
                object_type="Memberships"
            )
        except ApiException as error:
            print(f"Exception when deleting Membership: {error}\n")

    print("SUCCESS: Deleted Memberships on Hubspot")
    return True

def create_workspaces(workspaces):
    """
    Create workspaces in HubSpot.

    Args:
        workspaces (list): A list of tuples containing workspace information.
            Each tuple should have the format (sketchid, name, team_id, stripe_customer_id, domain, ..., timestamp).

    Returns:
        list: A list of tuples containing HubSpot object IDs, corresponding sketch IDs, and creation dates.
    """
    print("START: Adding new Workspaces to HubSpot")
    api_client = HubSpot()
    api_client.access_token = token
    hubspot_ids = []

    for workspace in workspaces:
        payload = {
            "sketchid": workspace[0],
            "name": workspace[1],
            "email": workspace[4],
            "domain": workspace[4],
            "link_to_cloud_admin": f"https://cloudadmin.sketchsrv.com/team/{workspace[2]}",
            "link_to_stripe": f"https://dashboard.stripe.com/customers/{workspace[3]}",
            "status": workspace[8],
            "renewal_date": datetime.datetime.utcfromtimestamp(workspace[15]).strftime('%Y-%m-%d'),
            "paid_seats": workspace[11],
            "plan": workspace[10],
            "created": workspace[6].strftime('%Y-%m-%d')
        }
        # Create a SimplePublicObjectInputForCreate instance
        object = SimplePublicObjectInputForCreate(properties=payload)

        try:
            # Create a workspace object in HubSpot
            api_response = api_client.crm.objects.basic_api.create(
                object_type="Workspaces",
                simple_public_object_input_for_create=object
            )
        except ApiException as error:
            print(f"Exception when creating Workspaces: {error}\n")
    
        hubspot_ids.append((api_response.id, workspace[0], workspace[3], workspace[6].strftime('%Y-%m-%d %H:%M:%S')))

    return hubspot_ids


def get_subscriptions(workspaces):
    """
    Retrieve subscriptions from Stripe for the given workspaces.

    Args:
        workspaces (list): A list of tuples containing workspace information.
            Each tuple should have the format (sketchid, name, team_id, stripe_customer_id, domain, ..., timestamp).

    Returns:
        list: A list of subscription data retrieved from Stripe.
    """
    print("START: retrieving workspaces from Stripe")
    stripe.api_key = api_key
    data = []
    for workspace in workspaces:
        customer = stripe.Customer.retrieve(workspace[5])
        subscription = stripe.Subscription.list(customer=workspace[5])
        if len(subscription["data"]) > 0:
            subscription = subscription["data"][0]

            customer_data = [customer.id, customer.email]
            id = subscription["id"],
            created = workspace[3],
            customer = subscription["customer"],
            ended_at = subscription["ended_at"],
            plan = subscription["plan"]["id"],
            interval = subscription["plan"]["interval"],
            quantity = subscription["quantity"],
            status = subscription["status"],
            trial_start = subscription["trial_start"],
            trial_end = subscription["trial_end"],
            canceled_at = subscription["ended_at"],
            current_period_end = subscription["current_period_end"]

            data.append([workspace[0], workspace[1], workspace[2], customer_data[0], customer_data[1], id[0], created[0], plan[0], status[0], ended_at[0], interval[0], quantity[0], trial_start[0], trial_end[0], canceled_at[0], current_period_end])
        else:
            print(f"{workspace[5]} Subscription not found")
    return data


def update_workspace(payload, hubspotid):
    """
    Update workspace details in HubSpot.

    Args:
        payload (dict): A dictionary containing updated workspace details.
        hubspotid (int): HubSpot object ID of the workspace to be updated.

    Returns:
        None
    """
    api_client = HubSpot()
    api_client.access_token = token
    print("START: Updating workspace details")

    # Create a SimplePublicObjectInputForCreate instance
    object = SimplePublicObjectInput(properties=payload)

    try:
        # Update the workspace object in HubSpot
        api_client.crm.objects.basic_api.update(
            object_type="Workspaces",
            object_id=hubspotid,
            simple_public_object_input=object
        )
    except ApiException as error:
        print(f"Exception when updating Workspaces: {error}\n")


def delete_workspaces(workspaces):
    """
    Delete workspaces in HubSpot.

    Args:
        workspaces (list): A list of workspace HubSpot IDs to be deleted.

    Returns:
        bool: True if the delete operation was successful, False otherwise.
    """
    print("START: Deleting workspaces on Hubspot")
    api_client = HubSpot()
    api_client.access_token = token

    for workspace in workspaces:
        try:
            # Delete a workspace object in HubSpot
            api_response = api_client.crm.objects.basic_api.archive(
                object_id=workspace,
                object_type="Workspaces"
            )
        except ApiException as error:
            print(f"Exception when deleting Workspace: {error}\n")

    print("SUCCESS: Deleted workspaces on Hubspot")
    return True
