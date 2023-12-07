""" run workflows sequentially """
import sys
import database as db # Import your database functions module
import hubspot_api as hubspot # Import your HubSpot functions module
import stripe_api as stripe # Import your Stripe functions module

REQUEST_DELAY = 0.1  # Delay between API requests in seconds

def add_contacts_workflow():
    """
    Workflow function to add contacts to HubSpot.

    Retrieves contacts from the database, creates contacts in HubSpot,
    and handles duplicates and invalid emails.

    Returns:
        None
    """
    try:
        contacts = db.get_contacts()
        if len(contacts) == 0:
            print("No new contacts found")
        else:
            result = hubspot.create_contacts(contacts)
            if len(result[0]) > 0:
                db.insert_contact_ids(result[0])
            while len(result[1]) > 0 or len(result[2]) > 0:
                # Handling duplicate contacts and invalid emails
                result3 = db.duplicate_contacts(result[1]) if len(result[1]) > 0 else []
                db.invalid_emails(result[2]) if len(result[2]) > 0 else []
                contacts_batch = result[3]
                filtered_contacts = [contact for contact in contacts_batch 
                                     if contact[0] not in result[2]]
                result = hubspot.create_contacts(result3 + filtered_contacts)
                if len(result[0]) > 0:
                    db.insert_contact_ids(result[0])
    except Exception as e:
        print(f"An error occurred: {e}")

def update_serials_workflow():
    """
    Workflow function to update serials in HubSpot.

    Retrieves updated serials from the database and updates corresponding serials in HubSpot.

    Returns:
        None
    """
    try:
        updated_serials = db.get_updated_serials()
        if len(updated_serials) == 0:
            print("No Serials to be updated")
        else:
            hubspot.update_serials(updated_serials)
    except Exception as e:
        print(f"An error occurred: {e}")

def add_serials_workflow():
    """
    Workflow function to add new serials to HubSpot.

    Retrieves new serials from the database, creates serials in HubSpot, 
    and stores corresponding HubSpot IDs in the database.

    Returns:
        None
    """
    try:
        new_serials = db.get_serials()
        if len(new_serials[0]) == 0:
            print("No new Serials found")
        else:
            serial_hubspot_ids = hubspot.create_serials(new_serials)
            db.insert_serial_ids(serial_hubspot_ids)
    except Exception as e:
        print(f"An error occurred: {e}")

def add_workspaces_workflow():
    """
    Workflow function to add new workspaces to HubSpot.

    Retrieves new workspaces from the database, retrieves corresponding 
    subscriptions from Stripe,creates workspaces in HubSpot, associates 
    contacts and workspaces, and stores corresponding HubSpot IDs in the database.

    Returns:
        None
    """
    try:
        new_workspaces = db.get_workspaces()
        if len(new_workspaces) == 0:
            print("No new Workspaces found")
        else:
            workspaces = stripe.get_subscriptions(new_workspaces)
            workspaces_hubspot_ids = hubspot.create_workspaces(workspaces)
            workspaces_complete = db.add_contact_hubspot_id(workspaces_hubspot_ids)
            db.insert_workspace_ids(workspaces_hubspot_ids)
            hubspot.workspaces_associate(workspaces_complete)
    except Exception as e:
        print(f"An error occurred: {e}")

def delete_serials_workflow():
    """
    Workflow function to delete serials from HubSpot.

    Retrieves HubSpot IDs of serials to be deleted from the database,
    deletes corresponding serials from HubSpot, and updates the database to reflect the deletion.

    Returns:
        None
    """
    try:
        hubspot_ids = db.delete_serial_ids()
        if len(hubspot_ids) > 0:
            hubspot.delete_serials(hubspot_ids)
    except Exception as e:
        print(f"An error occurred: {e}")

def create_all_workspaces():
    """
    Workflow function to create all workspaces in HubSpot.

    Retrieves all subscriptions from Stripe, retrieves corresponding workspaces from the database,
    creates workspaces in HubSpot, associates contacts and workspaces, 
    and stores corresponding HubSpot IDs in the database.

    Returns:
        None
    """
    try:
        stripe.get_all_subscriptions()
        
    except Exception as e:
        print(f"An error occurred: {e}")

def add_memberships():
    """
    Workflow function to add memberships to HubSpot.

    Retrieves new memberships from the database, creates memberships in HubSpot, 
    and stores corresponding HubSpot IDs in the database.

    Returns:
        None
    """
    try:
        new_memberships = db.get_memberships()
        if len(new_memberships) == 0:
            print("No new Memberships found")
        else:
            hubspot.create_memberships(new_memberships)
    except Exception as e:
        print(f"An error occurred: {e}")

def delete_workpaces_and_memberships():
    """
    Workflow function to delete workspaces and memberships from HubSpot.

    Retrieves workspaces and memberships HubSpot IDs to be deleted from the database,
    deletes corresponding workspaces and memberships from HubSpot, 
    and updates the database to reflect the deletion.

    Returns:
        None
    """
    try:
        workspaces = db.delete_workspace_ids()
        if len(workspaces[0]) > 0:
            hubspot.delete_workspaces(workspaces[0])
            hubspotids = db.delete_memberships(workspaces[1])

            hubspot.delete_memberships(hubspotids)
    except Exception as e:
        print(f"An error occurred: {e}")

def update_memberships():
    """
    Workflow function to update memberships in HubSpot.

    Retrieves updated memberships from the database, updates corresponding memberships in HubSpot,
    and handles any exceptions that occur during the update process.

    Returns:
        None
    """
    try:
        updated_memberships = db.get_updated_memberships()
        if len(updated_memberships) == 0:
            print("No Memberships to be updated")
        else:
            hubspot.update_memberships(updated_memberships)
    except Exception as e:
        print(f"An error occurred: {e}")

def main():
    """
    The main function that orchestrates the execution of various workflows.

    Usage:
        python script.py 

    Commands:
        - create_all_workspaces
    """
    delete_workpaces_and_memberships()
    #delete_serials_workflow()
    add_contacts_workflow()
    update_serials_workflow()
    add_serials_workflow()
    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command == "create_all_workspaces":
            create_all_workspaces()
        else:
            print("Invalid command. Available commands: create_all_workspaces")
    else:
            # Default behavior when no arguments are provided
        print("No command specified. Default behavior.")
    add_workspaces_workflow()
    update_memberships()
    add_memberships()
if __name__ == "__main__":
    main()
