""" Contains all hubspot operations"""
import postgres as db

def get_contacts():
    """
    Fetch new contacts from the database.

    This function retrieves new contacts from the cloud.Users table whose "createdAt" timestamp
    is greater than the last synchronization timestamp obtained 
    from the contacts table.

    Returns:
        list: A list of new contacts, each represented as a tuple (email, name).
    """
    try:
        # Fetch the last sync timestamp from the database
        last_sync_query = 'select max(created) from contacts'
        last_sync_timestamp = db.analytics_db("GET",last_sync_query, None)

        # Fetch new contacts from cloud.Users table
        user_query = 'select email, "createdAt",name from "Users" where "createdAt" > %s   order by "createdAt" asc limit 50'
        cloud_users = db.cloud_db(user_query,last_sync_timestamp[0])
        print(last_sync_timestamp[0])
        #fetch new users from Serials
        last_sync_query = "select max(created) from serials"
        last_sync = db.analytics_db("GET",last_sync_query, None)
        unix_timestamp = int(last_sync[0][0].timestamp())
        print(last_sync[0][0])
        user_licenses = f"select email,FROM_UNIXTIME(date) as date from serials where date > '{unix_timestamp}' limit 50 "
        serial_users = db.legacy_db(user_licenses,None)
        print("SUCCESS: New contacts retrieved from Database")
    except Exception as get_exception:
        print(f"Error in Contacts (GET): {get_exception}")
    return cloud_users+serial_users



def insert_contact_ids(hubspotids):
    """
    Insert HubSpot contact IDs into the database.

    Args:
        hubspotids (list): A list of tuples containing HubSpot contact IDs, 
        email addresses, and created timestamps.

    Returns:
        None
    """
    try:
        query = 'insert into contacts ("hubspotID", email, created) values (%s, %s, %s)'
        # Iterate through the list of HubSpot contact IDs and insert them into the database
        for contact in hubspotids:
                     
            values = (int(contact[1]), contact[0], contact[2])
            db.analytics_db("UPDATE", query, values)
    except Exception as get_exception:
        print(f"Error in insertHubspotID (contacts): {get_exception}")  # Handle any exceptions
    print("SUCCESS: Contacts succesfully added to the DB")

def delete_contacts(contacts):
    """
    Delete contacts from the database.

    Args:
        contacts (list): A list of email addresses to delete.

    Returns:
        list: A list of HubSpot contact IDs corresponding to the deleted contacts.
    """
    try:
        # Delete the contacts from both the database
        contact_list = ', '.join([f"'{contact[0]}'" for contact in contacts])
        db_query = f'delete from contacts where "hubspotID" in ({contact_list})'
        db.analytics_db("DELETE", db_query, None)
    except Exception as get_exception:
        print(f"Error in deleting HubSpotIDs (GET): {get_exception}")
    return True

def get_serials():
    """
    Retrieve new serials from the database based on the last synchronization timestamp.

    Returns:
        list: A list of dictionaries representing the new serials.
    """
    try:
        print("START: Getting new serials")
        joined_list = []
        # Query to retrieve the last sync timestamp from the database
        last_sync_query = "select max(created) from serials"
        last_sync = db.analytics_db("GET",last_sync_query, None)
        # Query to retrieve new serials from the database
        serials_query = f"""
                        select
                            s.serial,
                            s.email,
                            s.date as created,
                            s.id,
                            s.maxUse,
                            s.update_expirationdate
                            from
                                serials s
                            where s.date > UNIX_TIMESTAMP('{last_sync[0][0]}')
                            order by
                                s.date asc
                            limit
                                50
        """
        new_serials = db.legacy_db(serials_query,None)
        serial_list = ', '.join([f"'{serial[1]}'" for serial in new_serials])
        if serial_list =="":
            print("DB: No New Serials")
        else:
            # Query to retrieve contact hubspot ids
            hubspotids_query = f'select email,"hubspotID" from contacts where email in ({serial_list})'
            hubspotids = db.analytics_db("GET",hubspotids_query, None)
            if hubspotids == []:
                print("NOTE: Serial contacts not yet added to hubspot")
            else:
                # Create a dictionary mapping email addresses to hubspot IDs
                hubspot_dict = {email: hubspot_id for email, hubspot_id in hubspotids}

                # Join the two lists based on email addresses
                
                for serial in new_serials:
                    email = serial[1]  # Assuming email is at index 1 in the new_serials tuples
                    hubspot_id = hubspot_dict.get(email, None)
                    if hubspot_id is not None:
                        joined_list.append((*serial, hubspot_id))  # Assuming you want to add hubspot_id to the new_serials tuples
    
    except Exception as get_exception:
        print(f"Error in Serials (GET): {get_exception}")
    return joined_list

def get_updated_serials():
    """
    Retrieve updated serials from the database based on the last synchronization timestamp.
    Run this before updating associations because they both rely on the updated column

    Returns:
        list: A list of dictionaries representing the updated serials.
    """
    print("START: Getting updated serials")
    final = []
    try:
        # Query to retrieve the minimum updated timestamp from the database
        query = "select max(created) from serials"
        last_sync = db.analytics_db( "GET",query, None)
        # Query to retrieve updated serials from the database
        query = f"""
        select
            s.serial,
            s.email,
            s.date as created,
            s.id,
            s.maxUse,
            s.update_expirationdate
        from
            serials s
            where last_updated_on > '{last_sync[0][0]}'

        """
        updated_serials = db.legacy_db( query, None)
        print("SUCCESS: Updated serials retrieved from DB")
        serial_list = ', '.join([f"'{serial[0]}'" for serial in updated_serials])
        if serial_list =="":
            print("No New Serials")
        else:
            # Query to retrieve contact hubspot ids
            hubspotids_query = f'select serial,"hubspotID" from serials where serial in ({serial_list})'
            hubspotids = db.analytics_db("GET",hubspotids_query, None)
        # Create a dictionary mapping email addresses to hubspot IDs
            hubspot_dict = {serial: hubspot_id for serial, hubspot_id in hubspotids}
            # Join the two lists based on email addresses
            for serial in updated_serials:
                serialid = serial[0]  # Assuming email is at index 1 in the new_serials tuples
                hubspot_id = hubspot_dict.get(serialid, None)
                if hubspot_id is not None:
                    final.append((*serial, hubspot_id))  # Assuming you want to add hubspot_id to the new_serials tuples


    except Exception as get_exception:
        print(f"Error in Serials (UPDATED): {get_exception}")
    return final

def insert_serial_ids(hubspotids):
    """
    Insert new HubSpot IDs for serials into the database.

    Args:
        hubspotids (list): A list of tuples containing HubSpot ID, serial, and created date.

    Returns:
        None
    """
    print("START: Inserting new serial HubSpot IDs")
    try:
        # Define the SQL query for inserting data
        query = 'insert into serials ("hubspotID", serial, created) values (%s, %s, %s)'
        # Iterate over the provided HubSpot IDs
        for serial in hubspotids:
            values = (int(serial[0]), serial[1], serial[2])
            db.analytics_db("UPDATE", query, values)
        print("SUCCESS: New serial HubSpot IDs inserted")
    except Exception as get_exception:
        print(f"Error in insertHubspotID (serials): {get_exception}")


def delete_serial_ids(serials):
    """
    Delete HubSpot IDs associated with serials from the database.

    Args:
        serials (list): A list of serials to be deleted.

    Returns:
        True and False
    """
    print("START: Deleting serials on DB")
    try:
        # Query to retrieve HubSpot IDs for the provided serials
        serial_list = ', '.join([f"{serial}" for serial in serials])
        query = f'delete from serials where "hubspotID" in ({serial_list})'
        db.analytics_db("DELETE", query, None)
        print("SUCCESS: Serial IDs deleted from the database")
    except Exception as get_exception:
        print(f"Error in Serials (DELETE): {get_exception}")
    return True

def get_memberships():
    """
    Retrieves new organization memberships from the database.

    Returns:
        list: A list of new organization memberships.
    """
    print("START: Getting new memberships")
    joined_list = []
    final_list = []
    try:
        # Query to retrieve the last created membership 
        query = "select max(created) from memberships"
        last_sync = db.analytics_db("GET",query, None)
        # Query to retrieve new memberships from the database
        query = f"""
        SELECT 
            om.id, 
            om."UserId", 
            om."OrganizationId", 
            om.role, 
            om."isPrimary", 
            om."isContributor", 
            om."createdAt", 
            u.email 
            FROM "OrganizationMemberships" om
            LEFT JOIN "Users" u on (u.id = om."UserId")
            where om."createdAt" > '{last_sync[0][0]}'
            order by om."createdAt" asc
            limit 20 
        """
        new_memberships = db.cloud_db( query, None)
        #get workspace hubspot ids
        workspaces_list = ', '.join([f"'{member[2]}'" for member in new_memberships])
        if workspaces_list =="":
            print("DB: No New Memberships")
        else:
            # Query to retrieve workspace hubspot ids
            hubspotids_query = f'select workspace,"hubspotID" from workspaces where workspace in ({workspaces_list})'
            hubspotids = db.analytics_db("GET",hubspotids_query, None)
            if hubspotids == []:
                print("NOTE: workspaces not yet added to hubspot")
            else:
                # Create a dictionary mapping workspace ids to hubspot IDs
                hubspot_dict = {workspace: hubspot_id for workspace, hubspot_id in hubspotids}
                # Join the two lists based on workspaceids
                
                for membership in new_memberships:
                    hubspot_id = hubspot_dict.get(int(membership[2]), None)
                    if hubspot_id is not None:
                        modified_membership = [*membership, hubspot_id]
                        joined_list.append(modified_membership)

            # Query to retrieve contact hubspot ids
        email_list = ', '.join([f"'{member[7]}'" for member in new_memberships])
        if email_list =="":
            print("DB: No New Memberships")
        else:
            # Query to retrieve contact hubspot ids
            hubspotids_query = f'select email,"hubspotID" from contacts where email in ({email_list})'
            hubspotids = db.analytics_db("GET",hubspotids_query, None)
            if hubspotids == []:
                print("NOTE: Membership contacts not yet added to hubspot")
            else:
                # Create a dictionary mapping email addresses to hubspot IDs
                hubspot_dict = {email: hubspot_id for email, hubspot_id in hubspotids}

                # Join the two lists based on email addresses
                for member in joined_list:
                    hubspot_id = hubspot_dict.get(member[7], None)
                    if hubspot_id is not None:
                        final_list.append((*member, hubspot_id))  # Assuming you want to add hubspot_id to the new_serials tuples
    
    except Exception as get_exception:
        print(f"Error in Serials (UPDATED): {get_exception}")
    return final_list

def get_updated_memberships():
    """
    Retrieves updated organization memberships from the database.

    Returns:
        list: A list of updated organization memberships.
    """
    print("START: Get updated memberships")
    final =[]
    try:
        # Query to retrieve the last updated memberships
        query = "select max(created) from memberships"
        last_sync = db.analytics_db("GET",query, None)
        # Query to retrieve updated memberships from the database
        query = f"""
        SELECT 
            om.id, 
            om."UserId", 
            om."OrganizationId", 
            om.role, 
            om."isPrimary", 
            om."isContributor", 
            om."createdAt", 
            u.email 
            FROM "OrganizationMemberships" om
            LEFT JOIN "Users" u on (u.id = om."UserId")
            where om.updated > '{last_sync[0][0]}'
        """
        memberships = db.cloud_db( query, None)

        memberships_list = ', '.join([f"'{member[0]}'" for member in memberships])
        if memberships_list =="":
            print("No New Memberships")
        else:
            # Query to retrieve members hubspot ids
            hubspotids_query = f'select member,"hubspotID" from memberships where member in ({memberships_list})'
            hubspotids = db.analytics_db("GET",hubspotids_query, None)
        # Create a dictionary mapping sketchids  to hubspot IDs
            hubspot_dict = {member: hubspot_id for member, hubspot_id in hubspotids}
            # Join the two lists based on sketchids
            for member in memberships:
                sketchid = member[0]  # Assuming email is at index 1 in the new_serials tuples
                hubspot_id = hubspot_dict.get(sketchid, None)
                if hubspot_id is not None:
                    final.append((*member, hubspot_id))  


    except Exception as get_exception:
        print(f"Error in Serials (UPDATED): {get_exception}")
    return final

def insert_membership_ids(hubspotids):
    """
    Insert new HubSpot IDs for memberships into the database.

    Args:
        hubspotids (list): A list of tuples containing HubSpot ID, membershipID, and created date.

    Returns:
        None
    """
    print("START: Inserting new Memberships HubSpot IDs")
    try:
        # Define the SQL query for inserting data
        query = 'insert into memberships ("hubspotID", member, created) values (%s, %s, %s)'
        # Iterate over the provided HubSpot IDs
        for membership in hubspotids:
            values = (int(membership[0]), membership[1], membership[2])
            db.analytics_db("UPDATE", query, values)
        print("SUCCESS: New Memberships HubSpot IDs inserted")
    except Exception as get_exception:
        print(f"Error in insertHubspotID (Memberships): {get_exception}")

def delete_membership_ids(memberships):
    """
    Delete HubSpot IDs associated with memberships from the database.

    Args:
        memberships (list): A list of memberships to be deleted.

    Returns:
        True and False
    """
    print("START: Deleting memberships on DB")
    try:
        # Query to retrieve HubSpot IDs for the provided memberships
        memberships_list = ', '.join([f"{membership}" for membership in memberships])
        query = f'delete from memberships where "hubspotID" in ({memberships_list})'
        db.analytics_db("DELETE", query, None)
        print("SUCCESS: Memberships IDs deleted from the database")
    except Exception as get_exception:
        print(f"Error in Memberships (DELETE): {get_exception}")
    return True

def get_workspaces():
    try:
        print("START: retrieveing workspaces from DB")
        final_workspaces = []
        # Query to retrieve the last updated memberships
        query = "select max(created) from workspaces"
        last_sync = db.analytics_db("GET",query, None)
        # Query to retrieve new memberships from the database
        query = f"""
        SELECT 
            id, 
            name,
            identifier,
            "createdAt",
            "customerId"
            FROM "Organizations" 
            where "createdAt" > '{last_sync[0][0]}'
            order by "createdAt" asc
            limit 20
        """
        workspaces = db.cloud_db( query, None)

        workspaces_list = ', '.join([f"'{workspace[4]}'" for workspace in workspaces])
        # Retrieve customer ids from paymnts DB
        query = f"""
        SELECT 
            id, 
            external_id
            FROM customers 
            where id in  ({workspaces_list})
        """
        workspace_customers = db.payments_db( query, None)

        # Create a dictionary mapping sketchids  to hubspot IDs
        customers_dict = {id: external_id for id, external_id in workspace_customers}
        # Join the two lists based on sketchids
        for workspace in workspaces:
            paymentsid = workspace[4] 
            customer_id = customers_dict.get(paymentsid, None)
            if customer_id is not None:
                final_workspaces.append((*workspace, customer_id))  
    except Exception as get_exception:
        print(f"Error in Serials (UPDATED): {get_exception}")
    return final_workspaces

def get_workspace_hubspot_id(customer):
    query = f"""select "hubspotID" from workspaces where customer = '{customer}'"""
    hubspotid = db.analytics_db("GET",query, None)
    return hubspotid


def insert_workspace_ids(hubspotids):
    """
    Insert new HubSpot IDs for Workspaces into the database.

    Args:
        hubspotids (list): A list of tuples containing HubSpot ID, SketchID, and created date.

    Returns:
        None
    """
    print("START: Inserting new Workspaces HubSpot IDs")
    try:
        # Define the SQL query for inserting data
        query = 'insert into workspaces ("hubspotID", workspace,customer, created) values (%s, %s, %s, %s)'
        # Iterate over the provided HubSpot IDs
        for workspace in hubspotids:
            values = (int(workspace[0]), workspace[1], str(workspace[2]),workspace[3])
            db.analytics_db("UPDATE", query, values)
        print("SUCCESS: New Workspace HubSpot IDs inserted")
    except Exception as get_exception:
        print(f"Error in insertHubspotID (Workspaces): {get_exception}")

def delete_workspace_ids(workspaces):
    """
    Delete HubSpot IDs associated with workspaces from the database.

    Args:
        workspaces (list): A list of workspaces to be deleted.

    Returns:
        True and False
    """
    print("START: Deleting workspaces on DB")
    try:
        # Query to retrieve HubSpot IDs for the provided memberships
        workspaces_list = ', '.join([f"{workspace}" for workspace in workspaces])
        query = f'delete from workspaces where workspace in ({workspaces_list})'
        db.analytics_db("DELETE", query, None)
        print("SUCCESS: Workspace IDs deleted from the database")
    except Exception as get_exception:
        print(f"Error in Workspaces (DELETE): {get_exception}")
 