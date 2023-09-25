""" Contains all hubspot operations"""
import datetime
import postgres as db


def get_contacts():
    """
    Fetch new contacts from the database.

    This function retrieves new contacts from the cloud.Users table whose "createdAt" timestamp
    is greater than the last synchronization timestamp obtained 
    from the hubspot_integration.contacts table.

    Returns:
        list: A list of new contacts, each represented as a tuple (email, name).
    """
    try:
        # Fetch the last sync timestamp from the database
        last_sync_query = 'select max(created) from hubspot_integration.contacts'
        last_sync_timestamp = db.conn("GET", last_sync_query, None)
        value = last_sync_timestamp[0]

        # Fetch new contacts from cloud.Users table
        user_query = 'select email, name from cloud."Users" where "createdAt" > %s and email is not null order by "createdAt" asc limit 10'
        new_contacts = db.conn("GET", user_query, value)
    except Exception as get_exception:
        print(f"Error in Contacts (GET): {get_exception}")
    return new_contacts


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
        query = 'insert into hubspot_integration.contacts ("hubspotID", email, created) values (%s, %s, %s)'
        # Iterate through the list of HubSpot contact IDs and insert them into the database
        for contact in hubspotids:
            current_time = str(datetime.datetime.now())
            values = (contact[0], contact[1], current_time)
            db.conn("UPDATE", query, values)
    except Exception as get_exception:
        print(f"Error in insertHubspotID (contacts): {get_exception}")  # Handle any exceptions


def delete_contacts(contacts):
    """
    Delete contacts from the database.

    Args:
        contacts (list): A list of email addresses to delete.

    Returns:
        list: A list of HubSpot contact IDs corresponding to the deleted contacts.
    """
    try:
        # Query the database to retrieve HubSpot IDs for the given contacts
        query = 'select "hubspotID" from hubspot_integration.contacts where email = ANY(%s)'
        contact_ids = db.conn("GET", query, (contacts,))
        # Delete the contacts from both the database
        for contact in contacts:
            db_query = 'delete from hubspot_integration.contacts where email = %s'
            print(contact)
            db.conn("DELETE", db_query, (contact,))
    except Exception as get_exception:
        print(f"Error in deleting HubSpotIDs (GET): {get_exception}")
    return contact_ids

def get_serials():
    """
    Retrieve new serials from the database based on the last synchronization timestamp.

    Returns:
        list: A list of dictionaries representing the new serials.
    """
    try:
        print("Getting new serials")
        # Query to retrieve the last sync timestamp from the database
        last_sync_query = "select max(created) from hubspot_integration.serials"
        last_sync = db.conn("GET", last_sync_query, None)
        # Query to retrieve new serials from the database
        serials_query = """
        select
            s.serial,
            s.email,
            s.date as created,
            s.id,
            s."maxUse",
            to_timestamp(s.update_expirationdate::double precision),
            contacts."hubspotID"
        from
            licensing.serials s
        left join
            hubspot_integration.contacts
        on
            s.email = contacts.email
        where
            to_timestamp(date::double precision)::date > %s
            and contacts.email is not null
        order by
            s.date asc
        limit
            30
        """
        new_serials = db.conn("GET", serials_query, last_sync)
        print("New serials retrieved from DB")
    except Exception as get_exception:
        print(f"Error in Serials (GET): {get_exception}")
    return new_serials

def get_updated_serials():
    """
    Retrieve updated serials from the database based on the last synchronization timestamp.
    Run this before updating associations because they both rely on the updated column

    Returns:
        list: A list of dictionaries representing the updated serials.
    """
    print("Getting updated serials")
    try:
        # Query to retrieve the minimum updated timestamp from the database
        query = "select min(updated) from hubspot_integration.serials"
        last_sync = db.conn("GET", query, None)
        # Query to retrieve updated serials from the database
        query = """
        select
            s.serial,
            s.email,
            s.date as created,
            hb."hubspotID",
            s.id,
            s."maxUse",
            to_timestamp(s.update_expirationdate::double precision)
        from
            licensing.serials s
        left join
            hubspot_integration.serials hb
        on
            s.serial = hb.serial
        where
            hb."hubspotID" is not null
            and s.last_updated_on > %s
        limit
            10
        """
        updated_serials = db.conn("GET", query, last_sync)
        print("Updated serials retrieved from DB")
    except Exception as get_exception:
        print(f"Error in Serials (UPDATED): {get_exception}")
    return updated_serials

def insert_serial_ids(hubspotids):
    """
    Insert new HubSpot IDs for serials into the database.

    Args:
        hubspotids (list): A list of tuples containing HubSpot ID, serial, and created date.

    Returns:
        None
    """
    print("Inserting new serial HubSpot IDs")
    try:
        # Define the SQL query for inserting data
        query = 'insert into hubspot_integration.serials ("hubspotID", serial, created) values (%s, %s, %s)'
        # Iterate over the provided HubSpot IDs
        for serial in hubspotids:
            values = (int(serial[0]), serial[1], serial[2])
            db.conn("UPDATE", query, values)
        print("New serial HubSpot IDs inserted")
    except Exception as get_exception:
        print(f"Error in insertHubspotID (contacts): {get_exception}")
    print("Serial IDs added to the database")


def delete_serial_ids(serials):
    """
    Delete HubSpot IDs associated with serials from the database.

    Args:
        serials (list): A list of serials to be deleted.

    Returns:
        list: A list of HubSpot IDs that were deleted.
    """
    print("Retrieving HubSpot IDs to correspond deletion on HubSpot")
    try:
        # Query to retrieve HubSpot IDs for the provided serials
        query = 'select "hubspotID" from hubspot_integration.serials where serial = ANY(%s)'
        serial_ids = db.conn("GET", query, (serials,))
        # Check if any HubSpot IDs were retrieved
        if serial_ids is not None:
            query = 'delete from hubspot_integration.serials where "hubspotID" = %s'
            # Delete each serial's HubSpot ID from the database
            for serial_id in serial_ids:
                db.conn("DELETE", query, serial_id)
            print("Serial IDs deleted from the database")
    except Exception as get_exception:
        print(f"Error in Serials (DELETE): {get_exception}")
    return serial_ids