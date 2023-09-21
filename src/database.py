import db 
import datetime


def ContactsDB(action, task=None,payload=None):
    try:
        # Check for valid action
        if action not in ('GET', 'DELETE'):
            raise ValueError("Invalid contact action parameter")

        # Handle GET action
        if action == "GET":
            try:
                # Fetch the last sync timestamp from the database
                value=None
                last_sync_query = 'select max(created) from hubspot_integration.contacts'
                last_sync_timestamp = db.conn(action, last_sync_query, value)
                value = last_sync_timestamp[0]

                # Fetch new contacts from cloud.Users table
                user_query = 'select email, name from cloud."Users" where "createdAt" > %s and email is not null order by "createdAt" asc limit 30'
                
                new_contacts = db.conn(action, user_query, value)
                return new_contacts

            except Exception as get_exception:
                print(f"Error in Contacts (GET): {get_exception}")
                return []

        # Handle DELETE action
        elif action == "DELETE":
            if task not in ("query", "list"):
                raise ValueError("Invalid deletion task type")
            try:
                # Execute either a query or a list-based deletion
               if task == 'query':
                  
                  # Get hubspotIDs to correspond deletion with Hubspot 
                try:
                    query = f'select "hubspotID" from hubspot_integration.contacts where {payload}'
                    new_action= "GET"
                    serials = db.conn(new_action,query,payload)
                    print(serials)
                  
                    # Delete Serials from DB on if we have successfully retrieved Hubspot IDs
                    query = f'delete from hubspot_integration.contacts where {payload}'
                    db.conn(action, query, None)
                except Exception as get_exception:
                     print(f"Error in getting HubspotIDs (GET): {get_exception}")   
                return serials
            
               elif task == 'list':
                  try:
                     # Get hubspotIDs to correspond deletion with Hubspot 
                     query = 'select "hubspotID" from hubspot_integration.serials where serial in %s'
                     new_action= "GET"
                     serials = db.conn(new_action,query,(tuple(payload),))
                  except Exception as get_exception:
                     print(f"Error in getting HubspotIDs (GET): {get_exception}")
                  
                  # Delete Serials from DB on if we have successfully retrieved Hubspot IDs
                  if serials is not None:
                     query = "delete from hubspot_integration.contacts where email = %s"
                     for contact_email in payload:
                        db.conn(action, query, (contact_email,))
                  return serials

            except Exception as delete_exception:
                print(f"Error in Contacts (DELETE): {delete_exception}")
                return []

    except ValueError as value_error:
        print(f"ValueError in Contacts: {value_error}")
        return []
    
    except Exception as generic_exception:
        print(f"Unhandled Exception in Contacts: {generic_exception}")
        return []

# Example usage:
# result = Contacts("GET")
# result = Contacts("DELETE", payload=["contact1@example.com", "contact2@example.com"], task="list")
# result = Contacts("DELETE", payload=["contact1@example.com", "contact2@example.com"], task="list")







def SerialsDB(action, task=None,payload=None):
    try:
        # Handle GET action to retrieve new serials
        if action == "GET":
            try:
               print("Getting new serials")
               query = "select max(created) from hubspot_integration.serials"
               last_sync = db.conn(action, query,None)
               query = 'select s.serial, s.email, s.date as created, s.id, s."maxUse", to_timestamp(s.update_expirationdate::double precision), contacts."hubspotID" from licensing.serials s left join hubspot_integration.contacts on  s.email= contacts.email where to_timestamp(date::double precision)::date > %s  and contacts.email is not null order by s.date asc limit 30'
               new_serials = db.conn(action, query, last_sync)
               print("New serials retrieved from DB")
               return new_serials
            except Exception as get_exception:
                print(f"Error in Serials (GET): {get_exception}")

        # Handle UPDATED action to retrieve updated serials
        elif action == "UPDATE":
            # Run this before updating associations because they both rely on the updated column
            print("Getting updated serials")
            try:
               query = "select min(updated) from hubspot_integration.serials"
               last_sync = db.conn("GET", query,None)
               print(last_sync)
               query = 'select s.serial, s.email, s.date as created, hb."hubspotID", s.id, s."maxUse", to_timestamp(s.update_expirationdate::double precision) from licensing.serials s left join hubspot_integration.serials hb on s.serial=hb.serial where hb."hubspotID" is not null and s.last_updated_on > %s limit 50'
               updated_serials = db.conn("GET", query, last_sync)
               print("Updated serials retrieved from DB")
               return updated_serials
            except Exception as get_exception:
               print(f"Error in Serials (UPDATED): {get_exception}")

        # Handle ASSOCIATE action (GET and POST)
        elif action == "ASSOCIATE":
               print("DB call for serial contacts association")
               serials = []
               for serial in payload:
                   serials.append(serial[0])
               try:
                  print(f"Serial for association{tuple(serials)}")
                  query = f'select s."hubspotID" as serial, c."hubspotID" as contact from hubspot_integration.serials s left join licensing.serials ls on s.serial = ls.serial left join hubspot_integration.contacts c on ls.email = c.email  where s.updated is null and c."hubspotID" is not null and s.serial in {tuple(serials)}'
                  data = db.conn("GET", query,None)
                  print("Serials for associations retrieved")
                  print (data)
               except Exception as get_exception:
                  print(f"Error in Serials (Associate GET): {get_exception}")
               return data

        # Handle DELETE action (query and list)
        elif action == "DELETE":
            if task == "query":
               print("Retrieving hubspotIDs to correspond deletion on Hubspot")
               try:
                  query = f'select "hubspotID" from hubspot_integration.serials where {payload}'
                  serials = db.conn("get", query, payload)
                                    
                  # Delete Serials from DB on if we have successfully retrieved Hubspot IDs
                  if serials is not None:
                     query = f'delete from hubspot_integration.serials where {payload}'
                     db.conn(action, query, payload)
                     print("Deletion query executed")
                     return serials
               except Exception as get_exception:
                  print(f"Error in Serials (DELETE): {get_exception}")
            elif task == "list":
               try:
                  query = 'select "hubspotID" from hubspot_integration.serials where serial in %s'
                  serials = db.conn("get", query, (tuple(payload),))

                  print("Serials deleted")
                  query = 'delete from hubspot_integration.serials where serial = %s'
                  for serial in payload:
                     db.conn(action, query, (serial,))
               except Exception as get_exception:
                  print(f"Error in Serials (DELETE): {get_exception}")

        elif action == "POST":
            print("Inserting new serial HubSpot IDs")
            try:
                query = 'insert into hubspot_integration.serials ("hubspotID", serial, created) values (%s, %s, %s)'
                for serial in payload:
                    action = "UPDATE"
                    values = (int(serial[1]), serial[0], serial[2])
                    print(values)
                    db.conn(action, query, values)
                print("New serial HubSpot IDs inserted")
            except Exception as get_exception:
                  print(f"Error in insertHubspotID (contacts): {get_exception}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

    return []

# Example usage:
# result = Serials("GET_NEW")
# result = Serials("GET_UPDATED")
# result = Serials("ASSOCIATE", task="GET")
# result = Serials("ASSOCIATE", task="POST", payload=["serial1", "serial2"])
# result = Serials("DELETE", task="query", payload="hubspotID='123'")
# result = Serials("DELETE", task="list", payload=["serial1", "serial2"])


import datetime

def insertHubspotID(table, payload):
    try:
        # Insert HubSpot IDs for contacts
        if table == "contacts":
            print("Inserting new contact HubSpot IDs")
            try:
                query = 'insert into hubspot_integration.contacts ("hubspotID", email, created) values (%s, %s, %s)'
                for contact in payload:
                    action = "UPDATE"
                    current_time = str(datetime.datetime.now())
                    values = (contact[1], contact[0], current_time)
                    db.conn(action, query, values)
            except Exception as get_exception:
                  print(f"Error in insertHubspotID (contacts): {get_exception}")

        # Insert HubSpot IDs for serials
        elif table == "serials":
            print("Inserting new serial HubSpot IDs")
            try:
                query = 'insert into hubspot_integration.serials ("hubspotID", serial, created) values (%s, %s, %s)'
                for serial in payload:
                    action = "update"
                    current_time = str(datetime.datetime.now())
                    values = (serial[1], serial[0], current_time)
                    db.conn(action, query, values)
                print("New serial HubSpot IDs inserted")
            except Exception as get_exception:
                  print(f"Error in insertHubspotID (contacts): {get_exception}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")

# Example usage:
# insert_hubspot_id("contacts", [("contact1@example.com", "hubspot_id_1"), ("contact2@example.com", "hubspot_id_2")])
# insert_hubspot_id("serials", [("serial1", "hubspot_id_1"), ("serial2", "hubspot_id_2")])
