""" ruun workflows sequentially """
import  database as db
import hubspot_api as hubspot
import postgres as data

#adding contacts workflow
try:
    contacts = db.get_contacts()
    if len(contacts) == 0:
        print("No new contacts found")
    else:
        result= hubspot.create_contacts(contacts)
        if len(result[0]) > 0:
            db.insert_contact_ids(result[0])
        while len(result[1]) > 0 or len(result[2]) > 0:
            result3 = []
            filtered_contacts = []
            if len(result[1]) > 0:
                print("Handling Duplicate contacts")
                result3 =db.duplicate_contacts(result[1])
            if len(result[2]) > 0:
                print("Handling invalid emails")
                db.invalid_emails(result[2])
                contacts_batch = result[3]
                filtered_contacts = [contact for contact in contacts_batch 
                                     if contact[0] not in result[2]]
            result = hubspot.create_contacts(result3+filtered_contacts)

            if len(result[0]) > 0:
                db.insert_contact_ids(result[0])
except Exception as e:
    print(f"An error occurred: {e}")


#updating serials workflow
try:
    updated_serials = db.get_updated_serials()
    if len(updated_serials) == 0:
        print("No Serials to be updated")
    else:
        hubspot.update_serials(updated_serials)
except Exception as e:
    print(f"An error occurred: {e}")
#adding serials workflow
try:
    newserials = db.get_serials()
    len(newserials)
    if len(newserials) == 0:
        print("No new Serials found")
    else:
        serialhubspotids = hubspot.create_serials(newserials)
        db.insert_serial_ids(serialhubspotids)
except Exception as e:
    print(f"An error occurred: {e}")

# delete contacts
try:
    delete_contacts = data.analytics_db("GET",'''select "hubspotID" from contacts 
    order by created asc limit 275''',None)
    hubspot.delete_contacts(delete_contacts)
    db.delete_contacts(delete_contacts)
except Exception as e:
    print(f"An error occurred: {e}")

# delete serials
try:
    delete_serial = data.analytics_db("GET",""" select "hubspotID" from serials 
    order by created asc limit 73""",None)
    list_of_serials = [tup[0] for tup in delete_serial]
    hubspot.delete_serials(list_of_serials)
    #db.delete_serial_ids(list_of_serials)
except Exception as e:
    print(f"An error occurred: {e}")
