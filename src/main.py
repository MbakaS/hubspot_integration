import  database as db
import hubspot_api as hubspot
import postgres as data


#adding contacts workflow
try:
    contacts = db.get_contacts()
    if len(contacts) == 0:
        print("No new contacts found")
    else:
        hubspotids= hubspot.create_contacts(contacts)
        db.insert_contact_ids(hubspotids)
except:
    print("An error occurred adding contacts")

#updating serials workflow
try:
    updated_serials = db.get_updated_serials()
    if len(updated_serials) == 0:
        print("No Serials to be updated")
    else:
        hubspot.update_serials(updated_serials)
except:
    print("An error occurred updating Serials")

#adding serials workflow
try:
    newserials = db.get_serials()
    print()
    if len(newserials) == 0:
        print("No new Serials found")
    else:
        serialhubspotids = hubspot.create_serials(newserials)
        db.insert_serial_ids(serialhubspotids)
except:
    print("An error occurred adding Serials")



# delete contacts
try:
    delete_contacts = data.analytics_db("GET",'select "hubspotID" from contacts order by created asc limit 10',None)
    hubspot.delete_batch_contacts(delete_contacts)
    db.delete_contacts(delete_contacts)
except:
    print("An error occurred deleting contacts")


# delete serials
try:
    delete_serial = data.analytics_db("GET",'select "hubspotID" from serials order by created asc limit 10',None)
    list_of_serials = [tup[0] for tup in delete_serial]
    hubspot.delete_serials(list_of_serials)
    db.delete_serial_ids(list_of_serials)
except:
    print("An error occurred deleting serials")

