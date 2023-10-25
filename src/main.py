""" """
import  database as db
import hubspot_api as hubspot
import postgres as data
import logging


#adding contacts workflow
try:
    contacts = db.get_contacts()
    if len(contacts) == 0:
        print("No new contacts found")
    else:
        hubspotids= hubspot.create_contacts(contacts)
        db.insert_contact_ids(hubspotids)
except:
    logging.exception("ERROR: adding contacts")



#updating serials workflow
try:
    updated_serials = db.get_updated_serials()
    if len(updated_serials) == 0:
        print("No Serials to be updated")
    else:
        hubspot.update_serials(updated_serials)
except:
    logging.exception("ERROR: updating Serials")

#adding serials workflow
try:
    newserials = db.get_serials()
    if len(newserials) == 0:
        print("No new Serials found")
    else:
        serialhubspotids = hubspot.create_serials(newserials)
        db.insert_serial_ids(serialhubspotids)
except:
    logging.exception("ERROR: adding Serials")

#updating memberships workflow
try:
    updated_memberships = db.get_updated_memberships()
    if len(updated_memberships) == 0:
        print("No Memberships to be updated")
    else:
        hubspot.update_memberships(updated_memberships)
except:
    logging.exception("ERROR: updating Memberships")

# adding workspaces workflow
try:
    new_workspaces= db.get_workspaces()
    if len(new_workspaces) == 0:
            print("No new Workspaces found")
    else:
        workspaces = hubspot.get_subscriptions(new_workspaces)
        workspaces_hubspotids = hubspot.create_workspaces(workspaces)
        db.insert_workspace_ids(workspaces_hubspotids)
except:
    logging.exception("ERROR: adding Workspaces")

#adding memberships workflow
try:
    new_memberships = db.get_memberships()
    if len(new_memberships) == 0:
        print("No new Memberships found")
    else:
        membership_hubspotids = hubspot.create_memberships(new_memberships)
        db.insert_membership_ids(membership_hubspotids)
except:
    logging.exception("ERROR: adding Memberships")


"""
# delete memerbships
try:
    delete_membership = data.analytics_db("GET",'select "hubspotID" from memberships order by created asc limit 1',None)
    list_of_memberships = [tup[0] for tup in delete_membership]
    hubspot.delete_memberships(list_of_memberships)
    db.delete_membership_ids(list_of_memberships)
except:
    logging.exception("ERROR: adding Memberships")



# delete Workspaces
try:
    delete_workspaces = data.analytics_db("GET",'select workspace,"hubspotID" from workspaces order by created asc limit 3',None)
    list_of_hubspotids = [tup[1] for tup in delete_workspaces]
    list_of_workspaces = ', '.join([f"'{tup[0]}'" for tup in delete_workspaces])

    delete_memberships = data.cloud_db(f'select id from "OrganizationMemberships" where "OrganizationId" in ({list_of_workspaces})',None)
    list_of_memberids = ', '.join([f"'{tup[0]}'" for tup in delete_memberships])

    delete_memberships_ids = data.analytics_db("GET",f'select "hubspotID" from memberships where member in ({list_of_memberids})',None)

    list_of_members = [tup[0] for tup in delete_memberships_ids]
    hubspot.delete_memberships(list_of_members)
    db.delete_membership_ids(list_of_members)
    hubspot.delete_workspaces(list_of_hubspotids)
    db.delete_workspace_ids(list_of_hubspotids)
except:
    logging.exception("ERROR: deleting workspaces")

# delete contacts

try:
    delete_contacts = data.analytics_db("GET",'select "hubspotID" from contacts order by created asc limit 10',None)
    hubspot.delete_batch_contacts(delete_contacts)
    db.delete_contacts(delete_contacts)
except:
    logging.exception("ERROR: deleting contacts")



# delete serials
try:
    delete_serial = data.analytics_db("GET",'select "hubspotID" from serials order by created asc limit 10',None)
    list_of_serials = [tup[0] for tup in delete_serial]
    hubspot.delete_serials(list_of_serials)
    db.delete_serial_ids(list_of_serials)
except:
    logging.exception("ERROR: deleting serials")

"""