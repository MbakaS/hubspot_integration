from  database import SerialsDB,ContactsDB
from hubspot import ContactsHS,SerialsHS



#adding contacts workflow
try:
    contacts = ContactsDB("GET")
    if len(contacts) == 0:
        print("No new contacts found")
    else:
        hubspotids= ContactsHS("POST",contacts)
        ContactsDB("POST",payload=hubspotids)
except:
    print("An error occurred adding contacts")


#updating serials workflow
try:
    updated_serials = SerialsDB("UPDATE")
    if len(updated_serials) == 0:
        print("No Serials to be updated")
    else:
        SerialsHS("PUT",updated_serials)
except:
    print("An error occurred updating Serials")

#adding serials workflow
try:
    newserials = SerialsDB("GET")
    if len(newserials) == 0:
        print("No new Serials found")
    else:
        serialhubspotids = SerialsHS("POST",newserials)
        SerialsDB("POST",payload=serialhubspotids)
        SerialsHS("ASSOCIATE",payload=serialhubspotids)
except:
    print("An error occurred adding Serials")






#deleting serials
#del_serials = deleteSerials('list',['SK3-3008-2758-9826-3915-8871','SK3-9473-7505-4147-5673-0244'])
#del_serials = deleteSerials('query',"created > '2023-09-20 13:58:42'")
