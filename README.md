# hubspot_integration
An analytics service updating Hubspot custom objects. 

In HubSpot, we manage four object schemas: Contacts, Serials, Workspaces, and Memberships. Additionally, we create associations between these entities. To ensure the data within HubSpot remains current, we rely on information from the Cloud schema, Stripe, and Licensing schema. 

## Workflow
`Create` new objects.

`Update` existing objects.

`Delete` objects that are no longer needed in Hubspot. 


## hubspot_integration Schema
We internally retain the HubSpot ID of an object once it is generated. These IDs are stored within the hubspot_integration schema, part of our analytics schema. They serve as a means for us to interface with the HubSpot API seamlessly. Within this schema, we have established four distinct tables, each dedicated to one of the objects we oversee within HubSpot.

# Requirements to run it
Make sure to set up the needed ENV vars. The env.sample contains a list of the required env vars.

Install all the requirements.

#### Start the Docker containers:
```bash
docker-compose up -d
```
#### Bootstrap the database with sample data:
```bash
python bootstrap.py
```
#### Test the workflows:
```bash
cd src
python main.py
```
