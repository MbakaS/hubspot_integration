""" Processes Stripe webhook events related to customer and subscription events """
import datetime
import os
import stripe
from flask import Flask, jsonify, request
from dotenv import load_dotenv, find_dotenv
import database 
import hubspot_api as hubspot

# Load environment variables from a .env file
load_dotenv(find_dotenv())
api_key = os.getenv('API_KEY')
app = Flask(__name__)

@app.route('/', methods=['POST'])
def webhook():
    """
    Handle incoming Stripe webhook events.

    This endpoint processes Stripe webhook events related to customer creation,
    customer subscription updates, and customer subscription deletion.

    Returns:
        tuple: A tuple containing a JSON response and HTTP status code.
    """
    stripe.api_key = api_key

    data = request.get_json(silent=True)

    # Handle the specific events you're interested in
    event_type = data['type']

    if event_type == 'customer.created':
        if data['data']['object']['email'] is None:
            pass
        else:
            date = datetime.datetime.utcfromtimestamp(data["data"]["object"]["created"])
            iso_date_string_long = date.strftime('%Y-%m-%d %H:%M:%S')
            hubspotids = hubspot.create_contacts([[data["data"]["object"]["email"], iso_date_string_long]])
            database.insert_contact_ids(hubspotids)
    elif event_type == 'customer.subscription.updated':
        print("subscription updated")
        customer = stripe.Customer.retrieve(data["data"]["object"]["customer"])
        date = datetime.datetime.utcfromtimestamp(data["data"]["object"]["current_period_end"])
        renewal_date = date.strftime('%Y-%m-%d')
        payload = {
            "plan": data["data"]["object"]["items"]["data"][0]["plan"]["interval"],
            "paid_seats": data["data"]["object"]["quantity"],
            "status": data["data"]["object"]["status"],
            "renewal_date": renewal_date,
            "email": customer.email
        }
        hubspotid = database.get_workspace_hubspot_id(customer.id)
        hubspot.update_workspace(payload, hubspotid[0][0])

    # Acknowledge receipt of the event to Stripe
    return jsonify({'message': 'Event received'}), 200

if __name__ == '__main__':
    app.run(port=8080)
