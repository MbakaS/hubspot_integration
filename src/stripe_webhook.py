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
api_key = os.getenv('STRIPE_KEY')
stripe_secret = os.getenv('STRIPE_SECRET')
app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def webhook():
    """
    Handle incoming Stripe webhook events.

    This endpoint processes Stripe webhook events related to customer creation,
    customer subscription updates, and customer subscription deletion.

    Returns:
        tuple: A tuple containing a JSON response and HTTP status code.
    """
    stripe.api_key = api_key
    payload = request.get_data(as_text=True)
    data = request.get_json(silent=True)
    sig_header = request.headers.get('Stripe-Signature')

    try:
        # Verify the webhook signature
        event_type = stripe.Webhook.construct_event(
            payload, sig_header, stripe_secret
        )
    except Exception as e:
        print(f"An error occurred: {e}")
        return 'Invalid signature', 400
    # Handle the specific events you're interested in
    event_type = data['type']
    if event_type == 'customer.subscription.updated':
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
        if len(hubspotid) >0:
            hubspot.update_workspace(payload, hubspotid[0][0])
        else:
            print("")
            return jsonify({'message': 'Try again later: Workspace yet to be synced'}), 500

    # Acknowledge receipt of the event to Stripe
    return jsonify({'message': 'Event received'}), 200

if __name__ == '__main__':
    app.run(port=8080)
