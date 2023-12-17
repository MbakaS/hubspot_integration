""" This module contains all API funtions to Hubspot """
import os
import time
import re
import stripe
from dotenv import load_dotenv, find_dotenv
from ratelimit import limits, sleep_and_retry
import database as db # Import your database functions module
import hubspot_api as hubspot # Import your HubSpot functions module

# Load environment variable from a .env file
load_dotenv(find_dotenv())
api_key = os.getenv('STRIPE_KEY')

stripe.api_key = api_key
stripe.max_network_retries = 2
REQUEST_DELAY = 0.01  # Specify the delay between API requests in seconds 

# Define the rate limit: 50 requests per minute (adjust this based on Stripe's rate limits)
@sleep_and_retry
@limits(calls=30, period=60)  # 50 requests per 60 seconds

def get_all_subscriptions():
    """
    Retrieve all active and trialing subscriptions from the Stripe API.

    Returns:
        list: List of tuples containing subscription information.
            Each tuple format: (id, created, customer, ended_at, plan_id, 
            interval, quantity, status, trial_start, trial_end, current_period_end, email)
    """
    print("START: Get all active and trialing subscriptions from Stripe API")
    page_size = 100  # Specify the desired page size
    # Retrieve all subscriptions from the Stripe API
    i = 0
    n = 0
    active_subscriptions = []
    customer_list = []
    for subscription in stripe.Subscription.auto_paging_iter(status='trialing', limit=page_size):
        i=i+1
        customer = stripe.Customer.retrieve(subscription.customer)
        cancel_at_period_end = subscription.cancel_at_period_end
        if cancel_at_period_end is True:
            auto_renew =  False
        else:
            auto_renew = True
        priority = False,
        if subscription.plan in ('Legacy Business Plan / Yearly ($231 per Editor)',
                    'Legacy Business Plan / Yearly ($207.90 per Editor)',
                    'Business Plan / Yearly (2.38% discount)',
                    'Business Plan / Yearly (10% discount)',
                    'Business Plan / Yearly',
                    'Business Example Price',
                    'Alibaba Pricing'):
            priority = 'yes'
        else:
            priority = 'no'
        subscription_tuple = (subscription.id, subscription.created, subscription.customer,
            subscription.ended_at,subscription.plan.id, subscription.plan.interval,
            subscription.quantity,subscription.status,subscription.trial_start, 
            subscription.trial_end,subscription.current_period_end, customer.email,
            priority,subscription.collection_method,auto_renew)
        active_subscriptions.append(subscription_tuple)
        customer_list.append(subscription.customer)
        if i == 500:
            n=n+1
            print(f"Batch: {n}")
            add_to_database([active_subscriptions, customer_list])
            i=0
            active_subscriptions = []
            customer_list = [] 
    print("Success: All active and trialing subscriptions retrieved")
    return True

def add_to_database(all_subscriptions):
    all_workspaces = db.get_workspaces(all_subscriptions[0], all_subscriptions[1])
    # Convert the DataFrame to a NumPy array of records
    workspaces_records = all_workspaces.to_records(index=False)

    # Convert the NumPy array of records to a list of tuples
    final_workspaces_list = list(workspaces_records)
    # Print the final_workspaces_list
    workspaces_hubspotids = hubspot.create_workspaces(final_workspaces_list)
    workspaces_complete = db.add_contact_hubspot_id(workspaces_hubspotids)
    db.insert_workspace_ids(workspaces_hubspotids)
    hubspot.workspaces_associate(workspaces_complete)
    return True

def get_subscriptions(workspaces):
    """
    Retrieve subscriptions from Stripe for the given workspaces.

    Args:
        workspaces (list): A list of tuples containing workspace information.
        Each tuple format: (sketchid, name, team_id, stripe_customer_id,
                     domain, timestamp).

    Returns:
        list: A list of subscription data retrieved from Stripe.
    """
    print("START: retrieving workspaces from Stripe")
    stripe.api_key = api_key
    data = []
    missing = []
    for workspace in workspaces:
        try:
            customer = stripe.Customer.retrieve(workspace[5])
            subscription = stripe.Subscription.list(customer=workspace[5])
            time.sleep(REQUEST_DELAY)

            if len(subscription["data"]) > 0:
                subscription = subscription["data"][0]
                customer_data = [customer.id, customer.email]
                id = subscription["id"],
                created = subscription["created"],
                customer = subscription["customer"],
                ended_at = subscription["ended_at"],
                plan = subscription["plan"]["nickname"],
                interval = subscription["plan"]["interval"],
                quantity = subscription["quantity"],
                status = subscription["status"],
                trial_start = subscription["trial_start"],
                trial_end = subscription["trial_end"],
                current_period_end = subscription["current_period_end"],
                payment_menthod = subscription["collection_method"],
                cancel_at_period_end = subscription["cancel_at_period_end"],
                if cancel_at_period_end is True:
                    auto_renew =  False
                else:
                    auto_renew = True
                priority = False,
                if plan in ('Legacy Business Plan / Yearly ($231 per Editor)',
                            'Legacy Business Plan / Yearly ($207.90 per Editor)',
                            'Business Plan / Yearly (2.38% discount)',
                            'Business Plan / Yearly (10% discount)',
                            'Business Plan / Yearly',
                            'Business Example Price',
                            'Alibaba Pricing'):
                    priority = 'yes'
                else:
                    priority = 'no'
                if trial_end[0] is None:
                    trial_end = [0]
                else:
                    trial_end
                email = re.sub(r"'", r"''", customer_data[1])
                data.append([workspace[0], workspace[1], workspace[2], customer_data[0],
                              email, id[0], created[0], plan[0], status[0], 
                              ended_at[0], interval[0], quantity[0], trial_start[0], trial_end[0],
                              current_period_end[0], workspace[3],priority,payment_menthod[0],auto_renew])
            else:
                continue
        except Exception as e:
            print(f"An error occurred: {e}")
            continue
    return data
