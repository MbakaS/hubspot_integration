""" This module contains all API funtions to Hubspot """
import os
import time
import stripe
from dotenv import load_dotenv, find_dotenv

# Load environment variable from a .env file
load_dotenv(find_dotenv())
api_key = os.getenv('STRIPE_TEST')

stripe.api_key = api_key
stripe.max_network_retries = 2
REQUEST_DELAY = 0.6  # Specify the delay between API requests in seconds 

def get_all_subscriptions():
    """
    Retrieve all active and trialing subscriptions from the Stripe API.

    Returns:
        list: List of tuples containing subscription information.
            Each tuple format: (id, created, customer, ended_at, plan_id, 
            interval, quantity, status, trial_start, trial_end, current_period_end, email)
    """
    print("START: Get all active and trialing subscriptions from Stripe API")
    active_subscriptions = []
    customer_list = []
    page_size = 100  # Specify the desired page size

    # Retrieve all subscriptions from the Stripe API
    subscriptions = stripe.Subscription.auto_paging_iter(limit=page_size)

    # Filter active subscriptions
    for subscription in subscriptions:
        if subscription.status in ('active', 'trialing', 'unpaid', 'overdue'):
            customer = stripe.Customer.retrieve(subscription.customer)
            subscription_tuple = (subscription.id, subscription.created, subscription.customer,
                subscription.ended_at,subscription.plan.id, subscription.plan.interval,
                subscription.quantity,subscription.status,subscription.trial_start, 
                subscription.trial_end,subscription.current_period_end, customer.email)
            active_subscriptions.append(subscription_tuple)
            customer_list.append(subscription.customer)
        time.sleep(REQUEST_DELAY)
    print("Success: All active and trialing subscriptions retrieved")
    return [active_subscriptions, customer_list]

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
    for workspace in workspaces:
        try:
            customer = stripe.Customer.retrieve(workspace[5])
            subscription = stripe.Subscription.list(customer=workspace[5])
            time.sleep(REQUEST_DELAY)

            if len(subscription["data"]) > 0:
                subscription = subscription["data"][0]
                customer_data = [customer.id, customer.email]
                id = subscription["id"],
                created = workspace[3],
                customer = subscription["customer"],
                ended_at = subscription["ended_at"],
                plan = subscription["plan"]["id"],
                interval = subscription["plan"]["interval"],
                quantity = subscription["quantity"],
                status = subscription["status"],
                trial_start = subscription["trial_start"],
                trial_end = subscription["trial_end"],
                current_period_end = subscription["current_period_end"]

                formatted_date = created[0].strftime('%Y%m%d')
                # Convert the formatted date string to an integer
                date_integer = int(formatted_date)

                data.append([workspace[0], workspace[1], workspace[2], customer_data[0],
                              customer_data[1], id[0], date_integer, plan[0], status[0], 
                              ended_at[0], interval[0], quantity[0], trial_start[0], trial_end[0], 
                              current_period_end, workspace[3]])
            else:
                print("NULL")
        except Exception as e:
            print(f"An error occurred: {e}")
            continue
    return data
