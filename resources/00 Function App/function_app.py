import os
import uuid
import random
import logging
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List

import azure.functions as func
from azure.eventhub import EventHubProducerClient, EventData
from azure.storage.blob import BlobServiceClient
from faker import Faker
from dateutil.relativedelta import relativedelta

# ─────────────────────────────────────────────────────────────
# Register FunctionApp
app = func.FunctionApp()

# ─────────────────────────────────────────────────────────────
# Configuration
BLOB_CONN_STR = os.getenv("BLOB_CONN_STR")
BLOB_CONTAINER = os.getenv("BLOB_CONTAINER", "simulator")
BLOB_FILE_ALL = os.getenv("BLOB_FILE_ALL")
BLOB_FILE_MODIFY = os.getenv("BLOB_FILE_MODIFY")

EVENTHUB_CONN_STR = os.getenv("EVENTHUB_CONN_STR")
EVENTS_PER_TICK = int(os.getenv("EVENTS_PER_TICK", "5"))
SEED = os.getenv("SEED")


# ─────────────────────────────────────────────────────────────
blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
container_client = blob_service.get_container_client(BLOB_CONTAINER)


fake = Faker()
if SEED:
    Faker.seed(int(SEED))
    random.seed(int(SEED))



# ─────────────────────────────────────────────────────────────
# Shared memory for reservations to modify/cancel

future_reservations: Dict[str, Dict] = {}

# ─────────────────────────────────────────────────────────────
# Helper function: send payload to Eventstream

def send_to_eventstream(payloads: List[Dict]):
    if not EVENTHUB_CONN_STR:
        logging.error("EVENTHUB_CONN_STR is missing in application settings.")
        return

    try:
        producer = EventHubProducerClient.from_connection_string(conn_str=EVENTHUB_CONN_STR)
        events = [EventData(json.dumps(p)) for p in payloads]
        with producer:
            producer.send_batch(events)
        logging.info(f"✅ Sent {len(events)} events to Eventstream")
    except Exception as e:
        logging.exception(f"❌ Error sending to Eventstream: {e}")


# ─────────────────────────────────────────────────────────────
# JSON normalization (final format send to eventstream)

def normalize_payload(raw: Dict) -> Dict:
    """
    Flatten structures, remove hyphens, unify field names.
    """

    data = raw.get("data", {})
    # flatten apartment objects if present
    apartment = data.get("apartment", {})

    normalized = {
        "action": raw.get("action"),
        "user": raw.get("user"),
        "id": data.get("id"),
        "reference_id": data.get("reference-id"),
        "arrival": data.get("arrival"),
        "departure": data.get("departure"),
        "created_at": data.get("created-at") or data.get("createdAt"),
        "modified_at": data.get("modified-at") or data.get("modifiedAt"),
        "apartment_id": apartment.get("id") or data.get("apartment-id"),
        "apartment_name": apartment.get("name") or data.get("apartment-name"),
        "channel_name": data.get("channel"),
        "guest_name": data.get("guest-name"),
        "email": data.get("email"),
        "adults": data.get("adults"),
        "children": data.get("children"),
        "price": data.get("price"),
        "price_paid": data.get("price-paid"),
        "commission_included": data.get("commission-included"),
        "prepayment": data.get("prepayment"),
        "prepayment_paid": data.get("prepayment-paid"),
        "deposit": data.get("deposit"),
        "deposit_paid": data.get("deposit-paid"),
        "language": data.get("language"),
        "guest_id": data.get("guest-id") or data.get("guestId"),
    }
    return normalized

# ─────────────────────────────────────────────────────────────
# EVENT SIMULATOR: NEW RESERVATION

def generate_new_reservation(property_id: int, 
                             now_utc: datetime, 
                             booking_shift_days: int = 0, 
                             checkin_shift_days: int = 3, 
                             stay: int = 5) -> Dict:
    """
    Simulate webhook events newReservation.
    Assumption: property_id in [1,30] --> short-term rental, property_id in [31,35] --> long-term rental

    """

    ref_utc = now_utc + timedelta(days=booking_shift_days)

    rid = f"R-{ref_utc.strftime('%y%m%d')}-{uuid.uuid4().hex[:8].upper()}"

    checkin = (ref_utc + timedelta(days=checkin_shift_days)).date()
    
    if property_id <31:
        #short-term rental, stay in days
        checkout = checkin + timedelta(days=stay)   
        rate = random.uniform(200, 800)
        total_price = round(rate * stay, 2)
        commission = round(total_price * 0.12, 2)
        prepayment = round(total_price * 0.2, 2)
        deposit = None

    else:
        #long-term rental, stay in months
        checkout = checkin + relativedelta(months=stay)   
        rate = random.uniform(5000, 8000)
        total_price = round(rate * stay, 2)
        commission = round(total_price * 0.12, 2)
        prepayment = None
        deposit = 3000


    reservation = {
        "id": random.randint(10000, 99999),
        "reference-id": rid,
        "arrival": checkin.isoformat(),
        "departure": checkout.isoformat(),
        "created-at": ref_utc.strftime("%Y-%m-%d %H:%M:%S"),
        "modified-at": ref_utc.strftime("%Y-%m-%d %H:%M:%S"),
        "apartment": {"id": f"KRA-{property_id:03d}", "name": f"apartment{property_id}"},
        "channel": random.choices(["booking","airbnb", "website", "phone"], weights=[0.5,0.2,0.2,0.1], k=1)[0],
        "guest-name": fake.name(),
        "email": fake.email(),
        "adults": random.randint(1, 4),
        "children": random.choices([0, 1, 2], weights=[0.5,0.25,0.25], k=1)[0],
        "price": total_price,
        "price-paid": "no",
        "commission-included": commission,
        "prepayment": prepayment,
        "prepayment-paid": "no" if prepayment is None else "yes",
        "deposit": deposit,
        "deposit-paid": "no" if deposit is None else "yes",    
        "language": random.choice(["pl", "de", "en", "it", "es"]),
        "guest-id": random.randint(1000, 9999)
    }

    return reservation

# ─────────────────────────────────────────────────────────────
# EVENT SIMULATOR 

def generate_event(now_utc: datetime) -> Dict:
    """
    Simulate webhook events new/modify/cancelReservation.
    """
    global future_reservations

    if not future_reservations:
        action = "newReservation"
    else:
        action = random.choices(["newReservation","modifyReservation", "cancelReservation"], weights=[0.6,0.2,0.2], k=1)[0]

    if action == "newReservation":

        rental_type = random.choice(["short", "long"])
        if rental_type == "short":
            reservation = generate_new_reservation(
                property_id=random.randint(1, 30),
                now_utc=now_utc,
                checkin_shift_days=random.randint(3, 60),
                stay=random.randint(1, 6) #stay in days
            )
        else:
            reservation = generate_new_reservation(
                property_id=random.randint(31, 35),
                now_utc=now_utc,
                checkin_shift_days=random.randint(3, 60),
                stay=random.randint(1, 3) #stay in months
            )

    elif action == "modifyReservation":
        #future_reservations store events in format before normalization
        rid = random.choice(list(future_reservations.keys()))
        
        raw_event = future_reservations[rid]
        reservation = raw_event["data"]

        # update timestamps and price
        reservation["modified-at"] = now_utc.strftime("%Y-%m-%d %H:%M:%S")
        reservation["price"] = round(reservation["price"] * random.uniform(0.95, 1.05), 2)
        
        # update arrival and departure
        shift_days = random.randint(1, 7)
        new_checkin = (datetime.fromisoformat(reservation["arrival"]) + relativedelta(days=shift_days)).date()
        new_checkout = (datetime.fromisoformat(reservation["departure"]) + relativedelta(days=shift_days)).date()
        
        reservation["arrival"]  = new_checkin.isoformat()
        reservation["departure"] = new_checkout.isoformat()


    elif action == "cancelReservation":
        #future_reservations store events in format before normalization
        rid = random.choice(list(future_reservations.keys()))
        
        raw_event = future_reservations[rid]
        reservation = raw_event["data"]

        reservation["modified-at"] = now_utc.strftime("%Y-%m-%d %H:%M:%S")


    return {"action": action, "user": 1, "data": reservation}



# ─────────────────────────────────────────────────────────────
# EVENT SIMULATOR (Timer Trigger)
# ─────────────────────────────────────────────────────────────

@app.timer_trigger(
    schedule="0 */1 * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=True
)

def reservation_simulator(myTimer: func.TimerRequest) -> None:
    
    global future_reservations
    
    now = datetime.now(timezone.utc)
    if myTimer.past_due:
        logging.warning("Timer is past due!")
        

    # --- load reservation history from Blob ---
    reservations_summary = {}
    try:
        blob_client = container_client.get_blob_client(BLOB_FILE_ALL)
        blob_data = blob_client.download_blob().readall()
        reservations_summary = json.loads(blob_data)
        logging.info(f"Loaded {len(reservations_summary)} active reservations from Blob.")
    
    except Exception as e:
        logging.warning(f"Failed to load reservation history from {BLOB_FILE_ALL}: {e}")

    # --- load reservation history (candidates for modify/cancel) from Blob ---

  #  future_reservations = {}
    full_reservations = {}
    try:
        blob_client = container_client.get_blob_client(BLOB_FILE_MODIFY)
        blob_data = blob_client.download_blob().readall()
        full_reservations = json.loads(blob_data)
        
        # filter only future reservations
        future_reservations = {
            rid: r
            for rid, r in full_reservations.items()
            if datetime.fromisoformat(r["data"]["arrival"]).date() > now.date()
        }


        logging.info(f"Loaded {len(future_reservations)} future reservations from Blob.") 
    
    except Exception as e:
        logging.warning(f"Failed to load full reservation history from {BLOB_FILE_MODIFY}: {e}")

    # --- Generate new events considering active reservations ---
    events = []
    _counter = 0
    
    while _counter < EVENTS_PER_TICK:
        # --- generate event ---
        raw_event = generate_event(now)
        event = normalize_payload(raw_event)

        # --- validate data accuracy (reservations overlapping) and update local history files ---        
        rid = event["reference_id"]
        apartment_id = event["apartment_id"]
        arrival = event["arrival"]
        departure = event["departure"]

       
        if event["action"] == "newReservation":
            # check overlapping:
            res_map = reservations_summary.setdefault(apartment_id, {})
            overlaps = [
                r for r in res_map
                if not (
                    departure <= res_map[r]["arrival"] or
                    arrival >= res_map[r]["departure"]
                )
            ]   
            if overlaps:
                logging.info(f"Skipped newReservation — overlap for apartment_id={apartment_id}")
                continue
            else:
                #add reservation to history
                res_map[rid] = {"arrival": arrival, "departure": departure}
                future_reservations[rid] = raw_event

        elif event["action"] == "cancelReservation":
            # remove from history
            reservations_summary[apartment_id].pop(rid, None)
            future_reservations.pop(rid, None)

        elif event["action"] == "modifyReservation":
            arrival_dt = datetime.fromisoformat(arrival).date()
            departure_dt = datetime.fromisoformat(departure).date()
            
            # check overlapping
            res_map = reservations_summary.setdefault(apartment_id, {})
            overlaps = [
                r for r in res_map
                if r != rid and not (
                    departure_dt <= datetime.fromisoformat(res_map[r]["arrival"]).date() or
                    arrival_dt >= datetime.fromisoformat(res_map[r]["departure"]).date()
                )
            ]   
            if overlaps:
                logging.info(f"Skipped modifyReservation — overlap for apartment_id={apartment_id}")
                continue
            else:
                #update reservation history
                res_map[rid].update({'arrival': arrival, 'departure': departure})            
                #remove from future reservations history
                future_reservations.pop(rid, None)
        
        _counter+=1
        events.append(event)


    # --- save updated reservation state to Blob ---
    try:
        blob_client = container_client.get_blob_client(BLOB_FILE_ALL)
        blob_client.upload_blob(json.dumps(reservations_summary), overwrite=True)
        logging.info(f"Updated {len(reservations_summary)} active reservations in Blob.")
    except Exception as e:
        logging.error(f"Error saving to Blob Storage: {e}")

    try:
        blob_client = container_client.get_blob_client(BLOB_FILE_MODIFY)
        blob_client.upload_blob(json.dumps(future_reservations), overwrite=True)
        logging.info(f"Updated {len(future_reservations)} full reservations in Blob.")
    except Exception as e:
        logging.error(f"Error saving to Blob Storage: {e}")

    # --- send new events to eventstream ---
    if events:
        send_to_eventstream(events)