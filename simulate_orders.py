
import asyncio
import json
import random
from datetime import datetime
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

print("Script started")

# ── your event hub connection details ────────────────────────────────────────
CONNECTION_STRING = "Endpoint=sb://retail-eventhubs.servicebus.windows.net/;SharedAccessKeyName=orders-stream-policy;SharedAccessKey=ZlE71gfGANsMDWna9+V5rQLEsoRCy0LFN+AEhPDlrUY=;EntityPath=orders-stream"
EVENTHUB_NAME     = "orders-stream"

# ── reference data (must match your existing customers/products) ──────────────
customer_ids = list(range(1, 5001))        # matches your 5000 customers
product_ids  = list(range(101, 181))       # matches your 80 products
cities = [
    "Bangalore","Delhi","Mumbai","Chennai","Hyderabad","Pune",
    "Kolkata","Ahmedabad","Jaipur","Lucknow","Surat","Nagpur",
    "Indore","Bhopal","Patna","Vadodara","Coimbatore",
    "Visakhapatnam","Kochi","Chandigarh"
]

product_prices = {
    101:20000, 102:3000, 103:70000, 104:5000, 105:2000,
    106:1500,  107:3000, 108:45000, 109:1200, 110:800,
    111:18000, 112:8000, 113:2500,  114:4000, 115:500,
    116:600,   117:1200, 118:35000, 119:40000,120:2200,
    121:3000,  122:5000, 123:1500,  124:4000, 125:800,
    126:1200,  127:1500, 128:2000,  129:700,  130:3500,
    131:2500,  132:4500, 133:500,   134:600,  135:2200,
    136:900,   137:1800, 138:600,   139:1100, 140:1700,
    141:3500,  142:1800, 143:2200,  144:1500, 145:4500,
    146:8000,  147:25000,148:20000, 149:5000, 150:1200,
    151:900,   152:3000, 153:12000, 154:800,  155:600,
    156:1200,  157:700,  158:900,   159:2500, 160:1100,
    161:700,   162:600,  163:800,   164:750,  165:500,
    166:650,   167:800,  168:700,   169:850,  170:700,
    171:2500,  172:800,  173:1200,  174:600,  175:3000,
    176:1500,  177:400,  178:3500,  179:300,  180:900
}

# ── order ID counter (starts after batch data ends) ───────────────────────────
order_id_counter = 10001

async def send_order():
    global order_id_counter

    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        eventhub_name=EVENTHUB_NAME
    )

    async with producer:
        while True:
            # generate one fake order
            customer_id = random.choice(customer_ids)
            product_id  = random.choice(product_ids)
            base_price  = product_prices.get(product_id, 1000)
            amount      = base_price + random.randint(-100, 100)
            amount      = max(amount, 1)
            city        = random.choice(cities)

            order = {
                "order_id":   order_id_counter,
                "customer_id": customer_id,
                "product_id":  product_id,
                "amount":      amount,
                "currency":    "INR",
                "city":        city,
                "timestamp":   datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

            # send to Event Hubs
            event_batch = await producer.create_batch()
            event_batch.add(EventData(json.dumps(order)))
            await producer.send_batch(event_batch)

            print(f"Sent order {order_id_counter} | "
                  f"customer={customer_id} | "
                  f"product={product_id} | "
                  f"amount=₹{amount} | "
                  f"city={city}")

            order_id_counter += 1

            # send one order every 5 seconds
            await asyncio.sleep(3)

# ── run ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Starting order simulation... Press Ctrl+C to stop")
    asyncio.run(send_order())