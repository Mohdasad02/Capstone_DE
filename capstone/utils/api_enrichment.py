# Databricks notebook source
import requests
from datetime import datetime, date
from pyspark.sql import Row
from pyspark.sql.functions import (
    col, date_format, hour
)

# cache coordinates so geocoding API
# is not called twice for same city
city_coords_cache = {}

print("Imports done")

# COMMAND ----------

def get_city_coordinates(city):
    """
    converts city name to lat/lon
    caches result so each city
    is geocoded only once per session
    """
    if city in city_coords_cache:
        return city_coords_cache[city]

    try:
        url = (
            f"https://geocoding-api.open-meteo.com/v1/search"
            f"?name={city}&count=1&language=en&format=json"
        )
        response = requests.get(url, timeout=10)
        data     = response.json()

        if "results" in data and len(data["results"]) > 0:
            result = data["results"][0]
            coords = {
                "lat": result["latitude"],
                "lon": result["longitude"]
            }
            city_coords_cache[city] = coords
            return coords
        else:
            print(f"  ✗ {city} not found in geocoding")
            return None

    except Exception as e:
        print(f"  ✗ Geocoding failed for {city}: {e}")
        return None

def get_weather_description(code):
    """converts WMO weather code to text"""
    codes = {
        0: "Clear Sky",      1: "Mostly Clear",
        2: "Partly Cloudy",  3: "Cloudy",
        45: "Foggy",         48: "Foggy",
        51: "Light Drizzle", 53: "Drizzle",
        55: "Heavy Drizzle", 61: "Light Rain",
        63: "Rain",          65: "Heavy Rain",
        80: "Rain Showers",  81: "Heavy Showers",
        95: "Thunderstorm",  99: "Thunderstorm with Hail"
    }
    return codes.get(code, "Unknown")

print("Geocoding and weather code functions loaded")

# COMMAND ----------

def get_missing_weather_combos(orders_df, base_path):
    """
    compares city+date+hour needed by orders
    vs what is already stored in bronze/delta/weather/
    returns only missing combinations
    so API is never called twice for same combo
    """
    weather_path = f"{base_path}/bronze/delta/weather/"

    # what orders need
    orders_need = orders_df \
        .withColumn(
            "order_date",
            date_format(col("order_time"), "yyyy-MM-dd")
        ) \
        .withColumn("order_hour", hour(col("order_time"))) \
        .select("city", "order_date", "order_hour") \
        .distinct()

    try:
        # what is already in bronze
        already_have = spark.read \
            .format("delta") \
            .load(weather_path) \
            .select(
                col("city"),
                col("weather_date").alias("order_date"),
                col("weather_hour").alias("order_hour")
            ) \
            .distinct()

        # missing = needed minus already have
        missing      = orders_need.subtract(already_have)
        missing_list = missing.collect()

        print(f"Orders need  : {orders_need.count()} city+date+hour combos")
        print(f"Already have : {already_have.count()} in Bronze")
        print(f"Missing      : {len(missing_list)} → will call API for these")
        return missing_list

    except Exception as e:
        # table doesn't exist yet → fetch everything
        print(f"Weather table not found → fetching all")
        return orders_need.collect()

print("Missing weather check function loaded")

# COMMAND ----------

def get_missing_currency_dates(orders_df, base_path):
    """
    compares order dates needed
    vs USD rates already stored in bronze/delta/currency/
    returns only missing dates
    """
    currency_path = f"{base_path}/bronze/delta/currency/"

    # unique dates needed from orders
    dates_needed = orders_df \
        .withColumn(
            "order_date",
            date_format(col("order_time"), "yyyy-MM-dd")
        ) \
        .select("order_date") \
        .distinct()

    try:
        # dates already in bronze
        dates_have = spark.read \
            .format("delta") \
            .load(currency_path) \
            .select(
                col("rate_date").alias("order_date")
            ) \
            .distinct()

        # missing dates
        missing      = dates_needed.subtract(dates_have)
        missing_list = missing.collect()

        print(f"Dates needed : {dates_needed.count()}")
        print(f"Already have : {dates_have.count()} in Bronze")
        print(f"Missing      : {len(missing_list)} → will call API")
        return [row["order_date"] for row in missing_list]

    except Exception as e:
        print(f"Currency table not found → fetching all dates")
        return [
            row["order_date"]
            for row in dates_needed.collect()
        ]

print("Missing currency check function loaded")

# COMMAND ----------

def fetch_weather_for_city_date(city, date_str):
    """
    fetches hourly weather for ONE specific city
    on ONE specific date
    only called when that exact city+date
    combination exists in orders
    """
    today    = date.today().strftime("%Y-%m-%d")
    is_today = (date_str == today)
    records  = []
    coords   = get_city_coordinates(city)

    if not coords:
        # return unknown for all 24 hours
        for h in range(24):
            records.append(Row(
                city              = city,
                order_date        = date_str,
                order_hour        = h,
                temperature       = 0.0,
                weather_condition = "Unknown",
                wind_speed        = 0.0,
                humidity          = 0,
                fetched_at        = datetime.now().strftime(
                                    "%Y-%m-%d %H:%M:%S")
            ))
        return records

    try:
        if is_today:
            url = (
                f"https://api.open-meteo.com/v1/forecast"
                f"?latitude={coords['lat']}"
                f"&longitude={coords['lon']}"
                f"&start_date={date_str}"
                f"&end_date={date_str}"
                f"&hourly=temperature_2m,weathercode,"
                f"windspeed_10m,relativehumidity_2m"
                f"&timezone=Asia/Kolkata"
            )
        else:
            url = (
                f"https://archive-api.open-meteo.com/v1/archive"
                f"?latitude={coords['lat']}"
                f"&longitude={coords['lon']}"
                f"&start_date={date_str}"
                f"&end_date={date_str}"
                f"&hourly=temperature_2m,weathercode,"
                f"windspeed_10m,relativehumidity_2m"
                f"&timezone=Asia/Kolkata"
            )

        response = requests.get(url, timeout=15)
        data     = response.json()
        hourly   = data["hourly"]

        # store all 24 hours for this city+date
        # so any order at any hour can be joined
        for h in range(24):
            records.append(Row(
                city              = city,
                order_date        = date_str,
                order_hour        = h,
                temperature       = float(
                                    hourly["temperature_2m"][h]),
                weather_condition = get_weather_description(
                                    hourly["weathercode"][h]),
                wind_speed        = float(
                                    hourly["windspeed_10m"][h]),
                humidity          = int(
                                    hourly["relativehumidity_2m"][h]),
                fetched_at        = datetime.now().strftime(
                                    "%Y-%m-%d %H:%M:%S")
            ))

        tag = "LIVE" if is_today else "HISTORICAL"
        print(f"  ✓ [{tag}] {city} | {date_str} | 24 hrs fetched")
        return records

    except Exception as e:
        print(f"  ✗ {city} | {date_str} failed: {e}")
        for h in range(24):
            records.append(Row(
                city              = city,
                order_date        = date_str,
                order_hour        = h,
                temperature       = 0.0,
                weather_condition = "Unknown",
                wind_speed        = 0.0,
                humidity          = 0,
                fetched_at        = datetime.now().strftime(
                                    "%Y-%m-%d %H:%M:%S")
            ))
        return records

print("City+date specific weather fetch loaded")

# COMMAND ----------

def fetch_usd_rate_for_date(date_str):
    """
    fetches exact INR to USD rate on a specific date
    uses frankfurter.app which supports
    free historical rates back to 1999
    """
    today = date.today().strftime("%Y-%m-%d")

    try:
        if date_str == today:
            # today's rate
            url = "https://api.frankfurter.app/latest?from=INR&to=USD"
        else:
            # historical rate for exact date
            url = f"https://api.frankfurter.app/{date_str}?from=INR&to=USD"

        response = requests.get(url, timeout=10)
        data     = response.json()

        if "rates" in data and "USD" in data["rates"]:
            rate = float(data["rates"]["USD"])
            tag  = "LIVE" if date_str == today else "HISTORICAL"
            print(f"✓ [{tag}] {date_str}: 1 INR = {rate:.6f} USD")
            return rate
        else:
            print(f"✗ {date_str}: rate not found, using fallback")
            return 0.012

    except Exception as e:
        print(f"✗ {date_str}: failed → {e}, using fallback")
        return 0.012

print("Currency function updated with frankfurter.app")

# COMMAND ----------

def save_weather_to_bronze(records, base_path):
    from delta.tables import DeltaTable

    weather_path = f"{base_path}/bronze/delta/weather/"

    if not records:
        print("No weather records to save")
        return

    weather_df = spark.createDataFrame(records)

    try:
        weather_table = DeltaTable.forPath(spark, weather_path)
        weather_table.alias("existing").merge(
            weather_df.alias("new"),
            """existing.city       = new.city
               AND existing.order_date = new.order_date
               AND existing.order_hour = new.order_hour"""
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        print(f"Weather merged → {len(records)} records")

    except:
        weather_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(weather_path)
        print(f"Weather table created → {len(records)} records")

    count = spark.read.format("delta").load(weather_path).count()
    print(f"Total in bronze/delta/weather/: {count} records")

print("Save weather function updated")

# COMMAND ----------

def save_currency_to_bronze(currency_records, base_path):
    """
    saves USD rates to bronze/delta/currency/
    uses merge so same date never duplicated
    live rate gets updated on each pipeline run
    """
    from delta.tables import DeltaTable

    currency_path = f"{base_path}/bronze/delta/currency/"

    if not currency_records:
        print("No currency records to save")
        return

    currency_df = spark.createDataFrame(currency_records)

    try:
        currency_table = DeltaTable.forPath(spark, currency_path)
        currency_table.alias("existing").merge(
            currency_df.alias("new"),
            """existing.base_currency   = new.base_currency
               AND existing.target_currency = new.target_currency
               AND existing.rate_date       = new.rate_date"""
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        print(f"Currency merged → {len(currency_records)} records")

    except:
        currency_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(currency_path)
        print(f"Currency table created → {len(currency_records)} records")

    count = spark.read.format("delta").load(currency_path).count()
    print(f"Total in bronze/delta/currency/: {count} records")

print("Save currency function loaded")

# COMMAND ----------

def run_api_enrichment(orders_df, base_path):
    """
    MASTER FUNCTION
    fetches weather and currency only for
    city+date combinations that actually
    exist in orders
    no wasted API calls
    """
    today = date.today().strftime("%Y-%m-%d")

    print("=" * 55)
    print("WEATHER ENRICHMENT")
    print("=" * 55)

    # step 1 → find missing city+date+hour combos
    print("\nChecking missing weather in Bronze...")
    missing_combos = get_missing_weather_combos(
                        orders_df, base_path)

    if missing_combos:
        # group by city+date pairs only
        # (not hour - we fetch all 24 hours per city+date)
        missing_city_dates = list(set([
            (str(row["city"]), str(row["order_date"]))
            for row in missing_combos
        ]))
        missing_city_dates.sort()

        print(f"\nUnique city+date pairs to fetch: "
              f"{len(missing_city_dates)}")
        print(f"(previously was fetching all cities "
              f"per date — now only what orders need)\n")

        all_weather_records = []

        for i, (city, date_str) in enumerate(missing_city_dates):
            print(f"[{i+1}/{len(missing_city_dates)}] ", end="")
            records = fetch_weather_for_city_date(city, date_str)
            all_weather_records.extend(records)

        print(f"\nSaving {len(all_weather_records)} records...")
        save_weather_to_bronze(all_weather_records, base_path)

    else:
        print("All weather already in Bronze ✓ no API calls needed")

    print("\n" + "=" * 55)
    print("CURRENCY ENRICHMENT")
    print("=" * 55)

    print("\nChecking missing USD rates in Bronze...")
    missing_dates = get_missing_currency_dates(
                        orders_df, base_path)

    if missing_dates:
        print(f"Fetching USD rates for {len(missing_dates)} dates...")
        currency_records = []

        for i, date_str in enumerate(missing_dates):
            print(f"[{i+1}/{len(missing_dates)}] ", end="")
            rate = fetch_usd_rate_for_date(date_str)
            currency_records.append(Row(
                base_currency   = "INR",
                target_currency = "USD",
                exchange_rate   = rate,
                rate_date       = date_str,
                fetched_at      = datetime.now().strftime(
                                  "%Y-%m-%d %H:%M:%S")
            ))

        save_currency_to_bronze(currency_records, base_path)
    else:
        print("All USD rates already in Bronze ✓ no API calls needed")

    print("\n" + "=" * 55)
    print("ENRICHMENT COMPLETE")
    print("=" * 55)

print("Master enrichment function updated")