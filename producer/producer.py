from opensky_api import OpenSkyApi, TokenManager, StateVector
import time
from kafka import KafkaProducer
import json

class FlightProducer:
    def __init__(self, topic='flights', frequency=400):
        #self.api = OpenSkyApi()
        self.address = "kafka"
        self.port = "9092"

        self.api = OpenSkyApi(token_manager=TokenManager.from_json_file("credentials.json"))
        self.topic = topic
        self.frequency = frequency
        self.producer = KafkaProducer(bootstrap_servers=[f'{self.address}:{self.port}'], api_version=(2, 8, 0), value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    def produce(self, flight: StateVector):
        try:
            flight_data = {
                'icao24': flight.icao24,
                'callsign': flight.callsign.strip() if flight.callsign else None,
                'origin_country': flight.origin_country,
                'time_position': flight.time_position,
                'last_contact': flight.last_contact,
                'longitude': flight.longitude,
                'latitude': flight.latitude,
                'baro_altitude': flight.baro_altitude,
                'on_ground': flight.on_ground,
                'velocity': flight.velocity,
                'true_track': flight.true_track,
                'vertical_rate': flight.vertical_rate,
                'sensors': flight.sensors,
                'geo_altitude': flight.geo_altitude,
                'squawk': flight.squawk,
                'spi': flight.spi,
                'position_source': flight.position_source
            }
            self.producer.send(self.topic, value=flight_data, key=flight.icao24.encode('utf-8'))
            print(f"INFO | Flight data sent to Kafka topic '{self.topic}'.")
            return True
        except Exception as e:
            print(f"ERROR | Failed to send flight data to Kafka topic '{self.topic}': {e}")
            return False

    def get_flights(self):
        flights = []
        s = self.api.get_states()
        # s = self.api.get_states(bbox=(34.727404, 72.137679, -28.909987, 47.538162))
    
        if s is not None:        
            for state in s.states:
                if state.latitude is not None and state.longitude is not None:
                    latitude, longitude = state.latitude, state.longitude
                    print(latitude, longitude)
                    if (34.727404 <= latitude <= 72.137679) and (-28.909987 <= longitude <= 47.538162):
                        flights.append(state)
        return flights

if __name__ == "__main__":
    app_producer = FlightProducer()
    while True:
        print("INFO | Request to OpenSky API sent...")
        flights = app_producer.get_flights()
        if flights:
            print(f"INFO | {len(flights)} flights found.")
            for flight in flights:
                app_producer.produce(flight)
        else: print(f"ERROR | flights not found.")
        print(f"INFO | Sleeping for {app_producer.frequency} seconds.")
        time.sleep(app_producer.frequency)
