import psycopg2
import board
import adafruit_dht
import datetime
import time
import math

class Sensor:
    def __init__(self, id, room, pin = board.D17, update_intervall = 30, db_connection = None, number_outputs=3,
                 sensor_desc=["Outside Temp", "Humidity", "VPD"], output_unit = ["C", "%", "kPa"]):
        self.metadata = {"id": id, "room": room}
        self.pin = pin
        self.update_intervall = update_intervall
        self.db_connection = db_connection
        self.number_outputs = number_outputs
        self.sensor_desc = sensor_desc
        self.output_unit = output_unit
        if db_connection is not None:
            self.cursor = db_connection.cursor()


    def register(self):
        self.dhtDevice = adafruit_dht.DHT11(self.pin)

    def calc_vpd(self, temp, humidity):
        e_s = 610.78*math.exp(temp / (temp + 237.3) * 17.2694)
        vpd = (e_s*(1 - humidity/100))/1000
        return vpd
    
    def read(self):
        temp =  round(self.dhtDevice.temperature, 4)
        humidity =  round(self.dhtDevice.humidity, 4)
        vpd = round(self.calc_vpd(temp, humidity), 4)
        return temp, humidity, vpd
    
    def format_reading(self, data):
        data_formatted = []
        
        for i in range(self.number_outputs):
            entry_formatted = {
                "Sensor": self.sensor_desc[i],
                "Unit": self.output_unit[i], 
                "Value": data[i],
                "Timestamp": datetime.datetime.now()
            }
            data_formatted.append(entry_formatted)
            
        return data_formatted

    def exit(self):
        self.dhtDevice.exit()

    def write_to_db(self, data_formatted):
        if self.db_connection is not None:
            for i in range(self.number_outputs):
                sql = "INSERT INTO sensor_data (sensor_id, room, sensor_type, value, unit, timestamp) VALUES (%s, %s, %s, %s, %s, %s)"
                values = (self.metadata['id'], self.metadata['room'], data_formatted[i]['Sensor'], data_formatted[i]['Value'], data_formatted[i]['Unit'], data_formatted[i]['Timestamp'])
                self.cursor.execute(sql, values)
                self.db_connection.commit()
        else:
            for i in range(self.number_outputs):
                values = (self.metadata['id'], self.metadata['room'], data_formatted[i]['Sensor'], data_formatted[i]['Value'], data_formatted[i]['Unit'], data_formatted[i]['Timestamp'])
                print(values)
                    
    def run(self):
        self.register()        
        while True:
            try:
                data = self.read()
                data_formatted = self.format_reading(data)
                self.write_to_db(data_formatted)
            except RuntimeError as error:
                # Errors happen fairly often, DHT's are hard to read, just keep going
                print(error.args[0])
                time.sleep(0.1)
                continue
            except Exception as error:
                self.dhtDevice.exit()
                raise error

            time.sleep(self.update_intervall)

if __name__ == "__main__":
    # Connect to PostgreSQL database
    db_connection = psycopg2.connect(
            host="212.132.126.172",
            port="5488",
            user="my_data_wh_user",
            password="my_data_wh_pwd",
            database="my_data_wh_db"
        )

    sensor = Sensor(13, "Mutterraum", db_connection=db_connection)
    sensor.run()
