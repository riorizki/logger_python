import paho.mqtt.client as mqtt
import mysql.connector
import json

# Parsing JSON into dict


class Payload(object):
    def __init__(self, j):
        self.__dict__ = json.loads(j)


mydb = mysql.connector.connect(
    host="misaganiawsdb.chywzylq6qnb.ap-southeast-1.rds.amazonaws.com",
    user="misagani",
    passwd="QBurn8aws",
    database="calya_db"
)

mycursor = mydb.cursor()


def insert_data(data):
    dictData = Payload(data)

    sql = "INSERT INTO tb_log (calculated_engine_load, engine_rpm, intake_air_temperature, relative_throttle_position, throttle_position, vehicle_speed, warm_up_since_codes_cleared, timing_advance, time_since_trouble_codes_cleared, time_run_with_mil_on, short_term_fuel_bank, run_time_since_engine_start, oxygen_sensor_present, control_module_voltage, absolute_barometric_pressure, absolute_load_value, latitude, longitude, altitude, heading, speed_gps, timestamp, accX, accY, accZ, gyX, gyY, gyZ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    val = (dictData.CalculatedEngineLoad,
           dictData.EngineRpm,
           dictData.IntakeAirTemperature,
           dictData.RelativeThrottlePosition,
           dictData.ThrottlePosition,
           dictData.VehicleSpeed,
           dictData.WarmUpSinceCodesCleared,
           dictData.TimingAdvance,
           dictData.TimeSinceTroubleCodesCleared,
           dictData.TimeRunWithMilOn,
           dictData.ShortTermFuelBank,
           dictData.RunTimeSinceEngineStart,
           dictData.OxygenSensorPresent,
           dictData.ControlModuleVoltage,
           dictData.AbsoluteBarometricPressure,
           dictData.AbsoluteLoadValue,
           dictData.Latitude,
           dictData.Longitude,
           dictData.Altitude,
           dictData.Heading,
           dictData.SpeedGps,
           dictData.Timestamp,
           dictData.Heading,
           dictData.SpeedGps,
           dictData.Timestamp,
           dictData.Heading,
           dictData.SpeedGps,
           dictData.Timestamp,
           dictData.AccX,
           dictData.AccY,
           dictData.AccZ,
           dictData.GyX,
           dictData.GyY,
           dictData.GyZ)

    mycursor.execute(sql, val)
    mydb.commit()

    print(val)

    print(mycursor.rowcount, "record inserted.")


def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    # client.subscribe('$SYS/#')
    client.subscribe('phone_data')


# The callback for when a PUBLISH message is received from the server.


def on_message(client, userdata, msg):
    print('Messages arrived with topic ' + msg.topic +
          ' and message ' + str(msg.payload))
    topic = str(msg.topic)
    messages = str(msg.payload, 'utf-8')

    if topic == 'phone_data':
        print('we are on phone topic')
        insert_data(messages)


client = mqtt.Client()

client.on_connect = on_connect
client.on_message = on_message

client.connect('ec2-54-255-192-5.ap-southeast-1.compute.amazonaws.com', 5883)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
