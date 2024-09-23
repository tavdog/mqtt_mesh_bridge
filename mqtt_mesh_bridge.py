"""
Based on Meshtastic MQTT Connect Version 0.7.1 by https://github.com/pdxlocations
Powered by Meshtastic™ https://meshtastic.org/
"""

import paho.mqtt.client as mqtt
from meshtastic import mesh_pb2, mqtt_pb2, portnums_pb2, telemetry_pb2
import random
import threading
import sqlite3
import time
from datetime import datetime
from time import mktime
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
import base64
import json
import re

debug = True

# set key to "" for no encryption
key = "AQ=="
# key = ""
node_name = '!A3252978' # node id of the sending node, make something up, hopefully it's unique.

# Convert hex to int and remove '!'
node_number = int(node_name.replace("!", ""), 16)

mesh_client_short_name = "Kwind"
mesh_client_long_name = "Kwind Mesh Sender"
client_hw_model = 255
node_info_interval_minutes = 60
# ### Mesh Mqtt settings
# mesh_broker = "mqtt.meshtastic.org"
# mesh_port = 1883
# mesh_username = "meshdev"
# mesh_password = "large4cats"
# mesh_root_topic = "msh/EU_868/2/e/"
# mesh_channel = "Kwind"
### Mesh Mqtt settings
mesh_broker = "152.53.16.228"
mesh_port = 1883
mesh_username = ""
mesh_password = ""
mesh_root_topic = "msh/"
mesh_channel = "Kwind"
############################

### OMG MQTT settings

omg_server = "152.53.16.228"

# set the source topic, assumed to be plaintext json
# omg_subscribe_topic = "home/OMG_lilygo_rtl_433_ESP/RTL_433toMQTT/Fineoffset-WS80/10954"
omg_subscribe_topic = "crypto/bitcoin"

# define the fields from the json you want or set to None to forward entire message body
#omg_fields_tx = ["model","wind_dir_deg","wind_avg_m_s","wind_max_m_s","temperature_C"]
omg_fields_tx = None


mesh_client = mqtt.Client(client_id="kwind_MESHTX", clean_session=True, userdata=None)
omg_client = mqtt.Client(client_id="kwind_OMGRX", clean_session=True, userdata=None)

#################################
### Program variables

default_key = "1PG7OiApB1nwvP+rz05pAQ==" # AKA AQ==
broadcast_id = 4294967295


#################################
# Program Base Functions
    
def set_topic():
    if debug: print("set_topic")
    global subscribe_topic, publish_topic, node_number, node_name
    node_name = '!' + hex(node_number)[2:]
    subscribe_topic = mesh_root_topic + mesh_channel + "/#"
    publish_topic = mesh_root_topic + mesh_channel + "/" + node_name

def current_time():
    current_time_seconds = time.time()
    current_time_struct = time.localtime(current_time_seconds)
    current_time_str = time.strftime("%H:%M:%S", current_time_struct)
    return(current_time_str)

def xor_hash(data):
    result = 0
    for char in data:
        result ^= char
    return result

def generate_hash(name, key):
    replaced_key = key.replace('-', '+').replace('_', '/')
    key_bytes = base64.b64decode(replaced_key.encode('utf-8'))
    h_name = xor_hash(bytes(name, 'utf-8'))
    h_key = xor_hash(key_bytes)
    result = h_name ^ h_key
    return result

def get_short_name_by_id(user_id):
    try:
        table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(mesh_channel) + "_nodeinfo"
        with sqlite3.connect(db_file_path) as db_connection:
            db_cursor = db_connection.cursor()
    
            # Convert the user_id to hex and prepend '!'
            hex_user_id = '!' + hex(user_id)[2:]

            # Fetch the short name based on the hex user ID
            result = db_cursor.execute(f'SELECT short_name FROM {table_name} WHERE user_id=?', (hex_user_id,)).fetchone()

            if result:
                return result[0]
            # If we don't find a user id in the db, ask for an id
            else:
                if user_id != broadcast_id:
                    if debug: print("didn't find user in db")
                    send_node_info(user_id)  # DM unknown user a nodeinfo with want_response
                return f"Unknown User ({hex_user_id})"
    
    except sqlite3.Error as e:
        print(f"SQLite error in get_short_name_by_id: {e}")
    
    finally:
        db_connection.close()

def sanitize_string(input_str):
    # Check if the string starts with a letter (a-z, A-Z) or an underscore (_)
    if not re.match(r'^[a-zA-Z_]', input_str):
        # If not, add "_"
        input_str = '_' + input_str

    # Replace special characters with underscores (for database tables)
    sanitized_str = re.sub(r'[^a-zA-Z0-9_]', '_', input_str)
    return sanitized_str

#################################
# Receive Messages

def on_message(client, userdata, msg):
    # if debug: print("on_message")
    se = mqtt_pb2.ServiceEnvelope()
    is_encrypted = False
    try:
        se.ParseFromString(msg.payload)
        if print_service_envelope:
            print ("")
            print ("Service Envelope:")
            print (se)
        mp = se.packet
        if print_message_packet: 
            print ("")
            print ("Message Packet:")
            print(mp)
    except Exception as e:
        print(f"*** ParseFromString: {str(e)}")
        return
    
    if mp.HasField("encrypted") and not mp.HasField("decoded"):
        decode_encrypted(mp)
        is_encrypted=True

    if mp.decoded.portnum == portnums_pb2.TEXT_MESSAGE_APP:
        text_payload = mp.decoded.payload.decode("utf-8")
        process_message(mp, text_payload, is_encrypted)
        # print(f"{text_payload}")
        
    elif mp.decoded.portnum == portnums_pb2.NODEINFO_APP:
        info = mesh_pb2.User()
        info.ParseFromString(mp.decoded.payload)
        maybe_store_nodeinfo_in_db(info)
        if print_node_info:
            print("")
            print("NodeInfo:")
            print(info)
        
    elif mp.decoded.portnum == portnums_pb2.POSITION_APP:
        pos = mesh_pb2.Position()
        pos.ParseFromString(mp.decoded.payload)
        if record_locations:
            maybe_store_position_in_db(getattr(mp, "from"), pos)

    elif mp.decoded.portnum == portnums_pb2.TELEMETRY_APP:
        env = telemetry_pb2.Telemetry()
        env.ParseFromString(mp.decoded.payload)

        # Device Metrics
        device_metrics_dict = {
            'Battery Level': env.device_metrics.battery_level,
            'Voltage': round(env.device_metrics.voltage, 2),
            'Channel Utilization': round(env.device_metrics.channel_utilization, 1),
            'Air Utilization': round(env.device_metrics.air_util_tx, 1)
        }
        # Environment Metrics
        environment_metrics_dict = {
            'Temp': round(env.environment_metrics.temperature, 2),
            'Humidity': round(env.environment_metrics.relative_humidity, 0),
            'Pressure': round(env.environment_metrics.barometric_pressure, 2),
            'Gas Resistance': round(env.environment_metrics.gas_resistance, 2)
        }
        # Power Metrics
            # TODO
        # Air Quality Metrics
            # TODO

        if print_telemetry: 

            device_metrics_string = "From: " + get_short_name_by_id(getattr(mp, "from")) + ", "
            environment_metrics_string = "From: " + get_short_name_by_id(getattr(mp, "from")) + ", "

            # Only use metrics that are non-zero
            has_device_metrics = True
            has_environment_metrics = True
            has_device_metrics = all(value != 0 for value in device_metrics_dict.values())
            has_environment_metrics = all(value != 0 for value in environment_metrics_dict.values())

            # Loop through the dictionary and append non-empty values to the string
            for label, value in device_metrics_dict.items():
                if value is not None:
                    device_metrics_string += f"{label}: {value}, "

            for label, value in environment_metrics_dict.items():
                if value is not None:
                    environment_metrics_string += f"{label}: {value}, "

            # Remove the trailing comma and space
            device_metrics_string = device_metrics_string.rstrip(", ")
            environment_metrics_string = environment_metrics_string.rstrip(", ")

            # Print or use the final string
            if has_device_metrics:
                print(device_metrics_string)
            if has_environment_metrics:
                print(environment_metrics_string)


def decode_encrypted(mp):
        
        try:
            # Convert key to bytes
            key_bytes = base64.b64decode(key.encode('ascii'))
      
            nonce_packet_id = getattr(mp, "id").to_bytes(8, "little")
            nonce_from_node = getattr(mp, "from").to_bytes(8, "little")

            # Put both parts into a single byte array.
            nonce = nonce_packet_id + nonce_from_node

            cipher = Cipher(algorithms.AES(key_bytes), modes.CTR(nonce), backend=default_backend())
            decryptor = cipher.decryptor()
            decrypted_bytes = decryptor.update(getattr(mp, "encrypted")) + decryptor.finalize()

            data = mesh_pb2.Data()
            data.ParseFromString(decrypted_bytes)
            mp.decoded.CopyFrom(data)

        except Exception as e:

            if print_message_packet: print(f"failed to decrypt: \n{mp}")
            if debug: print(f"*** Decryption failed: {str(e)}")
            return


def process_message(mp, text_payload, is_encrypted):
    if debug: print("process_message")
    if not message_exists(mp):
        from_node = getattr(mp, "from")
        to_node = getattr(mp, "to")
        sender_short_name = get_short_name_by_id(from_node)
        receiver_short_name = get_short_name_by_id(to_node)
        string = ""
        private_dm = False

        if to_node == node_number:
            string = f"{current_time()} DM from {sender_short_name}: {text_payload}"
            if display_dm_emoji: string = string[:9] + dm_emoji + string[9:]

        elif from_node == node_number and to_node != broadcast_id:
            string = f"{current_time()} DM to {receiver_short_name}: {text_payload}"
            
        elif from_node != node_number and to_node != broadcast_id:
            if display_private_dms:
                string = f"{current_time()} DM from {sender_short_name} to {receiver_short_name}: {text_payload}"
                if display_dm_emoji: string = string[:9] + dm_emoji + string[9:]
            else:
                if debug: print("Private DM Ignored")
                private_dm = True
            
        else:    
            string = f"{current_time()} {sender_short_name}: {text_payload}"

        if is_encrypted and not private_dm:
            color="encrypted"
            if display_encrypted_emoji: string = string[:9] + encrypted_emoji + string[9:]
        else:
            color="unencrypted"
        if not private_dm:
            update_gui(string, text_widget=message_history, tag=color)
        m_id = getattr(mp, "id")
        insert_message_to_db(current_time(), sender_short_name, text_payload, m_id, is_encrypted)

        text = {
            "message": text_payload,
            "from": getattr(mp, "from"),
            "id": getattr(mp, "id"),
            "to": getattr(mp, "to")
        }
        if print_text_message: 
            print("")
            print(text)
    else:
        if debug: print("duplicate message ignored")

# check for message id in db, ignore duplicates
def message_exists(mp):
    if debug: print("message_exists")
    try:
        table_name = sanitize_string(mqtt_broker) + "_" + sanitize_string(root_topic) + sanitize_string(mesh_channel) + "_messages"

        with sqlite3.connect(db_file_path) as db_connection:
            db_cursor = db_connection.cursor()

            # Check if a record with the same message_id already exists
            existing_record = db_cursor.execute(f'SELECT * FROM {table_name} WHERE message_id=?', (str(getattr(mp, "id")),)).fetchone()

            return existing_record is not None

    except sqlite3.Error as e:
        print(f"SQLite error in message_exists: {e}")

    finally:
        db_connection.close()

#################################
# Send Messages

def direct_message(destination_id):
    if debug: print("direct_message")
    destination_id = int(destination_id[1:], 16)
    publish_message(destination_id)


def publish_message(destination_id,message_text):
    global key
    if debug:
        # print("publish_message")
        # print(message_text)
        pass

    if message_text:
        encoded_message = mesh_pb2.Data()
        encoded_message.portnum = portnums_pb2.TEXT_MESSAGE_APP 
        encoded_message.payload = message_text.encode("utf-8")

    generate_mesh_packet(destination_id, encoded_message)


def generate_mesh_packet(destination_id, encoded_message):
    mesh_packet = mesh_pb2.MeshPacket()

    print("setting from to:" + str(node_number))
    setattr(mesh_packet, "from", node_number)
    mesh_packet.id = random.getrandbits(32)
    mesh_packet.to = destination_id
    mesh_packet.want_ack = False
    mesh_packet.channel = generate_hash(mesh_channel, key)
    mesh_packet.hop_limit = 3

    if key == "":
        mesh_packet.decoded.CopyFrom(encoded_message)
        if debug: print("key is none")
    else:
        mesh_packet.encrypted = encrypt_message(mesh_channel, key, mesh_packet, encoded_message)
        if debug: print("key present")

    service_envelope = mqtt_pb2.ServiceEnvelope()
    service_envelope.packet.CopyFrom(mesh_packet)
    service_envelope.channel_id = mesh_channel
    service_envelope.gateway_id = node_name
    # print (service_envelope)

    payload = service_envelope.SerializeToString()
    set_topic()
    print("publishing to topic:" + publish_topic)
    print(mesh_packet.decoded)
    mesh_client.publish(topic=publish_topic, payload=payload,qos=0,retain=True)


def encrypt_message(mesh_channel, key, mesh_packet, encoded_message):
    if debug: print("encrypt_message")

    mesh_packet.channel = generate_hash(mesh_channel, key)
    key_bytes = base64.b64decode(key.encode('ascii'))

    # print (f"id = {mesh_packet.id}")
    nonce_packet_id = mesh_packet.id.to_bytes(8, "little")
    nonce_from_node = node_number.to_bytes(8, "little")
    # Put both parts into a single byte array.
    nonce = nonce_packet_id + nonce_from_node

    cipher = Cipher(algorithms.AES(key_bytes), modes.CTR(nonce), backend=default_backend())
    encryptor = cipher.encryptor()
    encrypted_bytes = encryptor.update(encoded_message.SerializeToString()) + encryptor.finalize()

    return encrypted_bytes


#################################
# MQTT Server 
    
def mesh_connect_mqtt():
    if debug: print("mesh_coonect_mqtt")
    global mesh_broker, mesh_username, mesh_password, root_topic, mesh_channel, node_number, db_file_path, key
    if not mesh_client.is_connected():
        try:

            if key == "AQ==":
                if debug: print("key is default, expanding to AES128")
                key = "1PG7OiApB1nwvP+rz05pAQ=="

            node_number = int(node_number)  # Convert the input to an integer

            padded_key = key.ljust(len(key) + ((4 - (len(key) % 4)) % 4), '=')
            replaced_key = padded_key.replace('-', '+').replace('_', '/')
            key = replaced_key

            if debug: print (f"padded & replaced key = {key}")
            if mesh_username != "":
                mesh_client.username_pw_set(mesh_username, mesh_password)
            mesh_client.connect(mesh_broker, mesh_port, 120)
            mesh_client.loop_start()

        except Exception as e:
            print(f"{current_time()} >>> Failed to connect to MESH broker: {str(e)}")

def disconnect_mqtt():
    if debug: print("disconnect_mqtt")
    if mesh_client.is_connected():
        mesh_client.loop_stop()
        mesh_client.disconnect()

def on_connect(client, userdata, flags, reason_code):

    set_topic()
    
    if debug: print("on_connect")
    if debug: 
        if client.is_connected():
            print("client is connected")
    
    # if reason_code == 0:
    #     if debug: print(f"Subscribe Topic is: {subscribe_topic}")
    #     client.subscribe(subscribe_topic)
    

def omg_on_message(client, userdata, message):
    try:
        data = json.loads(str(message.payload.decode("utf-8")))
        print("Got Packet")
        print(data)
        max = dict()

        # Create a new dictionary with only the desired fields
        if omg_fields_tx:
            trimmed_data = {key: data[key] for key in omg_fields_tx}
        else:
            trimmed_data = data

        json_string = json.dumps(trimmed_data, indent=0)
        publish_message(broadcast_id,json_string)
            
    except Exception as e:
        print(e)
        exit(1)

mesh_client.on_connect = on_connect
omg_client.on_message = omg_on_message
mesh_connect_mqtt()
omg_client.connect(mesh_broker, mesh_port, 60)
omg_client.loop_start()
omg_client.subscribe(omg_subscribe_topic)


# Main Threads

while(True): # reconnect if not connected and all that
    if (not mesh_client.is_connected()) :
        print(f"Connecting to mesh server at {datetime.now()}")
        mesh_client.set
        mesh_client.connect(mesh_broker, mesh_port)
        mesh_client.loop_start()
        time.sleep(1)
        
    if (not omg_client.is_connected()):
        print("connecting to omg server")
        omg_client.connect(omg_server, 1883, 60)
        omg_client.loop_start()
        omg_client.subscribe(omg_subscribe_topic)
        time.sleep(1)

    time.sleep(.01)

disconnect_mqtt()