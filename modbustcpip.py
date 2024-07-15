import time
import threading
from pyModbusTCP.client import ModbusClient
from datetime import datetime
import pandas as pd
import os, sys
import json
import paho.mqtt.client as mqtt

is_raspberry = 1

if is_raspberry:
    header_topic_mqtt = '/home/'
else:
    header_topic_mqtt = '/home/'

lock = threading.Lock()

#Tạo các ngắt với các task khi bị mất kết nối ethernet
reConEth_flg = threading.Event()
enStoreData_flg = threading.Event()
connectWifi_flg = threading.Event()
connectPLC_flg = threading.Event()
thread_readdata_0_interupt = threading.Event()
thread_readdata_1_interupt = threading.Event()
thread_readdata_2_interupt = threading.Event()
thread_readdata_int_interupt = threading.Event()
thread_readdata_real_interupt = threading.Event()

thread_readdata_0_interupt.set()
thread_readdata_1_interupt.set()
thread_readdata_2_interupt.set()
thread_readdata_int_interupt.set()
thread_readdata_real_interupt.set()

connectPLC_flg.clear()
connectWifi_flg.clear()

# Cac bien input, data_input_bit_0
data_input_0 = pd.read_csv(header_topic_mqtt + 'pi/data_input_bit_0.csv', index_col=0)
data_input_index_0 = [*data_input_0['Index']]
data_input_name_0 = [*data_input_0['English Name']]
DATA_INPUT_LENGTH_0 = len(data_input_index_0)
data_input_old_0 = [-1]*DATA_INPUT_LENGTH_0
#---------------------------------------------------------------------------
# Cac bien input, data_input_bit_1
data_input_1 = pd.read_csv(header_topic_mqtt + 'pi/data_input_bit_1.csv', index_col=0)
data_input_index_1 = [*data_input_1['Index']]
data_input_name_1 = [*data_input_1['English Name']]
DATA_INPUT_LENGTH_1 = len(data_input_index_1)
data_input_old_1 = [-1]*DATA_INPUT_LENGTH_1

# Cac bien input, data_input_bit_2
data_input_2 = pd.read_csv(header_topic_mqtt + 'pi/data_input_bit_2.csv', index_col=0)
data_input_index_2 = [*data_input_2['Index']]
data_input_name_2 = [*data_input_2['English Name']]
DATA_INPUT_LENGTH_2 = len(data_input_index_2)
data_input_old_2 = [-1]*DATA_INPUT_LENGTH_2

# Cac bien so nguyen, data_input_int
data_input = pd.read_csv(header_topic_mqtt + 'pi/data_input_int.csv', index_col=0)
data_input_index = [*data_input['Index']]
data_input_id = [*data_input['Address']]
data_input_name = [*data_input['English Name']]
DATA_INPUT_LENGTH = len(data_input_id)
data_input_old = [-1]*DATA_INPUT_LENGTH

# Cac bien so thuc, data_input_real
data_input_real = pd.read_csv(header_topic_mqtt + "pi/data_input_real.csv", index_col=0)
data_input_index_real = [*data_input_real['Index']]
data_input_id_real = [*data_input_real['Address']]
data_input_name_real = [*data_input_real['English Name']]
DATA_INPUT_LENGTH_REAL = len(data_input_id_real)
data_input_old_real = [-1]*DATA_INPUT_LENGTH_REAL

#is_connectWifi = 0
status_old = -1
initRunSt = 1
runStTimestamp = None
onStTimestamp = None
is_connectWifi = 0 #thêm
is_connectPLC = 0 #thêm

#---------------------------------------------------------------------------
def create_excel_file(file_name):
    try:
        with open(header_topic_mqtt + 'pi/' + file_name) as store_data:
            pass
    except FileNotFoundError:
        with open(header_topic_mqtt + 'pi/' + file_name,'w+') as store_data:
            store_data.write('{0},{1},{2},{3},{4},{5},{6}\n'.format('No.','ID','Name Variable','Value','Timestamp','Kind of Data','Connected Wifi'))

save_name_path = header_topic_mqtt + 'pi/' + 'saved_name_filed.txt'
try:
    with open(save_name_path) as store_data:
        name_file = store_data.read()
except FileNotFoundError:
    timestamp_ = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
    name_file = f"stored_data_{timestamp_}.csv"
    create_excel_file(name_file)

    with open(save_name_path, "w") as store_data:
        store_data.write(name_file)
    pass

#------------------------------------------------------------------
def check_log_size(max_size_bytes=500):
    global header_topic_mqtt
    """
    Kiểm tra dung lượng của file log và tạo file log mới nếu vượt quá dung lượng tối đa cho phép.

    Parameters:
    log_file_path (str): Đường dẫn đến file log.
    max_size_bytes (int): Dung lượng tối đa cho phép, mặc định là 500 MB.

    Returns:
    bool: True nếu file log đã được thay thế, False nếu không.
    """
    log_file_path = header_topic_mqtt + 'pi/' + 'stored_data.csv'
    max_size_bytes = max_size_bytes*1024*1024

    if os.path.exists(log_file_path):
        # Kiểm tra dung lượng file log hiện tại
        log_size = os.path.getsize(log_file_path)
        if log_size >= max_size_bytes:
            # Tạo tên file mới dựa trên thời gian
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            new_log_file_path = f"{log_file_path}_{timestamp}.txt"
            # Mở file cũ và đọc nội dung
            with open(new_log_file_path,'w+') as store_data:
                store_data.write('{0},{1},{2},{3},{4},{5},{6}\n'.format('No.','ID','Name Variable','Value','Timestamp','Kind of Data','Connected Wifi'))
            # Xóa nội dung của file cũ
            with open(log_file_path, "w"):
                pass
            print(f"Đã xóa nội dung của file log cũ: {log_file_path}")
            return True
    return False

#----------------------------------------------------------

#-------------------------------------------------------------
# Khởi tạo Modbus TCP Client
__HOST='20.20.20.65' #IP của PLC
__PORT=502
__UNIT_ID=1
count = 0
while True:
    if is_raspberry:
        res = os.system('ping -c 1 ' + str(__HOST) + ' > /dev/null 2>&1')
    else:
        res = os.system('ping -n 1 ' + str(__HOST) + ' > nul')
    time.sleep(1)
    if res == 0:
        print('Connected to the ip')
        is_connectPLC = 1
        connectPLC_flg.clear()
        break
    else:
        count +=1
        print(count, "Can't connect to PLC")
        if count == 10:
            print(f'Raspberry was disconnected to PLC! --> Please Reset Raspberry again!')
            connectPLC_flg.set()
            break
try:
    modbus_client = ModbusClient(host=__HOST,port=__PORT, unit_id=__UNIT_ID, auto_open=True )
except Exception as e:
    print(e)


#---------- Generate Json Payload -------------------------------
def generate_data_status(state, value):
    data = [{
                'name': 'machineStatus',
                'value': value,
                'timestamp': datetime.now().isoformat(timespec='microseconds')
    }]
    return (json.dumps(data))


def generate_data(data_name, data_value):
    data = {
                'name': str(data_name),
                'value': data_value,
                'timestamp': datetime.now().isoformat(timespec='microseconds')
    }
    return (json.dumps(data))


def generate_data_disconnectWifi(data_name, data_value, timestamp):
    data = [{
                'name': str(data_name),
                'value': data_value,
                'timestamp': timestamp
    }]
    return (json.dumps(data))

#----------------------Option Functions-----------------------
def restart_program():
    python = sys.executable
    os.execl(python,python, *sys.argv)
#-------------------------------------------------------------
def restart_raspberry():
    os.system('sudo reboot')
    
topic_standard = 'TestModbusTCPIP/'
# topic_standard = 'Test/HC001/Metric/'
def store_and_pubish_data(No, Name, RealValue, KindOfData):
    global is_connectWifi, name_file
    with lock:
        Timestamp = datetime.now().isoformat(timespec='microseconds')
        with open(header_topic_mqtt + 'pi/' + name_file,'a+') as store_data:
            store_data.write('{0},{1},{2},{3},{4},{5}\n'.format(No, Name, RealValue, Timestamp, KindOfData, is_connectWifi))
        data = generate_data(Name, RealValue)
        print(data)
        mqtt_topic = topic_standard + str(Name)
        client.publish(mqtt_topic,str(data),1,1)
        if is_connectWifi:
            # print(f'{No}: ',Device, Name, RealValue)
            pass
        else:
            print('LOG ->', f'{No}: ', Name, RealValue)
            with open(header_topic_mqtt + 'pi/stored_disconnectWifi_data.txt', 'a+') as file:
                file.write(str(data)+'\n')

def store_and_publish_status(No, nameStatus, IDStatus):
    global is_connectWifi, topic_standard
    with lock:
        Timestamp = datetime.now().isoformat(timespec='microseconds')
        with open(header_topic_mqtt + 'pi/' + name_file,'a+') as store_data:
            store_data.write('{0},{1},{2},{3},{4},{5}\n'.format(No, nameStatus, nameStatus, IDStatus, Timestamp, 'MachineStatus', is_connectWifi))
        data = str(generate_data_status(nameStatus, IDStatus))
        mqtt_topic = topic_standard + "Status/machineStatus"
        client.publish(mqtt_topic,data,1,1)
        print(data)

        if not is_connectWifi:
            with open(header_topic_mqtt + 'pi/stored_disconnectWifi_data.txt', 'a+') as file:
                file.write(data+'\n')
# --------------------------- Setup MQTT -------------------------------------
# Define MQTT call-back function
def on_connect(client, userdata, flags, rc):
    global status_old, is_connectWifi, initRunSt, onStTimestamp, runStTimestamp
    print('Connected to MQTT broker with result code ' + str(rc))

    if status_old == 0:
        client.publish(topic_standard + 'Status/machineStatus', str(generate_data_status('On', 0)),1,1)
    elif status_old == 1:
        client.publish(topic_standard + 'Status/machineStatus',str(generate_data_status('Run', 1)),1,1)
    elif status_old == 2:
        client.publish(topic_standard + 'Status/machineStatus',str(generate_data_status('Idle', 2)),1,1)
    elif status_old == 3:
        client.publish(topic_standard + 'Status/machineStatus',str(generate_data_status('Alarm', 3)),1,1)
    elif status_old == 4:
        client.publish(topic_standard + 'Status/machineStatus',str(generate_data_status('Setup', 4)),1,1)
    elif status_old == 5:
        client.publish(topic_standard + 'Status/machineStatus',str(generate_data_status('OFF', 5)),1,1)
    elif status_old == 6:
        client.publish(topic_standard + 'Status/machineStatus',str(generate_data_status('Ready', 6)),1,1)
    elif status_old == 7:
        client.publish(topic_standard + 'Status/machineStatus',str(generate_data_status('Wifi disconnect', 7)),1,1)

    if onStTimestamp != None:
        client.publish(topic_standard + 'Status/machineStatus', str(onStTimestamp), 1, 1)
        print(onStTimestamp)
        onStTimestamp = None
        
    if runStTimestamp != None:
        client.publish(topic_standard + 'Status/machineStatus', str(runStTimestamp), 1, 1)
        print(runStTimestamp)
        runStTimestamp = None
    
    initRunSt = 0
    is_connectWifi = 1
    connectWifi_flg.clear()


def on_disconnect(client, userdata, rc):
    global is_connectWifi
    if rc != 0:
        print('Unexpected disconnection from MQTT broker')
        is_connectWifi = 0
        connectWifi_flg.set()
        
mqttBroker = '52.141.29.70'  # cloud
mqttPort = 1883
mqttKeepAliveINTERVAL = 45

# Initiate Mqtt Client
client = mqtt.Client()
# if machine is immediately turned off --> last_will sends 'machineStatus: Off' to topic
# client.will_set(topic_standard + 'Status/machineStatus',str(generate_data_status('Off', ST.Off)),1,1)
# Register callback function
client.on_connect = on_connect
client.on_disconnect = on_disconnect
# Connect with MQTT Broker
print('connecting to broker ',mqttBroker)
# Check connection to MQTT Broker 
try:
	client.connect(mqttBroker, mqttPort, mqttKeepAliveINTERVAL)
except:
    print("Can't connect MQTT Broker!")

client.loop_start()
time.sleep(1)

#--------------------------------------------------------------------------
def read_data_0():
    count = 0
    try:
        address = 0
        with lock:
            read_result = modbus_client.read_holding_registers(address, 1)
        binary_value_read = int(read_result[0])
        binary_value = bin(binary_value_read)[2:]
        binary_value = binary_value.zfill(16)
        print(binary_value)
        binary_list = [int(bit) for bit in binary_value]
        for i, bit in enumerate(reversed(binary_list)):
            Name=data_input_name_0[i]
            store_and_pubish_data(count, Name, bit, 'bit')
            data_input_old_0[i] = bit
            count+=1
    except Exception as e:
        print('task_input_bit_0')
        print(e)

    while True:
        thread_readdata_0_interupt.wait()
        try:
            time.sleep(0.01)
            address = 0
            with lock:
                read_result = modbus_client.read_holding_registers(address, 1) # Đọc giá trị của thanh ghi
            binary_value_read = int(read_result[0])
            binary_value = bin(binary_value_read)[2:]
            binary_value = binary_value.zfill(16)
            binary_list = [int(bit) for bit in binary_value]
            for i, bit in enumerate(reversed(binary_list)):
                Name=data_input_name_0[i]
                if data_input_old_0[i] != bit:
                    store_and_pubish_data(count, Name, bit, 'bit')
                    data_input_old_0[i] = bit
                    count+=1
        except Exception as e:
            print('task_input_bit_0')
            print(e)

def read_data_1():
    count = 0
    try:
        address = 1
        with lock:
            read_result = modbus_client.read_holding_registers(address, 1)
        binary_value_read = int(read_result[0])
        binary_value = bin(binary_value_read)[2:]
        binary_value = binary_value.zfill(16)
        print(binary_value)
        binary_list = [int(bit) for bit in binary_value]
        for i, bit in enumerate(reversed(binary_list)):
            Name=data_input_name_1[i]
            store_and_pubish_data(count, Name, bit, 'bit')
            data_input_old_1[i] = bit
            count+=1
    except Exception as e:
        print('task_input_bit_1')
        print(e)

    while True:
        thread_readdata_1_interupt.wait()
        try:
            time.sleep(0.01)
            address = 1
            with lock:
                read_result = modbus_client.read_holding_registers(address, 1) # Đọc giá trị của thanh ghi
            binary_value_read = int(read_result[0])
            binary_value = bin(binary_value_read)[2:]
            binary_value = binary_value.zfill(16)
            binary_list = [int(bit) for bit in binary_value]
            for i, bit in enumerate(reversed(binary_list)):
                Name=data_input_name_1[i]
                if data_input_old_1[i] != bit:
                    store_and_pubish_data(count, Name, bit, 'bit')
                    data_input_old_1[i] = bit
                    count+=1
        except Exception as e:
            print('task_input_bit_1')
            print(e)
def read_data_2():
    count = 0
    try:
        address = 2
        with lock:
            read_result = modbus_client.read_holding_registers(address, 1)
        binary_value_read = int(read_result[0])
        binary_value = bin(binary_value_read)[2:]
        binary_value = binary_value.zfill(16)
        print(binary_value)
        binary_list = [int(bit) for bit in binary_value]
        for i, bit in enumerate(reversed(binary_list)):
            Name=data_input_name_2[i]
            store_and_pubish_data(count, Name, bit, 'bit')
            data_input_old_2[i] = bit
            count+=1
    except Exception as e:
        print('task_input_bit_2')
        print(e)

    while True:
        thread_readdata_2_interupt.wait()
        try:
            time.sleep(0.01)
            address = 2
            with lock:
                read_result = modbus_client.read_holding_registers(address, 1) # Đọc giá trị của thanh ghi
            binary_value_read = int(read_result[0])
            binary_value = bin(binary_value_read)[2:]
            binary_value = binary_value.zfill(16)
            binary_list = [int(bit) for bit in binary_value]
            for i, bit in enumerate(reversed(binary_list)):
                Name=data_input_name_2[i]
                if data_input_old_2[i] != bit:
                    store_and_pubish_data(count, Name, bit, 'bit')
                    data_input_old_2[i] = bit
                    count+=1
        except Exception as e:
            print('task_input_bit_2')
            print(e)
def read_data_int():
    count = 0
    try:
        for i in range(DATA_INPUT_LENGTH):
            address = data_input['Address'].values[i]
            with lock:
                read_result = modbus_client.read_holding_registers(address, 1)
            RealValue = int(read_result[0])
            Name = data_input_name[i]
            store_and_pubish_data(count, Name, RealValue, 'int')
            data_input_old[i] = RealValue
            count+=1
    except Exception as e:
        print('task_input_int')
        print(e)

    while True:
        thread_readdata_int_interupt.set()
        for i in range(DATA_INPUT_LENGTH):
            time.sleep(0.01)
            try:
                address = data_input['Address'].values[i]
                with lock:
                    read_result = modbus_client.read_holding_registers(address, 1) # Đọc giá trị của thanh ghi
                RealValue = int(read_result[0])
                Name = data_input_name[i]
                if data_input_old[i] != RealValue:
                    store_and_pubish_data(count, Name, RealValue, 'int')
                    data_input_old[i] = RealValue
                    count+=1
                    
            except Exception as e:
                print('task_input_int')
                print(e)
def read_data_real():
    count = 0
    try:
        for i in range(DATA_INPUT_LENGTH_REAL):
            address = data_input_real['Address'].values[i]
            with lock:
                read_result = modbus_client.read_holding_registers(address, 1)
            if address == 38 or address == 39 or address == 40 or address == 41 or address == 50 or address == 51 or address == 52: #Quá tầm INT nên giữ nguyên
                RealValue = int(read_result[0])
            else:
                RealValue = int(read_result[0])/100
            Name = data_input_name_real[i]
            store_and_pubish_data(count, Name, RealValue, 'real')
            data_input_old_real[i] = RealValue
            count+=1
    except Exception as e:
        print('task_input_real')
        print(e)

    while True:
        thread_readdata_real_interupt.wait()
        for i in range(DATA_INPUT_LENGTH_REAL):
            time.sleep(0.01)
            try:
                address = data_input_real['Address'].values[i]
                with lock:
                    read_result = modbus_client.read_holding_registers(address, 1) # Đọc giá trị của thanh ghi
                if address == 38 or address == 39 or address == 40 or address == 41 or address == 50 or address == 51 or address == 52:
                    RealValue = int(read_result[0])
                else:
                    RealValue = int(read_result[0])/100
                Name = data_input_name_real[i]
                if data_input_old_real[i] != RealValue:
                    store_and_pubish_data(count, Name, RealValue, 'int')
                    data_input_old_real[i] = RealValue
                    count+=1
                    
            except Exception as e:
                print('task_input_real')
                print(e)

def task_check_log_size():
    global header_topic_mqtt, name_file, save_name_path
    """
    Kiểm tra dung lượng của file log và tạo file log mới nếu vượt quá dung lượng tối đa cho phép.
    """
    max_size_bytes=75
    log_file_path = header_topic_mqtt + 'pi/' + name_file
    max_size_bytes = max_size_bytes*1024*1024

    while True:
        if os.path.exists(log_file_path):
            # Kiểm tra dung lượng file log hiện tại
            log_size = os.path.getsize(log_file_path)
            # print(f"Log file size: {log_size} bytes")
            if log_size >= max_size_bytes:
                # Tạo tên file mới dựa trên thời gian
                timestamp_ = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
                with lock:
                    name_file = f"stored_data_{timestamp_}.csv"
                    create_excel_file(name_file)
                # Xóa nội dung của file cũ
                with open(save_name_path, "w") as file:
                    file.write(name_file)

                log_file_path = header_topic_mqtt + 'pi/' + name_file
        time.sleep(5)

def task_board_IIOT_gateway_status_isconnectPLC():
    """
        is_connectPLC = 0   ---->   Red Led = 1
    """
    global is_connectPLC, is_connectWifi

    old_status_connectPLC = -1
    old_status_connectWifi = -1 
    while True:
        if (old_status_connectPLC != is_connectPLC) or (old_status_connectWifi != is_connectWifi):
            data = generate_data('isConnectPLC', is_connectPLC)
            with lock:
                client.publish(topic_standard + 'Status/isConnectPLC', data, 1, 1)
            old_status_connectPLC = is_connectPLC
            old_status_connectWifi = is_connectWifi

        connectPLC_flg.wait()

#---------------------------------------------------------------------------
if __name__ == '__main__':
    thread_readdata_0 = threading.Thread(target=read_data_0)
    thread_readdata_1 = threading.Thread(target=read_data_1)
    thread_readdata_2 = threading.Thread(target=read_data_2)
    thread_readdata_int = threading.Thread(target=read_data_int)
    thread_readdata_real = threading.Thread(target=read_data_real)
    thread_readdata_0.start()
    thread_readdata_1.start()
    thread_readdata_2.start()
    thread_readdata_int.start()
    thread_readdata_real.start()
    