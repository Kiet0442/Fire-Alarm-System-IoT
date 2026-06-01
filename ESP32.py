# =============================
# main_mqtt.py
# ESP32 MQTT + SENSOR FIRE ALARM + WEBSERVER + OFFLINE MODE
# =============================

import network
import time
from umqtt.simple import MQTTClient
import machine
from dht import DHT11
import socket
import gc
import json

# =============================
# 1. CẤU HÌNH WIFI & MQTT
# =============================
WIFI_SSID = "phuongmai24"
WIFI_PASS = "phuongmuaa"

MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "fire_alarm/control"
MQTT_STATUS_TOPIC = "fire_alarm/status"
MQTT_CLIENT_ID = "esp32_fire_alarm"

# =============================
# 2. CẤU HÌNH CHÂN
# =============================
LED_PIN = 2
BUZZER_PIN = 5
WIFI_LED_PIN = 17  # LED báo trạng thái WiFi (tùy chọn)
MQTT_LED_PIN = 16  # LED báo trạng thái MQTT (tùy chọn)

led = machine.Pin(LED_PIN, machine.Pin.OUT)
buzzer = machine.Pin(BUZZER_PIN, machine.Pin.OUT)
wifi_led = machine.Pin(WIFI_LED_PIN, machine.Pin.OUT) if WIFI_LED_PIN else None
mqtt_led = machine.Pin(MQTT_LED_PIN, machine.Pin.OUT) if MQTT_LED_PIN else None

# Khởi tạo tất cả LED
led.off()
buzzer.off()
if wifi_led:
    wifi_led.off()
if mqtt_led:
    mqtt_led.off()

# =============================
# 2.1. CẢM BIẾN
# =============================
mq135 = machine.ADC(machine.Pin(34))
mq135.atten(machine.ADC.ATTN_11DB)
mq135.width(machine.ADC.WIDTH_12BIT)

dht = DHT11(machine.Pin(4))  # GPIO4

# =============================
# 2.2. NGƯỠNG CẢNH BÁO & TIMER
# =============================
TEMP_THRESHOLD = 35  # °C
MQ135_THRESHOLD = 1500  # ADC
SENSOR_INTERVAL = 2  # giây
BUZZER_DURATION = 5  # giây cho còi kêu
WEBSERVER_PORT = 80  # Port webserver

# =============================
# 2.3. CẤU HÌNH KẾT NỐI LẠI
# =============================
WIFI_RETRY_INTERVAL = 10  # giây giữa các lần thử kết nối WiFi
MQTT_RETRY_INTERVAL = 5  # giây giữa các lần thử kết nối MQTT
MAX_WIFI_RETRIES = 3  # Số lần thử tối đa trước khi chuyển sang offline mode

# =============================
# 2.4. BIẾN TOÀN CỤC
# =============================
alarm_active = False
buzzer_start_time = 0
last_alarm_time = 0
ALARM_COOLDOWN = 10  # 10 giây chờ giữa các lần cảnh báo

# Trạng thái kết nối
connection_status = {
    "wifi_connected": False,
    "mqtt_connected": False,
    "offline_mode": True,
    "last_wifi_attempt": 0,
    "last_mqtt_attempt": 0,
    "wifi_retry_count": 0,
    "ip_address": "N/A"
}

# Dữ liệu cảm biến
sensor_data = {
    "temperature": 0,
    "humidity": 0,
    "gas": 0,
    "gas_level": "NORMAL",
    "temp_level": "NORMAL",
    "alarm_status": "SAFE",
    "last_update": "N/A",
    "system_mode": "OFFLINE"  # ONLINE/OFFLINE
}

# Lưu trữ tạm các MQTT message khi offline
mqtt_queue = []

# =============================
# 3. KẾT NỐI WIFI VỚI CHẾ ĐỘ TỰ ĐỘNG KẾT NỐI LẠI
# =============================
def connect_wifi(auto_reconnect=True):
    global connection_status
    
    # Nếu đang kết nối thì không làm gì
    if connection_status["wifi_connected"]:
        return True
    
    # Kiểm tra thời gian giữa các lần thử
    current_time = time.time()
    if auto_reconnect and (current_time - connection_status["last_wifi_attempt"] < WIFI_RETRY_INTERVAL):
        return False
    
    connection_status["last_wifi_attempt"] = current_time
    connection_status["wifi_retry_count"] += 1
    
    sta = network.WLAN(network.STA_IF)
    
    # Nếu interface chưa active thì active nó
    if not sta.active():
        sta.active(True)
        time.sleep(0.5)
    
    print(f"📡 Đang thử kết nối WiFi... (lần {connection_status['wifi_retry_count']})")
    
    # Nếu không kết nối thì thử kết nối
    if not sta.isconnected():
        try:
            sta.connect(WIFI_SSID, WIFI_PASS)
            
            # Chờ kết nối với timeout
            for i in range(15):  # 15 * 0.5 = 7.5 giây timeout
                if sta.isconnected():
                    break
                time.sleep(0.5)
                
        except Exception as e:
            print(f"❌ Lỗi kết nối WiFi: {e}")
    
    # Kiểm tra kết nối
    if sta.isconnected():
        ip_address = sta.ifconfig()[0]
        connection_status["wifi_connected"] = True
        connection_status["ip_address"] = ip_address
        connection_status["wifi_retry_count"] = 0
        sensor_data["ip_address"] = ip_address
        
        # Bật LED WiFi nếu có
        if wifi_led:
            wifi_led.on()
            
        print(f"✅ WiFi đã kết nối: {ip_address}")
        
        # Cập nhật chế độ
        sensor_data["system_mode"] = "ONLINE"
        
        return True
    else:
        connection_status["wifi_connected"] = False
        
        # Nhấp nháy LED WiFi nếu có
        if wifi_led:
            wifi_led.value(not wifi_led.value())
            
        print("❌ Không thể kết nối WiFi")
        
        # Nếu đã thử nhiều lần, chuyển sang offline mode
        if connection_status["wifi_retry_count"] >= MAX_WIFI_RETRIES:
            connection_status["offline_mode"] = True
            sensor_data["system_mode"] = "OFFLINE"
            print("🔌 Chuyển sang chế độ OFFLINE")
        
        return False

# =============================
# 4. QUẢN LÝ MQTT VỚI CHẾ ĐỘ OFFLINE
# =============================
def mqtt_callback(topic, msg):
    cmd = msg.decode().strip().upper()
    print(f"📥 MQTT CMD: {cmd}")

    if cmd == "FIRE":
        print("🔥 BÁO CHÁY THỦ CÔNG!")
        activate_alarm("MANUAL_FIRE", manual=True)
        queue_mqtt_message(MQTT_STATUS_TOPIC, "FIRE_MANUAL")

    elif cmd == "SAFE":
        print("✅ AN TOÀN")
        deactivate_alarm()
        queue_mqtt_message(MQTT_STATUS_TOPIC, "SAFE")

    elif cmd == "TEST":
        print("🔊 KIỂM TRA THIẾT BỊ")
        test_devices()
        queue_mqtt_message(MQTT_STATUS_TOPIC, "TEST_OK")

    elif cmd == "LED_ON":
        led.on()
        print("💡 LED BẬT")

    elif cmd == "LED_OFF":
        led.off()
        print("💡 LED TẮT")
        
    elif cmd == "STATUS":
        # Gửi toàn bộ trạng thái hiện tại
        send_system_status()

def queue_mqtt_message(topic, message):
    """Lưu tin nhắn MQTT vào hàng đợi khi offline"""
    mqtt_queue.append({
        "topic": topic,
        "message": message,
        "timestamp": time.time()
    })
    
    # Giới hạn độ dài hàng đợi
    if len(mqtt_queue) > 50:
        mqtt_queue.pop(0)

def process_mqtt_queue():
    """Xử lý hàng đợi MQTT khi online lại"""
    if not connection_status["mqtt_connected"] or not mqtt_queue:
        return
    
    processed = []
    for i, msg in enumerate(mqtt_queue):
        try:
            client.publish(msg["topic"], msg["message"])
            processed.append(i)
            time.sleep(0.1)  # Tránh gửi quá nhanh
        except Exception as e:
            print(f"❌ Lỗi gửi MQTT từ hàng đợi: {e}")
            break
    
    # Xóa các tin đã xử lý
    for i in reversed(processed):
        mqtt_queue.pop(i)
    
    if processed:
        print(f"✅ Đã xử lý {len(processed)} tin nhắn từ hàng đợi MQTT")

def connect_mqtt():
    global client, connection_status
    
    # Chỉ kết nối MQTT nếu WiFi đã kết nối
    if not connection_status["wifi_connected"]:
        return False
    
    # Kiểm tra thời gian giữa các lần thử
    current_time = time.time()
    if current_time - connection_status["last_mqtt_attempt"] < MQTT_RETRY_INTERVAL:
        return False
    
    connection_status["last_mqtt_attempt"] = current_time
    
    try:
        # Tạo client ID duy nhất với thời gian
        unique_id = f"{MQTT_CLIENT_ID}_{time.time()}"
        client = MQTTClient(unique_id, MQTT_BROKER, port=MQTT_PORT, keepalive=30)
        client.set_callback(mqtt_callback)
        client.connect()
        client.subscribe(MQTT_TOPIC)
        
        connection_status["mqtt_connected"] = True
        
        # Bật LED MQTT nếu có
        if mqtt_led:
            mqtt_led.on()
            
        print("✅ Đã kết nối MQTT")
        
        # Thông báo online
        client.publish(MQTT_STATUS_TOPIC, "SYSTEM_ONLINE")
        
        # Gửi trạng thái hệ thống
        send_system_status()
        
        # Xử lý hàng đợi tin nhắn
        process_mqtt_queue()
        
        return True
        
    except Exception as e:
        connection_status["mqtt_connected"] = False
        
        # Nhấp nháy LED MQTT nếu có
        if mqtt_led:
            mqtt_led.value(not mqtt_led.value())
            
        print(f"❌ Lỗi kết nối MQTT: {e}")
        return False

def send_system_status():
    """Gửi trạng thái hệ thống qua MQTT"""
    if not connection_status["mqtt_connected"]:
        return
    
    try:
        status_data = {
            "temperature": sensor_data["temperature"],
            "humidity": sensor_data["humidity"],
            "gas": sensor_data["gas"],
            "alarm_status": sensor_data["alarm_status"],
            "system_mode": sensor_data["system_mode"],
            "wifi_connected": connection_status["wifi_connected"],
            "timestamp": time.time()
        }
        
        client.publish("fire_alarm/system_status", json.dumps(status_data))
        print("📤 Đã gửi trạng thái hệ thống")
        
    except Exception as e:
        print(f"❌ Lỗi gửi trạng thái: {e}")

# =============================
# 5. QUẢN LÝ CẢNH BÁO
# =============================
def activate_alarm(reason, manual=False):
    global alarm_active, buzzer_start_time, last_alarm_time
    
    current_time = time.time()
    
    # Kiểm tra cooldown để tránh cảnh báo liên tục
    if current_time - last_alarm_time < ALARM_COOLDOWN:
        return
    
    print(f"🚨 {'THỦ CÔNG' if manual else 'TỰ ĐỘNG'}: {reason}")
    
    # Bật LED và còi
    led.on()
    buzzer.on()
    
    # Cập nhật trạng thái
    alarm_active = True
    buzzer_start_time = current_time
    last_alarm_time = current_time
    
    alarm_type = "MANUAL_FIRE" if manual else "AUTO_FIRE"
    sensor_data["alarm_status"] = f"{alarm_type} - {reason}"
    
    # Lưu vào hàng đợi MQTT
    queue_mqtt_message(MQTT_STATUS_TOPIC, "FIRE_ALARM")
    queue_mqtt_message("fire_alarm/reason", reason)

def deactivate_alarm():
    global alarm_active
    
    print("✅ TẮT CẢNH BÁO")
    
    # Tắt LED và còi
    led.off()
    buzzer.off()
    
    # Cập nhật trạng thái
    alarm_active = False
    sensor_data["alarm_status"] = "SAFE"
    
    # Lưu vào hàng đợi MQTT
    queue_mqtt_message(MQTT_STATUS_TOPIC, "SAFE")

def check_buzzer_timeout():
    global alarm_active
    
    if alarm_active:
        current_time = time.time()
        # Nếu đã kêu được 5 giây thì tắt còi
        if current_time - buzzer_start_time >= BUZZER_DURATION:
            buzzer.off()
            alarm_active = False  # QUAN TRỌNG: Đặt lại trạng thái alarm_active
            print("🔇 Đã tắt còi sau 5 giây")

def test_devices():
    """Kiểm tra thiết bị"""
    print("🔊 Đang kiểm tra thiết bị...")
    
    for pin in [led, buzzer]:
        pin.on()
        time.sleep(0.3)
        pin.off()
        time.sleep(0.2)
    
    print("✅ Hoàn tất kiểm tra")

# =============================
# 6. WEBSERVER HOẠT ĐỘNG CẢ OFFLINE
# =============================
def start_webserver():
    try:
        addr = socket.getaddrinfo('0.0.0.0', WEBSERVER_PORT)[0][-1]
        server_socket = socket.socket()
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(addr)
        server_socket.listen(5)
        server_socket.setblocking(False)
        print(f"🌐 WebServer đang chạy tại cổng {WEBSERVER_PORT}")
        return server_socket
    except Exception as e:
        print(f"❌ Lỗi khởi động webserver: {e}")
        return None

def handle_web_request(server_socket):
    if not server_socket:
        return
    
    try:
        cl, addr = server_socket.accept()
        print(f"🌐 Kết nối từ: {addr[0]}")
        
        request = cl.recv(1024).decode()
        
        if "GET / " in request or "GET /index" in request:
            send_html_page(cl)
        elif "GET /data" in request:
            send_json_data(cl)
        elif "GET /control" in request:
            handle_control_request(request, cl)
        elif "GET /status" in request:
            send_connection_status(cl)
        else:
            send_404(cl)
        
        cl.close()
        
    except (OSError, AttributeError) as e:
        pass

def send_html_page(cl):
    """Gửi trang HTML chính với thông tin offline/online"""
    offline_notice = """
    <div style="background: #ff9800; color: white; padding: 10px; border-radius: 10px; margin-bottom: 15px; text-align: center;">
        <strong>⚠️ CHẾ ĐỘ OFFLINE</strong><br>
        Hệ thống đang hoạt động độc lập. Cảnh báo vẫn hoạt động bình thường.
    </div>
    """ if sensor_data["system_mode"] == "OFFLINE" else ""
    
    connection_status_html = f"""
    <div class="status-card">
        <div class="status-title">
            <h3>📶 Trạng thái kết nối</h3>
            <div class="status-badge {'status-danger' if sensor_data['system_mode'] == 'OFFLINE' else 'status-success'}">
                {sensor_data['system_mode']}
            </div>
        </div>
        <p>• WiFi: <strong>{'✅ Đã kết nối' if connection_status['wifi_connected'] else '❌ Mất kết nối'}</strong></p>
        <p>• MQTT: <strong>{'✅ Đã kết nối' if connection_status['mqtt_connected'] else '❌ Mất kết nối'}</strong></p>
        <p>• IP: <strong>{connection_status['ip_address']}</strong></p>
        <p>• Hàng đợi MQTT: <strong>{len(mqtt_queue)} tin chờ</strong></p>
    </div>
    """
    
    # HTML template (giữ nguyên phần trước, chỉ thêm phần mới)
    html = f"""<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ESP32 Fire Alarm System</title>
    <style>
        /* Giữ nguyên toàn bộ CSS từ code gốc */
        * {{ margin: 0; padding: 0; box-sizing: border-box; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }}
        body {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; display: flex; justify-content: center; align-items: center; padding: 20px; }}
        .container {{ background: rgba(255, 255, 255, 0.95); border-radius: 20px; box-shadow: 0 15px 35px rgba(0, 0, 0, 0.2); padding: 30px; width: 100%; max-width: 500px; }}
        .header {{ text-align: center; margin-bottom: 30px; padding-bottom: 20px; border-bottom: 2px solid #667eea; }}
        .header h1 {{ color: #333; font-size: 28px; margin-bottom: 10px; }}
        .sensor-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin-bottom: 25px; }}
        .sensor-card {{ background: white; border-radius: 15px; padding: 20px; text-align: center; box-shadow: 0 5px 15px rgba(0, 0, 0, 0.1); }}
        .sensor-value {{ font-size: 32px; font-weight: bold; color: #333; margin-bottom: 5px; }}
        .status-card {{ background: white; border-radius: 15px; padding: 25px; margin-bottom: 25px; box-shadow: 0 5px 15px rgba(0, 0, 0, 0.1); }}
        .status-badge {{ padding: 5px 15px; border-radius: 20px; font-size: 12px; font-weight: bold; text-transform: uppercase; }}
        .status-normal {{ background: #4CAF50; color: white; }}
        .status-warning {{ background: #FF9800; color: white; }}
        .status-danger {{ background: #F44336; color: white; }}
        .control-buttons {{ display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-top: 20px; }}
        .btn {{ padding: 12px 20px; border: none; border-radius: 10px; font-size: 14px; font-weight: bold; cursor: pointer; transition: all 0.3s; }}
        .btn-primary {{ background: #667eea; color: white; }}
        .btn-success {{ background: #4CAF50; color: white; }}
        .btn-danger {{ background: #F44336; color: white; }}
        .btn-warning {{ background: #FF9800; color: white; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔥 HỆ THỐNG BÁO CHÁY ESP32</h1>
            <p>🕐 Cập nhật: <span id="last-update">--:--:--</span></p>
            <p>🔌 Chế độ: <span id="system-mode">{sensor_data['system_mode']}</span></p>
        </div>
        
        {offline_notice}
        
        <div class="sensor-grid">
            <div class="sensor-card">
                <div class="sensor-icon">🌡️</div>
                <div class="sensor-value" id="temperature">--</div>
                <div class="sensor-label">Nhiệt độ</div>
                <div class="status-badge" id="temp-status">...</div>
            </div>
            
            <div class="sensor-card">
                <div class="sensor-icon">💧</div>
                <div class="sensor-value" id="humidity">--</div>
                <div class="sensor-label">Độ ẩm</div>
            </div>
            
            <div class="sensor-card">
                <div class="sensor-icon">⚠️</div>
                <div class="sensor-value" id="gas">--</div>
                <div class="sensor-label">Khí gas</div>
                <div class="status-badge" id="gas-status">...</div>
            </div>
            
            <div class="sensor-card">
                <div class="sensor-icon">🔔</div>
                <div class="sensor-value" id="alarm">SAFE</div>
                <div class="sensor-label">Trạng thái</div>
            </div>
        </div>
        
        {connection_status_html}
        
        <div class="status-card">
            <div class="status-title">
                <h3>📊 Thông số cảnh báo</h3>
                <div class="status-badge" id="system-status">NORMAL</div>
            </div>
            <p>• Nhiệt độ: > <strong>{TEMP_THRESHOLD}°C</strong> (cảnh báo)</p>
            <p>• Khí gas: > <strong>{MQ135_THRESHOLD}</strong> (cảnh báo)</p>
            <p>• Còi kêu: <strong>{BUZZER_DURATION} giây</strong> khi phát hiện</p>
        </div>
        
        <div class="control-buttons">
            <button class="btn btn-danger" onclick="sendCommand('FIRE')">🔥 BÁO CHÁY</button>
            <button class="btn btn-success" onclick="sendCommand('SAFE')">✅ AN TOÀN</button>
            <button class="btn btn-warning" onclick="sendCommand('TEST')">🔊 TEST</button>
            <button class="btn btn-primary" onclick="sendCommand('LED_ON')">💡 LED ON</button>
            <button class="btn" onclick="sendCommand('LED_OFF')" style="background: #666; color: white">💡 LED OFF</button>
            <button class="btn" onclick="sendCommand('RECONNECT')" style="background: #2196F3; color: white">🔄 KẾT NỐI LẠI</button>
        </div>
        
        <button class="btn" onclick="location.reload()" style="width: 100%; margin-top: 15px; background: #764ba2; color: white">🔄 Làm mới trang</button>
        
        <div class="info" style="text-align: center; margin-top: 20px; color: #666; font-size: 12px;">
            <p>Hệ thống hoạt động cả online và offline • Tự động kết nối lại</p>
        </div>
    </div>
    
    <script>
        function sendCommand(cmd) {{
            fetch('/control?cmd=' + cmd)
                .then(response => response.text())
                .then(data => {{
                    console.log('Command sent:', cmd);
                    if (cmd === 'RECONNECT') {{
                        alert('Đang thử kết nối lại...');
                    }}
                    updateData();
                }})
                .catch(error => console.error('Error:', error));
        }}
        
        function updateData() {{
            fetch('/data')
                .then(response => response.json())
                .then(data => {{
                    // Cập nhật dữ liệu
                    document.getElementById('temperature').textContent = data.temperature + '°C';
                    document.getElementById('humidity').textContent = data.humidity + '%';
                    document.getElementById('gas').textContent = data.gas;
                    document.getElementById('alarm').textContent = data.alarm_status;
                    document.getElementById('last-update').textContent = data.last_update;
                    document.getElementById('system-mode').textContent = data.system_mode;
                    
                    // Cập nhật trạng thái
                    const tempStatus = document.getElementById('temp-status');
                    tempStatus.textContent = data.temp_level;
                    tempStatus.className = 'status-badge ' + 
                        (data.temp_level === 'HIGH' ? 'status-danger' : 'status-normal');
                    
                    const gasStatus = document.getElementById('gas-status');
                    gasStatus.textContent = data.gas_level;
                    gasStatus.className = 'status-badge ' + 
                        (data.gas_level === 'HIGH' ? 'status-danger' : 'status-normal');
                    
                    const systemStatus = document.getElementById('system-status');
                    if (data.alarm_status.includes('FIRE')) {{
                        systemStatus.textContent = 'ALARM';
                        systemStatus.className = 'status-badge status-danger';
                    }} else {{
                        systemStatus.textContent = 'NORMAL';
                        systemStatus.className = 'status-badge status-normal';
                    }}
                }})
                .catch(error => console.error('Error updating data:', error));
        }}
        
        // Tự động cập nhật
        setInterval(updateData, 2000);
        updateData();
    </script>
</body>
</html>"""
    
    cl.send('HTTP/1.0 200 OK\r\n')
    cl.send('Content-Type: text/html\r\n')
    cl.send('Connection: close\r\n\r\n')
    cl.send(html)
    cl.close()

def send_json_data(cl):
    """Gửi dữ liệu dạng JSON"""
    # Tạo response bằng cách merge thủ công
    response = {}
    
    # Sao chép tất cả key-value từ sensor_data
    for key in sensor_data:
        response[key] = sensor_data[key]
    
    # Thêm các trường bổ sung
    response["connection_status"] = {
        "wifi_connected": connection_status["wifi_connected"],
        "mqtt_connected": connection_status["mqtt_connected"],
        "offline_mode": connection_status["offline_mode"],
        "ip_address": connection_status["ip_address"]
    }
    
    response["mqtt_queue_length"] = len(mqtt_queue)
    response["thresholds"] = {
        "temperature": TEMP_THRESHOLD,
        "gas": MQ135_THRESHOLD
    }
    
    # Chuyển đổi sang JSON
    import json
    json_data = json.dumps(response)
    
    # Gửi HTTP response
    cl.send('HTTP/1.0 200 OK\r\n')
    cl.send('Content-Type: application/json\r\n')
    cl.send('Access-Control-Allow-Origin: *\r\n')
    cl.send('Connection: close\r\n\r\n')
    cl.send(json_data)
    cl.close()

def send_connection_status(cl):
    """Gửi trạng thái kết nối"""
    status = {
        "wifi": connection_status["wifi_connected"],
        "mqtt": connection_status["mqtt_connected"],
        "mode": sensor_data["system_mode"],
        "ip": connection_status["ip_address"],
        "queue": len(mqtt_queue)
    }
    
    cl.send('HTTP/1.0 200 OK\r\n')
    cl.send('Content-Type: application/json\r\n')
    cl.send('Connection: close\r\n\r\n')
    cl.send(json.dumps(status))
    cl.close()

def handle_control_request(request, cl):
    """Xử lý control command từ web"""
    response = "Unknown command"
    
    if "cmd=FIRE" in request:
        activate_alarm("WEB_CONTROL", manual=True)
        response = "Command FIRE sent"
    elif "cmd=SAFE" in request:
        deactivate_alarm()
        response = "Command SAFE sent"
    elif "cmd=TEST" in request:
        test_devices()
        response = "Command TEST sent"
    elif "cmd=LED_ON" in request:
        led.on()
        response = "Command LED_ON sent"
    elif "cmd=LED_OFF" in request:
        led.off()
        response = "Command LED_OFF sent"
    elif "cmd=RECONNECT" in request:
        connection_status["wifi_retry_count"] = 0  # Reset retry counter
        response = "Attempting to reconnect..."
    
    cl.send('HTTP/1.0 200 OK\r\n')
    cl.send('Content-Type: text/plain\r\n')
    cl.send('Connection: close\r\n\r\n')
    cl.send(response)
    cl.close()

def send_404(cl):
    """Gửi trang 404"""
    cl.send('HTTP/1.0 404 Not Found\r\n')
    cl.send('Content-Type: text/plain\r\n')
    cl.send('Connection: close\r\n\r\n')
    cl.send('404 - Page Not Found')
    cl.close()

# =============================
# 7. ĐỌC CẢM BIẾN VÀ KIỂM TRA CẢNH BÁO
# =============================
def read_sensors():
    """Đọc giá trị từ cảm biến"""
    try:
        # Đọc MQ135
        mq_value = mq135.read()
        
        # Đọc DHT11
        try:
            dht.measure()
            temp = dht.temperature()
            hum = dht.humidity()
        except:
            temp = 0
            hum = 0
        
        # Cập nhật dữ liệu
        sensor_data["temperature"] = temp
        sensor_data["humidity"] = hum
        sensor_data["gas"] = mq_value
        
        # Xác định mức cảnh báo
        sensor_data["gas_level"] = "HIGH" if mq_value > MQ135_THRESHOLD else "NORMAL"
        sensor_data["temp_level"] = "HIGH" if temp > TEMP_THRESHOLD else "NORMAL"
        
        # Thời gian cập nhật
        t = time.localtime()
        sensor_data["last_update"] = f"{t[3]:02d}:{t[4]:02d}:{t[5]:02d}"
        
        # Kiểm tra cảnh báo tự động (chỉ khi không có cảnh báo thủ công)
        if not alarm_active:
            if mq_value > MQ135_THRESHOLD:
                activate_alarm("GAS_HIGH")
            elif temp > TEMP_THRESHOLD:
                activate_alarm("TEMP_HIGH")
        
        # Hiển thị log
        print(f"📊 Temp: {temp}°C, Hum: {hum}%, Gas: {mq_value}")
        
    except Exception as e:
        print(f"❌ Lỗi đọc cảm biến: {e}")

# =============================
# 8. MAIN LOOP - VÒNG LẶP CHÍNH
# =============================
def main():
    print("🔥 ESP32 FIRE ALARM SYSTEM - OFFLINE MODE ENABLED")
    
    # Khởi tạo kết nối WiFi
    connect_wifi()
    
    # Khởi động webserver (luôn chạy dù có WiFi hay không)
    server_socket = start_webserver()
    
    # Biến thời gian
    last_sensor_time = 0
    last_status_time = 0
    last_connection_check = 0
    
    while True:
        current_time = time.time()
        
        # 1. KIỂM TRA KẾT NỐI ĐỊNH KỲ (mỗi 5 giây)
        if current_time - last_connection_check >= 5:
            last_connection_check = current_time
            
            # Nếu đang offline, thử kết nối lại WiFi
            if not connection_status["wifi_connected"]:
                connect_wifi()
            
            # Nếu WiFi đã kết nối, thử kết nối MQTT
            if connection_status["wifi_connected"] and not connection_status["mqtt_connected"]:
                connect_mqtt()
            
            # Nếu MQTT đã kết nối, kiểm tra tin nhắn
            if connection_status["mqtt_connected"]:
                try:
                    client.check_msg()
                except:
                    connection_status["mqtt_connected"] = False
                    if mqtt_led:
                        mqtt_led.off()
        
        # 2. ĐỌC CẢM BIẾN ĐỊNH KỲ (mỗi 2 giây)
        if current_time - last_sensor_time >= SENSOR_INTERVAL:
            last_sensor_time = current_time
            read_sensors()
            
            # Gửi dữ liệu qua MQTT nếu kết nối
            if connection_status["mqtt_connected"]:
                try:
                    send_system_status()
                except:
                    connection_status["mqtt_connected"] = False
        
        # 3. KIỂM TRA TIMEOUT CÒI
        check_buzzer_timeout()
        
        # 4. XỬ LÝ WEBSERVER
        if server_socket:
            handle_web_request(server_socket)
        
        # 5. GỌI GARBAGE COLLECTOR ĐỊNH KỲ
        if current_time - last_status_time >= 30:
            last_status_time = current_time
            gc.collect()
            print(f"🧹 Memory free: {gc.mem_free()} bytes")
        
        # 6. CHỜ
        time.sleep(0.1)



# 9. CHẠY CHƯƠNG TRÌNH
# =============================
if __name__ == "__main__":
    main()
