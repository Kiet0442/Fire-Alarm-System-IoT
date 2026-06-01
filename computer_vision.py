from ultralytics import YOLO
import cv2
import paho.mqtt.client as mqtt
import time
import json
import threading
from collections import deque

# =============================
# 1. CẤU HÌNH MQTT
# =============================
MQTT_CONFIG = {
    "broker": "broker.hivemq.com",  # Public broker miễn phí
    "port": 1883,
    "username": None,  # Không cần username/password cho public broker
    "password": None,
    "topics": {
        "control": "fire_alarm/control",  # Gửi lệnh điều khiển
        "status": "fire_alarm/status",  # Trạng thái hệ thống
        "alert": "fire_alarm/alert",  # Cảnh báo chi tiết
        "sensor": "fire_alarm/sensor",  # Dữ liệu cảm biến
        "debug": "fire_alarm/debug"  # Debug info
    }
}

# Tạo Client ID duy nhất
import uuid

CLIENT_ID = f"fire_detection_pc_{uuid.uuid4().hex[:8]}"


# =============================
# 2. LỚP MQTT MANAGER
# =============================
class MQTTManager:
    def __init__(self, config=MQTT_CONFIG):
        self.config = config
        self.client = None
        self.connected = False
        self.last_messages = deque(maxlen=10)  # Lưu 10 tin nhắn gần nhất
        self.connection_status = "disconnected"

        # Callbacks
        self.on_connect_callback = None
        self.on_message_callback = None

    def setup(self):
        """Thiết lập kết nối MQTT"""
        try:
            self.client = mqtt.Client(client_id=CLIENT_ID, protocol=mqtt.MQTTv311)

            # Thiết lập callback
            self.client.on_connect = self._on_connect
            self.client.on_disconnect = self._on_disconnect
            self.client.on_message = self._on_message

            # Đặt username/password nếu có
            if self.config["username"] and self.config["password"]:
                self.client.username_pw_set(self.config["username"], self.config["password"])

            # Kết nối
            self.client.connect(self.config["broker"], self.config["port"], 60)

            # Bắt đầu loop trong thread riêng
            self.client.loop_start()

            # Chờ kết nối
            for i in range(10):
                if self.connected:
                    print(f"✅ MQTT connected to {self.config['broker']}")
                    return True
                time.sleep(0.5)

            print("❌ MQTT connection timeout")
            return False

        except Exception as e:
            print(f"❌ MQTT setup error: {e}")
            return False

    def _on_connect(self, client, userdata, flags, rc):
        """Callback khi kết nối thành công"""
        if rc == 0:
            self.connected = True
            self.connection_status = "connected"

            # Subscribe tất cả topics
            for topic in self.config["topics"].values():
                client.subscribe(topic)
                print(f"📫 Subscribed to: {topic}")

            # Publish trạng thái online
            self.publish_status("system_online", {"client_id": CLIENT_ID})

            # Gọi callback nếu có
            if self.on_connect_callback:
                self.on_connect_callback()

        else:
            print(f"❌ MQTT connection failed: {rc}")

    def _on_disconnect(self, client, userdata, rc):
        """Callback khi mất kết nối"""
        self.connected = False
        self.connection_status = "disconnected"
        print("⚠️ MQTT disconnected")

    def _on_message(self, client, userdata, msg):
        """Callback khi nhận message"""
        try:
            topic = msg.topic
            payload = msg.payload.decode()

            # Lưu tin nhắn gần nhất
            self.last_messages.append({
                "topic": topic,
                "payload": payload,
                "timestamp": time.time()
            })

            # In ra console
            print(f"📥 MQTT [{topic}]: {payload[:50]}..." if len(payload) > 50 else f"📥 MQTT [{topic}]: {payload}")

            # Gọi callback nếu có
            if self.on_message_callback:
                self.on_message_callback(topic, payload)

        except Exception as e:
            print(f"❌ MQTT message error: {e}")

    def publish(self, topic, message, retain=False):
        """Publish message đến topic"""
        if not self.connected:
            return False

        try:
            if isinstance(message, dict):
                message = json.dumps(message)

            result = self.client.publish(topic, message, qos=1, retain=retain)

            # Kiểm tra kết quả
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                return True
            else:
                print(f"❌ Publish failed: {result.rc}")
                return False

        except Exception as e:
            print(f"❌ Publish error: {e}")
            return False

    def publish_control(self, command):
        """Publish lệnh điều khiển"""
        return self.publish(self.config["topics"]["control"], command)

    def publish_status(self, status, data=None):
        """Publish trạng thái hệ thống"""
        payload = {"status": status, "timestamp": time.time()}
        if data:
            payload.update(data)
        return self.publish(self.config["topics"]["status"], payload)

    def publish_alert(self, fire_data):
        """Publish cảnh báo lửa"""
        return self.publish(self.config["topics"]["alert"], fire_data)

    def publish_debug(self, message):
        """Publish thông tin debug"""
        return self.publish(self.config["topics"]["debug"], message)

    def cleanup(self):
        """Dọn dẹp khi thoát"""
        if self.client:
            self.publish_status("system_offline")
            time.sleep(0.5)
            self.client.loop_stop()
            self.client.disconnect()
            print("✅ MQTT cleaned up")


# =============================
# 3. LỚP FIRE DETECTOR
# =============================
class FireDetector:
    def __init__(self, model_path, mqtt_manager=None):
        self.model_path = model_path
        self.model = None
        self.mqtt = mqtt_manager

        # Trạng thái phát hiện lửa
        self.fire_detected = False
        self.fire_counter = 0
        self.fire_threshold = 3  # Cần 3 lần phát hiện liên tiếp
        self.last_alert_time = 0
        self.alert_cooldown = 1.0  # 1 giây giữa các cảnh báo

        # Thống kê
        self.frame_count = 0
        self.fire_frames = 0
        self.start_time = time.time()

        # Lịch sử phát hiện
        self.detection_history = deque(maxlen=30)  # 30 frame gần nhất

        print("🔥 Fire Detector Initializing...")

    def load_model(self):
        """Tải model YOLO"""
        try:
            print("⚡ Loading YOLO model...")
            self.model = YOLO(self.model_path)
            self.model.fuse()  # Tối ưu hóa model
            print("✅ Model loaded successfully")
            return True
        except Exception as e:
            print(f"❌ Failed to load model: {e}")
            return False

    def detect(self, frame):
        """Phát hiện lửa trong frame"""
        if self.model is None:
            return False, []

        # Resize để tăng tốc độ xử lý
        frame_small = cv2.resize(frame, (320, 240))

        # Phát hiện
        results = self.model(frame_small, conf=0.4, verbose=False, imgsz=320)

        fire_boxes = []
        max_confidence = 0

        # Phân tích kết quả
        for r in results:
            if r.boxes is not None:
                for box in r.boxes:
                    if int(box.cls[0]) == 0:  # Class Fire
                        conf = float(box.conf[0])
                        max_confidence = max(max_confidence, conf)

                            # Lấy tọa độ và scale về kích thước gốc
                        x1, y1, x2, y2 = map(int, box.xyxy[0])
                        x1 = int(x1 * frame.shape[1] / 320)
                        y1 = int(y1 * frame.shape[0] / 240)
                        x2 = int(x2 * frame.shape[1] / 320)
                        y2 = int(y2 * frame.shape[0] / 240)

                        fire_boxes.append({
                            "bbox": [x1, y1, x2, y2],
                            "confidence": conf,
                            "area": (x2 - x1) * (y2 - y1)
                        })

        return len(fire_boxes) > 0, fire_boxes

    def update_state(self, has_fire, fire_boxes):
        """Cập nhật trạng thái và gửi cảnh báo qua MQTT nếu cần"""
        current_time = time.time()
        self.frame_count += 1

        # Cập nhật lịch sử
        self.detection_history.append(has_fire)

        # Đếm số frame có lửa trong 10 frame gần nhất
        recent_fires = sum(list(self.detection_history)[-10:]) if len(self.detection_history) >= 10 else 0

        # Xử lý trạng thái
        if has_fire:
            self.fire_frames += 1
            self.fire_counter += 1

            # Gửi cảnh báo nếu đủ ngưỡng
            if self.fire_counter >= self.fire_threshold and not self.fire_detected:
                if current_time - self.last_alert_time >= self.alert_cooldown:
                    self.fire_detected = True
                    self.last_alert_time = current_time

                    # Gửi cảnh báo qua MQTT
                    if self.mqtt and self.mqtt.connected:
                        alert_data = {
                            "alert": "FIRE_DETECTED",
                            "fire_count": len(fire_boxes),
                            "max_confidence": max([b["confidence"] for b in fire_boxes]) if fire_boxes else 0,
                            "total_area": sum([b["area"] for b in fire_boxes]),
                            "timestamp": current_time,
                            "frame_number": self.frame_count
                        }
                        self.mqtt.publish_control("FIRE")
                        self.mqtt.publish_alert(alert_data)
                        self.mqtt.publish_status("fire_detected", alert_data)

                    print(f"🔥 FIRE DETECTED! Boxes: {len(fire_boxes)}")

        else:
            self.fire_counter = max(0, self.fire_counter - 1)

            # Tắt cảnh báo nếu không còn lửa
            if self.fire_detected and self.fire_counter == 0:
                self.fire_detected = False

                # Gửi trạng thái an toàn
                if self.mqtt and self.mqtt.connected:
                    self.mqtt.publish_control("SAFE")
                    self.mqtt.publish_status("fire_extinguished", {
                        "timestamp": current_time,
                        "frames_with_fire": self.fire_frames
                    })

                print("✅ Fire extinguished")

        return self.fire_detected, recent_fires

    def get_stats(self):
        """Lấy thống kê hiệu suất"""
        elapsed = time.time() - self.start_time
        fps = self.frame_count / elapsed if elapsed > 0 else 0
        fire_rate = (self.fire_frames / self.frame_count * 100) if self.frame_count > 0 else 0

        return {
            "fps": fps,
            "total_frames": self.frame_count,
            "fire_frames": self.fire_frames,
            "fire_rate_percent": fire_rate,
            "current_state": "FIRE" if self.fire_detected else "SAFE",
            "fire_counter": self.fire_counter
        }


# =============================
# 4. LỚP CAMERA MANAGER
# =============================
class CameraManager:
    def __init__(self, camera_urls):
        self.camera_urls = camera_urls
        self.cap = None
        self.current_url = None

    def connect(self):
        """Kết nối đến camera"""
        for url in self.camera_urls:
            print(f"📡 Trying camera: {url}")
            cap = cv2.VideoCapture(url)
            cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)

            # Kiểm tra kết nối
            for _ in range(3):
                ret, _ = cap.read()
                if ret:
                    self.cap = cap
                    self.current_url = url
                    print(f"✅ Camera connected: {url}")
                    return True
                time.sleep(0.1)

            cap.release()

        print("❌ Failed to connect to any camera")
        return False

    def read_frame(self):
        """Đọc frame từ camera"""
        if self.cap is None:
            return False, None

        ret, frame = self.cap.read()
        if not ret:
            # Thử reconnect
            print("⚠️ Camera disconnected, reconnecting...")
            self.cap.release()
            time.sleep(1)
            if self.connect():
                ret, frame = self.cap.read()

        return ret, frame

    def release(self):
        """Giải phóng camera"""
        if self.cap:
            self.cap.release()


# =============================
# 5. MAIN APPLICATION
# =============================
class FireDetectionApp:
    def __init__(self):
        # Config
        self.model_path = r"C:\Users\kieth\Downloads\best.pt"
        self.camera_urls = [
            "http://172.20.10.2:81/stream",
            "http://172.20.10.2:80/stream",
            "http://172.20.10.2/stream"
        ]

        # Managers
        self.mqtt = MQTTManager()
        self.detector = FireDetector(self.model_path, self.mqtt)
        self.camera = CameraManager(self.camera_urls)

        # Control flags
        self.running = False
        self.show_video = True
        self.record_video = False
        self.out_writer = None

    def setup(self):
        """Thiết lập hệ thống"""
        print("\n" + "=" * 60)
        print("🔥 FIRE DETECTION SYSTEM WITH MQTT")
        print("=" * 60)

        # 1. Kết nối MQTT
        print("\n1. Connecting to MQTT broker...")
        if not self.mqtt.setup():
            print("⚠️ Continuing without MQTT...")

        # 2. Tải model
        print("\n2. Loading AI model...")
        if not self.detector.load_model():
            return False

        # 3. Kết nối camera
        print("\n3. Connecting to camera...")
        if not self.camera.connect():
            return False

        # 4. Thiết lập MQTT callbacks
        self.mqtt.on_message_callback = self.handle_mqtt_message

        print("\n" + "=" * 60)
        print("✅ System ready!")
        print("=" * 60)
        print("\nControls:")
        print("  q: Quit")
        print("  t: Test alarm")
        print("  m: Toggle MQTT status display")
        print("  v: Toggle video display")
        print("  r: Start/stop recording")
        print("  s: Show statistics")
        print("  f: Force FIRE alarm")
        print("  o: Force SAFE")
        print("=" * 60)

        return True

    def handle_mqtt_message(self, topic, payload):
        """Xử lý message MQTT nhận được"""
        try:
            if topic == MQTT_CONFIG["topics"]["control"]:
                # Lệnh từ ESP32 hoặc dashboard
                if payload == "STATUS_REQUEST":
                    stats = self.detector.get_stats()
                    self.mqtt.publish_status("system_status", stats)

        except Exception as e:
            print(f"❌ MQTT handler error: {e}")

    def run(self):
        """Chạy ứng dụng chính"""
        self.running = True
        stats_timer = time.time()
        mqtt_status_timer = time.time()

        try:
            while self.running:
                frame_start = time.time()

                # 1. Đọc frame từ camera
                ret, frame = self.camera.read_frame()
                if not ret:
                    time.sleep(0.1)
                    continue

                # 2. Phát hiện lửa
                has_fire, fire_boxes = self.detector.detect(frame)

                # 3. Cập nhật trạng thái và gửi cảnh báo
                fire_detected, recent_fires = self.detector.update_state(has_fire, fire_boxes)

                # 4. Vẽ kết quả lên frame
                processed_frame = self.draw_results(frame, fire_boxes, fire_detected, recent_fires)

                # 5. Ghi video nếu đang record
                if self.record_video and self.out_writer is None:
                    self.start_recording(frame.shape)
                if self.record_video and self.out_writer:
                    self.out_writer.write(processed_frame)

                # 6. Hiển thị frame
                if self.show_video:
                    cv2.imshow("Fire Detection - MQTT", processed_frame)

                # 7. Gửi thống kê định kỳ qua MQTT
                current_time = time.time()
                if current_time - stats_timer > 5.0:  # Mỗi 5 giây
                    stats = self.detector.get_stats()
                    if self.mqtt.connected:
                        self.mqtt.publish_status("stats_update", stats)
                    stats_timer = current_time

                # 8. Hiển thị MQTT status định kỳ
                if current_time - mqtt_status_timer > 10.0:
                    print(f"📊 Stats: {self.detector.get_stats()}")
                    mqtt_status_timer = current_time

                # 9. Xử lý phím
                self.handle_keys()

                # 10. Giới hạn FPS
                frame_time = time.time() - frame_start
                if frame_time < 0.033:  # ~30 FPS
                    time.sleep(0.033 - frame_time)

        except KeyboardInterrupt:
            print("\n⏹️ Stopping...")
        except Exception as e:
            print(f"\n❌ Error: {e}")
        finally:
            self.cleanup()

    def draw_results(self, frame, fire_boxes, fire_detected, recent_fires):
        """Vẽ kết quả lên frame"""
        display_frame = frame.copy()

        # 1. Vẽ bounding boxes
        for box in fire_boxes[:5]:  # Chỉ vẽ 5 box đầu
            x1, y1, x2, y2 = box["bbox"]
            conf = box["confidence"]

            # Màu sắc theo confidence
            color_intensity = int(conf * 255)
            color = (0, 0, color_intensity)  # Xanh → Đỏ

            cv2.rectangle(display_frame, (x1, y1), (x2, y2), color, 2)
            cv2.putText(display_frame, f"Fire {conf:.2f}", (x1, y1 - 10),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.6, color, 2)

        # 2. Vẽ trạng thái
        status_color = (0, 0, 255) if fire_detected else (0, 255, 0)
        status_text = f"🔥 FIRE! ({recent_fires}/10 frames)" if fire_detected else f"✅ SAFE ({recent_fires}/10)"

        cv2.putText(display_frame, status_text, (20, 40),
                    cv2.FONT_HERSHEY_SIMPLEX, 1.2, status_color, 3)

        # 3. Vẽ thông tin MQTT
        mqtt_status = "MQTT: ✅" if self.mqtt.connected else "MQTT: ❌"
        mqtt_color = (0, 255, 0) if self.mqtt.connected else (0, 0, 255)

        cv2.putText(display_frame, mqtt_status, (20, 80),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.8, mqtt_color, 2)

        # 4. Vẽ thông tin FPS
        stats = self.detector.get_stats()
        fps_text = f"FPS: {stats['fps']:.1f} | Frames: {stats['total_frames']}"
        cv2.putText(display_frame, fps_text, (20, 110),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 2)

        # 5. Recording indicator
        if self.record_video:
            cv2.putText(display_frame, "🔴 REC", (frame.shape[1] - 100, 40),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 0, 255), 2)

        return display_frame

    def handle_keys(self):
        """Xử lý phím bấm"""
        key = cv2.waitKey(1) & 0xFF

        if key == ord('q'):
            self.running = False
        elif key == ord('t'):
            print("🔊 Sending test command...")
            if self.mqtt.connected:
                self.mqtt.publish_control("TEST")
        elif key == ord('v'):
            self.show_video = not self.show_video
            print(f"📺 Video display: {'ON' if self.show_video else 'OFF'}")
        elif key == ord('r'):
            self.toggle_recording()
        elif key == ord('s'):
            stats = self.detector.get_stats()
            print(f"\n📊 Statistics:")
            for key, value in stats.items():
                print(f"  {key}: {value}")
        elif key == ord('f'):
            print("🔥 Forcing FIRE alarm...")
            if self.mqtt.connected:
                self.mqtt.publish_control("FIRE")
        elif key == ord('o'):
            print("✅ Forcing SAFE...")
            if self.mqtt.connected:
                self.mqtt.publish_control("SAFE")
        elif key == ord('m'):
            print(f"\n📡 MQTT Status: {self.mqtt.connection_status}")
            print(f"   Last messages:")
            for msg in list(self.mqtt.last_messages)[-3:]:
                print(f"   [{msg['topic']}]: {msg['payload'][:50]}...")

    def start_recording(self, frame_shape):
        """Bắt đầu ghi video"""
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = f"fire_detection_{timestamp}.avi"

        fourcc = cv2.VideoWriter_fourcc(*'XVID')
        self.out_writer = cv2.VideoWriter(filename, fourcc, 20.0,
                                          (frame_shape[1], frame_shape[0]))
        print(f"🎥 Started recording: {filename}")

    def toggle_recording(self):
        """Bật/tắt ghi video"""
        self.record_video = not self.record_video

        if not self.record_video and self.out_writer:
            self.out_writer.release()
            self.out_writer = None
            print("⏹️ Recording stopped")
        elif self.record_video:
            print("🔴 Recording started")

    def cleanup(self):
        """Dọn dẹp khi thoát"""
        print("\n🧹 Cleaning up...")

        # Gửi trạng thái offline
        if self.mqtt.connected:
            self.mqtt.publish_status("system_offline")

        # Dừng recording
        if self.out_writer:
            self.out_writer.release()

        # Giải phóng camera
        self.camera.release()

        # Đóng cửa sổ
        cv2.destroyAllWindows()

        # Dọn dẹp MQTT
        self.mqtt.cleanup()

        # Hiển thị thống kê cuối cùng
        stats = self.detector.get_stats()
        print(f"\n📈 Final Statistics:")
        for key, value in stats.items():
            print(f"  {key}: {value}")

        print("✅ Cleanup completed!")


# =============================
# 6. CHẠY ỨNG DỤNG
# =============================
if __name__ == "__main__":
    app = FireDetectionApp()

    if app.setup():
        app.run()
    else:
        print("❌ Failed to setup system")