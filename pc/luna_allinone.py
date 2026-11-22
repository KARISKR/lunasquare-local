"""LunaSquare Wi-Fi Humidifier All-in-One Local Controller (MQTT + DNS + Web PWA + GUI)."""
import json
import os
import socket
import struct
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse
import webbrowser

MQTT_PORT = int(os.environ.get("LUNA_MQTT_PORT", "59001"))
HTTP_PORT = int(os.environ.get("LUNA_HTTP_PORT", "8090"))
DNS_PORT = int(os.environ.get("LUNA_DNS_PORT", "53"))
TARGET_DOMAIN = "smartlight.co.kr"
UPSTREAM_DNS = ("8.8.8.8", 53)


def resource_path(name):
    base = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
    p = os.path.join(base, name)
    if os.path.exists(p):
        return p
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", name)


def get_lan_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def hex2int(s):
    try:
        return int(str(s).strip(), 16)
    except Exception:
        return 0


def mqtt_topic_match(sub, topic):
    if sub == "#" or sub == topic:
        return True
    s_parts, t_parts = sub.split("/"), topic.split("/")
    for i, sp in enumerate(s_parts):
        if sp == "#":
            return True
        if i >= len(t_parts):
            return False
        if sp != "+" and sp != t_parts[i]:
            return False
    return len(s_parts) == len(t_parts)


def mqtt_encode_len(n):
    out = bytearray()
    while True:
        b = n % 128
        n //= 128
        if n > 0:
            b |= 0x80
        out.append(b)
        if n == 0:
            break
    return bytes(out)


def recv_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


class MiniMqttBroker:
    """Minimal pure-Python MQTT 3.1.1 broker for LunaSquare devices."""

    def __init__(self, port, on_message, on_log):
        self.port = port
        self.on_message = on_message
        self.on_log = on_log
        self.lock = threading.Lock()
        self.clients = {}  # sock -> set(topics)
        self.running = False
        self.srv = None

    def start(self):
        self.srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.srv.bind(("0.0.0.0", self.port))
        self.srv.listen(32)
        self.running = True
        threading.Thread(target=self._accept_loop, daemon=True).start()
        self.on_log(f"[MQTT] 브로커 시작 (0.0.0.0:{self.port})")

    def _accept_loop(self):
        while self.running:
            try:
                cli, addr = self.srv.accept()
                cli.settimeout(120)
                with self.lock:
                    self.clients[cli] = set()
                threading.Thread(target=self._handle_client, args=(cli, addr), daemon=True).start()
            except Exception:
                break

    def _handle_client(self, cli, addr):
        self.on_log(f"[MQTT] 기기 접속: {addr[0]}:{addr[1]}")
        try:
            while self.running:
                hdr = recv_exact(cli, 1)
                if not hdr:
                    break
                ptype = hdr[0] >> 4
                flags = hdr[0] & 0x0F
                rem_len, mult = 0, 1
                while True:
                    b = recv_exact(cli, 1)
                    if not b:
                        return
                    rem_len += (b[0] & 0x7F) * mult
                    if not (b[0] & 0x80):
                        break
                    mult *= 128
                body = recv_exact(cli, rem_len) if rem_len else b""
                if body is None:
                    break

                if ptype == 1:  # CONNECT
                    cli.sendall(b"\x20\x02\x00\x00")
                elif ptype == 3:  # PUBLISH
                    qos = (flags >> 1) & 0x03
                    tlen = struct.unpack("!H", body[:2])[0]
                    topic = body[2 : 2 + tlen].decode("utf-8", "ignore")
                    pos = 2 + tlen
                    if qos > 0:
                        pkt_id = struct.unpack("!H", body[pos : pos + 2])[0]
                        pos += 2
                        cli.sendall(b"\x40\x02" + struct.pack("!H", pkt_id))
                    payload = body[pos:].decode("utf-8", "ignore")
                    self.on_message(topic, payload)
                    self.publish(topic, payload, exclude=cli)
                elif ptype == 8:  # SUBSCRIBE
                    pkt_id = struct.unpack("!H", body[:2])[0]
                    pos, granted = 2, []
                    while pos < len(body):
                        tlen = struct.unpack("!H", body[pos : pos + 2])[0]
                        tfilter = body[pos + 2 : pos + 2 + tlen].decode("utf-8", "ignore")
                        pos += 2 + tlen + 1
                        with self.lock:
                            if cli in self.clients:
                                self.clients[cli].add(tfilter)
                        granted.append(0)
                    cli.sendall(b"\x90" + mqtt_encode_len(2 + len(granted)) + struct.pack("!H", pkt_id) + bytes(granted))
                elif ptype == 12:  # PINGREQ
                    cli.sendall(b"\xd0\x00")
                elif ptype == 14:  # DISCONNECT
                    break
        except Exception:
            pass
        finally:
            with self.lock:
                self.clients.pop(cli, None)
            try:
                cli.close()
            except Exception:
                pass
            self.on_log(f"[MQTT] 연결 종료: {addr[0]}:{addr[1]}")

    def publish(self, topic, payload, exclude=None):
        tb = topic.encode("utf-8")
        pb = payload.encode("utf-8")
        body = struct.pack("!H", len(tb)) + tb + pb
        pkt = b"\x30" + mqtt_encode_len(len(body)) + body
        with self.lock:
            targets = [c for c, subs in self.clients.items() if c is not exclude and any(mqtt_topic_match(s, topic) for s in subs)]
        for c in targets:
            try:
                c.sendall(pkt)
            except Exception:
                pass


class MiniDnsServer:
    """Minimal DNS proxy redirecting smartlight.co.kr to LAN IP."""

    def __init__(self, port, lan_ip, on_log):
        self.port = port
        self.lan_ip = lan_ip
        self.on_log = on_log
        self.running = False
        self.sock = None

    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", self.port))
        self.running = True
        threading.Thread(target=self._loop, daemon=True).start()
        self.on_log(f"[DNS] 내장 DNS 시작 (0.0.0.0:{self.port}) -> {TARGET_DOMAIN} = {self.lan_ip}")

    def _parse_qname(self, data):
        labels, idx = [], 12
        while idx < len(data):
            ln = data[idx]
            if ln == 0:
                return ".".join(labels), idx + 1
            labels.append(data[idx + 1 : idx + 1 + ln].decode("ascii", "ignore"))
            idx += 1 + ln
        return "", len(data)

    def _handle(self, data, addr):
        try:
            qname, end = self._parse_qname(data)
            qtype = struct.unpack("!H", data[end : end + 2])[0] if end + 4 <= len(data) else 0
            if qname.lower().rstrip(".") == TARGET_DOMAIN and qtype == 1:
                resp = (
                    data[:2]
                    + b"\x81\x80"
                    + struct.pack("!HHHH", 1, 1, 0, 0)
                    + data[12 : end + 4]
                    + b"\xc0\x0c\x00\x01\x00\x01\x00\x00\x00\x3c\x00\x04"
                    + socket.inet_aton(self.lan_ip)
                )
                self.sock.sendto(resp, addr)
                self.on_log(f"[DNS] {addr[0]} -> {qname} 응답({self.lan_ip})")
                return
            up = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            up.settimeout(2.5)
            up.sendto(data, UPSTREAM_DNS)
            reply, _ = up.recvfrom(4096)
            up.close()
            self.sock.sendto(reply, addr)
        except Exception:
            pass

    def _loop(self):
        while self.running:
            try:
                data, addr = self.sock.recvfrom(4096)
                threading.Thread(target=self._handle, args=(data, addr), daemon=True).start()
            except Exception:
                break


def provision_wifi(ssid, password, dev_id=1001):
    payload = {
        "ActionType": "AP_INFO_TRANS",
        "SourceDevice": "M",
        "TargetDevice": "D",
        "NeedResponse": "N",
        "Value": {"apSsid": ssid, "apPass": password, "gbDevId": int(dev_id), "apMac": "00:00:00:00:00:00"},
    }
    s = socket.socket()
    s.settimeout(12)
    s.connect(("192.168.4.1", 8888))
    s.sendall((json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8"))
    try:
        s.recv(1024)
    except Exception:
        pass
    s.close()


class AppState:
    def __init__(self, on_log):
        self.on_log = on_log
        self.lock = threading.Lock()
        self.devices = {}  # dev_id -> dict
        self.broker = MiniMqttBroker(MQTT_PORT, self.on_mqtt_msg, on_log)
        self.lan_ip = get_lan_ip()
        self.dns = MiniDnsServer(DNS_PORT, self.lan_ip, on_log)
        self.dns_active = False

    def _ensure_dev(self, dev_id, prd_cd=54):
        if dev_id not in self.devices:
            self.devices[dev_id] = {
                "dev_id": dev_id,
                "prd_cd": prd_cd,
                "power": None,
                "auto": False,
                "spray": 0,
                "target_humid": 0,
                "timer": 0,
                "sleep": False,
                "warm": False,
                "humidity": None,
                "temp": None,
                "water_low": False,
                "last_seen": None,
            }
            self.on_log(f"[기기 발견] ID={dev_id} (prdCd={prd_cd})")
        return self.devices[dev_id]

    def on_mqtt_msg(self, topic, raw):
        parts = topic.split("/")
        if len(parts) != 3 or parts[0] != "client" or parts[2] != "info":
            return
        dev_id = parts[1]
        try:
            data = json.loads(raw)
        except Exception:
            return
        act_cd = data.get("actCd")
        act_val = str(data.get("actVal", ""))
        prd_cd = int(data.get("prdCd", 54))
        with self.lock:
            d = self._ensure_dev(dev_id, prd_cd)
            d["last_seen"] = time.strftime("%H:%M:%S")
            if act_cd == 10:
                vals = act_val.split(",")
                if len(vals) >= 7:
                    d["power"] = int(vals[0]) == 1
                    d["auto"] = int(vals[1]) == 1
                    d["spray"] = int(vals[2])
                    d["target_humid"] = hex2int(vals[3])
                    d["timer"] = hex2int(vals[4])
                    d["sleep"] = int(vals[5]) == 1
                    d["warm"] = int(vals[6]) != 0
            elif act_cd == 58:
                d["power"] = True
            elif act_cd == 59:
                d["power"] = False
            elif act_cd == 62:
                d["auto"] = act_val == "01"
            elif act_cd == 63:
                d["sleep"] = act_val == "01"
            elif act_cd == 64:
                vals = act_val.split(",")
                if len(vals) >= 2:
                    d["humidity"] = hex2int(vals[0])
                    d["temp"] = hex2int(vals[1])
            elif act_cd == 69:
                d["water_low"] = act_val == "00"
            elif act_cd == 70:
                d["timer"] = hex2int(act_val)
            elif act_cd == 72:
                d["warm"] = act_val == "01"

    def send_cmd(self, dev_id, act_cd, act_val=""):
        with self.lock:
            prd_cd = self.devices.get(dev_id, {}).get("prd_cd", 54)
        payload = json.dumps(
            {"devId": dev_id, "actVal": act_val, "actCd": int(act_cd), "prdCd": prd_cd},
            separators=(",", ":"),
        )
        self.broker.publish(f"client/{dev_id}/set", payload)
        self.on_log(f"[명령] {dev_id} <- actCd={act_cd} val='{act_val}'")


HTML = """<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<meta name="theme-color" content="#0f172a">
<meta name="apple-mobile-web-app-capable" content="yes">
<link rel="manifest" href="/manifest.json">
<link rel="icon" href="/icon.png">
<link rel="apple-touch-icon" href="/icon.png">
<title>LunaSquare 가습기</title>
<style>
  :root{--bg:#0f172a;--card:#1e293b;--btn:#334155;--on:#38bdf8;--text:#f8fafc;--muted:#94a3b8}
  *{box-sizing:border-box;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
  body{margin:0;background:var(--bg);color:var(--text);padding:16px;display:flex;justify-content:center}
  .wrap{width:100%;max-width:420px;display:flex;flex-direction:column;gap:12px}
  .card{background:var(--card);border-radius:16px;padding:16px}
  .top{display:flex;justify-content:space-between;align-items:center;gap:8px}
  .title{font-size:18px;font-weight:700}
  .sub{font-size:12px;color:var(--muted)}
  select{background:var(--btn);color:var(--text);border:0;border-radius:8px;padding:6px 10px;font-size:13px}
  .metrics{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:12px}
  .metric{background:#0f172a88;border-radius:12px;padding:12px;text-align:center}
  .metric b{display:block;font-size:26px;margin-top:4px;color:var(--on)}
  .warn{background:#7f1d1d;color:#fecaca;padding:10px;border-radius:10px;text-align:center;font-weight:600;display:none}
  .row{display:grid;grid-template-columns:repeat(auto-fit,minmax(70px,1fr));gap:8px;margin-top:8px}
  button{border:0;border-radius:12px;padding:12px 8px;background:var(--btn);color:var(--text);font-size:14px;font-weight:600;cursor:pointer}
  button.active{background:var(--on);color:#0f172a}
  label{font-size:13px;color:var(--muted);font-weight:600}
</style>
</head>
<body>
<div class="wrap">
  <div class="card">
    <div class="top">
      <div><div class="title">LunaSquare 가습기</div><div class="sub" id="seen">연결된 기기 대기 중...</div></div>
      <select id="devSel" onchange="curDev=this.value;load()"></select>
      <button onclick="cmd(9,'')">새로고침</button>
    </div>
    <div class="metrics">
      <div class="metric"><span class="sub">현재 습도</span><b id="hum">--%</b></div>
      <div class="metric"><span class="sub">현재 온도</span><b id="tmp">--℃</b></div>
    </div>
  </div>
  <div class="warn" id="water">물 부족! 물을 보충해주세요</div>
  <div class="card"><label>전원</label><div class="row"><button id="b-on" onclick="cmd(50,'')">켜기</button><button id="b-off" onclick="cmd(51,'')">끄기</button></div></div>
  <div class="card"><label>분무량 / 모드</label><div class="row"><button id="s1" onclick="cmd(53,'01')">1단</button><button id="s2" onclick="cmd(53,'02')">2단</button><button id="s3" onclick="cmd(53,'03')">3단</button><button id="auto" onclick="cmd(55,S.auto?'00':'01')">자동</button></div></div>
  <div class="card"><label>희망 습도</label><div class="row" id="hums"></div></div>
  <div class="card"><label>부가 기능</label><div class="row"><button id="warm" onclick="cmd(67,S.warm?'02':'01')">가온</button><button id="sleep" onclick="cmd(56,S.sleep?'02':'01')">수면등</button></div></div>
  <div class="card"><label>타이머 (시간)</label><div class="row" id="timers"></div></div>
</div>
<script>
let S={}, curDev='';
const hex=n=>n.toString(16).padStart(2,'0');
const hums=[0,40,45,50,55,60,65,70,75,80];
document.getElementById('hums').innerHTML=hums.map(h=>`<button id="h${h}" onclick="cmd(54,'${hex(h)}')">${h?h+'%':'연속'}</button>`).join('');
document.getElementById('timers').innerHTML=[0,1,2,4,6,8,10].map(t=>`<button id="t${t}" onclick="cmd(65,'${hex(t)}')">${t?t+'h':'해제'}</button>`).join('');
async function cmd(actCd,actVal){
  if(!curDev) return;
  await fetch('/api/cmd',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({devId:curDev,actCd,actVal})});
  setTimeout(load,350);
}
async function load(){
  const r=await fetch('/api/devices'); const devs=await r.json();
  const ids=Object.keys(devs);
  const sel=document.getElementById('devSel');
  if(ids.length && !devs[curDev]) curDev=ids[0];
  sel.innerHTML=ids.length?ids.map(id=>`<option value="${id}" ${id===curDev?'selected':''}>ID ${id}</option>`).join(''):'<option value="">기기 없음</option>';
  if(!curDev || !devs[curDev]) return;
  S=devs[curDev];
  document.getElementById('seen').textContent=S.last_seen?('최근 수신: '+S.last_seen):'대기 중...';
  document.getElementById('hum').textContent=S.humidity!=null?S.humidity+'%':'--%';
  document.getElementById('tmp').textContent=S.temp!=null?S.temp+'℃':'--℃';
  document.getElementById('water').style.display=S.water_low?'block':'none';
  document.getElementById('b-on').classList.toggle('active',S.power===true);
  document.getElementById('b-off').classList.toggle('active',S.power===false);
  [1,2,3].forEach(i=>document.getElementById('s'+i).classList.toggle('active',S.spray===i));
  document.getElementById('auto').classList.toggle('active',!!S.auto);
  document.getElementById('warm').classList.toggle('active',!!S.warm);
  document.getElementById('sleep').classList.toggle('active',!!S.sleep);
  hums.forEach(h=>document.getElementById('h'+h).classList.toggle('active',S.target_humid===h));
  [0,1,2,4,6,8,10].forEach(t=>document.getElementById('t'+t).classList.toggle('active',S.timer===t));
}
load();setInterval(load,2500);
</script>
</body>
</html>
"""

MANIFEST = json.dumps({
    "name": "LunaSquare 가습기",
    "short_name": "Luna가습기",
    "start_url": "/",
    "display": "standalone",
    "background_color": "#0f172a",
    "theme_color": "#0f172a",
    "icons": [{"src": "/icon.png", "sizes": "512x512", "type": "image/png"}],
}, ensure_ascii=False)


def start_http(app_state):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            pass

        def _send(self, code, ctype, body):
            data = body.encode("utf-8") if isinstance(body, str) else body
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            p = urlparse(self.path).path
            if p == "/":
                self._send(200, "text/html; charset=utf-8", HTML)
            elif p == "/manifest.json":
                self._send(200, "application/manifest+json; charset=utf-8", MANIFEST)
            elif p == "/icon.png":
                ip = resource_path("icon.png")
                if os.path.exists(ip):
                    with open(ip, "rb") as f:
                        self._send(200, "image/png", f.read())
                else:
                    self._send(404, "text/plain", "No icon")
            elif p == "/api/devices":
                with app_state.lock:
                    self._send(200, "application/json", json.dumps(app_state.devices))
            else:
                self._send(404, "text/plain", "Not found")

        def do_POST(self):
            if urlparse(self.path).path == "/api/cmd":
                n = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(n) or "{}")
                app_state.send_cmd(str(body["devId"]), body["actCd"], body.get("actVal", ""))
                self._send(200, "application/json", '{"ok":true}')
            else:
                self._send(404, "text/plain", "Not found")

    srv = ThreadingHTTPServer(("0.0.0.0", HTTP_PORT), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    app_state.on_log(f"[WEB] 웹 리모컨 시작 -> http://{app_state.lan_ip}:{HTTP_PORT}")


BG = "#0f172a"
CARD = "#1e293b"
BTN = "#334155"
ACCENT = "#38bdf8"
TEXT = "#f8fafc"
MUTED = "#94a3b8"
OK_BG = "#14532d"
OK_FG = "#86efac"
ERR_BG = "#7f1d1d"
ERR_FG = "#fecaca"
FONT = ("Malgun Gothic", 10)
FONT_B = ("Malgun Gothic", 10, "bold")


class LunaGui:
    def __init__(self, root):
        self.root = root
        self.root.title("LunaSquare 가습기 로컬 서버")
        self.root.geometry("500x760")
        self.root.configure(bg=BG)
        self.root.resizable(False, False)
        self.log_queue = []
        self.state = AppState(self.enqueue_log)
        self.active_dev = None
        self.cur_auto = False
        self.cur_warm = False
        self.cur_sleep = False

        wrap = tk.Frame(root, bg=BG, padx=16, pady=12)
        wrap.pack(fill="both", expand=True)

        # Header + Status pills
        hdr = tk.Frame(wrap, bg=BG)
        hdr.pack(fill="x", pady=(0, 8))
        tk.Label(hdr, text="LunaSquare 가습기", font=("Malgun Gothic", 15, "bold"), bg=BG, fg=TEXT).pack(side="left")
        self.lbl_ip = tk.Label(hdr, text=f"{self.state.lan_ip}:{HTTP_PORT}", font=("Consolas", 9), bg=CARD, fg=ACCENT, padx=8, pady=3)
        self.lbl_ip.pack(side="right")

        pills = tk.Frame(wrap, bg=BG)
        pills.pack(fill="x", pady=(0, 8))
        self.p_mqtt = self._pill(pills, f"MQTT {MQTT_PORT}")
        self.p_web = self._pill(pills, f"WEB {HTTP_PORT}")
        self.p_dns = self._pill(pills, f"DNS {DNS_PORT}")

        # Device & Live Metrics Card
        dev_card = tk.Frame(wrap, bg=CARD, padx=14, pady=10)
        dev_card.pack(fill="x", pady=(0, 8))
        dev_top = tk.Frame(dev_card, bg=CARD)
        dev_top.pack(fill="x")
        tk.Label(dev_top, text="가습기 리모컨", font=FONT_B, bg=CARD, fg=MUTED).pack(side="left")
        self.lbl_seen = tk.Label(dev_top, text="대기 중...", font=("Malgun Gothic", 9), bg=CARD, fg=MUTED)
        self.lbl_seen.pack(side="right")

        self.lbl_water = tk.Label(dev_card, text="물 부족! 물을 보충해주세요", font=("Malgun Gothic", 9, "bold"), bg=ERR_BG, fg=ERR_FG, pady=4)

        metrics = tk.Frame(dev_card, bg=CARD)
        metrics.pack(fill="x", pady=(8, 8))
        self.lbl_hum = self._metric(metrics, "현재 습도", "--%")
        self.lbl_tmp = self._metric(metrics, "현재 온도", "--℃", right=True)

        # Power + Refresh
        pwr_row = tk.Frame(dev_card, bg=CARD)
        pwr_row.pack(fill="x", pady=(2, 6))
        self.btn_on = self._btn(pwr_row, "켜기", lambda: self._quick_cmd(50), bg=BTN)
        self.btn_on.pack(side="left", fill="x", expand=True, padx=(0, 5))
        self.btn_off = self._btn(pwr_row, "끄기", lambda: self._quick_cmd(51), bg=BTN)
        self.btn_off.pack(side="left", fill="x", expand=True, padx=(0, 5))
        self._btn(pwr_row, "새로고침", lambda: self._quick_cmd(9), bg=BTN).pack(side="left", fill="x", expand=True)

        # Spray + Auto + Warm + Sleep
        mode_row = tk.Frame(dev_card, bg=CARD)
        mode_row.pack(fill="x", pady=(0, 6))
        self.spray_btns = {}
        for i in (1, 2, 3):
            b = self._btn(mode_row, f"{i}단", lambda v=f"0{i}": self._quick_cmd(53, v), pady=6)
            b.pack(side="left", fill="x", expand=True, padx=(0, 4))
            self.spray_btns[i] = b
        self.btn_auto = self._btn(mode_row, "자동", lambda: self._quick_cmd(55, "00" if self.cur_auto else "01"), pady=6)
        self.btn_auto.pack(side="left", fill="x", expand=True, padx=(0, 4))
        self.btn_warm = self._btn(mode_row, "가온", lambda: self._quick_cmd(67, "02" if self.cur_warm else "01"), pady=6)
        self.btn_warm.pack(side="left", fill="x", expand=True, padx=(0, 4))
        self.btn_sleep = self._btn(mode_row, "수면등", lambda: self._quick_cmd(56, "02" if self.cur_sleep else "01"), pady=6)
        self.btn_sleep.pack(side="left", fill="x", expand=True)

        # Target Humidity + Timer
        hum_row = tk.Frame(dev_card, bg=CARD)
        hum_row.pack(fill="x", pady=(0, 5))
        tk.Label(hum_row, text="습도", font=("Malgun Gothic", 9, "bold"), bg=CARD, fg=MUTED, width=5, anchor="w").pack(side="left")
        self.hum_btns = {}
        for h in (0, 40, 50, 60, 70, 80):
            b = self._btn(hum_row, f"{h}%" if h else "연속", lambda v=f"{h:02x}": self._quick_cmd(54, v), pady=4, font=("Malgun Gothic", 9, "bold"))
            b.pack(side="left", fill="x", expand=True, padx=(0, 3))
            self.hum_btns[h] = b

        tmr_row = tk.Frame(dev_card, bg=CARD)
        tmr_row.pack(fill="x")
        tk.Label(tmr_row, text="타이머", font=("Malgun Gothic", 9, "bold"), bg=CARD, fg=MUTED, width=5, anchor="w").pack(side="left")
        self.tmr_btns = {}
        for t in (0, 1, 2, 4, 8, 10):
            b = self._btn(tmr_row, f"{t}h" if t else "해제", lambda v=f"{t:02x}": self._quick_cmd(65, v), pady=4, font=("Malgun Gothic", 9, "bold"))
            b.pack(side="left", fill="x", expand=True, padx=(0, 3))
            self.tmr_btns[t] = b

        # Action Buttons
        acts = tk.Frame(wrap, bg=BG)
        acts.pack(fill="x", pady=(0, 8))
        self._btn(acts, "웹 리모컨 열기", lambda: webbrowser.open(f"http://localhost:{HTTP_PORT}"), bg=ACCENT, fg=BG).pack(side="left", fill="x", expand=True, padx=(0, 6))
        self._btn(acts, "새 Wi-Fi 등록", self.open_provision).pack(side="left", fill="x", expand=True, padx=(0, 6))
        self._btn(acts, "공유기 안내", self.show_guide).pack(side="left", fill="x", expand=True)

        # Log Console
        log_card = tk.Frame(wrap, bg=CARD, padx=10, pady=8)
        log_card.pack(fill="both", expand=True)
        tk.Label(log_card, text="실시간 로그", font=FONT_B, bg=CARD, fg=MUTED).pack(anchor="w", pady=(0, 4))
        self.txt_log = scrolledtext.ScrolledText(
            log_card, height=7, state="disabled", font=("Consolas", 9),
            bg="#0b1120", fg="#cbd5e1", insertbackground=TEXT, relief="flat", bd=0, padx=8, pady=6
        )
        self.txt_log.pack(fill="both", expand=True)

        self.start_services()
        self.tick()

    def _pill(self, parent, text):
        lbl = tk.Label(parent, text=text, font=("Malgun Gothic", 9, "bold"), bg=BTN, fg=MUTED, padx=10, pady=4)
        lbl.pack(side="left", padx=(0, 6))
        return lbl

    def _metric(self, parent, title, val, right=False):
        box = tk.Frame(parent, bg="#0f172a", padx=10, pady=8)
        box.pack(side="left", fill="x", expand=True, padx=(6 if right else 0, 0))
        tk.Label(box, text=title, font=("Malgun Gothic", 9), bg="#0f172a", fg=MUTED).pack()
        lbl = tk.Label(box, text=val, font=("Malgun Gothic", 20, "bold"), bg="#0f172a", fg=ACCENT)
        lbl.pack()
        return lbl

    def _btn(self, parent, text, cmd, bg=BTN, fg=TEXT, pady=8, font=FONT_B):
        return tk.Button(
            parent, text=text, command=cmd, font=font, bg=bg, fg=fg,
            activebackground=ACCENT, activeforeground=BG, relief="flat", bd=0, pady=pady, cursor="hand2"
        )

    def _quick_cmd(self, act_cd, act_val=""):
        if not self.active_dev:
            messagebox.showinfo("알림", "아직 연결된 가습기가 없습니다.")
            return
        self.state.send_cmd(self.active_dev, act_cd, act_val)

    def enqueue_log(self, msg):
        self.log_queue.append(f"[{time.strftime('%H:%M:%S')}] {msg}")

    def start_services(self):
        mqtt_ok = web_ok = False
        try:
            self.state.broker.start()
            mqtt_ok = True
        except Exception as e:
            self.enqueue_log(f"[MQTT 오류] 포트 {MQTT_PORT} 열기 실패: {e}")
        try:
            start_http(self.state)
            web_ok = True
        except Exception as e:
            self.enqueue_log(f"[WEB 오류] 포트 {HTTP_PORT} 열기 실패: {e}")
        try:
            self.state.dns.start()
            self.state.dns_active = True
        except Exception as e:
            self.enqueue_log(f"[DNS 비활성] 53포트 바인딩 불가(관리자 권한 필요 또는 사용 중): {e}")

        self.p_mqtt.config(text=f"MQTT {MQTT_PORT}: {'ON' if mqtt_ok else 'ERR'}", bg=OK_BG if mqtt_ok else ERR_BG, fg=OK_FG if mqtt_ok else ERR_FG)
        self.p_web.config(text=f"WEB {HTTP_PORT}: {'ON' if web_ok else 'ERR'}", bg=OK_BG if web_ok else ERR_BG, fg=OK_FG if web_ok else ERR_FG)
        self.p_dns.config(text=f"DNS {DNS_PORT}: {'ON' if self.state.dns_active else 'OFF'}", bg=OK_BG if self.state.dns_active else BTN, fg=OK_FG if self.state.dns_active else MUTED)

    def open_provision(self):
        win = tk.Toplevel(self.root)
        win.title("새 가습기 Wi-Fi 등록")
        win.geometry("380x235")
        win.configure(bg=BG)
        win.resizable(False, False)
        f = tk.Frame(win, bg=BG, padx=18, pady=14)
        f.pack(fill="both", expand=True)
        tk.Label(f, text="1. PC Wi-Fi를 'GB_DHF2_...'에 연결하세요.\n2. 아래에 집 2.4GHz Wi-Fi 정보를 입력 후 전송하세요.", font=("Malgun Gothic", 9), bg=BG, fg=MUTED, justify="left").pack(anchor="w", pady=(0, 12))
        grid = tk.Frame(f, bg=BG)
        grid.pack(fill="x")
        tk.Label(grid, text="2.4GHz SSID", font=FONT_B, bg=BG, fg=TEXT).grid(row=0, column=0, sticky="w", pady=6)
        e_ssid = tk.Entry(grid, width=24, font=FONT, bg=CARD, fg=TEXT, insertbackground=TEXT, relief="flat", bd=6)
        e_ssid.grid(row=0, column=1, padx=(10, 0), pady=6)
        tk.Label(grid, text="비밀번호", font=FONT_B, bg=BG, fg=TEXT).grid(row=1, column=0, sticky="w", pady=6)
        e_pass = tk.Entry(grid, width=24, font=FONT, show="*", bg=CARD, fg=TEXT, insertbackground=TEXT, relief="flat", bd=6)
        e_pass.grid(row=1, column=1, padx=(10, 0), pady=6)

        def go():
            try:
                provision_wifi(e_ssid.get().strip(), e_pass.get())
                self.enqueue_log(f"[등록] Wi-Fi 정보({e_ssid.get().strip()}) 전송 완료!")
                messagebox.showinfo("전송 완료", "가습기로 Wi-Fi 정보를 전송했습니다.\n잠시 후 가습기 Wi-Fi 불이 켜지면 자동 감지됩니다.", parent=win)
                win.destroy()
            except Exception as ex:
                messagebox.showerror("전송 실패", f"192.168.4.1:8888 연결 실패:\n{ex}\n\nPC Wi-Fi가 GB_DHF2_...에 연결되었는지 확인하세요.", parent=win)

        self._btn(f, "가습기로 Wi-Fi 정보 전송", go, bg=ACCENT, fg=BG).pack(fill="x", pady=(14, 0))

    def show_guide(self):
        ip = self.state.lan_ip
        msg = (
            f"가습기가 {TARGET_DOMAIN} 대신 이 PC({ip})로 접속하도록 공유기 설정이 필요합니다.\n\n"
            f"[방법 A - ipTIME / 통신사 공유기 등 모든 공유기]\n"
            f"1. 이 프로그램을 '관리자 권한으로 실행' (DNS 53: ON 확인)\n"
            f"2. 공유기 설정 → DHCP/LAN → 기본 DNS를 {ip}, 보조 DNS를 8.8.8.8로 설정\n\n"
            f"[방법 B - ASUS 공유기 (SSH)]\n"
            f"공유기 SSH에서 아래 실행:\n"
            f"echo '{ip} {TARGET_DOMAIN}' >> /etc/hosts && killall -HUP dnsmasq\n\n"
            f"[스마트폰 앱처럼 쓰기]\n"
            f"같은 Wi-Fi의 폰 브라우저에서 http://{ip}:{HTTP_PORT} 접속 → '홈 화면에 추가'"
        )
        messagebox.showinfo("공유기 설정 안내", msg)

    def tick(self):
        if self.log_queue:
            self.txt_log.config(state="normal")
            while self.log_queue:
                self.txt_log.insert("end", self.log_queue.pop(0) + "\n")
            self.txt_log.see("end")
            self.txt_log.config(state="disabled")
        with self.state.lock:
            if self.state.devices:
                did, d = next(iter(self.state.devices.items()))
                self.active_dev = did
                self.cur_auto, self.cur_warm, self.cur_sleep = bool(d["auto"]), bool(d["warm"]), bool(d["sleep"])
                pwr = "ON" if d["power"] is True else ("OFF" if d["power"] is False else "-")
                self.lbl_seen.config(text=f"ID {did} · 전원 {pwr} · {d['last_seen']}", fg=ACCENT)
                self.lbl_hum.config(text=f"{d['humidity']}%" if d["humidity"] is not None else "--%")
                self.lbl_tmp.config(text=f"{d['temp']}℃" if d["temp"] is not None else "--℃")
                if d["water_low"]:
                    self.lbl_water.pack(fill="x", pady=(6, 0))
                else:
                    self.lbl_water.pack_forget()
                self.btn_on.config(bg=ACCENT if d["power"] is True else BTN, fg=BG if d["power"] is True else TEXT)
                self.btn_off.config(bg=ACCENT if d["power"] is False else BTN, fg=BG if d["power"] is False else TEXT)
                for i, b in self.spray_btns.items():
                    on = d["spray"] == i
                    b.config(bg=ACCENT if on else BTN, fg=BG if on else TEXT)
                self.btn_auto.config(bg=ACCENT if self.cur_auto else BTN, fg=BG if self.cur_auto else TEXT)
                self.btn_warm.config(bg=ACCENT if self.cur_warm else BTN, fg=BG if self.cur_warm else TEXT)
                self.btn_sleep.config(bg=ACCENT if self.cur_sleep else BTN, fg=BG if self.cur_sleep else TEXT)
                for h, b in self.hum_btns.items():
                    on = d["target_humid"] == h
                    b.config(bg=ACCENT if on else BTN, fg=BG if on else TEXT)
                for t, b in self.tmr_btns.items():
                    on = d["timer"] == t
                    b.config(bg=ACCENT if on else BTN, fg=BG if on else TEXT)
        self.root.after(1000, self.tick)


def main():
    if "--headless" in sys.argv:
        st = AppState(print)
        st.broker.start()
        start_http(st)
        try:
            st.dns.start()
        except Exception as e:
            print(f"[DNS skipped] {e}")
        threading.Event().wait()
    else:
        global tk, messagebox, scrolledtext, ttk
        import tkinter as tk
        from tkinter import messagebox, scrolledtext, ttk
        root = tk.Tk()
        try:
            ip = resource_path("icon.png")
            if os.path.exists(ip):
                root.iconphoto(True, tk.PhotoImage(file=ip))
        except Exception:
            pass
        LunaGui(root)
        root.mainloop()


if __name__ == "__main__":
    main()
