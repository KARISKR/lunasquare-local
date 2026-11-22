package com.lunasquare.local;

import android.content.Context;
import android.net.wifi.WifiManager;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class LunaCore {
    public static final int MQTT_PORT = 59001;
    public static final int HTTP_PORT = 8090;

    private final Context ctx;
    private volatile boolean running = false;
    private ServerSocket mqttSrv;
    private ServerSocket httpSrv;

    private final Object mqttLock = new Object();
    private final Map<Socket, Set<String>> mqttClients = new HashMap<>();

    private final Object devLock = new Object();
    private final Map<String, JSONObject> devices = new HashMap<>();

    public LunaCore(Context ctx) {
        this.ctx = ctx;
    }

    public void start() {
        running = true;
        new Thread(this::runMqttServer, "MqttBroker").start();
        new Thread(this::runHttpServer, "HttpServer").start();
    }

    public void stop() {
        running = false;
        try { if (mqttSrv != null) mqttSrv.close(); } catch (Exception ignored) {}
        try { if (httpSrv != null) httpSrv.close(); } catch (Exception ignored) {}
    }

    private String getWifiIp() {
        try {
            WifiManager wm = (WifiManager) ctx.getApplicationContext().getSystemService(Context.WIFI_SERVICE);
            if (wm != null) {
                int ip = wm.getConnectionInfo().getIpAddress();
                if (ip != 0) {
                    return String.format(Locale.US, "%d.%d.%d.%d",
                            (ip & 0xff), (ip >> 8 & 0xff), (ip >> 16 & 0xff), (ip >> 24 & 0xff));
                }
            }
            for (NetworkInterface nif : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                for (InetAddress addr : Collections.list(nif.getInetAddresses())) {
                    if (!addr.isLoopbackAddress() && addr.getAddress().length == 4) {
                        return addr.getHostAddress();
                    }
                }
            }
        } catch (Exception ignored) {}
        return "127.0.0.1";
    }

    private void runMqttServer() {
        try {
            mqttSrv = new ServerSocket(MQTT_PORT);
            while (running) {
                Socket cli = mqttSrv.accept();
                cli.setSoTimeout(120000);
                synchronized (mqttLock) {
                    mqttClients.put(cli, new HashSet<String>());
                }
                new Thread(() -> handleMqttClient(cli), "MqttCli").start();
            }
        } catch (Exception ignored) {}
    }

    private void handleMqttClient(Socket cli) {
        try {
            DataInputStream in = new DataInputStream(cli.getInputStream());
            OutputStream out = cli.getOutputStream();
            while (running) {
                int first = in.read();
                if (first < 0) break;
                int ptype = (first >> 4) & 0x0f;
                int flags = first & 0x0f;

                int remLen = 0, mult = 1;
                while (true) {
                    int b = in.read();
                    if (b < 0) return;
                    remLen += (b & 0x7f) * mult;
                    if ((b & 0x80) == 0) break;
                    mult *= 128;
                }
                byte[] body = new byte[remLen];
                if (remLen > 0) in.readFully(body);

                if (ptype == 1) { // CONNECT
                    out.write(new byte[]{0x20, 0x02, 0x00, 0x00});
                    out.flush();
                } else if (ptype == 3) { // PUBLISH
                    int qos = (flags >> 1) & 0x03;
                    int tlen = ((body[0] & 0xff) << 8) | (body[1] & 0xff);
                    String topic = new String(body, 2, tlen, StandardCharsets.UTF_8);
                    int pos = 2 + tlen;
                    if (qos > 0) {
                        byte hi = body[pos], lo = body[pos + 1];
                        pos += 2;
                        out.write(new byte[]{0x40, 0x02, hi, lo});
                        out.flush();
                    }
                    String payload = new String(body, pos, body.length - pos, StandardCharsets.UTF_8);
                    onMqttMessage(topic, payload);
                    mqttPublish(topic, payload, cli);
                } else if (ptype == 8) { // SUBSCRIBE
                    byte hi = body[0], lo = body[1];
                    int pos = 2;
                    List<Byte> granted = new ArrayList<>();
                    while (pos < body.length) {
                        int tlen = ((body[pos] & 0xff) << 8) | (body[pos + 1] & 0xff);
                        String filter = new String(body, pos + 2, tlen, StandardCharsets.UTF_8);
                        pos += 2 + tlen + 1;
                        synchronized (mqttLock) {
                            Set<String> s = mqttClients.get(cli);
                            if (s != null) s.add(filter);
                        }
                        granted.add((byte) 0);
                    }
                    ByteArrayOutputStream suback = new ByteArrayOutputStream();
                    suback.write(0x90);
                    writeRemLen(suback, 2 + granted.size());
                    suback.write(hi);
                    suback.write(lo);
                    for (byte g : granted) suback.write(g);
                    out.write(suback.toByteArray());
                    out.flush();
                } else if (ptype == 12) { // PINGREQ
                    out.write(new byte[]{(byte) 0xd0, 0x00});
                    out.flush();
                } else if (ptype == 14) { // DISCONNECT
                    break;
                }
            }
        } catch (Exception ignored) {
        } finally {
            synchronized (mqttLock) { mqttClients.remove(cli); }
            try { cli.close(); } catch (Exception ignored) {}
        }
    }

    private static void writeRemLen(ByteArrayOutputStream out, int n) {
        do {
            int b = n % 128;
            n /= 128;
            if (n > 0) b |= 0x80;
            out.write(b);
        } while (n > 0);
    }

    private static boolean topicMatch(String sub, String topic) {
        if ("#".equals(sub) || sub.equals(topic)) return true;
        String[] sp = sub.split("/"), tp = topic.split("/");
        for (int i = 0; i < sp.length; i++) {
            if ("#".equals(sp[i])) return true;
            if (i >= tp.length) return false;
            if (!"+".equals(sp[i]) && !sp[i].equals(tp[i])) return false;
        }
        return sp.length == tp.length;
    }

    private void mqttPublish(String topic, String payload, Socket exclude) {
        try {
            byte[] tb = topic.getBytes(StandardCharsets.UTF_8);
            byte[] pb = payload.getBytes(StandardCharsets.UTF_8);
            ByteArrayOutputStream pkt = new ByteArrayOutputStream();
            pkt.write(0x30);
            writeRemLen(pkt, 2 + tb.length + pb.length);
            pkt.write((tb.length >> 8) & 0xff);
            pkt.write(tb.length & 0xff);
            pkt.write(tb);
            pkt.write(pb);
            byte[] bytes = pkt.toByteArray();
            List<Socket> targets = new ArrayList<>();
            synchronized (mqttLock) {
                for (Map.Entry<Socket, Set<String>> e : mqttClients.entrySet()) {
                    if (e.getKey() == exclude) continue;
                    for (String sub : e.getValue()) {
                        if (topicMatch(sub, topic)) { targets.add(e.getKey()); break; }
                    }
                }
            }
            for (Socket c : targets) {
                try { c.getOutputStream().write(bytes); c.getOutputStream().flush(); } catch (Exception ignored) {}
            }
        } catch (Exception ignored) {}
    }

    private static int hex2int(String s) {
        try { return Integer.parseInt(s.trim(), 16); } catch (Exception e) { return 0; }
    }

    private void onMqttMessage(String topic, String raw) {
        String[] parts = topic.split("/");
        if (parts.length != 3 || !"client".equals(parts[0]) || !"info".equals(parts[2])) return;
        String devId = parts[1];
        try {
            JSONObject msg = new JSONObject(raw);
            int actCd = msg.optInt("actCd", -1);
            String actVal = msg.optString("actVal", "");
            int prdCd = msg.optInt("prdCd", 54);
            synchronized (devLock) {
                JSONObject d = devices.get(devId);
                if (d == null) {
                    d = new JSONObject();
                    d.put("dev_id", devId);
                    d.put("prd_cd", prdCd);
                    d.put("power", JSONObject.NULL);
                    d.put("auto", false);
                    d.put("spray", 0);
                    d.put("target_humid", 0);
                    d.put("timer", 0);
                    d.put("sleep", false);
                    d.put("warm", false);
                    d.put("humidity", JSONObject.NULL);
                    d.put("temp", JSONObject.NULL);
                    d.put("water_low", false);
                    devices.put(devId, d);
                }
                d.put("last_seen", new SimpleDateFormat("HH:mm:ss", Locale.US).format(new Date()));
                if (actCd == 10) {
                    String[] v = actVal.split(",");
                    if (v.length >= 7) {
                        d.put("power", Integer.parseInt(v[0].trim()) == 1);
                        d.put("auto", Integer.parseInt(v[1].trim()) == 1);
                        d.put("spray", Integer.parseInt(v[2].trim()));
                        d.put("target_humid", hex2int(v[3]));
                        d.put("timer", hex2int(v[4]));
                        d.put("sleep", Integer.parseInt(v[5].trim()) == 1);
                        d.put("warm", Integer.parseInt(v[6].trim()) != 0);
                    }
                } else if (actCd == 58) {
                    d.put("power", true);
                } else if (actCd == 59) {
                    d.put("power", false);
                } else if (actCd == 62) {
                    d.put("auto", "01".equals(actVal));
                } else if (actCd == 63) {
                    d.put("sleep", "01".equals(actVal));
                } else if (actCd == 64) {
                    String[] v = actVal.split(",");
                    if (v.length >= 2) {
                        d.put("humidity", hex2int(v[0]));
                        d.put("temp", hex2int(v[1]));
                    }
                } else if (actCd == 69) {
                    d.put("water_low", "00".equals(actVal));
                } else if (actCd == 70) {
                    d.put("timer", hex2int(actVal));
                } else if (actCd == 72) {
                    d.put("warm", "01".equals(actVal));
                }
            }
        } catch (Exception ignored) {}
    }

    private void sendCmd(String devId, int actCd, String actVal) throws Exception {
        int prdCd = 54;
        synchronized (devLock) {
            JSONObject d = devices.get(devId);
            if (d != null) prdCd = d.optInt("prd_cd", 54);
        }
        JSONObject p = new JSONObject();
        p.put("devId", devId);
        p.put("actVal", actVal);
        p.put("actCd", actCd);
        p.put("prdCd", prdCd);
        mqttPublish("client/" + devId + "/set", p.toString(), null);
    }

    private void provisionWifi(String ssid, String pass) throws Exception {
        JSONObject val = new JSONObject();
        val.put("apSsid", ssid);
        val.put("apPass", pass);
        val.put("gbDevId", 1001);
        val.put("apMac", "00:00:00:00:00:00");
        JSONObject root = new JSONObject();
        root.put("ActionType", "AP_INFO_TRANS");
        root.put("SourceDevice", "M");
        root.put("TargetDevice", "D");
        root.put("NeedResponse", "N");
        root.put("Value", val);
        Socket s = new Socket("192.168.4.1", 8888);
        s.setSoTimeout(10000);
        s.getOutputStream().write((root.toString() + "\n").getBytes(StandardCharsets.UTF_8));
        s.getOutputStream().flush();
        try { s.getInputStream().read(new byte[512]); } catch (Exception ignored) {}
        s.close();
    }

    private void runHttpServer() {
        try {
            httpSrv = new ServerSocket(HTTP_PORT);
            while (running) {
                Socket cli = httpSrv.accept();
                new Thread(() -> handleHttp(cli), "HttpCli").start();
            }
        } catch (Exception ignored) {}
    }

    private void handleHttp(Socket cli) {
        try {
            InputStream in = cli.getInputStream();
            StringBuilder hdr = new StringBuilder();
            int c, contentLen = 0;
            while ((c = in.read()) != -1) {
                hdr.append((char) c);
                int len = hdr.length();
                if (len >= 4 && hdr.substring(len - 4).equals("\r\n\r\n")) break;
            }
            String[] lines = hdr.toString().split("\r\n");
            if (lines.length == 0) { cli.close(); return; }
            String[] req = lines[0].split(" ");
            if (req.length < 2) { cli.close(); return; }
            String method = req[0], path = req[1];
            for (String ln : lines) {
                if (ln.toLowerCase(Locale.US).startsWith("content-length:")) {
                    contentLen = Integer.parseInt(ln.substring(15).trim());
                }
            }
            byte[] bodyBytes = new byte[contentLen];
            int read = 0;
            while (read < contentLen) {
                int r = in.read(bodyBytes, read, contentLen - read);
                if (r < 0) break;
                read += r;
            }
            String body = new String(bodyBytes, StandardCharsets.UTF_8);

            OutputStream out = cli.getOutputStream();
            if ("GET".equals(method) && "/".equals(path)) {
                sendResp(out, 200, "text/html; charset=utf-8", buildHtml().getBytes(StandardCharsets.UTF_8));
            } else if ("GET".equals(method) && "/api/devices".equals(path)) {
                JSONObject res = new JSONObject();
                synchronized (devLock) {
                    for (Map.Entry<String, JSONObject> e : devices.entrySet()) res.put(e.getKey(), e.getValue());
                }
                sendResp(out, 200, "application/json", res.toString().getBytes(StandardCharsets.UTF_8));
            } else if ("GET".equals(method) && "/manifest.json".equals(path)) {
                String mf = "{\"name\":\"LunaSquare 가습기\",\"short_name\":\"Luna가습기\",\"start_url\":\"/\",\"display\":\"standalone\",\"background_color\":\"#0f172a\",\"theme_color\":\"#0f172a\",\"icons\":[{\"src\":\"/icon.png\",\"sizes\":\"512x512\",\"type\":\"image/png\"}]}";
                sendResp(out, 200, "application/manifest+json; charset=utf-8", mf.getBytes(StandardCharsets.UTF_8));
            } else if ("GET".equals(method) && "/icon.png".equals(path)) {
                InputStream is = ctx.getResources().openRawResource(R.drawable.ic_launcher);
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                byte[] buf = new byte[4096];
                int n;
                while ((n = is.read(buf)) > 0) bos.write(buf, 0, n);
                is.close();
                sendResp(out, 200, "image/png", bos.toByteArray());
            } else if ("POST".equals(method) && "/api/cmd".equals(path)) {
                JSONObject j = new JSONObject(body);
                sendCmd(j.getString("devId"), j.getInt("actCd"), j.optString("actVal", ""));
                sendResp(out, 200, "application/json", "{\"ok\":true}".getBytes(StandardCharsets.UTF_8));
            } else if ("POST".equals(method) && "/api/provision".equals(path)) {
                try {
                    JSONObject j = new JSONObject(body);
                    provisionWifi(j.getString("ssid"), j.optString("pass", ""));
                    sendResp(out, 200, "application/json", "{\"ok\":true}".getBytes(StandardCharsets.UTF_8));
                } catch (Exception ex) {
                    String err = new JSONObject().put("ok", false).put("error", ex.getMessage()).toString();
                    sendResp(out, 500, "application/json", err.getBytes(StandardCharsets.UTF_8));
                }
            } else {
                sendResp(out, 404, "text/plain", "Not found".getBytes(StandardCharsets.UTF_8));
            }
            cli.close();
        } catch (Exception ignored) {
            try { cli.close(); } catch (Exception ignored2) {}
        }
    }

    private void sendResp(OutputStream out, int code, String ctype, byte[] data) throws Exception {
        String hdr = "HTTP/1.1 " + code + " OK\r\nContent-Type: " + ctype + "\r\nContent-Length: " + data.length + "\r\nConnection: close\r\n\r\n";
        out.write(hdr.getBytes(StandardCharsets.UTF_8));
        out.write(data);
        out.flush();
    }

    private String buildHtml() {
        String ip = getWifiIp();
        return "<!doctype html><html lang='ko'><head><meta charset='utf-8'>"
            + "<meta name='viewport' content='width=device-width,initial-scale=1,maximum-scale=1'>"
            + "<meta name='theme-color' content='#0f172a'><meta name='apple-mobile-web-app-capable' content='yes'>"
            + "<link rel='manifest' href='/manifest.json'><link rel='icon' href='/icon.png'>"
            + "<title>LunaSquare 로컬</title><style>"
            + ":root{--bg:#0f172a;--card:#1e293b;--btn:#334155;--on:#38bdf8;--text:#f8fafc;--muted:#94a3b8}"
            + "*{box-sizing:border-box;font-family:-apple-system,sans-serif}"
            + "body{margin:0;background:var(--bg);color:var(--text);padding:14px;display:flex;justify-content:center}"
            + ".wrap{width:100%;max-width:440px;display:flex;flex-direction:column;gap:10px}"
            + ".card{background:var(--card);border-radius:16px;padding:14px}"
            + ".top{display:flex;justify-content:space-between;align-items:center;gap:6px}"
            + ".title{font-size:17px;font-weight:700}.sub{font-size:12px;color:var(--muted)}"
            + ".pill{background:#14532d;color:#86efac;padding:3px 8px;border-radius:8px;font-size:11px;font-weight:700}"
            + "select,input{background:var(--btn);color:var(--text);border:0;border-radius:10px;padding:9px 10px;font-size:13px;width:100%}"
            + "select{width:auto}"
            + ".metrics{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:10px}"
            + ".metric{background:#0f172a99;border-radius:12px;padding:10px;text-align:center}"
            + ".metric b{display:block;font-size:24px;margin-top:2px;color:var(--on)}"
            + ".warn{background:#7f1d1d;color:#fecaca;padding:10px;border-radius:10px;text-align:center;font-weight:600;display:none}"
            + ".row{display:grid;grid-template-columns:repeat(auto-fit,minmax(64px,1fr));gap:7px;margin-top:8px}"
            + "button{border:0;border-radius:11px;padding:11px 6px;background:var(--btn);color:var(--text);font-size:13px;font-weight:600}"
            + "button.active{background:var(--on);color:#0f172a}"
            + "button.accent{background:#0284c7}"
            + "label{font-size:12px;color:var(--muted);font-weight:600}"
            + "</style></head><body><div class='wrap'>"
            + "<div class='card'><div class='top'><div><div class='title'>LunaSquare 로컬 서버</div>"
            + "<div class='sub'>접속 주소: http://" + ip + ":" + HTTP_PORT + "</div></div>"
            + "<span class='pill'>MQTT :59001 ON</span></div>"
            + "<div class='top' style='margin-top:10px'><span class='sub' id='seen'>가습기 대기 중...</span>"
            + "<div style='display:flex;gap:6px'><select id='devSel' onchange='curDev=this.value;load()'></select>"
            + "<button onclick=\"cmd(9,'')\">새로고침</button></div></div>"
            + "<div class='metrics'><div class='metric'><span class='sub'>현재 습도</span><b id='hum'>--%</b></div>"
            + "<div class='metric'><span class='sub'>현재 온도</span><b id='tmp'>--℃</b></div></div></div>"
            + "<div class='warn' id='water'>물 부족! 물을 보충해주세요</div>"
            + "<div class='card'><label>전원</label><div class='row'>"
            + "<button id='b-on' onclick=\"cmd(50,'')\">켜기</button><button id='b-off' onclick=\"cmd(51,'')\">끄기</button></div></div>"
            + "<div class='card'><label>분무량 / 모드</label><div class='row'>"
            + "<button id='s1' onclick=\"cmd(53,'01')\">1단</button><button id='s2' onclick=\"cmd(53,'02')\">2단</button>"
            + "<button id='s3' onclick=\"cmd(53,'03')\">3단</button><button id='auto' onclick=\"cmd(55,S.auto?'00':'01')\">자동</button></div></div>"
            + "<div class='card'><label>희망 습도</label><div class='row' id='hums'></div></div>"
            + "<div class='card'><label>부가 기능</label><div class='row'>"
            + "<button id='warm' onclick=\"cmd(67,S.warm?'02':'01')\">가온</button>"
            + "<button id='sleep' onclick=\"cmd(56,S.sleep?'02':'01')\">수면등</button></div></div>"
            + "<div class='card'><label>타이머 (시간)</label><div class='row' id='timers'></div></div>"
            + "<div class='card'><div class='top'><label>새 가습기 Wi-Fi 연결</label>"
            + "<button onclick=\"var x=document.getElementById('prov');x.style.display=x.style.display==='none'?'block':'none'\">열기/닫기</button></div>"
            + "<div id='prov' style='display:none;margin-top:10px'>"
            + "<div class='sub' style='margin-bottom:8px'>1. 폰 Wi-Fi를 GB_DHF2_...에 먼저 연결하세요.<br>2. 집 2.4GHz Wi-Fi 이름(대소문자 구분)과 비번 입력 후 전송하세요.</div>"
            + "<input id='pssid' placeholder='집 2.4GHz Wi-Fi 이름' style='margin-bottom:6px'>"
            + "<input id='ppass' type='password' placeholder='Wi-Fi 비밀번호' style='margin-bottom:8px'>"
            + "<button class='accent' style='width:100%' onclick='prov()'>가습기로 Wi-Fi 정보 전송</button>"
            + "<div id='pmsg' class='sub' style='margin-top:6px'></div></div></div>"
            + "<div class='card'><div class='top'><label>공유기 DNS 설정 안내 (최초 1회)</label>"
            + "<button onclick=\"var g=document.getElementById('guide');g.style.display=g.style.display==='none'?'block':'none'\">보기</button></div>"
            + "<div id='guide' style='display:none;margin-top:8px' class='sub'>"
            + "1. 공유기 설정에서 이 폰 IP(<b>" + ip + "</b>)를 고정하세요.<br>"
            + "2. ASUS 공유기 SSH: <code>echo '" + ip + " smartlight.co.kr' &gt;&gt; /etc/hosts &amp;&amp; killall -HUP dnsmasq</code><br>"
            + "3. 다른 폰에서도 <b>http://" + ip + ":" + HTTP_PORT + "</b> 로 접속해 함께 제어할 수 있습니다.</div></div>"
            + "</div><script>"
            + "let S={},curDev='';const hex=n=>n.toString(16).padStart(2,'0');"
            + "const hums=[0,40,45,50,55,60,65,70,75,80];"
            + "document.getElementById('hums').innerHTML=hums.map(h=>`<button id='h${h}' onclick=\"cmd(54,'${hex(h)}')\">${h?h+'%':'연속'}</button>`).join('');"
            + "document.getElementById('timers').innerHTML=[0,1,2,4,6,8,10].map(t=>`<button id='t${t}' onclick=\"cmd(65,'${hex(t)}')\">${t?t+'h':'해제'}</button>`).join('');"
            + "async function cmd(actCd,actVal){if(!curDev)return;await fetch('/api/cmd',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({devId:curDev,actCd,actVal})});setTimeout(load,350);}"
            + "async function prov(){const r=await fetch('/api/provision',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({ssid:document.getElementById('pssid').value.trim(),pass:document.getElementById('ppass').value})});const d=await r.json();document.getElementById('pmsg').textContent=d.ok?'전송 완료! 이제 폰 Wi-Fi를 다시 집 와이파이로 돌려주세요.':'실패: '+(d.error||'GB_DHF2_ 연결 확인');}"
            + "async function load(){try{const r=await fetch('/api/devices');const devs=await r.json();const ids=Object.keys(devs);const sel=document.getElementById('devSel');"
            + "if(ids.length&&!devs[curDev])curDev=ids[0];"
            + "sel.innerHTML=ids.length?ids.map(id=>`<option value='${id}' ${id===curDev?'selected':''}>ID ${id}</option>`).join(''):\"<option value=''>기기 없음</option>\";"
            + "if(!curDev||!devs[curDev])return;S=devs[curDev];"
            + "document.getElementById('seen').textContent=S.last_seen?('최근 수신: '+S.last_seen):'대기 중...';"
            + "document.getElementById('hum').textContent=S.humidity!=null?S.humidity+'%':'--%';"
            + "document.getElementById('tmp').textContent=S.temp!=null?S.temp+'℃':'--℃';"
            + "document.getElementById('water').style.display=S.water_low?'block':'none';"
            + "document.getElementById('b-on').classList.toggle('active',S.power===true);"
            + "document.getElementById('b-off').classList.toggle('active',S.power===false);"
            + "[1,2,3].forEach(i=>document.getElementById('s'+i).classList.toggle('active',S.spray===i));"
            + "document.getElementById('auto').classList.toggle('active',!!S.auto);"
            + "document.getElementById('warm').classList.toggle('active',!!S.warm);"
            + "document.getElementById('sleep').classList.toggle('active',!!S.sleep);"
            + "hums.forEach(h=>document.getElementById('h'+h).classList.toggle('active',S.target_humid===h));"
            + "[0,1,2,4,6,8,10].forEach(t=>document.getElementById('t'+t).classList.toggle('active',S.timer===t));}catch(e){}}"
            + "load();setInterval(load,2500);</script></body></html>";
    }
}
