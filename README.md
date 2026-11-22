# LunaSquare Wi-Fi 가습기 로컬 제어 서버 (PC & Android)

제조사 서비스(`smartlight.co.kr`) 종료로 Wi-Fi 원격 제어가 막힌 **LunaSquare IoT 가습기**를  
외부 클라우드 없이 **집 내부 네트워크**만으로 되살려 제어할 수 있는 올인원 로컬 서버 프로젝트입니다.

---

## 1. 작동 원리

기존 가습기는 회사 서버(`smartlight.co.kr:59001`)의 MQTT 브로커로 접속해 명령을 기다리는 구조였습니다.  
공식 앱 분석을 통해 **비암호화 TCP(`59001`)**, **도메인 기반 접속**, **평문 JSON 프로토콜**을 확인하여, 공유기 DNS만 집 내부 서버 IP로 돌리면 가습기가 내 PC 또는 안드로이드 공기계로 직접 연결되도록 재구현했습니다.

```text
가습기 ──▶ 공유기 DNS 우회 (smartlight.co.kr → 내 서버 IP)
       ──▶ 로컬 서버 (:59001 MQTT + :8090 웹 PWA 리모컨)
       ◀── 스마트폰 / PC 브라우저 (또는 전용 APK / EXE)
```

---

## 2. 다운로드 및 사용 방법 (Releases)

오른쪽 **Releases** 페이지에서 환경에 맞는 파일을 받아 바로 실행하세요.

### A. Windows PC용 (`LunaSquare_Local.exe`) — **모든 공유기 지원 (내장 DNS 포함)**
1. `LunaSquare_Local.exe`를 **마우스 우클릭 → 관리자 권한으로 실행**합니다. (방화벽 창이 뜨면 **개인 네트워크 허용**)
2. 상단 배지가 `MQTT 59001: ON`, `WEB 8090: ON`, `DNS 53: ON`인지 확인합니다.
3. **공유기 DNS 설정 (최초 1회)**:
   - GUI의 **「공유기 안내」** 버튼을 눌러 내 PC IP(`<내_PC_IP>`)를 확인합니다.
   - **ipTIME 공유기 기준 (`http://192.168.0.1`)**:
     1. **고급 설정 → 네트워크 관리 → DHCP 서버 설정**으로 이동
     2. 하단 **수동 IP 할당(또는 등록된 주소 관리)**에서 현재 PC의 IP(`<내_PC_IP>`)를 고정
     3. 같은 화면의 **기본 DNS 서버**에 `<내_PC_IP>`, **보조 DNS 서버**에 `8.8.8.8` 입력 후 **적용**
     4. 가습기 전원 코드를 뺐다 다시 꽂기
     *(※ PC가 꺼져 있을 때는 보조 DNS `8.8.8.8`로 넘어가며 인터넷 첫 접속이 1~2초 느려질 수 있습니다.)*
   - **ASUS 공유기 기준 (SSH 사용 시)**:
     - 관리자 실행 없이도 공유기 SSH에서 아래 실행 후 가습기 전원 재연결:
       ```sh
       echo "<내_PC_IP> smartlight.co.kr" >> /etc/hosts && killall -HUP dnsmasq
       ```
4. **가습기 Wi-Fi 등록 (초기화 상태일 때)**:
   - PC Wi-Fi를 가습기의 `GB_DHF2_...`에 연결 후, GUI의 **「새 Wi-Fi 등록」**에서 집 **2.4GHz** Wi-Fi 이름(대소문자 구분)과 비밀번호를 전송합니다.
5. **제어하기**:
   - GUI 창 안에서 바로 제어하거나, 같은 Wi-Fi의 스마트폰 브라우저에서 `http://<내_PC_IP>:8090` 접속 → **「홈 화면에 추가」**로 앱처럼 사용합니다.

---

### B. Android 공기계용 (`LunaSquare_Local.apk`) — **PC 없이 24시간 구동**
남는 안드로이드 폰(Android 6.0 이상)에 설치하면 **PC를 꺼도 24시간** 가습기를 제어할 수 있습니다.
1. 공기계에 `LunaSquare_Local.apk`를 설치하고 **실행**합니다. (추가 앱 설치나 별도 서버 생성 버튼 없이 **실행 즉시** 상단 알림바에 `MQTT :59001 / 웹 :8090` 서버가 자동 시작됩니다.)
2. 앱 화면 맨 위에 표시된 **`접속 주소: http://<공기계_IP>:8090`**에서 공기계의 IP를 확인합니다.
3. **공유기 DNS 설정 (최초 1회)**:
   - 공유기 LAN/DHCP 설정에서 `<공기계_IP>`를 수동 고정합니다.
   - 공유기 SSH(ASUS 등)에서 `smartlight.co.kr`이 공기계를 가리키도록 설정합니다:
     ```sh
     sed -i '/smartlight.co.kr/d' /etc/hosts && echo "<공기계_IP> smartlight.co.kr" >> /etc/hosts && killall -HUP dnsmasq
     ```
4. **가습기 재접속**: 이미 집 Wi-Fi에 연결된 가습기라면 전원 코드를 뺐다 다시 꽂아주세요. (초기화 상태라면 앱 내 **「새 가습기 Wi-Fi 연결」**로 등록)
5. **배터리 설정 & 원격 제어**:
   - 공기계 설정 → 앱 → `LunaSquare 로컬` → 배터리 → **제한 없음**으로 설정하면 화면이 꺼져도 24시간 유지됩니다.
   - 공기계 앱 화면에서 바로 제어하거나, 주 사용 폰 브라우저에서도 `http://<공기계_IP>:8090`으로 접속해 함께 제어할 수 있습니다.

*(※ 안드로이드 앱은 루팅 없이 53번 DNS 포트를 열 수 없으므로, **ASUS 공유기**처럼 `/etc/hosts` 수정이 가능한 공유기나 별도 커스텀 DNS(AdGuard Home 등) 환경에 적합합니다. **ipTIME 순정 공유기**에서는 내장 DNS가 포함된 **PC용 `LunaSquare_Local.exe`**를 사용하세요.)*

---

## 3. 소스에서 직접 빌드하기

### PC (`pc/luna_allinone.py`)
```sh
pip install pyinstaller
python pc/luna_allinone.py          # GUI 실행
python pc/luna_allinone.py --headless  # Linux/Termux 등 헤드리스 실행
```

### Android (`android/`)
- 외부 라이브러리 없이 Android SDK(`minSdk 23`, `targetSdk 33`) 표준 API만으로 작성되었습니다.
- `android/AndroidManifest.xml`, `android/src/com/lunasquare/local/*.java`, `android/res/`

---

## 4. 지원 문서
- 상세 원리 및 배포 가이드 PDF: [docs/LunaSquare_가습기_로컬제어_배포가이드.pdf](docs/LunaSquare_가습기_로컬제어_배포가이드.pdf)
