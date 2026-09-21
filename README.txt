분산 환경 Fault-Tolerant 키-값 저장소

1. 프로그램 개요 및 조원 정보

Master 1개와 Worker 4개가 TCP Socket으로 통신하며 고유 Key-Value 작업 5,000개를 처리한다.
Master는 AWS EC2 외부 서버에서, Worker 4개는 한 로컬 PC의 독립 Thread로 실행한다.
Key는 고유한 4자리 16진수, Value는 1~100의 정수이다. Worker는 최대 10개의 Ready Queue,
80% 성공/20% 실패 시뮬레이션, 실패 재할당과 P2P 부하 분산에 참여한다.

- 20223134 장승훈: 전체 초기 구현, Master·AWS 구축
- 20213132 정우진: Worker 실행·로그 UX, 종료·Queue·P2P 안정성 보완, README 작성·정리
- 20223099 김현중: 시연 영상

2. 파일 구성

src/DistributedKvApp.java: master/workers 역할을 선택하는 실행 진입점
src/MasterNode.java: 작업 생성·배정, 가상 시계, 결과·재시도·통계·종료 관리
src/WorkerNode.java: Worker Thread, Queue, 작업 처리, P2P 통신, Worker 로그
src/Shared.java: Task, VirtualClock, EventLogger, WorkerInfo 등 공통 구성요소
launcher/WorkerLauncher.java: 연결 검사·화면 제어·로그 보관·통합 요약의 공통 도우미

distributed-kv.jar: 분산 프로그램 실행 파일 (Java 17 대상)
worker-launcher.jar: 편의 실행 도우미 (Java 17 대상, 위 JAR를 자식 프로세스로 실행)
run-workers.cmd: Windows 더블클릭 실행 및 종료 후 창 유지
run-workers.ps1: Java 검사 후 도우미 실행 (Windows 기본 PowerShell 5.1 또는 PowerShell 7)
run-workers.sh: Linux/macOS Bash에서 Java 검사 후 같은 도우미 실행
AllDefinedLogs.txt: EVENT·STATUS 및 통계 정의
Master.txt, Worker1.txt~Worker4.txt: 결과 로그
logs/: 편의 실행 시 실행별 로그 보관 폴더
tests/: 개발용 검증 코드
run-master.ps1: AWS 관리자용 배포·실행 관리 도구 (일반 Worker 실행에는 불필요)

실행 스크립트와 두 JAR는 프로젝트 최상위 폴더에 함께 둔다.

3. 사전 준비

3-1. 공통

- JAR 실행: Java 17 이상 실행 환경 필요. javac는 필요하지 않다.
- 소스 컴파일: javac와 jar가 포함된 JDK 17 이상 필요. 빌드는 --release 17을 사용한다.
- JAVA_HOME은 JDK 설치 폴더(bin 제외), PATH에는 해당 폴더의 bin을 추가한다.
- 기존 PATH를 덮어쓰지 않는다. 설치·환경변수 변경 후 터미널을 새로 연다.
- 실행 스크립트는 JAVA_HOME, PATH 순으로 사용 가능한 Java 17 이상을 찾는다.
- Python, 추가 패키지, Java 자동 설치는 사용하지 않는다.

3-2. Windows

Temurin 등 JDK 17을 설치한다. 설치 시 PATH/JAVA_HOME 설정 옵션을 사용할 수 있다.
자동 설정되지 않았다면 '시스템 환경 변수 편집 → 환경 변수'에서 사용자 변수를 설정한다.
  JAVA_HOME: C:\Program Files\Eclipse Adoptium\<실제 JDK 설치 폴더>
  Path에 추가: %JAVA_HOME%\bin
경로 값에 따옴표를 넣지 않는다. 새 PowerShell에서 확인한다.
  java -version
  javac -version
  jar --version
다른 Java가 선택되면 where.exe java, where.exe javac로 경로를 확인한다.

3-3. macOS

PC 아키텍처에 맞는 JDK 17을 설치한다(Apple Silicon: aarch64, Intel: x64).
기본 zsh 환경에서는 ~/.zshrc에 다음을 추가하고 적용한다.
  export JAVA_HOME=$(/usr/libexec/java_home -v 17)
  export PATH="$JAVA_HOME/bin:$PATH"
  source ~/.zshrc
  java -version
  javac -version
  jar --version

3-4. Linux

Ubuntu:
  sudo apt update
  sudo apt install openjdk-17-jdk
Amazon Linux 2023:
  sudo yum install java-17-amazon-corretto-devel

설치된 JDK의 위치는 다음으로 확인한다.
  javac -version
  readlink -f "$(command -v javac)"
출력의 /bin/javac 앞까지가 JDK 폴더이다. 다른 버전이 선택되면 JDK 17 경로를 확인한다.
Bash의 ~/.bashrc에 다음을 설정한다(zsh는 ~/.zshrc).
  export JAVA_HOME="<실제 JDK 설치 경로>"
  export PATH="$JAVA_HOME/bin:$PATH"
  source ~/.bashrc
  java -version
  javac -version
  jar --version

java를 못 찾으면 설치 및 PATH를, java만 되고 javac가 없으면 JDK 설치 및 경로를 확인한다.
macOS/Linux의 경로 확인은 command -v java, command -v javac를 사용한다.
JDK 다운로드 참고: https://adoptium.net/temurin/releases/?version=17

4. 컴파일 및 실행

4-1. 직접 컴파일 (프로젝트 폴더에서 실행)

Windows PowerShell:
  New-Item -ItemType Directory -Path out -Force | Out-Null
  javac --release 17 -encoding UTF-8 -d out src/DistributedKvApp.java src/MasterNode.java src/Shared.java src/WorkerNode.java
  jar cfe distributed-kv.jar DistributedKvApp -C out .

Linux/macOS:
  mkdir -p out
  javac --release 17 -encoding UTF-8 -d out src/*.java
  jar cfe distributed-kv.jar DistributedKvApp -C out .

out은 빌드 중간 결과이며 실행할 때 필요하지 않다.

4-2. Master 직접 실행

AWS EC2 등 외부 서버에 distributed-kv.jar를 두고 실행한다.
  java -Dfile.encoding=UTF-8 -jar distributed-kv.jar master 5000
현재 팀 Master 주소는 32.236.94.251:5000이다. Master는 작업 생성 후 Worker 4개의 연결을
기다리고 배정을 시작한다. 서버 자동 재시작은 Java 기능이 아닌 AWS의 별도 실행 설정이다.

4-3. Worker 직접 실행

로컬 PC에서 실행한다. <MASTER_IP>는 실제 Master 주소로 바꾼다.
  java -Dfile.encoding=UTF-8 -jar distributed-kv.jar workers <MASTER_IP> 5000
한 번의 명령이 Worker Thread 4개와 P2P 포트 6001~6004를 생성한다.
직접 실행은 상세 콘솔 출력과 현재 작업 폴더의 로그 기록을 사용한다.
편의 도구의 출력 선택·실행별 보관·통합 요약은 적용되지 않는다.

4-4. Worker 편의 실행

Windows:
  .\run-workers.cmd
  .\run-workers.cmd -MasterHost <MASTER_IP> -Port 5000
PowerShell 직접 실행:
  powershell -NoProfile -ExecutionPolicy Bypass -File .\run-workers.ps1
PowerShell 7은 powershell 대신 pwsh를 사용한다. -NoPause를 추가하면 종료 후 대기를 생략한다.
CMD는 자식 PowerShell에만 실행 정책 Bypass를 적용하며 시스템 설정을 바꾸지 않는다.

Linux/macOS (PowerShell 불필요):
  bash ./run-workers.sh
  bash ./run-workers.sh <MASTER_IP> 5000

주소 생략 시 32.236.94.251:5000을 사용한다. Java/JAR·P2P 포트·Master 연결을 검사한 뒤
'상세 로그를 화면에 표시할까요? [y/N]'에 답한다. y는 상세 출력, n 또는 Enter는 진행 상태를
표시한다. 오류는 두 모드 모두 표시하며 실행 중 모드 전환은 제공하지 않는다.
Master 연결 확인은 최대 30초 대기한다. 연결 검사 요청은 Worker ID를 등록하지 않는다.
CMD와 대화형 Bash 실행은 종료 후 Enter를 기다린다. Bash에 입력을 파이프로 전달하면 추가 대기는 없다.

편의 실행은 프로젝트 폴더에서 Java를 실행한다. 기존 로그는 logs/before-<실행 ID>/에 보관하고,
이번 로그는 프로젝트 폴더에 유지하면서 logs/<실행 ID>/에 복사한다.
전체 콘솔 출력은 console.txt, 통합 요약은 summary.txt로 해당 실행별 폴더에 저장한다.

4-5. WorkerLauncher 빌드 (JDK 17 이상, 실행만 할 때는 불필요)

Windows PowerShell:
  New-Item -ItemType Directory -Path launcher-out -Force | Out-Null
  javac --release 17 -encoding UTF-8 -d launcher-out launcher/WorkerLauncher.java
  jar cfe worker-launcher.jar WorkerLauncher -C launcher-out .

Linux/macOS:
  mkdir -p launcher-out
  javac --release 17 -encoding UTF-8 -d launcher-out launcher/WorkerLauncher.java
  jar cfe worker-launcher.jar WorkerLauncher -C launcher-out .

launcher-out에는 도우미 클래스만 빌드한다. 도우미 UX 변경 시 worker-launcher.jar를 재빌드한다.
분산 프로그램 변경 시 distributed-kv.jar를 재빌드하고 AWS Master와 Worker 버전을 맞춘다.
worker-launcher.jar는 Worker PC용이며 AWS Master에 배포할 필요가 없다.

5. 동적 작업 분배 알고리즘

알고리즘: 재시도 우선·최소 Ready Queue 우선 배정, 동률 순환 선택

1) 재시도 Queue 작업을 일반 작업보다 먼저 선택한다. 재시도끼리는 시도 횟수가 높은 순,
   동률이면 Task ID가 작은 순으로 선택한다.
2) 연결되어 있고 예상 Queue 크기가 10 미만인 Worker만 후보로 선택한다.
3) 최소 Queue 후보를 Worker ID 오름차순으로 정렬하고 전역 순환 커서로 선택한다.
4) TASK 전송 전에 예상 Queue 크기를 증가시키고 STATUS/RESULT 수신 시 보고 값으로 갱신한다.
5) 재할당은 직전 실패 Worker를 제외한다. 초기 배정 후 START를 보내 처리를 활성화한다.

장점: Queue 여유 기반 분산, 가득 찬 Worker 배정 중단, 실패 작업 우선 처리.
단점: 보고 지연에 따른 Queue 불일치 가능성, 실제 CPU/네트워크 성능 미반영.
작업 처리량은 최종 통계에 기록하며 실시간 배정 점수에는 직접 사용하지 않는다.

6. P2P 부하 분산 알고리즘

알고리즘: 최소 Ready Queue 조회 기반 후단 작업 이전

1) Master 가상 시각 기준으로 다음 점검 시각을 1~3초 뒤로 정한다.
   작업 수신·처리 과정에서 해당 시각 도달 여부를 검사한다. 별도의 실시간 주기 타이머는 아니다.
2) 실제 Ready Queue 작업 수 × 평균 처리시간 2초가 15초를 초과하면 부하 분산을 시도한다.
3) 다른 Worker 3개에 TCP로 Queue 상태를 조회하고, 여유가 있는 최소 Queue Worker를 선택한다.
4) 대상 여유 공간 안에서 Queue 후단의 최대 3개 작업을 선택한다. ACK 대기 중 복원 공간을 예약한다.
5) 수신 측이 용량을 재확인하여 수락 시 삽입 후 ACK, 거절 시 REJECT를 보낸다.
6) ACK 수신 시 예약을 해제한다. 같은 transferId로 재시도한 뒤에도 확인되지 않거나 REJECT되면
   TRANSFER_STATUS로 수신 측 상태를 확인한다. ACCEPTED이면 복원하지 않고 이전을 확정한다.
7) NOT_FOUND/REJECTED는 수신 측이 해당 ID의 향후 전송도 거절하도록 취소를 확정한 응답이다.
   이 응답을 받은 경우에만 작업을 복원한다. 늦게 도착한 동일 ID의 TRANSFER는 거절한다.
8) 조회마저 실패하면 UNKNOWN으로 취급하여 복원하지 않고 예약을 유지하며 별도 Thread에서 재조회한다.
   종료까지 소유권이 확인되지 않으면 Worker 최종 종료를 FAIL로 기록한다.

장점: Worker끼리 직접 분산, 수신 용량 재확인, 현재 Queue 상태를 반영한 대상 선택.
단점: 추가 조회 비용, 조회와 이전 사이 상태 변경에 따른 REJECT 가능성.
transferId는 동일 이전 요청의 재수신에 의한 중복 Queue 삽입을 방지한다.
단, 모든 네트워크 장애에 대한 exactly-once 전달을 보장하는 프로토콜은 아니다.
ACK 유실 뒤 즉시 복원하던 경로는 상태 확인과 취소 확정으로 대체했다.
전송 상태는 해당 Worker 프로세스의 메모리에 유지된다. 프로세스 재시작·상태 소실까지 복구하는
영속 소유권 프로토콜은 아니며, 지속적인 통신 단절 시 안전한 완료를 보장하지 않는다.
P2P 통신은 동일 PC의 127.0.0.1:6001~6004를 사용한다.

7. 장애 처리 및 Ready Queue

- 작업은 매 시도 80% 성공/20% 실패 확률을 적용한다. 실패는 RESULT FAIL로 보고한다.
- Master는 실패 작업을 Priority Retry Queue에 넣고 다른 Worker에 우선 재할당한다.
  재시도에도 같은 성공 확률을 적용하며 성공할 때까지 반복한다(전체 처리 제한시간 적용).
- Worker 연결 해제 시 Master는 해당 Worker의 미완료 작업을 재시도 Queue에 복구한다.
- Master는 taskId + attempt로 중복 결과를 걸러내고, 이미 완료된 taskId도 중복 집계하지 않는다.
- 실제 Queue와 P2P 복원 예약 슬롯을 합쳐 최대 10개 용량을 적용한다.
  초과 작업은 FAIL로 보고하며, 처리 실패와 Queue 초과 거부/재시도 통계는 구분한다.
- WARN은 실제 Queue 작업 수의 변경 전 또는 후가 7을 초과하면 작업마다 기록한다.
  7→8, 8→9, 9→10, 10→9, 9→8, 8→7은 WARN이며 7→6은 아니다.
- P2P 묶음 송수신·실패 복원·종료/연결 오류 정리에도 작업별 기준을 적용한다.
  Queue 변경과 기록을 같은 잠금 안에서 처리하며 Task ID·사유·전후 크기를 남긴다.
  예약 슬롯은 용량 검사에만 포함하고 WARN의 실제 Queue 크기에서는 제외한다.

8. 가상 System Clock 및 종료

8-1. 시간 계산

Master가 전역 가상 시계를 관리한다. 작업 처리 1~3초와 노드 간 단방향 메시지 지연 1초를
누적하며 요청과 응답은 각각 계산한다. Worker 간 직접 통신은 Worker가 Master에 보고한다.
시각 동기화 및 종료 후 로그 파일 전달은 수행시간에서 제외한다.
시뮬레이션 시간을 실제 Thread.sleep으로 기다리지 않는다. 실행 도우미의 연결 재시도·출력 갱신
대기는 별도 실제 시간이며 가상 System Clock에 영향을 주지 않는다.

8-2. 전체 종료 흐름

고유 성공 taskId 5,000개와 KV 저장소 5,000개를 모두 확인하면 Master가 TERMINATE를 전송한다.
Worker는 처리 루프를 종료하고, 원격 Master 사용 시 Worker1이 LOG_REQUEST를 보낸다.
각 Worker는 TERMINATE_ACK를 보낸 뒤 FINAL_CLOCK을 기다리고, 수신 확인 후 최종 통계를 기록한다.
Master는 처리 완료 조건과 ACK 4개를 확인하여 FINAL_CLOCK을 전송하고 자신의 최종 통계와
종료 결과를 기록한다. 요청한 Worker1에는 Master.txt를 전달한다.
Worker 통계 기록과 Master 통계 기록은 병렬로 진행될 수 있다.

Master는 처리 제한시간 내 완료 조건과 ACK 수신을 모두 충족해야 TERMINATE SUCCESS를 기록한다.
전체 처리 제한시간(5분) 또는 종료 ACK 제한시간(30초) 초과 시 통계는 남기되 최종 종료는 FAIL이다.
Worker도 TERMINATE와 FINAL_CLOCK을 확인해야 SUCCESS이며 최종 시각 대기(30초) 초과·중단은 FAIL이다.
최초 Worker 연결 수락은 별도의 전체 연결 대기 제한 없이 기다린다. P2P 소켓 제한시간은 5초이다.

9. 로그 및 필수 성능 지표

로그 형식: [clock] NODE | EVENT | STATUS | message
STATUS: INFO / SUCCESS / FAIL / WARN. EVENT와 통계 정의는 AllDefinedLogs.txt를 참조한다.

Master 및 Worker의 최종 STAT에 필수 6개 지표를 기록한다.
1) 작업 처리량: 성공적으로 처리한 KV 수
2) 성공/실패 횟수: 성공과 20% 처리 실패를 별도 줄로 기록
3) 평균 작업 대기시간: Queue 입장 후 처리 시작까지의 평균 가상 시간
4) P2P 부하 분산 이벤트 횟수: 이전 작업 수와 구분
5) 장애 재할당 횟수
6) 전체 수행시간: 가상 System Clock 기준

Worker의 P2P 횟수는 송신+수신 참여 횟수이고 Master는 송신 보고 기준 이전 이벤트 수이다.
Worker의 장애 재할당 수는 해당 Worker의 처리 실패 수이다. Master는 연결 해제 재할당도 합산한다.
Master 최종 로그에는 전체 KV 5,000쌍과 전체·Worker별 통계가 포함된다(정상 완료 기준).
Master.txt는 서버 실행 폴더에 기록되며 원격 실행 시 Worker1이 요청하여 로컬로 받는다.
Worker1.txt~Worker4.txt는 Java의 작업 폴더에 기록된다. 편의 실행의 보관 위치는 4-4를 참조한다.

10. 추가 구현 사항 및 제약

- 공통 Java 도우미로 Windows/Linux/macOS에 같은 출력 선택·진행 표시·로그 보관·요약 기능 제공.
- 상세 출력 여부와 관계없이 노드별 로그와 전체 console.txt를 보존한다.
- summary.txt는 Worker별 통계, Master P2P/재할당, 실제/가상 시간과 검증 상태를 표시한다.
  COMPLETED: 로그 기준 고유 작업/KV 5,000개, 성공·실패 합계, 필수 통계와 종료 기록 확인.
  PARTIAL: Worker 완료는 확인했으나 Master 로그 누락 또는 검증 불일치.
  FAILED: 프로세스/연결/종료 오류나 Worker 완료 조건 미충족.
  종료 코드는 COMPLETED/PARTIAL 0, FAILED 1이다. PARTIAL은 전체 정상 완료 확인이 아니다.
- 이 검증은 로그 기반이며 모든 분산 장애 상황의 정확성을 보장하지 않는다.
- 잘못된 프로토콜 필드·범위는 PROTO WARN으로 기록한다. 동일 transferId 중복 삽입을 방지한다.
- 같은 PC의 동시 실행은 P2P 포트가 충돌한다. 여러 PC가 같은 Master를 동시에 공유하는
  실행별 세션 분리도 지원하지 않으므로 한 번에 한 팀원이 실행한다.
- Windows와 Linux(WSL)에서 실행 도구를 검증했다. WSL에서 기본·상세 출력 및 실행별 로그 보관을
  확인했으며 기본 모드 AWS 연동 5,000건 완료를 확인했다. macOS 직접 검증은 미실시이다.
  이번 P2P 상태 확인 수정본은 로컬 검증 완료이며 최종 AWS/WSL 재검증은 별도로 수행한다.

제출 구성 안내
- 전체 소스(src/, launcher/, 필요 시 tests/), AllDefinedLogs.txt, 같은 실행의 로그 5개,
  Readme.txt, download.txt(5분 이내 시연 영상 다운로드 링크)를 포함한다.
- 편의 실행도 제공하려면 두 JAR와 run-workers.cmd/.ps1/.sh를 함께 포함한다.
- 저장소 README.txt는 제출 ZIP에서 Readme.txt로 이름을 맞춘다. 별도 내용의 복사본은 관리하지 않는다.
- out/, launcher-out/, .tmp.ux-check/, 과거 logs/, temp/, 개발용 테스트 산출물은 제출에서 제외한다.
- 서버 개인키와 관리자 접속 자료는 제출물에 포함하지 않는다.
- 영상 링크·공유 권한·재생 및 ZIP 압축 해제를 확인하고 조별 1명이 최종 제출한다.
