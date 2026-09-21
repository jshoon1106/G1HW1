분산 환경 Fault-Tolerant 키-값 저장소

1. 조원 정보

조원 1: 20223134 장승훈 - 전체 초기 구현, Master·AWS 구축
조원 2: 20213132 정우진 - Worker 실행·로그·UX 개선, README 보완
조원 3: 20223099 김현중 - 시연 영상

2. 프로그램 구성요소

src/DistributedKvApp.java: 실행 진입점, master 또는 workers 역할 선택
src/MasterNode.java: Master Node, Worker 연결 수락, KV 생성, 동적 배정, 재시도, 통계, 종료
src/WorkerNode.java: Worker Node, Worker Thread 4개, Ready Queue, 작업 처리, P2P 이전, Worker 로그
src/Shared.java: 공통 구성요소, Task, VirtualClock, EventLogger, WorkerInfo
AllDefinedLogs.txt: 로그 명세, 이벤트와 상태 코드 설명
distributed-kv.jar: src의 Java 소스 4개를 Java 17 대상으로 컴파일한 실행 JAR
run-workers.cmd: PowerShell 실행 정책을 변경하지 않고 자동 실행
run-workers.ps1: Java 17과 Master 연결 확인, Worker 실행, 로그 보관 통합
run-workers-ui.ps1: 상세 화면 선택, 출력 수집, 진행 상태 및 종료 후 로그 기반 통합 요약

3. 실행 환경

- 언어: Java 17
- 소스 컴파일 및 JAR 생성: JDK 17 이상(javac, jar 포함)
- Master 실행 환경: AWS EC2 Amazon Linux 2023
- Worker 실행 환경: Windows, macOS, Linux 로컬 PC
- Master Public IP: 32.236.94.251
- Master 포트: 기본 5000/TCP
- Worker P2P 포트: 6001~6004/TCP, 동일 로컬 PC의 127.0.0.1 사용
- Worker 자동 실행 도구: Windows CMD 실행은 기본 Windows PowerShell 5.1 사용, macOS/Linux는 PowerShell 7(pwsh) 필요
- EC2 관리자 도구: Windows OpenSSH Client(ssh, scp)
- Java 프로그램과 distributed-kv.jar 자체는 Windows, macOS, Linux에서 동일하게 실행 가능

4. 사전 작업

4-1. 공통 준비사항

- 개발 기준은 JDK 17이다. JDK에는 java(실행), javac(컴파일), jar(JAR 생성)가 포함된다.
- 아래 빌드 명령은 --release 17을 사용한다. 실행 스크립트는 Java 17 이상을 허용한다.
- JAR 실행만 할 때는 javac가 필요하지 않지만, 소스 컴파일을 위해서는 JDK가 필요하다.
- JAVA_HOME은 JDK 설치 폴더, PATH는 실행 파일을 찾을 폴더 목록이다.
  JAVA_HOME에는 bin을 제외한 경로를, PATH에는 JDK의 bin 경로를 추가한다.
- 기존 PATH 전체를 지우거나 덮어쓰지 않는다. 설치 후 터미널을 새로 연다.

4-2. Windows

1) https://adoptium.net/temurin/releases/?version=17 에서 Windows용 JDK 17 MSI를 설치한다.
   PC 아키텍처에 맞는 파일을 선택한다. 설치 옵션에서 PATH 추가 및 JAVA_HOME 설정을 선택할 수 있다.
2) 자동 설정되지 않았다면 Windows 검색에서 '시스템 환경 변수 편집' -> '환경 변수'를 연다.
   사용자 변수 JAVA_HOME을 만들고 실제 JDK 설치 폴더를 입력한다.
   예: C:\Program Files\Eclipse Adoptium\<설치된 JDK 17 폴더명>
   사용자 변수 Path에 %JAVA_HOME%\bin 항목을 추가한다.
   꺾쇠 안의 폴더명은 실제 설치 폴더명으로 바꾸며, 경로 값에 따옴표를 넣지 않는다.
3) 새 PowerShell 또는 명령 프롬프트에서 확인한다.

   java -version
   javac -version
   jar --version

   JDK 17을 설치했다면 모두 17로 시작해야 한다. 다른 버전이 선택되면 다음 명령으로 경로를 확인한다.

   where.exe java
   where.exe javac

4-3. macOS (기본 zsh 기준)

1) 위 Temurin 다운로드 페이지에서 macOS용 JDK 17 PKG를 설치한다.
   Apple Silicon은 aarch64, Intel Mac은 x64를 선택한다.
2) ~/.zshrc에 아래 내용을 추가한다. 기존 Java 설정이 있다면 중복 추가 대신 수정한다.

   export JAVA_HOME=$(/usr/libexec/java_home -v 17)
   export PATH="$JAVA_HOME/bin:$PATH"

3) 설정을 적용하고 확인한다.

   source ~/.zshrc
   java -version
   javac -version
   jar --version

4-4. Linux

배포판에 맞는 설치 명령을 사용한다.

Ubuntu 22.04/24.04:
   sudo apt update
   sudo apt install openjdk-17-jdk

Amazon Linux 2023 (AWS Master):
   sudo yum install java-17-amazon-corretto-devel

설치 후 아래 명령으로 javac 버전과 실제 경로를 확인한다.

   javac -version
   readlink -f "$(command -v javac)"

javac가 17 버전인지 확인한다. 다른 버전이면 배포판의 alternatives 설정에서 JDK 17을 선택하거나
설치된 JDK 17 폴더를 직접 확인한다. 출력 경로의 마지막 /bin/javac를 제외한 부분이 JDK 폴더이다.
Bash 사용자는 ~/.bashrc에 다음을 추가한다. <JDK 17 설치 경로>는 실제 경로로 바꾼다.

   export JAVA_HOME="<JDK 17 설치 경로>"
   export PATH="$JAVA_HOME/bin:$PATH"

   source ~/.bashrc
   java -version
   javac -version
   jar --version

zsh 사용자는 ~/.zshrc에 설정하고 source ~/.zshrc로 적용한다.
macOS/Linux에서 실행 경로를 확인하려면 command -v java 및 command -v javac를 사용한다.

4-5. 검사 및 문제 해결

- java를 찾을 수 없음: 미설치 또는 PATH 설정 문제일 수 있다. JDK 설치 위치와 환경변수를 확인한다.
- java만 되고 javac가 안 됨: JDK 설치 여부와 JDK의 bin 경로를 확인한다.
- run-workers.ps1은 운영체제와 관계없이 JAVA_HOME, PATH 순서로 Java를 찾는다.
- macOS/Linux에서 실행 경로를 확인하려면 command -v java 및 command -v pwsh를 사용한다.
- 자동 설치, 시스템 환경변수 변경은 하지 않는다. Windows CMD는 자식 PowerShell 프로세스에만
  ExecutionPolicy Bypass를 적용하며 저장된 실행 정책은 변경하지 않는다. 조직의 그룹 정책은 우회하지 않는다.
- 현재 실행 스크립트에는 환경만 검사하는 -CheckOnly 또는 --check 옵션이 없다.

설치 참고 문서:
   https://adoptium.net/installation
   https://docs.oracle.com/en/java/javase/17/install/installation-guide.pdf
   https://docs.aws.amazon.com/corretto/latest/corretto-17-ug/amazon-linux-install.html

5. 컴파일 및 실행 방법

최소 실행 방법 (PowerShell 7 없이 Java 17 이상으로 실행 가능)
  Master PC: java -Dfile.encoding=UTF-8 -jar distributed-kv.jar master 5000
  Worker PC: java -Dfile.encoding=UTF-8 -jar distributed-kv.jar workers <MASTER_IP> 5000
  <MASTER_IP>는 실제 외부 Master 주소로 바꾼다. Master 실행 후 Worker 명령을 한 번 실행한다.
  직접 실행에는 스크립트의 화면 선택, 실행별 로그 보관, 통합 요약이 적용되지 않는다.

5-1. JAR 구성과 수동 컴파일

distributed-kv.jar는 다음 Java 소스 4개를 Java 17 대상으로 컴파일한 결과물이다.

  src/DistributedKvApp.java
  src/MasterNode.java
  src/Shared.java
  src/WorkerNode.java

JAR에는 Java 소스 원본이 아니라 위 소스에서 생성된 .class 파일과 내부 클래스 파일이 들어간다.
실행 진입점은 DistributedKvApp이다.

  javac -version

Windows에서는 PowerShell에서 프로젝트 최상위 폴더로 이동한 뒤 다음 명령을 실행한다.

  New-Item -ItemType Directory -Path out -Force | Out-Null
  javac --release 17 -encoding UTF-8 -d out src/DistributedKvApp.java src/MasterNode.java src/Shared.java src/WorkerNode.java
  jar cfe distributed-kv.jar DistributedKvApp -C out .

macOS와 Linux에서 직접 컴파일할 때는 프로젝트 최상위 폴더에서 다음 명령을 사용한다.

  mkdir -p out
  javac --release 17 -encoding UTF-8 -d out src/*.java
  jar cfe distributed-kv.jar DistributedKvApp -C out .

컴파일이 성공하면 프로젝트 최상위 폴더에 distributed-kv.jar가 생성된다. /out 폴더는 수동 컴파일용 중간 결과이며 JAR 실행에는 필요하지 않다.

5-2. Master 접속 정보, Workers 실행

5-2-1. Master 접속 정보

- Master Public IP: `32.236.94.251`
- Master TCP 포트: `5000`
- SSH 접속 시 키 파일 준비: `.\temp\master-node-key.pem`
- SSH 접속 예시: `ssh -i .\temp\master-node-key.pem ec2-user@32.236.94.251`
- macOS/Linux에서 키 파일 사용 시 권한 변경: `chmod 600 temp/master-node-key.pem`

Master를 직접 실행해야 하는 경우에는 Master를 구동할 PC에 distributed-kv.jar를 위치시킨 후 다음 명령을 실행한다.

  java -Dfile.encoding=UTF-8 -jar distributed-kv.jar master 5000

5-2-2. Workers 실행

Master의 Public IP를 입력하여 로컬 PC에서 실행한다. Java 17 이상이 설치되어 있으면 macOS와 Linux에서도 실행할 수 있다.

  java -Dfile.encoding=UTF-8 -jar distributed-kv.jar workers 32.236.94.251 5000

하나의 Worker 실행 명령은 Worker 1~4의 독립 Thread와 P2P 수신 포트 6001~6004를 생성한다.
Worker 실행이 끝나면 현재 로컬 폴더에 Worker1.txt~Worker4.txt가 생성된다. Master 주소가 원격 주소이면 Worker1은 종료 단계에서 TCP로 Master.txt를 수신하여 같은 로컬 폴더에 저장한다.

5-3. Windows에서 workers 자동 실행

프로젝트 최상위 폴더에서 다음 파일을 실행한다. 실행 전 EC2 Master가 이미 실행 중이고 5000/TCP에서 연결을 받아야 한다.

  .\run-workers.cmd

PowerShell 7에서 통합 스크립트를 직접 실행할 수도 있다.

  pwsh -NoProfile -ExecutionPolicy Bypass -File .\run-workers.ps1

실행 순서

1) Java Runtime 17 이상을 자동 탐색한다.
2) 현재 distributed-kv.jar의 존재를 확인한다.
3) Worker P2P 포트 6001~6004가 사용 가능한지 확인한다.
4) EC2 Master의 5000/TCP 연결을 최대 30초 동안 확인한다.
   연결 확인 후 '상세 로그를 화면에 표시할까요? [y/N]' 질문에 답한다.
   y는 모든 상세 출력, n 또는 Enter는 간단한 진행 상태를 표시한다.
5) 기존 로그를 logs/before-<실행 ID>로 이동한다.
6) Worker 1~4를 실행한다.
7) 완료 또는 실패 로그를 logs/<실행 ID>에 복사하여 보관한다. 이번 로그는 프로젝트 폴더에도 남는다.
8) 이번 실행 로그를 검증하여 통합 요약을 화면과 logs/<실행 ID>/summary.txt에 기록한다.

상세 출력을 끄더라도 Java의 노드별 로그 기록은 동일하게 수행된다.
실제 연결/프로그램 오류는 기본 화면에도 표시하며, 수집한 표준 출력·오류는 같은 폴더의 console.txt에 보관한다.
기본 화면의 진행 수치는 Worker 성공 로그의 고유 작업 ID 기준이며 최대 0.5초마다 갱신한다.
종료 요약은 Worker별 통계, Master의 P2P/재할당 횟수, 실제 경과시간과 가상 수행시간을 구분한다.
COMPLETED는 로그 기준 고유 성공 5,000개, KV 5,000개, 성공/실패 집계 및 종료 기록을 확인한 상태이다.
PARTIAL은 Worker 완료는 확인했으나 Master 로그가 없거나 불일치한 상태이며 정상 완료로 단정하지 않는다.
FAILED는 프로세스 오류 또는 Worker 완료 조건 미충족이며 스크립트도 오류로 종료한다.
검증은 저장된 로그를 대상으로 하며 모든 장애 상황이나 분산 알고리즘의 정확성을 보장하지 않는다.
실행 중 모드 전환은 제공하지 않는다. 상세 모드는 실행 시작 시 선택한다.
이 기능은 스크립트 실행에만 적용되며 java -jar 직접 실행은 기존 상세 출력을 유지한다.
Java 소스/JAR, AWS 설정, 통신 규격과 종료 절차는 화면 옵션에 따라 변경되지 않는다.

5-4. macOS와 Linux에서 workers 자동 실행

PowerShell 7이 설치되어 있으면 Windows와 동일한 포트 검사와 로그 보관 절차를 포함한 run-workers.ps1을 실행할 수 있다.

  pwsh -NoProfile -File ./run-workers.ps1

실행 전에 로컬 P2P 포트 6001~6004가 비어 있어야 한다. 이 명령 하나가 Worker 1~4를 모두 생성하므로 Worker별로 네 번 실행하지 않는다.

macOS와 Linux에서 직접 컴파일할 때는 프로젝트 최상위 폴더에서 다음 명령을 사용한다.

  mkdir -p out
  javac --release 17 -encoding UTF-8 -d out src/*.java
  jar cfe distributed-kv.jar DistributedKvApp -C out .

6. 동적 작업 분배 알고리즘

6-1. 알고리즘명: 재시도 우선·최소 Ready Queue 우선 배정, 동률 Worker ID 순환 선택

1) Master는 재시도 Queue를 일반 작업보다 먼저 선택한다. 재시도 작업끼리는 시도 횟수가 높은 작업을 우선하고, 동률이면 Task ID가 작은 작업을 선택한다.
2) 연결 상태이며 예상 Queue 크기가 10 미만인 Worker만 후보로 선택한다.
3) 후보 중 예상 Queue 크기가 가장 작은 Worker를 선택한다.
4) 동률이면 Worker ID 오름차순 후보 목록에 전역 순환 커서를 적용해 선택한다.
5) Master는 TASK 전송 전에 선택 Worker의 예상 Queue 크기를 1 증가시킨다.
6) Worker의 STATUS와 RESULT 메시지를 수신하면 Master의 예상 Queue 크기를 실제 값으로 갱신한다.
7) 재시도 작업은 직전에 실패한 Worker를 후보에서 제외하고 다른 Worker에 배정한다.
8) 초기 Queue 배정이 끝나면 START 메시지를 모든 Worker에 전송해 처리 Thread를 동시에 활성화한다.

작업 처리량은 배정 점수에 직접 사용하지 않고, Worker별 최종 통계로 기록한다. 따라서
현재 구현의 실시간 배정 기준은 연결 상태, Queue 여유, 재시도 우선순위, Worker ID 순환이다.

장점

- 구현 단순성, Queue 길이 기반 작업 분산
- 과부하 Worker 배정 방지
- 재시도 작업의 우선 처리

단점

- 실제 CPU 성능, 네트워크 품질, 처리시간 차이 미반영
- Worker 상태 메시지 지연 시 일시적 Queue 상태 불일치 가능

7. P2P 부하 분산 알고리즘

7-1. 알고리즘명: P2P 최소 Ready Queue 조회 기반 후단 작업 이전

1) Worker는 Master 가상 시간 기준 1~3초 랜덤 주기로 부하를 확인한다.
2) 예상 대기시간은 Ready Queue 작업 수 x 평균 처리시간 2초로 계산한다.
3) 예상 대기시간이 15초를 초과하면 다른 Worker 3개에 현재 Queue 크기를 P2P 소켓으로 조회한다.
4) 응답한 Worker 중 Queue가 가장 작고 여유 공간이 있는 Worker를 대상으로 선택한다.
5) 송신 Worker는 대상 여유 공간 안에서 Queue 후단 작업 최대 3개를 선택하고, ACK 대기 중 복구 공간을 예약한다.
6) 수신 Worker가 실제 Queue 여유 공간을 다시 확인한 후 ACK 또는 REJECT를 전송한다.
7) 각 이전에 transferId를 부여하여 ACK 재전송 시 같은 작업이 중복 삽입되지 않게 한다.
8) ACK 수신 시 예약 공간을 해제하고 이전을 완료한다. REJECT 또는 연결 실패 시 예약 공간에 작업을 복구한다.

장점

- Master를 거치지 않는 직접 부하 분산
- 현재 Queue 상태를 반영한 대상 선택
- transferId와 ACK 기반 이전 확인으로 작업 유실·중복 방지

단점

- Queue 조회를 위한 추가 P2P 연결 비용 발생
- 조회 직후 Queue 상태가 바뀌면 수신 단계에서 REJECT될 수 있음

8. 장애 처리 메커니즘

1) Worker는 각 작업을 80% 성공, 20% 실패 확률로 처리한다.
2) 실패 시 RESULT FAIL 메시지로 Master에 보고한다.
3) Master는 실패 작업을 Priority Retry Queue에 등록하고 장애 재할당 횟수를 증가시킨다.
4) Master는 다음 배정 시 재시도 작업을 최우선으로 선택하고 직전 실패 Worker를 제외한다.
5) 재할당 작업도 동일한 80% 성공, 20% 실패 규칙을 적용한다.
6) 성공 결과가 수신될 때까지 위 과정을 반복한다.
7) Worker Queue 초과 거부도 FAIL 결과로 보고하여 Master 재할당 대상으로 처리한다.
8) Master는 taskId와 attempt 조합으로 중복 결과를 제거하고, 고유 taskId 5,000개와 KV 저장소 5,000개가 모두 확인될 때만 종료한다.
9) Worker 연결이 끊어지면 Master가 해당 Worker의 미완료 작업을 우선 재시도 Queue로 복구한다.

9. 가상 시간, 로그, 종료

- Master가 하나의 가상 시계를 관리하며 실제 Thread.sleep은 사용하지 않는다.
- 작업 처리 시 1~3초, 노드 간 단방향 메시지 전송 시 1초를 가상 시계에 더한다.
- 요청과 응답은 각각 한 번의 전송으로 계산한다.
- Worker 간 통신은 Worker가 Master에 간단히 보고하여 시간을 반영한다. 시각 동기화와 종료 후 로그 전송은 수행시간에서 제외한다.
- TCP 텍스트 인코딩: 모든 Master-Worker 및 Worker-Worker 통신에 UTF-8 명시
- 네트워크 제한시간: 최초 Worker 연결은 제한 없이 대기, P2P ACK 5초, 정상 종료 ACK 30초
- Master 로그 위치: EC2 Master 실행 폴더의 Master.txt. 원격 Master 연결의 종료 단계에서 Worker1이 요청, 수신하여 Worker 로그가 저장되는 위치에 저장된다.
- Worker 로그 위치: Java 직접 실행은 현재 작업 폴더, 자동 실행 스크립트는 프로젝트 폴더의 Worker1.txt~Worker4.txt.
  스크립트 종료 후 logs/<실행 ID>/에 복사하며 프로젝트 폴더의 이번 로그도 유지한다.
- 로그 형식: [clock] NODE | EVENT | STATUS | message
- Worker 로그: INIT, CONNECT, RECV, PROC, QUEUE, LB 이벤트와 필수 6개 성능 지표 기록
- Master 로그: KV 생성, 배정, 결과, 재시도, P2P KV 번호, 최종 Key-Value 5,000쌍, 총 성공·실패, Queue 거부, 전체 합계와 Worker별 통계 기록
- Worker 최종 STAT 필수 지표: 작업 처리량, 성공·실패 횟수, 평균 대기시간,
  P2P 부하 분산 이벤트 횟수, 장애 재할당 횟수, 전체 수행시간
- 종료 조건: 고유 KV 5,000개 성공 처리 완료. 성공 완료 뒤의 중복 재시도 항목은 종료 시 폐기
- 종료 절차: Master TERMINATE 전송, Worker 최종 통계 및 TERMINATE_ACK 전송, Master 최종 통계 기록
- 정상 종료 판정: Master는 처리 제한시간 내 고유 작업/KV 5,000개 완료와 종료 ACK 4개 수신을 모두 확인한다.
  제한시간 초과 시 통계는 보존하되 최종 TERMINATE는 FAIL로 기록한다.
  Worker는 TERMINATE와 FINAL_CLOCK 수신을 확인해야 SUCCESS로 기록하며, 최종 시각 대기 초과·중단은 FAIL이다.

10. 추가 구현 사항

- 고유 4자리 16진수 Key 생성, Value 범위 1~100
- Worker Ready Queue 최대 10개 제한
- Queue의 실제 작업 수가 변경 전 또는 후에 7을 초과하면 작업마다 WARN 기록(7→8, 8→7 포함).
  P2P 묶음 송수신·실패 복원·종료 정리에도 적용하며, 전후 크기와 작업 ID·사유를 기록한다.
  복원 예약 슬롯은 용량 제한에만 포함하고 WARN 크기에서는 제외한다.
- Master와 Worker에 작업 처리량, 성공·실패 횟수, 평균 대기시간, P2P 부하 분산 이벤트 횟수, 장애 재할당 횟수, 전체 수행시간 기록
- AllDefinedLogs.txt의 전체 로그 이벤트 명세 제공
- 잘못된 필드 수·숫자·Worker ID·Task 직렬화 문자열을 PROTO WARN으로 거부
- P2P transferId 기반 중복 수신 방지
