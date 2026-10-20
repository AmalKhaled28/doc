# Open Questions for the Team

[Docs home](README.md)

Every blank in these docs has exactly one row below. Each row points to the place the
answer goes. Run commands from the project root unless the row names a host. Rows
marked **(new)** are questions raised by the team's 2026-10-20 Modal investigation
([archive/embedding-runtime-consistency.md](archive/embedding-runtime-consistency.md)).

**Total: 40 blanks.**

## Hardware

| ID | Question for the team | File and section | Command to get the answer (if any) | Answer | Status |
|---|---|---|---|---|---|
| H01 | How many CPU cores does the Debian host have? | [02 §3.1](02-hardware-and-infrastructure.md#31-known-and-unknown-specifications) | `nproc` (on the Debian host) | | |
| H02 | How much RAM does the Debian host have? | [02 §3.1](02-hardware-and-infrastructure.md#31-known-and-unknown-specifications) | `free -h` (on the Debian host) | | |
| H03 | What is the Debian host's disk size? | [02 §3.1](02-hardware-and-infrastructure.md#31-known-and-unknown-specifications) | `df -h` (on the Debian host) | | |
| H04 | How many vCPUs does the master VM have? | [02 §3.1](02-hardware-and-infrastructure.md#31-known-and-unknown-specifications) | `nproc` (on spark-master) | | |
| H05 | How much RAM does the master VM have? | [02 §3.1](02-hardware-and-infrastructure.md#31-known-and-unknown-specifications) | `free -h` (on spark-master) | | |
| H06 | What is the master VM's disk size? | [02 §3.1](02-hardware-and-infrastructure.md#31-known-and-unknown-specifications) | `df -h` (on spark-master) | | |
| H07 | How many vCPUs does each worker VM have? | [02 §3.1](02-hardware-and-infrastructure.md#31-known-and-unknown-specifications) | `nproc` (on worker1, worker2, worker3) | | |
| H08 | What is each worker VM's disk size? | [02 §3.1](02-hardware-and-infrastructure.md#31-known-and-unknown-specifications) | `df -h` (on each worker) | | |
| H09 | **(new)** What CPU model and instruction set do worker1–3 have: AVX2 only, or AVX-512? | [02 §3.1](02-hardware-and-infrastructure.md#31-known-and-unknown-specifications), CPU instruction set table | `lscpu` (on each worker); see [02 §7](02-hardware-and-infrastructure.md#7-commands-to-collect-real-specs) for the filter | | |
| H10 | How many vCPUs does the query node have? | [02 §3.1](02-hardware-and-infrastructure.md#31-known-and-unknown-specifications) | `nproc` (on the query node) | | |
| H11 | How much RAM does the query node have? | [02 §3.1](02-hardware-and-infrastructure.md#31-known-and-unknown-specifications) | `free -h` (on the query node) | | |
| H12 | What is the query node's disk size? | [02 §3.1](02-hardware-and-infrastructure.md#31-known-and-unknown-specifications) | `df -h` (on the query node) | | |
| H13 | Which host runs `scripts/ask.py` (the query node)? | [03 §2](03-cluster-design.md#2-node-roles-and-services) | None: ask the team | | |
| H14 | Which hosts run the HDFS NameNode and DataNodes? | [03 §2](03-cluster-design.md#2-node-roles-and-services) | `hdfs getconf -namenodes` and `hdfs dfsadmin -report` (on spark-master) | | |
| H15 | What is the HDFS NameNode RPC port (`fs.defaultFS`)? | [03 §6](03-cluster-design.md#6-networking-and-ports) | `hdfs getconf -confKey fs.defaultFS` (on spark-master) | | |
| H16 | What firewall rules exist between the master and the workers? | [03 §6](03-cluster-design.md#6-networking-and-ports) | `sudo ufw status verbose` or `sudo iptables -S` (on the master and each worker) | | |
| H17 | Is a Spark History Server running, and on which host? | [03 §7](03-cluster-design.md#7-storage-design) | `jps` (look for `HistoryServer`) on spark-master; `grep historyServer $SPARK_HOME/conf/spark-defaults.conf` | | |

## Versions

| ID | Question for the team | File and section | Command to get the answer (if any) | Answer | Status |
|---|---|---|---|---|---|
| V01 | Which Hadoop version does the cluster run? | [01 §8](01-system-overview.md#8-technology-stack) | `hdfs version` (on spark-master) | | |
| V02 | Which PostgreSQL server version does the live database run? | [01 §8](01-system-overview.md#8-technology-stack) | `psql -d warcdb -c "SELECT version();"` | | |
| V03 | Which pgvector extension version is installed? | [01 §8](01-system-overview.md#8-technology-stack) | `psql -d warcdb -c "SELECT extversion FROM pg_extension WHERE extname = 'vector';"` | | |
| V04 | Which VMware product and version runs on the Debian host? | [02 §2](02-hardware-and-infrastructure.md#2-node-roles) | `vmware --version` (on the Debian host) | | |
| V05 | Which Java version runs on the master and the workers? | [02 §2](02-hardware-and-infrastructure.md#2-node-roles) | `java -version` (on each node) | | |
| V06 | Which Python version and interpreter path does each worker have? They must match the master's packed venv (Python 3.13.5 locally). | [02 §2](02-hardware-and-infrastructure.md#2-node-roles) | `python3 --version` and `which python3` (on each worker); compare with `.venv/bin/python --version` and `readlink -f .venv/bin/python` on the master | | |

## Database host

| ID | Question for the team | File and section | Command to get the answer (if any) | Answer | Status |
|---|---|---|---|---|---|
| D01 | Which host runs PostgreSQL? | [03 §2](03-cluster-design.md#2-node-roles-and-services) | `psql -d warcdb -c "SELECT inet_server_addr(), inet_server_port();"` (from spark-master; empty means a local socket on the same host) | | |
| D02 | How many vCPUs does the PostgreSQL host have? | [02 §3.1](02-hardware-and-infrastructure.md#31-known-and-unknown-specifications) | `nproc` (on the PostgreSQL host) | | |
| D03 | How much RAM does the PostgreSQL host have? | [02 §3.1](02-hardware-and-infrastructure.md#31-known-and-unknown-specifications) | `free -h` (on the PostgreSQL host) | | |
| D04 | What size and type (SSD or HDD) is the PostgreSQL data disk? | [02 §3.1](02-hardware-and-infrastructure.md#31-known-and-unknown-specifications) | `psql -d warcdb -c "SHOW data_directory;"`, then `df -h <that directory>` and `lsblk -d -o NAME,ROTA,SIZE` (ROTA 0 = SSD) | | |
| D05 | Is the database backed up today, and how? | [03 §7](03-cluster-design.md#7-storage-design) | `crontab -l` and `sudo crontab -l` (on the PostgreSQL host), then ask the team | | |

## Models

| ID | Question for the team | File and section | Command to get the answer (if any) | Answer | Status |
|---|---|---|---|---|---|
| M01 | How do we obtain the six base `.models/bge-m3` files that the INT8 download script needs? | [README §4](README.md#4-quick-start-prototype), step 5 | None: ask the team (no repository script downloads them) | | |
| M02 | How many seconds does embedding one chunk take on the current cluster (*t*)? | [02 §6.3](02-hardware-and-infrastructure.md#63-embedding-time) | `./scripts/watch_embeddings.sh --once`, then `time ./scripts/run_embeddings.sh --embedding-limit 500`; *t* = seconds ÷ 500 | | |
| M03 | Which Groq account tier do we use, and what is the price per million tokens for the tagging model (`qwen/qwen3.8-27b`)? | [02 §6.4](02-hardware-and-infrastructure.md#64-llm-tagging-cost-and-duration) | None: Groq console (billing and limits pages) | | |

## Production decisions

| ID | Question for the team | File and section | Command to get the answer (if any) | Answer | Status |
|---|---|---|---|---|---|
| P01 | **(new)** Is moving embeddings to Modal (AVX2-only ONNX Runtime wheel) decided, or still a proposal? | [01 §9](01-system-overview.md#9-prototype-and-production-at-a-glance) | None: ask the team | | |
| P02 | Which scheduler will run the hourly cycle (for example cron, systemd timer, Airflow)? | [03 §11](03-cluster-design.md#11-prototype-vs-production-cluster) | None: ask the team | | |
| P03 | What are the production targets: number of articles (*A*) and queries per hour? | [02 §6](02-hardware-and-infrastructure.md#6-estimation-method) | None: ask the team | | |
| P04 | What size should the production master be? | [02 §6.5](02-hardware-and-infrastructure.md#65-prototype-vs-production-sizing) | None: decide after P03 | | |
| P05 | How many production workers, and what size? | [02 §6.5](02-hardware-and-infrastructure.md#65-prototype-vs-production-sizing) | None: decide after P01 and P03 | | |
| P06 | What size should the production PostgreSQL host be? | [02 §6.5](02-hardware-and-infrastructure.md#65-prototype-vs-production-sizing) | None: derive from [02 §6.2](02-hardware-and-infrastructure.md#62-storage) after P03 | | |
| P07 | What size should the production query node be? | [02 §6.5](02-hardware-and-infrastructure.md#65-prototype-vs-production-sizing) | None: decide after P03 (query rate) | | |
| P08 | Where will the scheduler, FastAPI service, monitoring, and backups run? | [02 §6.5](02-hardware-and-infrastructure.md#65-prototype-vs-production-sizing) | None: ask the team | | |

## Project decisions

| ID | Question for the team | File and section | Command to get the answer (if any) | Answer | Status |
|---|---|---|---|---|---|
| J01 | Where is VM, Hadoop, and PostgreSQL provisioning kept (including `cluster_up.sh` and `cluster_down.sh`, which the README mentions but the repository does not contain)? | [01 §2](01-system-overview.md#2-scope) | `ls ~/cluster_up.sh ~/cluster_down.sh` (on the Debian host), then ask the team | | |

## How to fill a blank

1. In VS Code, press **Ctrl+Shift+F** and search for `[[FILL:`. Set *files to exclude*
   to `archive, FILL-IN.md` so that only real blanks are listed (this section also
   contains the search text).
2. Open a match and replace the **whole** marker, from `[[` to `]]`, with the real value.
   Add a confidence label when it helps, for example `8 vCPU (Recorded 2026-10-25)`.
3. In this file, find the row with the same question, write the value in **Answer**, and
   put ✅ in **Status**.
4. If the answer changes other text, update that too. For example, if H09 shows an
   AVX-512 worker, keep it out of embedding runs until the team decides how to handle it.
5. Check: the number of open rows here must equal the number of markers left in the
   docs. From this folder:

   ```bash
   grep -o "\[\[FILL:" README.md 0*.md | wc -l
   ```
