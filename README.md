### Nama: Maulida Rahmayanti
### NIM: 11231038
### link report: https://drive.google.com/file/d/1RAPfEreGaaQeRFjPozWhw3nrzYQhyyOk/view?usp=sharing
### link Video: 

# Distributed Synchronization System

Sistem ini adalah implementasi dari sistem sinkronisasi terdistribusi yang dirancang untuk mensimulasikan lingkungan *real-world* dari *distributed systems*. Sistem ini mendukung banyak *node* yang dapat saling berkomunikasi dan mensinkronisasikan data secara konsisten.

Sistem dibangun menggunakan Python (FastAPI/Uvicorn) dan memanfaatkan arsitektur *containerization* melalui Docker untuk memungkinkan skalabilitas dan kemudahan di-deploy. Sistem ini berjalan dalam *cluster* yang terdiri dari beberapa node (default 4 node) dan didukung oleh Redis sebagai media untuk penyimpanan *state* dan *caching*.

---

## 🏛 Arsitektur Sistem

Sistem ini dibangun di atas konsep *peer-to-peer* (P2P) di mana setiap node dapat berkomunikasi secara langsung dengan node lainnya menggunakan antarmuka REST API. Proses sinkronisasi antar-node dikelola menggunakan algoritma **Raft Consensus** untuk menjamin data tetap konsisten meski terjadi kegagalan jaringan atau node mati (*fail-tolerance*). 

Setiap layanan (*Lock*, *Queue*, *Cache*) menggunakan basis dari sistem konsensus ini untuk mempertahankan keakuratan datanya secara global.

---

## ✨ Fitur Utama (Core Features)

### 1. Raft Consensus Algorithm
- **Leader Election**: Pemilihan *leader* secara otomatis jika *leader* yang ada mengalami kegagalan.
- **Log Replication**: *State* dan perubahan direplikasi secara konsisten dari *leader* ke semua *follower*.
- **Fault Tolerance**: Sistem akan terus beroperasi secara optimal selama mayoritas (*quorum*) node masih aktif.

### 2. Distributed Lock Manager
- **Shared & Exclusive Lock**: Mendukung *lock* untuk akses bersama (*read*) dan akses eksklusif (*write*).
- **Deadlock Detection**: Mendeteksi dan mencegah kondisi *deadlock* dalam lingkungan terdistribusi.
- **Network Partition Handling**: Mekanisme yang memastikan integritas *lock* saat koneksi antar-node terputus.

### 3. Distributed Queue System
- **Consistent Hashing**: Pemetaan distribusi antrean ke *node* yang tersedia secara merata menggunakan algoritma *consistent hashing*.
- **Message Persistence & Recovery**: Memastikan pesan tidak hilang meski terjadi kegagalan sistem.
- **At-Least-Once Delivery**: Menggaransi bahwa setiap pesan pasti akan sampai dan diproses minimal satu kali.

### 4. Cache Coherence Protocol (MOESI)
- Mengimplementasikan protokol MOESI (*Modified, Owned, Exclusive, Shared, Invalid*) untuk memastikan setiap *cache* yang berada di berbagai *node* selalu konsisten.
- **Cache Replacement Policy**: Menggunakan metode **LRU (Least Recently Used)** untuk eviksi saat *cache* sudah penuh.

---

## 🚀 Fitur Bonus (Advanced)

- **Security & Encryption**: Komunikasi API diamankan menggunakan `X-API-Key` (Role-Based Access Control). Payload dienkripsi dengan standar *AES-GCM*, dan semua aktivitas diawasi lewat *Tamper-evident Audit Log*.
- **PBFT (Practical Byzantine Fault Tolerance)**: Terdapat implementasi *quorum tracking* (Prepare/Commit) sebagai demo toleransi terhadap kemungkinan node yang berniat jahat (*Byzantine*).
- **Geo-Distributed & Load Balancing**: Simulasi pencarian region terbaik berdasarkan latensi jaringan terendah (*Latency-aware routing*), dan penyeimbang beban dinamis menggunakan kalkulasi *EWMA* (*Exponentially Weighted Moving Average*).

---

## 📋 Persyaratan Sistem (Requirements)

- **Python 3.11+**
- **Docker & Docker Compose** (Untuk environment terdistribusi)
- **Redis** (Menyediakan fitur *backend state*)

---

## 🛠 Instalasi dan Menjalankan (Setup & Run)

### 1. Instalasi Dependencies Lokal (Untuk Development/Testing Lokal)
```bash
pip install -r requirements.txt
```

### 2. Menjalankan Cluster (Menggunakan Docker Compose)
Perintah ini akan membangun dan menjalankan cluster *distributed system* (beserta Redis) dalam container.

```bash
docker compose -f docker/docker-compose.yml up --build
```
*(Node secara default akan tersedia pada port 8001, 8002, 8003, dan 8004)*

---

## 📊 Pengujian & Benchmarking

### 1. Unit & Integration Testing
Sistem diuji menggunakan **pytest** untuk memastikan seluruh modul berjalan dengan baik.
```bash
pytest tests/
```

### 2. Load Testing (Menggunakan Locust)
Kami telah menyediakan *script* benchmark untuk mengukur seberapa tangguh sistem ini saat menerima beban *request* yang banyak.

**Menjalankan Mode UI (Disarankan):**
```bash
python -m locust -f benchmarks/load_test.py
```
Lalu buka browser Anda ke `http://localhost:8089`.

**Menjalankan Mode Headless (Terminal saja, misalnya selama 1 menit):**
```bash
python -m locust -f benchmarks/load_test.py --headless -u 100 -r 10 --run-time 1m
```

---

## 📡 Daftar API Endpoints

Referensi spesifikasi OpenAPI (Swagger) lengkap dapat dilihat di folder `docs/api_spec.yaml` atau dapat diakses secara dinamis jika aplikasi dijalankan, misalnya di `http://localhost:8001/docs`.

### 🔹 Node Health & Status
- `GET /health` - Cek kesiapan node
- `GET /status` - Melihat status dari node (Leader/Follower, jumlah log, dsb)
- `GET /metrics` - Melihat metrik sistem

### 🔹 Distributed Lock Manager
- `POST /lock/acquire` - Meminta hak akses (*lock*)
- `POST /lock/release` - Melepaskan hak akses
- `GET /lock/status/{resource}` - Mengecek status ketersediaan *lock*

### 🔹 Distributed Queue
- `POST /queue/publish` - Mengirim pesan ke dalam *queue*
- `POST /queue/consume` - Mengambil pesan dari dalam *queue*
- `POST /queue/ack` - Memberikan *acknowledgement* bahwa pesan telah sukses diproses

### 🔹 Distributed Cache
- `GET /cache/get/{key}` - Mengambil *value* dari cache
- `POST /cache/put` - Menyimpan *value* ke dalam cache
- `POST /cache/invalidate/{key}` - Menghapus cache (*Invalidate*)

### 🔹 API Bonus & Advanced Features
Memerlukan header `X-API-Key`
- `POST /security/encrypt` & `POST /security/decrypt` - Fitur *Encryption*
- `GET /security/audit` - Melihat rantai *audit logs*
- `POST /bonus/pbft/commit` - Melakukan simulasi PBFT
- `GET /bonus/geo/route` - Mencari node terdekat secara latensi
- `POST /bonus/load-balance/choose` - Mendapatkan node tujuan *Load Balancing*
