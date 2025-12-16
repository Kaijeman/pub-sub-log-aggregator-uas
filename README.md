# Pub–Sub Aggregator

## Build dan Menjalankan Sistem

### Prasyarat

* Docker
* Docker Compose

### Build dan Run

```
docker compose up -d --build
```

### Cek Status Service

```
docker compose ps
```

Service yang diekspos ke host:

* aggregator: [http://localhost:8080](http://localhost:8080)

Redis dan PostgreSQL tidak mengekspos port ke host dan hanya tersedia di jaringan internal Docker.

---

## Arsitektur Sistem

Sistem menggunakan arsitektur **multi-service publish–subscribe** yang dijalankan dengan Docker Compose. Komponen utama terdiri dari:

* **aggregator**: service FastAPI yang menyediakan API dan worker paralel untuk memproses event.
* **broker (Redis)**: message queue untuk menampung event secara asynchronous.
* **storage (PostgreSQL)**: penyimpanan persisten sekaligus deduplication store.
* **publisher**: generator event untuk demo dan pengujian.

Alur sistem: publisher mengirim event ke aggregator, event dimasukkan ke Redis, lalu diproses oleh worker aggregator dan disimpan ke PostgreSQL.

---

## Endpoint API

### POST /publish

Menerima event dan memasukkannya ke broker untuk diproses asynchronous.

### GET /stats

Menampilkan metrik sistem:

* received
* unique_processed
* duplicate_dropped

Endpoint ini digunakan sebagai bukti idempotency dan deduplication.

### GET /events?topic=<topic>

Mengambil daftar event unik berdasarkan topic tertentu.

---

## Asumsi dan Keputusan Desain

* Sistem menggunakan **at-least-once delivery**, sehingga event dapat diproses lebih dari satu kali.
* Consumer dirancang **idempotent** menggunakan unique constraint (topic, event_id) di database.
* Tidak ada jaminan ordering global; timestamp hanya digunakan sebagai metadata.
* Pemrosesan dilakukan oleh beberapa worker secara paralel, konflik ditangani atomik oleh database.
* Data bersifat persisten melalui Docker volume pada PostgreSQL.

---

## Testing Sistem (Demo Runtime)

### Melihat Metrik Awal

```
curl http://localhost:8080/stats
```

### Mengirim Beban (Event)

```
docker compose run --rm publisher
```

### Memantau Proses Asynchronous

```
docker compose exec broker redis-cli LLEN events_queue
```

Nilai lebih dari nol menandakan event masih diproses.

### Melihat Log Worker

```
docker compose logs -f aggregator
```

### Kondisi Akhir

```
docker compose exec broker redis-cli LLEN events_queue
```

Output `(integer) 0` menandakan seluruh event telah diproses.

```
curl http://localhost:8080/stats
```

Invariant yang harus terpenuhi:

```
received = unique_processed + duplicate_dropped
```

---

## Testing Otomatis (Pytest)

### Menjalankan Service Inti

```
docker compose up -d aggregator broker storage
```

### Setup Environment Test

```
python -m venv .venv
source .venv/bin/activate
pip install pytest httpx
```

### Menjalankan Test

```
pytest -q
```

## Lampiran
Link video YouTube: 