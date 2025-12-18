# Pub–Sub Log Aggregator

## Build dan Menjalankan Sistem

### Build dan Run

```
docker compose up -d --build aggregator broker storage
```

### Cek Status Service

```
docker compose ps
```

Service yang diekspos ke host:

* aggregator: [http://localhost:8080]

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

## Pengujian Event dan Persistensi Data

### Mengecek Event yang Tersimpan

Setelah seluruh event selesai diproses, event unik yang tersimpan di database dapat dicek menggunakan endpoint berikut:

```
curl "http://localhost:8080/events?topic=auth"
```

Perintah ini menampilkan daftar event unik berdasarkan topic tertentu. Hasil ini menjadi bukti bahwa event duplikat tidak disimpan lebih dari satu kali dan mekanisme deduplication berjalan dengan benar.

### Menguji Persistensi Data (Crash dan Recreate Container)

Untuk membuktikan bahwa data tetap tersimpan meskipun container dihentikan atau dihapus, lakukan penghapusan container aggregator lalu jalankan kembali container tersebut:

```
docker compose rm -sf aggregator
docker compose up -d aggregator
```

Setelah container aktif kembali, lakukan pengecekan ulang metrik dan event:

```
curl http://localhost:8080/stats
curl "http://localhost:8080/events?topic=auth"
```

Jika nilai metrik dan daftar event tetap tersedia dan tidak kembali ke nol, maka persistensi data melalui Docker volume PostgreSQL terbukti berjalan dengan benar.

---

## Testing Otomatis (Pytest)

### Menjalankan Service Inti

```
docker compose up -d aggregator broker storage
```

### Setup Environment Test

```
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install pytest httpx
```

### Menjalankan Test

```
pytest -q -v
```

## Lampiran
Link Video YouTube: https://www.youtube.com/watch?v=Xi5UWjmE5Sg
