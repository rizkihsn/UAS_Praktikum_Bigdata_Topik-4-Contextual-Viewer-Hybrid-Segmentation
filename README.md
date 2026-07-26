# Contextual Viewer Hybrid Segmentation
## UAS Praktikum Big Data — Topik 4

### 📋 Deskripsi Project

Project ini melakukan **segmentasi pengguna** platform video streaming (VideoDotCom) berdasarkan perilaku menonton, kategori konten, platform, sistem operasi, dan status premium menggunakan **High-Dimensional Clustering** pada lingkungan **Big Data** terdistribusi.

Seluruh proses dijalankan menggunakan **Apache Spark MLlib** di atas infrastruktur **Docker + Hadoop HDFS + Spark Cluster**.

---

### 💼 Business Case

Perusahaan video streaming perlu memahami segmen penggunanya agar dapat:
- Mengoptimalkan strategi monetisasi (premium vs iklan).
- Mempersonalisasi rekomendasi konten.
- Meningkatkan retensi dan engagement pengguna.
- Mengalokasikan budget marketing secara efisien per segmen.

Dengan melakukan clustering terhadap jutaan data perilaku menonton, perusahaan dapat mengidentifikasi pola-pola tersembunyi dan mengambil keputusan berbasis data.

---

### 🎯 Tujuan

1. Membangun pipeline Big Data end-to-end menggunakan Apache Spark MLlib.
2. Melakukan preprocessing dan feature engineering pada data berskala besar.
3. Menerapkan algoritma **Distributed K-Means Clustering**.
4. Mengevaluasi model menggunakan **Silhouette Score**.
5. Menghasilkan insight bisnis dan rekomendasi strategi per segmen pengguna.

---

### 📊 Dataset

| Atribut | Detail |
|---------|--------|
| **Nama** | VideoDotCom Viewer Dataset |
| **Format** | CSV |
| **Jumlah Kolom** | 41 kolom (videodotcom_big.csv) |
| **Jumlah Baris** | ~7 juta+ (videodotcom_big.csv) |

**Kolom yang digunakan sebagai fitur:**

| Kolom | Tipe | Peran |
|-------|------|-------|
| `is_login` | Boolean | Status login pengguna |
| `platform` | String | Platform akses (app-android, web-mobile, dll.) |
| `playback_location` | String | Lokasi playback (direct, embed) |
| `completed` | Boolean | Apakah konten ditonton sampai selesai |
| `has_ad` | Boolean | Apakah ada iklan |
| `is_premium` | Boolean | Status premium pengguna |
| `play_duration` | Integer | Durasi menonton (detik) |
| `autoplay` | Boolean | Apakah autoplay aktif |
| `content_type` | String | Tipe konten (vod, catchup, dll.) |
| `category_name` | String | Kategori konten (Movies, News, Sports, dll.) |

---

### 🛠️ Tech Stack

| Teknologi | Fungsi |
|-----------|--------|
| **Apache Spark** | Distributed computing engine |
| **PySpark** | Python API untuk Apache Spark |
| **Spark MLlib** | Library machine learning terdistribusi |
| **Hadoop HDFS** | Distributed file system |
| **Docker** | Containerized environment |
| **Python** | Bahasa pemrograman utama |
| **Pandas** | Data manipulation untuk visualisasi |
| **Matplotlib** | Library visualisasi grafik |
| **Seaborn** | Library visualisasi statistik |

---

### 📁 Struktur Folder

```
lab_bigdata_rizkihasan/
│
├── README.md                    # Dokumentasi project (file ini)
├── requirements.txt             # Daftar dependensi Python
├── docker-compose.yml           # Orkestrasi container Docker
├── hadoop.env                   # Konfigurasi environment Hadoop
├── preprocessing.ipynb          # Notebook eksplorasi (referensi)
│
├── videodotcom1.csv             # Dataset utama
├── videodotcom_big.csv          # Dataset besar (opsional)
│
├── src/
│   └── pipeline.py              # Berkas pipeline tunggal (Preprocessing, Clustering, Visualisasi, Bisnis)
│
└── output/
    ├── cluster_result.csv       # Dataset dengan label cluster
    ├── silhouette_scores.csv    # Evaluasi Silhouette Score per K
    ├── silhouette_vs_k.png      # Grafik Silhouette Score vs K
    ├── cluster_distribution.png # Grafik distribusi anggota cluster
    ├── multi_dimensional_cluster_profile.png      # Heatmap profil fitur per cluster
    └── business_summary.txt     # Laporan insight bisnis
```

---

### 🔄 Pipeline Project

```
Dataset (HDFS)
     │
     ▼
preprocessing.py
  ├── Data Understanding (printSchema, count, describe, missing value, duplicate)
  ├── Data Cleaning (handle missing values)
  ├── Feature Engineering (boolean casting)
  ├── StringIndexer → OneHotEncoder
  ├── VectorAssembler
  └── StandardScaler (WAJIB)
     │
     ▼
  Parquet (HDFS)
     │
     ▼
clustering.py
  ├── Distributed K-Means (K = 2, 3, 4, 5, 6)
  ├── Silhouette Score evaluation
  ├── Pilih K optimal
  └── Simpan cluster_result.csv & silhouette_scores.csv
     │
     ▼
visualization.py
  ├── silhouette_vs_k.png
  ├── cluster_distribution.png
  └── multi_dimensional_cluster_profile.png
     │
     ▼
business_insight.py
  └── business_summary.txt
```

---

### 🚀 Cara Menjalankan

#### 1. Jalankan Docker

```bash
docker-compose up -d
```

Pastikan container `namenode`, `datanode`, `spark-master`, `spark-worker-1`, dan `spark-worker-2` berjalan.

#### 2. Upload Dataset ke HDFS

```bash
# Buat direktori di HDFS
docker exec namenode_rizkihsn hdfs dfs -mkdir -p /user/bigdata/dataset

# Copy dataset ke container
docker cp videodotcom_big.csv namenode_rizkihsn:/tmp/videodotcom_big.csv

# Upload ke HDFS
docker exec namenode_rizkihsn hdfs dfs -put -f /tmp/videodotcom_big.csv /user/bigdata/dataset/
```

Verifikasi:
```bash
docker exec namenode_rizkihsn hdfs dfs -ls /user/bigdata/dataset/
```

#### 3. Jalankan Preprocessing

```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /src/preprocessing.py
```

> **Catatan:** Pastikan folder `src/` ter-mount ke container `spark-master`. Jika belum, tambahkan volume di `docker-compose.yml`:
> ```yaml
> spark-master:
>   volumes:
>     - ./src:/src
>     - ./output:/output
> ```

#### 4. Jalankan Clustering

```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /src/clustering.py
```

#### 5. Jalankan Visualization
6. Ambil hasil output dari container ke lokal host

```bash
docker cp spark-master:/output/. ./output
```

7. Jalankan visualization.py secara lokal

```bash
python src/visualization.py
```

8. Jalankan business_insight.py secara lokal

```bash
python src/business_insight.py
```

---

### 📤 Output

| File | Deskripsi |
|------|-----------|
| `output/cluster_result.csv` | Dataset lengkap dengan kolom `cluster` yang berisi label segmen (0, 1, 2, ...) untuk setiap baris data |
| `output/silhouette_scores.csv` | Tabel evaluasi berisi kolom `K` dan `Silhouette` untuk setiap nilai K yang diuji (K=2 s/d K=6) |
| `output/silhouette_vs_k.png` | Line chart yang memvisualisasikan hubungan Silhouette Score terhadap nilai K, dengan anotasi K optimal |
| `output/cluster_distribution.png` | Bar chart yang menampilkan distribusi jumlah anggota pada setiap cluster |
| `output/multi_dimensional_cluster_profile.png` | Heatmap yang menampilkan profil rata-rata fitur numerik per cluster |
| `output/business_summary.txt` | Laporan insight bisnis yang mencakup persona, karakteristik, dan rekomendasi strategi per segmen |

---

### 📈 Hasil

- Nilai **K optimal** dipilih berdasarkan **Silhouette Score tertinggi** dari eksperimen K = 2, 3, 4, 5, 6.
- Silhouette Score mengukur kualitas pemisahan antar cluster (rentang -1 hingga 1, semakin mendekati 1 semakin baik).
- **StandardScaler** diterapkan sebelum K-Means untuk memastikan seluruh fitur berkontribusi secara seimbang dalam perhitungan jarak Euclidean.
- Setiap cluster diberi **label persona** deskriptif dan **rekomendasi bisnis** yang actionable.

---

### 👤 Author

**Nama:** Rizki Hasan

**Program Studi:** Teknik Informatika

**Universitas:** Universitas Bale Bandung

**Mata Kuliah:** Praktikum Big Data — UAS Tahun Akademik 2025/2026
