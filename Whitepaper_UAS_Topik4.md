# LAPORAN PORTOFOLIO & WHITEPAPER
## CONTEXTUAL VIEWER HYBRID SEGMENTATION (TOPIK 4)
**Praktikum Big Data — Ujian Akhir Semester**

---

### **BAB 1: Ringkasan Eksekutif & Diagram Arsitektur Digital**

#### **1.1 Perumusan Masalah Bisnis**
Dalam ekosistem media digital streaming modern seperti VideoDotCom, mempertahankan pengguna (*user retention*) dan meminimalkan tingkat pemberhentian layanan (*churn*) adalah kunci utama keberlanjutan bisnis. Pengguna memiliki perilaku menonton yang sangat bervariasi serta mengakses platform dari berbagai macam perangkat (Android, iOS, Web) dan metode pemutaran (Direct/Situs Utama vs Embed/Tertanam di situs lain).

Pendekatan segmentasi tradisional hanya mengelompokkan pengguna berdasarkan metrik tunggal seperti total durasi menonton. Pendekatan ini memiliki kelemahan besar karena mengabaikan konteks di mana pengguna tersebut menonton. Sebagai contoh, pengguna yang menonton 10 menit lewat aplikasi mobile memiliki kebutuhan monetisasi dan keterikatan yang berbeda dibanding pengguna desktop yang memutar video tertanam (embed).

Oleh karena itu, proyek ini berfokus pada **Contextual Viewer Hybrid Segmentation** menggunakan metode clustering berdimensi tinggi. Dengan mengawinkan metrik perilaku menonton (durasi, completion rate, autoplay) dengan aspek teknis perangkat (OS, platform, lokasi pemutaran), divisi marketing dan produk dapat:
1.  Merumuskan strategi monetisasi secara presisi (iklan tertarget vs konversi premium).
2.  Meningkatkan keterikatan pengguna melalui personalisasi kurasi dan rekomendasi konten.
3.  Memaksimalkan efisiensi alokasi bandwidth infrastruktur berdasarkan profil bitrate setiap segmen.

#### **1.2 Topologi Infrastruktur HDFS & Spark**
Proyek ini diimplementasikan di atas lingkungan kluster terdistribusi ter-kontainerisasi menggunakan Docker. Topologi arsitektur kluster terdiri dari:
1.  **Hadoop HDFS Cluster:**
    *   **Namenode (`namenode_rizkihsn`)**: Bertindak sebagai master node HDFS yang memetakan direktori file dan blok data di port `9000` (akses RPC) dan `9870` (Web UI).
    *   **Datanode (`datanode`)**: Worker penyimpanan yang menyimpan blok fisik data dataset `videodotcom_big.csv`.
2.  **Apache Spark Cluster (Standalone Mode):**
    *   **Spark Master (`spark-master`)**: Bertindak sebagai Resource Manager dan Driver Coordinator di port `7077` (akses internal) dan `8080` (Web UI). Alokasi RAM Driver diatur sebesar **1024 MB**.
    *   **Spark Workers (`spark-worker-1` dan `spark-worker-2`)**: Dua node pekerja terdistribusi yang masing-masing dialokasikan memori sebesar **1024 MB** dan **1 core CPU**. Total kapasitas komputasi kluster adalah 2 Core CPU dan 2 GB RAM Executor.

---

### **BAB 2: Pipeline Rekayasa Data Terdistribusi (Data Engineering)**

#### **2.1 Protokol Ingestion & Schema Casting**
Dataset berukuran ~4.7 GB (`videodotcom_big.csv`) diunggah terlebih dahulu ke dalam HDFS untuk memastikan data dapat diakses oleh seluruh worker node secara paralel.

**Perintah CLI untuk Ingestion ke HDFS:**
```bash
# 1. Membuat direktori tujuan di dalam HDFS
docker exec namenode_rizkihsn hdfs dfs -mkdir -p /user/bigdata/dataset

# 2. Menyalin dataset lokal ke dalam container Namenode
docker cp videodotcom_big.csv namenode_rizkihsn:/tmp/videodotcom_big.csv

# 3. Memasukkan dataset dari lokal container ke dalam HDFS
docker exec namenode_rizkihsn hdfs dfs -put -f /tmp/videodotcom_big.csv /user/bigdata/dataset/
```

Untuk mempercepat pembacaan data dan mencegah kegagalan memori (*Out Of Memory - OOM*) akibat pemindaian ganda schema (*inferSchema*), tipe data didefinisikan secara eksplisit sebelum dibaca oleh Spark Session.

**Blok Kode Schema Casting (PySpark):**
```python
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, LongType, DoubleType, TimestampType

videodotcom_schema = StructType([
    StructField("hash_content_id", StringType(), True),
    StructField("hash_play_id", StringType(), True),
    StructField("hash_visit_id", StringType(), True),
    StructField("hash_watcher_id", StringType(), True),
    StructField("hash_film_id", StringType(), True),
    StructField("hash_event_id", StringType(), True),
    StructField("is_login", BooleanType(), True),
    StructField("playback_location", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("play_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("referrer", StringType(), True),
    StructField("average_bitrate", IntegerType(), True),
    StructField("bitrate_range", StringType(), True),
    StructField("total_bytes", LongType(), True),
    StructField("buffer_duration", DoubleType(), True),
    StructField("referrer_group", StringType(), True),
    StructField("completed", BooleanType(), True),
    StructField("utm_source", StringType(), True),
    StructField("utm_medium", StringType(), True),
    StructField("utm_campaign", StringType(), True),
    StructField("player_name", StringType(), True),
    StructField("has_ad", BooleanType(), True),
    StructField("flash_version", StringType(), True),
    StructField("os_name", StringType(), True),
    StructField("os_version", StringType(), True),
    StructField("browser_name", StringType(), True),
    StructField("browser_version", StringType(), True),
    StructField("app_name", StringType(), True),
    StructField("autoplay", BooleanType(), True),
    StructField("is_premium", BooleanType(), True),
    StructField("app_version", StringType(), True),
    StructField("city", StringType(), True),
    StructField("play_duration", IntegerType(), True),
    StructField("content_type", StringType(), True),
    StructField("stream_type", StringType(), True),
    StructField("title", StringType(), True),
    StructField("category_name", StringType(), True),
    StructField("film_title", StringType(), True),
    StructField("season_name", StringType(), True),
    StructField("genre_name", StringType(), True)
])

# Membaca data dengan skema eksplisit
df = spark.read.csv("hdfs://namenode:9000/user/bigdata/dataset/videodotcom_big.csv", header=True, schema=videodotcom_schema)
```

#### **2.2 Manajemen Imputasi Data**
Untuk menjaga integritas dan validitas matematis model clustering, baris data yang memiliki nilai kosong (`Null` / `NaN`) pada kolom fitur dibersihkan menggunakan metode penghapusan baris terarah via Spark DataFrame API. 

**Blok Kode Data Cleaning:**
```python
# Daftar fitur yang digunakan untuk pemodelan
available_features = [
    "is_login", "platform", "playback_location", "os_name", "completed", "has_ad", 
    "is_premium", "play_duration", "autoplay", "content_type", "category_name",
    "average_bitrate", "total_bytes", "buffer_duration"
]

# Menghapus baris yang mengandung NULL pada kolom fitur terpilih
df_clean = df.dropna(subset=available_features)
```

---

### **BAB 3: Arsitektur Algoritma & Fase Eksperimen (Modeling)**

#### **3.1 Justifikasi Fitur Scaling (StandardScaler)**
Algoritma K-Means terdistribusi pada Spark MLlib menghitung kemiripan antar pengguna menggunakan **Jarak Euclidean**. Jarak Euclidean sangat sensitif terhadap skala data dari masing-masing fitur.

Dalam kasus *Contextual Viewer Hybrid Segmentation*, kita menggabungkan dua tipe data yang memiliki rentang nilai sangat berbeda jauh:
1.  **Data Kontinu**: `play_duration` (durasi menonton dalam skala ribuan detik) dan `total_bytes` (ukuran data dalam jutaan bytes).
2.  **Data Biner / Kategorikal hasil encoding**: Fitur biner bernilai `0` atau `1` (seperti variabel dummy dari platform, sistem operasi, status login, dan iklan).

Jika kita langsung mengumpankan data mentah ini ke algoritma K-Means tanpa proses standardisasi, nilai **`play_duration` yang bernilai ribuan detik akan mendominasi perhitungan jarak**. Vektor biner (seperti platform `0` atau `1`) tidak akan memberikan pengaruh yang signifikan bagi penentuan klaster karena selisih kuadrat jaraknya sangat kecil. Akibatnya, pengelompokan yang terbentuk hanya akan didasarkan pada durasi tontonan, mengabaikan aspek perangkat teknis sama sekali.

Untuk mengatasinya, komponen **`StandardScaler` terdistribusi** diterapkan untuk menormalisasikan setiap fitur sehingga memiliki standar deviasi bernilai 1. Ini menyetarakan kontribusi dari seluruh dimensi fitur.

**Blok Kode Implementasi StandardScaler di Pipeline:**
```python
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline

# 1. Menggabungkan kolom numerik & biner ke dalam VectorAssembler
assembler = VectorAssembler(
    inputCols=assembler_input_cols,
    outputCol="features",
    handleInvalid="skip"
)

# 2. Menerapkan StandardScaler terdistribusi pada vektor features
scaler = StandardScaler(
    inputCol="features",
    outputCol="scaled_features",
    withStd=True,  # membagi nilai fitur dengan standar deviasinya
    withMean=False # tidak memusatkan nilai ke 0 untuk menjaga sparse matrix
)

# 3. Menjalankan pipeline rekayasa data
pipeline = Pipeline(stages=indexers + [encoder, assembler, scaler])
pipeline_model = pipeline.fit(df_clean)
df_preprocessed = pipeline_model.transform(df_clean)
```

---

### **BAB 4: Papan Evaluasi & Validasi Matematika (Metrics Analysis)**

#### **4.1 Eksperimen Metode Elbow dengan Silhouette Score**
Untuk menentukan jumlah kelompok segmentasi yang paling logis dan optimal secara matematis, klasterisasi dilatih berulang kali dengan jumlah klaster $K \in \{2, 3, 4, 5, 6\}$. Evaluasi dilakukan menggunakan metrik **Silhouette Score** terdistribusi.

**Tabel Hasil Eksperimen Evaluasi K-Means:**

| Jumlah Klaster (K) | Silhouette Score | Keterangan |
|---|---|---|
| **K = 2** | **0.999970** | **Tertinggi (Sangat Terpisah Jelas / Terpilih)** |
| K = 3 | 0.930482 | Sangat baik |
| K = 4 | 0.509569 | Cukup kuat |
| K = 5 | 0.511865 | Cukup kuat |
| K = 6 | 0.362327 | Lemah / Banyak tumpang tindih |

Skor Silhouette bernilai mendekati `1.0` menandakan bahwa pemisahan antar klaster sangat tegas dan setiap data berada sangat dekat dengan pusat klasternya sendiri (centroid). Model memilih **K=2** sebagai model final karena memberikan performa metrik terbaik.

---

### **BAB 5: Laporan Rekomendasi Aksi & Strategi Bisnis (Business Insights)**

#### **5.1 Analisis & Profil Segmentasi Pengguna**
Berdasarkan visualisasi heatmap profil multi-dimensi (`multi_dimensional_cluster_profile.png`) dan hasil agregasi data, diperoleh 2 klaster pengguna:

*   **Cluster 0: "Free-Tier Mobile Web Casual Viewer"**
    *   **Persentase Anggota**: 99.99% (3.961.104 records)
    *   **Karakteristik**: Menonton menggunakan browser smartphone (Web Mobile: 90.3%), didominasi kategori hiburan dan berita (*Entertainment* & *News*). Menonton dalam durasi pendek (rata-rata 75.31 detik), login rate sangat rendah (1.09%), premium rate sangat rendah (0.06%), namun paparan iklan sangat tinggi (**97.84%**).
*   **Cluster 1: "Free-Tier Mobile Web Casual Viewer (Outlier)"**
    *   **Persentase Anggota**: 0.01% (1 record)
    *   **Karakteristik**: Deteksi anomali pengguna tunggal yang menonton kategori *Movies* menggunakan perangkat televisi bersistem operasi *TECHNIKA* lewat jalur pemutaran langsung (*direct*).

#### **5.2 Rekomendasi Aksi Korporat**
Berdasarkan karakteristik tersebut, berikut rekomendasi bisnis spesifik untuk tim produk dan pemasaran platform:

1.  **Advertising Optimization (Optimalisasi Iklan)**
    Karena 97.84% anggota Cluster 0 terekspos iklan namun belum mau membayar paket premium, maksimalkan pendapatan dengan menyisipkan format iklan yang lebih menguntungkan (seperti *non-skippable short ads* di awal pemutaran atau *rewarded ads*).
2.  **Login Incentive (Insentif Pendaftaran Akun)**
    Tingkat pendaftaran yang sangat rendah (1.09%) mempersulit platform melakukan personalisasi jangka panjang. Berikan insentif seperti pembatasan akses gratis atau fitur gratis simpan riwayat menonton (*Watch History* & *Watchlist*) jika pengguna bersedia membuat akun/login.
3.  **Mobile Web UX & Push Notification Optimization**
    Akses utama melalui Web Mobile menunjukkan pengguna menginginkan akses yang cepat tanpa harus mendownload aplikasi. Optimalkan kecepatan muat halaman situs mobile web dan kembangkan fitur push notification web untuk memicu pengguna kembali mengunjungi platform secara berkala.
4.  **Content Curation (Kurasi Konten Ringan)**
    Fokuskan penempatan konten bertipe pendek (*short-form*) untuk kategori *Entertainment* dan *News* di halaman utama situs mobile web karena rata-rata durasi tontonan pengguna di klaster dominan ini hanya berkisar 75 detik.

---

### **BAB 6: Dokumentasi Digital Repositori (GitHub Showcase)**

Seluruh arsitektur kode pemrograman, konfigurasi sistem, dan dokumen README.md telah disinkronisasikan ke dalam repositori publik GitHub demi transparansi rekayasa data.

*   **Tautan Repositori GitHub**: [https://github.com/RizkiHasan/lab_bigdata_rizkihasan](https://github.com/RizkiHasan/lab_bigdata_rizkihasan)
*   **Akses Hasil Output Lokal**: [D:\lab_bigdata_rizkihasan\output\](file:///d:/lab_bigdata_rizkihasan/output/)
    *   Grafik Elbow Silhouette Score: [silhouette_vs_k.png](file:///d:/lab_bigdata_rizkihasan/output/silhouette_vs_k.png)
    *   Grafik Distribusi Cluster: [cluster_distribution.png](file:///d:/lab_bigdata_rizkihasan/output/cluster_distribution.png)
    *   Grafik Heatmap Profil Cluster: [multi_dimensional_cluster_profile.png](file:///d:/lab_bigdata_rizkihasan/output/multi_dimensional_cluster_profile.png)
    *   Ringkasan Laporan PDF/Teks: [business_summary.txt](file:///d:/lab_bigdata_rizkihasan/output/business_summary.txt)
*   **Berkas Kode Utama**:
    *   Skrip Preprocessing: [preprocessing.py](file:///d:/lab_bigdata_rizkihasan/src/preprocessing.py)
    *   Skrip Clustering: [clustering.py](file:///d:/lab_bigdata_rizkihasan/src/clustering.py)
