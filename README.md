# 🎥 UAS Praktikum Big Data
# Contextual Viewer Hybrid Segmentation using Distributed K-Means Clustering

![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.x-orange.svg)
![Docker](https://img.shields.io/badge/Docker-Enabled-blue.svg)
![Hadoop](https://img.shields.io/badge/Hadoop-HDFS-yellowgreen.svg)
![Hive](https://img.shields.io/badge/Hive-Data%20Warehouse-yellow.svg)
![Kafka](https://img.shields.io/badge/Kafka-Streaming-black.svg)
![License](https://img.shields.io/badge/License-Academic-success.svg)

## 📌 Deskripsi Proyek

**Contextual Viewer Hybrid Segmentation** merupakan proyek **Ujian Akhir Semester (UAS) Praktikum Big Data** yang bertujuan melakukan segmentasi pengguna platform video streaming berdasarkan perilaku menonton (viewer behavior) menggunakan algoritma **Distributed K-Means Clustering** pada lingkungan **Big Data**.

Proyek ini mengimplementasikan pipeline analisis data secara end-to-end menggunakan **Apache Spark MLlib**, **Hadoop HDFS**, dan **Docker** sehingga mampu memproses dataset berskala besar secara terdistribusi.

Hasil segmentasi pengguna dimanfaatkan untuk membantu perusahaan memahami karakteristik setiap kelompok pengguna sehingga dapat digunakan sebagai dasar dalam pengambilan keputusan bisnis seperti personalisasi rekomendasi konten, strategi pemasaran, optimasi iklan, hingga peningkatan retensi pelanggan.

---

# 📚 Daftar Isi

- [Business Case](#-business-case)
- [Tujuan Proyek](#-tujuan-proyek)
- [Arsitektur Sistem](#-arsitektur-sistem)
- [Teknologi yang Digunakan](#-teknologi-yang-digunakan)
- [Dataset](#-dataset)
- [Struktur Repository](#-struktur-repository)
- [Pipeline Project](#-pipeline-project)
- [Cara Menjalankan Proyek](#-cara-menjalankan-proyek)
- [Output Project](#-output-project)
- [Business Insight](#-business-insight)
- [Author](#-author)

---

# 💼 Business Case

Perusahaan video streaming setiap harinya menghasilkan jutaan data aktivitas pengguna, seperti durasi menonton, perangkat yang digunakan, kategori konten favorit, status premium, lokasi akses, hingga interaksi pengguna terhadap iklan.

Volume data yang sangat besar menyebabkan perusahaan mengalami kesulitan dalam mengidentifikasi pola perilaku pengguna apabila hanya menggunakan analisis konvensional.

Melalui pendekatan **Big Data Analytics**, data pengguna dapat diproses secara paralel menggunakan Apache Spark sehingga memungkinkan proses segmentasi pengguna berjalan lebih cepat dan efisien.

Hasil segmentasi ini memberikan informasi penting mengenai karakteristik masing-masing kelompok pengguna sehingga perusahaan dapat menyusun strategi bisnis yang lebih tepat sasaran.

Beberapa manfaat yang diperoleh antara lain:

- Personalisasi rekomendasi konten.
- Optimalisasi strategi pemasaran.
- Peningkatan retensi pengguna.
- Optimasi penempatan iklan.
- Analisis perilaku pengguna premium dan non-premium.
- Pengambilan keputusan berbasis data (Data-Driven Decision Making).

---

# 🎯 Tujuan Proyek

Tujuan utama proyek ini adalah:

- Membangun pipeline Big Data menggunakan Apache Spark.
- Memproses dataset viewer behavior dalam skala besar menggunakan Hadoop HDFS.
- Melakukan preprocessing dan feature engineering terhadap data pengguna.
- Mengimplementasikan algoritma Distributed K-Means Clustering menggunakan Spark MLlib.
- Menentukan jumlah cluster terbaik menggunakan metode Silhouette Score.
- Menghasilkan visualisasi karakteristik setiap cluster.
- Menyusun business insight berdasarkan hasil clustering.

---

# 🏗 Arsitektur Sistem

Pipeline pada proyek ini dibangun menggunakan beberapa komponen Big Data yang saling terintegrasi.

```text
                        +----------------------+
                        |   VideoDotCom CSV    |
                        +----------+-----------+
                                   |
                                   |
                             Upload ke HDFS
                                   |
                                   ▼
                        +----------------------+
                        |   Hadoop HDFS        |
                        +----------+-----------+
                                   |
                                   ▼
                        +----------------------+
                        | Apache Spark MLlib   |
                        |  (Spark Cluster)     |
                        +----------+-----------+
                                   |
               +-------------------+------------------+
               |                                      |
               ▼                                      ▼
      Feature Engineering                  Distributed K-Means
               |                                      |
               +-------------------+------------------+
                                   |
                                   ▼
                        Silhouette Evaluation
                                   |
                                   ▼
                           Visualization
                                   |
                                   ▼
                         Business Insight
```

Seluruh proses analisis dijalankan secara terdistribusi menggunakan Apache Spark sehingga mampu menangani dataset berukuran jutaan baris dengan performa yang lebih baik dibandingkan pemrosesan konvensional.

---

# 🛠 Teknologi yang Digunakan

| Teknologi | Fungsi |
|-----------|--------|
| Apache Spark | Distributed Data Processing |
| Spark MLlib | Machine Learning Library |
| Hadoop HDFS | Distributed Storage |
| Docker | Containerization |
| Docker Compose | Orkestrasi Container |
| Hive | Data Warehouse |
| MySQL | Metadata Database |
| MongoDB | NoSQL Database |
| Kafka | Streaming Platform |
| ZooKeeper | Kafka Coordination |
| Pandas | Data Analysis |
| NumPy | Numerical Computing |
| Matplotlib | Data Visualization |
| Seaborn | Statistical Visualization |
| PySpark | Python API untuk Apache Spark |

---

# 📂 Dataset

Dataset yang digunakan merupakan dataset aktivitas pengguna platform video streaming (**VideoDotCom Viewer Dataset**) yang berisi jutaan data perilaku pengguna.

Dataset memiliki karakteristik sebagai berikut.

| Informasi | Keterangan |
|------------|------------|
| Jenis Dataset | Viewer Behavior |
| Format | CSV |
| Jumlah Baris | ±7 Juta Data |
| Jumlah Kolom | 41 Kolom |
| Penyimpanan | Hadoop HDFS |

Beberapa fitur yang digunakan dalam proses clustering antara lain:

- Platform
- Playback Location
- Play Duration
- Premium User
- Content Type
- Category Name
- Has Advertisement
- Autoplay
- Completed
- Login Status

Dataset diproses menggunakan Spark DataFrame sehingga mampu menangani data dalam skala besar secara efisien.

---
# 📁 Struktur Repository

Berikut merupakan struktur repository proyek yang digunakan.

```text
UAS_Praktikum_Bigdata_Topik-4-Contextual-Viewer-Hybrid-Segmentation/
│
├── src/
│   └── pipeline.py                 # Pipeline utama Spark MLlib
│
├── output/
│   ├── cluster_result.csv
│   ├── silhouette_scores.csv
│   ├── silhouette_vs_k.png
│   ├── cluster_distribution.png
│   ├── multi_dimensional_cluster_profile.png
│   └── business_summary.txt
│
├── docker-compose.yml              # Konfigurasi seluruh container
├── hadoop.env                      # Environment Hadoop
├── requirements.txt                # Dependency Python
├── README.md
│
└── dataset/
    └── videodotcom_big.csv         # Dataset (tidak disertakan karena ukuran besar)
```

> **Catatan:** Dataset tidak disertakan di dalam repository karena ukurannya sangat besar (±7 juta data). Silakan menempatkan dataset pada direktori yang sesuai sebelum proses upload ke HDFS.

---

# 🔄 Pipeline Project

Seluruh proses analisis telah diintegrasikan ke dalam satu pipeline utama yaitu:

```text
src/pipeline.py
```

Pipeline tersebut menjalankan seluruh tahapan analisis secara otomatis mulai dari membaca data hingga menghasilkan insight bisnis.

Alur pipeline adalah sebagai berikut.

```text
VideoDotCom Dataset
        │
        ▼
Load Data dari HDFS
        │
        ▼
Data Cleaning
        │
        ▼
Feature Engineering
(StringIndexer + OneHotEncoder)
        │
        ▼
VectorAssembler
        │
        ▼
StandardScaler
        │
        ▼
Distributed K-Means Clustering
        │
        ▼
Silhouette Score Evaluation
        │
        ▼
Visualisasi Hasil
        │
        ▼
Business Insight
```

Pipeline dijalankan menggunakan Apache Spark sehingga seluruh proses berlangsung secara paralel pada Spark Cluster.

---

# ⚙️ Cara Menjalankan Proyek

## 1. Clone Repository

```bash
git clone https://github.com/rizkihsn/UAS_Praktikum_Bigdata_Topik-4-Contextual-Viewer-Hybrid-Segmentation.git

cd UAS_Praktikum_Bigdata_Topik-4-Contextual-Viewer-Hybrid-Segmentation
```

---

## 2. Jalankan Docker

Pastikan Docker Desktop telah aktif.

Kemudian jalankan seluruh service.

```bash
docker compose up -d
```

Periksa seluruh container.

```bash
docker ps
```

Container yang harus aktif:

- Hadoop NameNode
- Hadoop DataNode
- Spark Master
- Spark Worker
- Hive Server
- Hive Metastore
- MySQL
- MongoDB
- ZooKeeper
- Kafka
- Kafdrop

---

## 3. Install Dependency

```bash
pip install -r requirements.txt
```

---

## 4. Upload Dataset ke HDFS

Masuk ke container Hadoop.

```bash
docker exec -it namenode bash
```

Buat direktori pada HDFS.

```bash
hdfs dfs -mkdir -p /data
```

Upload dataset.

```bash
hdfs dfs -put videodotcom_big.csv /data/
```

Pastikan dataset berhasil diunggah.

```bash
hdfs dfs -ls /data
```

Output yang diharapkan.

```text
Found 1 items
/data/videodotcom_big.csv
```

---

## 5. Jalankan Pipeline Spark

Seluruh proses dijalankan menggunakan satu file utama.

```bash
spark-submit src/pipeline.py
```

Apabila menjalankan melalui container Spark Master.

```bash
docker exec -it spark-master bash

spark-submit /opt/bitnami/spark/src/pipeline.py
```

---

## 6. Proses yang Dilakukan Pipeline

Pipeline akan menjalankan seluruh tahapan berikut secara otomatis.

### 📌 Data Loading

- Membaca dataset dari Hadoop HDFS.
- Menggunakan schema yang telah ditentukan agar proses lebih cepat dan konsisten.

---

### 📌 Data Cleaning

Tahapan preprocessing meliputi:

- Menghapus data kosong (null value).
- Memastikan tipe data sesuai.
- Memilih atribut yang relevan.
- Membersihkan data yang tidak valid.

---

### 📌 Feature Engineering

Data kategorikal tidak dapat diproses langsung oleh algoritma K-Means.

Oleh karena itu dilakukan proses:

- StringIndexer
- OneHotEncoder
- VectorAssembler

Seluruh fitur kemudian digabung menjadi sebuah feature vector.

---

### 📌 Feature Scaling

Feature vector dinormalisasi menggunakan:

```text
StandardScaler
```

Normalisasi dilakukan agar setiap fitur memiliki skala yang seimbang sehingga tidak mendominasi proses clustering.

---

### 📌 Distributed K-Means Clustering

Algoritma K-Means dijalankan menggunakan Spark MLlib.

Percobaan dilakukan pada beberapa nilai cluster.

```text
K = 2
K = 3
K = 4
K = 5
K = 6
```

Spark akan mendistribusikan proses clustering ke seluruh worker sehingga proses berjalan lebih cepat.

---

### 📌 Evaluasi Model

Kualitas cluster dievaluasi menggunakan:

```text
Silhouette Score
```

Nilai Silhouette terbesar dipilih sebagai jumlah cluster terbaik.

---

### 📌 Visualisasi

Pipeline menghasilkan beberapa visualisasi otomatis.

- Silhouette Score
- Distribusi Cluster
- Multi-Dimensional Cluster Profile

Visualisasi membantu memahami karakteristik masing-masing cluster.

---

### 📌 Business Insight

Tahap terakhir adalah menghasilkan ringkasan analisis bisnis secara otomatis.

Output berupa file:

```text
business_summary.txt
```

Laporan ini berisi interpretasi setiap cluster beserta rekomendasi strategi bisnis yang dapat digunakan perusahaan.

---
# 📊 Output Project

Setelah pipeline selesai dijalankan, sistem akan menghasilkan beberapa file output secara otomatis pada folder `output/`.

```text
output/
│
├── cluster_result.csv
├── silhouette_scores.csv
├── silhouette_vs_k.png
├── cluster_distribution.png
├── multi_dimensional_cluster_profile.png
└── business_summary.txt
```

## Penjelasan Output

| File | Deskripsi |
|------|-----------|
| `cluster_result.csv` | Menyimpan hasil segmentasi pengguna beserta label cluster yang diperoleh dari algoritma K-Means. |
| `silhouette_scores.csv` | Berisi nilai Silhouette Score untuk setiap percobaan jumlah cluster (K). |
| `silhouette_vs_k.png` | Grafik hubungan antara jumlah cluster dengan nilai Silhouette Score. |
| `cluster_distribution.png` | Visualisasi distribusi jumlah anggota pada setiap cluster. |
| `multi_dimensional_cluster_profile.png` | Visualisasi karakteristik masing-masing cluster berdasarkan fitur utama. |
| `business_summary.txt` | Ringkasan hasil analisis beserta rekomendasi strategi bisnis. |

---

# 📈 Visualisasi Hasil

Pipeline menghasilkan beberapa visualisasi yang membantu proses interpretasi hasil clustering.

## 1. Silhouette Score

Visualisasi ini digunakan untuk menentukan jumlah cluster terbaik.

Semakin tinggi nilai **Silhouette Score**, semakin baik kualitas pemisahan antar cluster.

---

## 2. Cluster Distribution

Grafik ini menunjukkan jumlah anggota pada setiap cluster.

Visualisasi ini membantu mengetahui apakah distribusi cluster sudah cukup seimbang atau terdapat cluster yang terlalu dominan.

---

## 3. Multi-Dimensional Cluster Profile

Visualisasi ini memperlihatkan karakteristik setiap cluster berdasarkan fitur-fitur utama.

Melalui visualisasi ini dapat diketahui perbedaan perilaku antar kelompok pengguna, seperti:

- Intensitas menonton
- Status premium
- Preferensi kategori konten
- Penggunaan autoplay
- Interaksi terhadap iklan

---

# 💼 Business Insight

Hasil clustering memberikan informasi penting mengenai karakteristik setiap kelompok pengguna.

Beberapa contoh pemanfaatannya antara lain:

### 🎯 Personalisasi Konten

Setiap cluster dapat diberikan rekomendasi konten yang berbeda sesuai dengan karakteristik perilakunya.

---

### 📢 Optimasi Strategi Pemasaran

Promosi dapat difokuskan pada kelompok pengguna yang memiliki peluang konversi lebih tinggi sehingga biaya pemasaran menjadi lebih efisien.

---

### ⭐ Strategi Premium

Pengguna aktif yang masih menggunakan layanan gratis dapat menjadi target utama untuk penawaran layanan premium.

---

### 📺 Optimasi Penayangan Iklan

Perusahaan dapat menentukan strategi penempatan iklan berdasarkan karakteristik masing-masing cluster agar tidak mengurangi pengalaman pengguna.

---

### ❤️ Peningkatan Retensi Pengguna

Cluster yang menunjukkan kecenderungan tidak aktif dapat diberikan promosi atau rekomendasi konten khusus untuk meningkatkan loyalitas pengguna.

---

# 🚀 Pengembangan Selanjutnya

Beberapa pengembangan yang dapat dilakukan pada proyek ini antara lain:

- Menambahkan algoritma clustering lain seperti Gaussian Mixture Model (GMM), DBSCAN, atau Bisecting K-Means.
- Mengimplementasikan Spark Structured Streaming untuk analisis data secara real-time.
- Mengembangkan dashboard interaktif menggunakan Streamlit atau Apache Superset.
- Menambahkan evaluasi clustering menggunakan Davies-Bouldin Index dan Calinski-Harabasz Index.
- Mengintegrasikan model rekomendasi (Recommendation System) berdasarkan hasil segmentasi pengguna.
- Meningkatkan performa pipeline menggunakan optimasi konfigurasi Spark Cluster.

---

# 📌 Kesimpulan

Proyek **Contextual Viewer Hybrid Segmentation** berhasil mengimplementasikan pipeline analisis Big Data secara end-to-end menggunakan Apache Spark MLlib pada lingkungan Hadoop.

Melalui proses preprocessing, feature engineering, clustering, evaluasi, dan visualisasi, proyek ini mampu menghasilkan segmentasi pengguna berdasarkan perilaku menonton yang dapat dimanfaatkan sebagai dasar pengambilan keputusan bisnis.

Pendekatan ini menunjukkan bahwa pemanfaatan teknologi Big Data memungkinkan proses analisis jutaan data dilakukan secara lebih cepat, efisien, dan scalable dibandingkan metode konvensional.

---

# 👥 Tim Pengembang

Proyek ini merupakan tugas **Ujian Akhir Semester (UAS) Praktikum Big Data** yang dikerjakan secara berkelompok oleh mahasiswa Program Studi Teknik Informatika, Fakultas Teknologi Informasi, Universitas Bale Bandung (UNIBBA).

| Nama |
|------|
| Rizki Hasan Fauzi | 
| Fadhilah Tri Anugerah Putra Pamungkas | 
| Tegar Bagus Permana | 
| Soni Moch Leviansyah | 
| Irfan Fauzi | 

**Program Studi:** Teknik Informatika  
**Fakultas:** Fakultas Teknologi Informasi  
**Universitas:** Universitas Bale Bandung (UNIBBA)

**Mata Kuliah:** Praktikum Big Data  
**Topik:** Contextual Viewer Hybrid Segmentation using Distributed K-Means Clustering

---

# 🙏 Acknowledgements

Ucapan terima kasih kepada:

- Universitas Bale Bandung (UNIBBA)
- Fakultas Teknologi Informasi
- Dosen Mata Kuliah Praktikum Big Data
- Apache Spark Community
- Apache Hadoop Community
- Docker Community
- Seluruh pihak yang mendukung penyelesaian proyek ini.

---

# 📄 License

Repository ini dibuat untuk keperluan akademik sebagai proyek **Ujian Akhir Semester Praktikum Big Data**.

Penggunaan kode sumber untuk tujuan pembelajaran diperbolehkan dengan tetap mencantumkan atribusi kepada penulis.

---

## ⭐ Jika repository ini bermanfaat

Apabila repository ini membantu proses pembelajaran atau menjadi referensi dalam pengembangan proyek Big Data, silakan berikan **⭐ Star** pada repository GitHub sebagai bentuk apresiasi.

Terima kasih telah mengunjungi repository ini.
