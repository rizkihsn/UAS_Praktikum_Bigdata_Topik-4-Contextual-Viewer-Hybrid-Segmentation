LAPORAN PORTOFOLIO & WHITEPAPER
CONTEXTUAL VIEWER HYBRID SEGMENTATION
(Topik 4 — Hybrid Segmentation: Clustering Dimensi Tinggi)
Ujian Akhir Semester — Praktikum Big Data
Studi Kasus: Platform Streaming Video "VideoDotCom"















Disusun oleh:

Rizki hasan	301240071
Irfan fauzi	301240068
Soni moch leviansyah	301240030
Fadilah tri	301240066
Tegar bagus permana	301240040
	


FAKULTAS TEKNOLOGI INFORMASI
PRODI TEKNIK INFORMATIKA 
UNIVERSITAS BALE BANDUNG
2026
	
DAFTAR ISI
BAB 1 RINGKASAN EKSEKUTIF & DIAGRAM ARSITEKTUR DIGITAL	3
1.1 Perumusan Masalah Bisnis	3
1.2 Topologi Infrastruktur HDFS & Spark	3
a. Hadoop HDFS Cluster	3
b. Apache Spark Cluster (Standalone Mode)	3
BAB 2 PIPELINE REKAYASA DATA TERDISTRIBUSI (DATA ENGINEERING)	5
2.1 Protokol Ingestion & Schema Casting	5
2.2 Manajemen Imputasi Data	5
BAB 3 ARSITEKTUR ALGORITMA & FASE EKSPERIMEN (MODELING)	7
3.1 Justifikasi Fitur Scaling (StandardScaler)	7
3.2 Konfigurasi Model K-Means	7
BAB 4 PAPAN EVALUASI & VALIDASI MATEMATIKA (METRICS ANALYSIS)	9
4.1 Eksperimen Metode Elbow: Komparasi Silhouette Score Lintas Nilai K	9
4.2 Interpretasi Nilai Metrik terhadap Kualitas Model	9
BAB 5 REKOMENDASI AKSI & STRATEGI BISNIS (BUSINESS INSIGHTS)	11
5.1 Ringkasan Data & Profil Segmentasi Pengguna	11
Cluster 0 — "Free-Tier Mobile Web Casual Viewer"	11
Cluster 1 — "Free-Tier Mobile Web Casual Viewer (Outlier)"	11
5.2 Tantangan Visualisasi: Profil Multi-Dimensi Antar-Klaster	12
5.3 Rekomendasi Aksi Korporat	13
BAB 6 DOKUMENTASI DIGITAL REPOSITORI (GITHUB SHOWCASE)	14
6.1 Tautan Repositori	14
6.2 Berkas Output & Artefak Visual	14
6.3 Berkas Kode Utama	14
6.4 Reproduktifitas	14
 BAB I
Ringkasan Eksekutif & Diagram Arsitektur Digital
1.1 Perumusan Masalah Bisnis
Dalam ekosistem media digital streaming modern seperti VideoDotCom, mempertahankan pengguna (user retention) dan menekan tingkat pemberhentian layanan (churn) merupakan kunci keberlanjutan bisnis. Pengguna memiliki perilaku menonton yang sangat beragam serta mengakses platform dari berbagai perangkat (Android, iOS, Web) dan metode pemutaran (Direct/situs utama vs Embed/tertanam di situs pihak ketiga).
Pendekatan segmentasi tradisional umumnya hanya mengelompokkan pengguna berdasarkan satu metrik tunggal, misalnya total durasi menonton. Pendekatan ini memiliki kelemahan besar karena mengabaikan konteks tempat pengguna tersebut menonton. Sebagai ilustrasi, pengguna yang menonton 10 menit lewat aplikasi mobile memiliki kebutuhan monetisasi dan pola keterikatan yang sangat berbeda dibanding pengguna desktop yang memutar video tertanam (embed) di situs berita.
Oleh karena itu, proyek ini mengangkat pendekatan Contextual Viewer Hybrid Segmentation menggunakan metode clustering berdimensi tinggi yang mengawinkan dua jenis sinyal sekaligus:
•	Metrik perilaku menonton — play_duration, completion rate, autoplay rate, login rate.
•	Aspek teknis perangkat & konteks akses — platform, sistem operasi, lokasi pemutaran (playback_location), average_bitrate, dan total_bytes.
Dengan menggabungkan kedua dimensi tersebut dalam satu ruang fitur, divisi bisnis dan produk dapat:
1.	Merumuskan strategi monetisasi secara presisi (iklan tertarget vs konversi ke paket premium).
2.	Meningkatkan keterikatan pengguna melalui personalisasi kurasi dan rekomendasi konten.
3.	Memaksimalkan efisiensi alokasi bandwidth infrastruktur berdasarkan profil bitrate setiap segmen pengguna.
1.2 Topologi Infrastruktur HDFS & Spark
Proyek ini diimplementasikan pada lingkungan kluster terdistribusi yang di-kontainerisasi menggunakan Docker, meniru arsitektur produksi skala kecil untuk pemrosesan big data. Topologi kluster terbagi menjadi dua lapisan utama sebagai berikut.
a. Hadoop HDFS Cluster
•	Namenode (namenode_rizkihsn): bertindak sebagai master node HDFS yang memetakan direktori dan blok data, diakses melalui port 9000 (RPC) dan 9870 (Web UI).
•	Datanode: worker penyimpanan yang menampung blok fisik dari dataset videodotcom_big.csv secara terdistribusi.
b. Apache Spark Cluster (Standalone Mode)
•	Spark Master (spark-master): berperan sebagai Resource Manager dan Driver Coordinator pada port 7077 (internal) dan 8080 (Web UI), dengan alokasi RAM Driver sebesar 1024 MB.
•	Spark Worker 1 & 2: dua node pekerja terdistribusi, masing-masing dialokasikan memori 1024 MB dan 1 core CPU, sehingga total kapasitas komputasi kluster adalah 2 core CPU dan 2 GB RAM Executor.
Diagram ringkas arsitektur dapat digambarkan sebagai berikut: Dataset mentah → diunggah ke HDFS (Namenode/Datanode) → dibaca terdistribusi oleh Spark Driver di Spark Master → diproses paralel oleh Spark Worker 1 dan Worker 2 (ETL, feature engineering, K-Means) → hasil agregasi dan model dituliskan kembali sebagai laporan bisnis dan artefak visual.
BAB II 
Pipeline Rekayasa Data Terdistribusi (Data Engineering)
2.1 Protokol Ingestion & Schema Casting
Dataset mentah berukuran kurang lebih 4,7 GB (videodotcom_big.csv) terlebih dahulu diunggah ke HDFS agar dapat diakses secara paralel oleh seluruh worker node. Proses ingestion dilakukan melalui tiga tahap perintah CLI berikut.
# 1. Membuat direktori tujuan di dalam HDFS
docker exec namenode_rizkihsn hdfs dfs -mkdir -p /user/bigdata/dataset
 
# 2. Menyalin dataset lokal ke dalam container Namenode
docker cp videodotcom_big.csv namenode_rizkihsn:/tmp/videodotcom_big.csv
 
# 3. Memasukkan dataset dari lokal container ke dalam HDFS
docker exec namenode_rizkihsn hdfs dfs -put -f /tmp/videodotcom_big.csv \
    /user/bigdata/dataset/
Untuk mempercepat pembacaan data dan mencegah kegagalan memori (Out Of Memory / OOM) akibat pemindaian ganda skema (inferSchema=True), tipe data setiap kolom didefinisikan secara eksplisit sebelum dibaca oleh Spark Session menggunakan StructType.
from pyspark.sql.types import (StructType, StructField, StringType,
    BooleanType, IntegerType, LongType, DoubleType, TimestampType)
 
videodotcom_schema = StructType([
    StructField("hash_content_id", StringType(), True),
    StructField("hash_watcher_id", StringType(), True),
    StructField("is_login", BooleanType(), True),
    StructField("playback_location", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("play_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("average_bitrate", IntegerType(), True),
    StructField("total_bytes", LongType(), True),
    StructField("buffer_duration", DoubleType(), True),
    StructField("completed", BooleanType(), True),
    StructField("os_name", StringType(), True),
    StructField("autoplay", BooleanType(), True),
    StructField("is_premium", BooleanType(), True),
    StructField("play_duration", IntegerType(), True),
    StructField("content_type", StringType(), True),
    StructField("category_name", StringType(), True),
    # ... kolom lain mengikuti struktur asli dataset
])
 
df = spark.read.csv(
    "hdfs://namenode:9000/user/bigdata/dataset/videodotcom_big.csv",
    header=True, schema=videodotcom_schema)
2.2 Manajemen Imputasi Data
Untuk menjaga integritas dan validitas matematis model clustering, baris data yang memiliki nilai kosong (Null/NaN) pada kolom-kolom fitur inti dibersihkan menggunakan fungsi native Spark DataFrame API, bukan pendekatan iteratif Python biasa, agar proses tetap berjalan secara terdistribusi di seluruh worker node.
# Daftar fitur yang digunakan untuk pemodelan
available_features = [
    "is_login", "platform", "playback_location", "os_name",
    "completed", "has_ad", "is_premium", "play_duration",
    "autoplay", "content_type", "category_name",
    "average_bitrate", "total_bytes", "buffer_duration"
]
 
# Menghapus baris yang mengandung NULL pada kolom fitur terpilih
df_clean = df.dropna(subset=available_features)
Strategi dropna dipilih (dibandingkan imputasi rata-rata/median) karena proporsi baris kosong pada dataset relatif kecil terhadap total 3.961.105 baris hasil akhir, sehingga penghapusan tidak menimbulkan bias signifikan namun menjamin setiap vektor fitur yang masuk ke tahap scaling benar-benar lengkap dan valid secara numerik.
 BAB III 
Arsitektur Algoritma & Fase Eksperimen (Modeling)
Bagian ini merupakan modul teknis khusus untuk Topik 4 — Hybrid Segmentation (Clustering Dimensi Tinggi), sesuai kerangka acuan proyek.
3.1 Justifikasi Fitur Scaling (StandardScaler)
Algoritma K-Means terdistribusi pada Spark MLlib menghitung kemiripan antar pengguna menggunakan jarak Euclidean, yang sangat sensitif terhadap skala data dari masing-masing fitur.
Pada kasus Contextual Viewer Hybrid Segmentation, dua tipe data dengan rentang nilai yang jauh berbeda digabungkan dalam satu vektor fitur:
•	Data kontinu berskala besar: play_duration (durasi menonton dalam ribuan detik) dan total_bytes (ukuran data dalam jutaan byte).
•	Data biner/kategorikal hasil encoding: fitur bernilai 0 atau 1, seperti variabel dummy dari platform, sistem operasi, status login, dan status iklan.
Jika data mentah tersebut langsung diumpankan ke algoritma K-Means tanpa proses standardisasi, nilai play_duration yang bernilai ribuan detik akan mendominasi perhitungan jarak. Vektor biner (misalnya platform bernilai 0 atau 1) tidak akan memberi pengaruh signifikan terhadap penentuan klaster karena selisih kuadrat jaraknya sangat kecil. Akibatnya, pengelompokan yang terbentuk hanya akan didasarkan pada durasi tontonan dan mengabaikan aspek perangkat teknis sama sekali — inilah bukti tertulis mengapa data durasi (ribuan detik) wajib disetarakan skalanya dengan data biner agar tidak mendominasi jarak K-Means.
Untuk mengatasi hal tersebut, komponen StandardScaler terdistribusi diterapkan agar setiap fitur memiliki standar deviasi bernilai 1, sehingga menyetarakan kontribusi seluruh dimensi fitur terhadap perhitungan jarak.
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
    withStd=True,   # membagi nilai fitur dengan standar deviasinya
    withMean=False  # tidak memusatkan ke 0 agar sparse matrix tetap efisien
)
 
# 3. Menjalankan pipeline rekayasa data
pipeline = Pipeline(stages=indexers + [encoder, assembler, scaler])
pipeline_model = pipeline.fit(df_clean)
df_preprocessed = pipeline_model.transform(df_clean)
Dengan withMean=False, scaler tidak menggeser pusat data ke nol sehingga struktur sparse matrix hasil one-hot encoding tetap terjaga dan proses komputasi terdistribusi menjadi lebih efisien secara memori.
3.2 Konfigurasi Model K-Means
Model K-Means dilatih secara terdistribusi menggunakan pyspark.ml.clustering.KMeans di atas kolom scaled_features, dengan featuresCol diarahkan ke hasil StandardScaler dan predictionCol menghasilkan label klaster untuk setiap baris data. Model dilatih berulang untuk beberapa nilai K guna menemukan jumlah segmen yang paling optimal secara matematis (dibahas pada BAB 4).
 BAB IV
Papan Evaluasi & Validasi Matematika (Metrics Analysis)
4.1 Eksperimen Metode Elbow: Komparasi Silhouette Score Lintas Nilai K
Untuk menentukan jumlah segmen yang paling logis dan optimal secara matematis, model klasterisasi dilatih berulang dengan jumlah klaster K ∈ {2, 3, 4, 5, 6}. Evaluasi dilakukan menggunakan metrik Silhouette Score terdistribusi (ClusteringEvaluator) yang mengukur seberapa dekat suatu titik data dengan klasternya sendiri dibandingkan dengan klaster terdekat lainnya.
Jumlah Klaster (K)	Silhouette Score	Keterangan
K = 2	0.999970	Tertinggi — pemisahan sangat jelas (Terpilih)
K = 3	0.930482	Sangat baik
K = 4	0.509569	Cukup kuat
K = 5	0.511865	Cukup kuat
K = 6	0.362327	Lemah — banyak tumpang tindih

 
Gambar 1. Grafik Elbow Method menggunakan Silhouette Score untuk K = 2 hingga K = 6 (silhouette_vs_k.png).
Skor Silhouette yang mendekati nilai 1,0 menandakan bahwa pemisahan antar klaster sangat tegas dan setiap titik data berada sangat dekat dengan centroid klasternya sendiri. Terlihat pada grafik bahwa skor menurun tajam setelah K = 3 dan sempat stabil di kisaran 0,51 pada K = 4–5 sebelum kembali menurun pada K = 6, yang mengindikasikan overfitting segmentasi (klaster dipecah terlalu kecil sehingga saling tumpang tindih). Model final memilih K = 2 karena memberikan performa metrik terbaik sekaligus interpretasi bisnis yang paling jelas.
4.2 Interpretasi Nilai Metrik terhadap Kualitas Model
Nilai Silhouette Score 0,999970 pada K = 2 tergolong sangat ekstrem untuk dataset perilaku pengguna riil. Hal ini konsisten dengan temuan pada BAB 5, di mana Cluster 1 hanya berisi 1 baris data (outlier tunggal) sementara Cluster 0 menampung 3.961.104 baris (99,99% populasi). Skor mendekati sempurna ini secara matematis benar — satu titik yang jauh terisolasi dari mayoritas data akan selalu menghasilkan silhouette mendekati 1 — namun secara bisnis perlu dibaca dengan hati-hati, karena K = 2 pada dasarnya hanya memisahkan satu anomali dari populasi utama, bukan menemukan dua segmen perilaku yang benar-benar seimbang.
Rekomendasi lanjutan: untuk kebutuhan segmentasi bisnis yang lebih actionable (misalnya membedakan pengguna berdasarkan tingkat engagement), tim data disarankan menelaah kembali hasil pada K = 3 (skor 0,930482) sebagai kandidat sekunder, atau menerapkan deteksi outlier terlebih dahulu sebelum melakukan clustering ulang pada populasi utama.
 BAB V
Rekomendasi Aksi & Strategi Bisnis (Business Insights)
5.1 Ringkasan Data & Profil Segmentasi Pengguna
Analisis dijalankan terhadap 3.961.105 records perilaku menonton pada platform VideoDotCom, mencakup 3 platform unik dan 15 kategori konten, dengan rata-rata durasi menonton keseluruhan 75,31 detik dan tingkat pengguna premium yang sangat rendah yaitu 0,06%. Model K-Means dengan K = 2 menghasilkan dua klaster berikut.
 
Gambar 2. Distribusi jumlah anggota per segmen klaster (cluster_distribution.png).
Cluster 0 — "Free-Tier Mobile Web Casual Viewer"
Jumlah anggota: 3.961.104 pengguna (100,0% dari total data).
Dimensi	Metrik	Nilai
Perilaku Menonton	Rata-rata durasi menonton	75,31 detik
Perilaku Menonton	Completion rate	23,60%
Perilaku Menonton	Autoplay rate	8,70%
Perilaku Menonton	Login rate	1,09%
Platform & Teknologi	Platform dominan	web-mobile
Platform & Teknologi	Top 3 platform	web-mobile 90,3% · web-desktop 9,5% · app-android 0,2%
Platform & Teknologi	Sistem operasi dominan	Android
Platform & Teknologi	Lokasi pemutaran	embed
Preferensi Konten	Kategori dominan	Entertainment
Preferensi Konten	Top 3 kategori	Entertainment 35,7% · News 34,7% · Sports 12,8%
Preferensi Konten	Tipe konten	VOD (Video on Demand)
Monetisasi	Premium rate	0,06%
Monetisasi	Ad exposure rate	97,84%
Cluster 1 — "Free-Tier Mobile Web Casual Viewer (Outlier)"
Jumlah anggota: 1 pengguna (0,0% dari total data) — terdeteksi sebagai anomali tunggal.
Dimensi	Metrik	Nilai
Perilaku Menonton	Rata-rata durasi menonton	120,00 detik
Perilaku Menonton	Completion rate	0,00%
Perilaku Menonton	Autoplay rate	0,00%
Perilaku Menonton	Login rate	0,00%
Platform & Teknologi	Platform dominan	web-mobile (100,0%)
Platform & Teknologi	Sistem operasi	TECHNIKA (perangkat Smart TV)
Platform & Teknologi	Lokasi pemutaran	direct
Preferensi Konten	Kategori dominan	Movies (100,0%)
Preferensi Konten	Tipe konten	VOD (Video on Demand)
Monetisasi	Premium rate	0,00%
Monetisasi	Ad exposure rate	100,00%
5.2 Tantangan Visualisasi: Profil Multi-Dimensi Antar-Klaster
Sesuai ketentuan modul teknis Topik 4, dilakukan komparasi karakteristik antar-klaster setelah data dinormalisasi melalui grafik multi_dimensional_cluster_profile.png berikut, yang menampilkan empat fitur perilaku menonton kunci pada skala relatifnya masing-masing.
 
Gambar 3. Heatmap profil karakteristik segmen pengguna lintas fitur play_duration, average_bitrate, total_bytes, dan buffer_duration (multi_dimensional_cluster_profile.png).
Heatmap ini memperjelas bahwa Cluster 1 (outlier) secara konsisten memiliki nilai lebih tinggi pada play_duration (120 detik vs 75,31 detik), average_bitrate (300.000 vs 170.774), dan total_bytes (4.500.000 vs 1.210.159) dibanding Cluster 0, namun nilai buffer_duration relatif serupa (9,26 vs 9,58 detik). Pola ini konsisten dengan profilnya sebagai pengguna Smart TV yang mengonsumsi konten resolusi lebih tinggi lewat koneksi direct, berbeda dari mayoritas pengguna mobile web yang mengonsumsi klip pendek berbitrate rendah.
5.3 Rekomendasi Aksi Korporat
Berdasarkan karakteristik kedua segmen di atas, berikut rekomendasi bisnis spesifik untuk tim produk dan pemasaran platform VideoDotCom.
4.	Advertising Optimization (Optimalisasi Iklan). Karena 97,84% anggota Cluster 0 terekspos iklan namun belum berlangganan premium, maksimalkan pendapatan dengan format iklan yang lebih menguntungkan seperti non-skippable short ads di awal pemutaran atau rewarded ads, khususnya pada konten kategori Entertainment dan News yang paling sering diakses.
5.	Login Incentive (Insentif Pendaftaran Akun). Tingkat login yang sangat rendah (1,09%) mempersulit personalisasi jangka panjang. Berikan insentif seperti fitur gratis Watch History dan Watchlist, atau pembatasan akses konten tertentu, agar pengguna bersedia membuat akun.
6.	Mobile Web UX & Push Notification Optimization. Dominasi akses via web-mobile (90,3%) menunjukkan preferensi pengguna terhadap akses cepat tanpa instalasi aplikasi. Optimalkan kecepatan muat halaman mobile web dan kembangkan push notification berbasis browser untuk memicu kunjungan ulang secara berkala.
7.	Content Curation (Kurasi Konten Ringan / Short-Form). Dengan rata-rata durasi tontonan hanya 75,31 detik dan completion rate 23,60%, fokuskan penempatan konten short-form pada kategori Entertainment dan News di posisi strategis halaman utama, serta evaluasi ulang thumbnail dan judul untuk menaikkan completion rate.
8.	Freemium & Bandwidth Strategy untuk Segmen Niche. Meski hanya berjumlah 1 pengguna, karakteristik Cluster 1 (Smart TV, bitrate tinggi, akses direct) mengindikasikan potensi segmen premium niche (konsumsi film berkualitas tinggi di layar besar). Segmen ini layak dipantau sebagai sinyal awal peluang ekspansi ke perangkat Smart TV, dengan alokasi bandwidth/CDN yang memperhitungkan kebutuhan bitrate lebih tinggi.
 BAB VI
Dokumentasi Digital Repositori (GitHub Showcase)
Seluruh arsitektur kode pemrograman, konfigurasi sistem, serta dokumen README.md telah disinkronisasikan ke dalam repositori publik GitHub demi transparansi rekayasa data dan keterulangan (reproducibility) eksperimen.
6.1 Tautan Repositori
•	Repositori GitHub: https://github.com/RizkiHasan/lab_bigdata_rizkihasan
•	Direktori output lokal: D:\lab_bigdata_rizkihasan\output\
6.2 Berkas Output & Artefak Visual
Nama Berkas	Deskripsi
silhouette_vs_k.png	Grafik Elbow Method / Silhouette Score lintas nilai K
cluster_distribution.png	Grafik distribusi jumlah anggota per segmen klaster
multi_dimensional_cluster_profile.png	Heatmap profil karakteristik multi-dimensi antar-klaster
silhouette_scores.csv	Tabel mentah skor Silhouette untuk K = 2 s.d. 6
business_summary.txt	Ringkasan laporan insight bisnis dalam format teks
6.3 Berkas Kode Utama
•	Skrip Preprocessing: src/preprocessing.py — schema casting, data cleaning, VectorAssembler, StandardScaler.
•	Skrip Clustering: src/clustering.py — eksperimen K-Means K=2..6, evaluasi ClusteringEvaluator, ekspor artefak visual dan business_summary.txt.
6.4 Reproduktifitas
README.md pada repositori menjelaskan langkah menjalankan ulang kluster Docker (docker-compose up), proses ingestion ke HDFS, serta perintah spark-submit untuk menjalankan kedua skrip di atas secara berurutan, sehingga hasil laporan ini dapat direproduksi penuh oleh pihak lain yang memiliki akses ke dataset videodotcom_big.csv.










DAFTAR PUSTAKA
Apache Hadoop. (2025). Apache Hadoop Documentation. https://hadoop.apache.org/docs/
Apache Spark. (2025). Apache Spark Documentation. https://spark.apache.org/docs/latest/
Arthur, D., & Vassilvitskii, S. (2007). k-means++: The Advantages of Careful Seeding. Proceedings of the Eighteenth Annual ACM-SIAM Symposium on Discrete Algorithms (SODA), 1027–1035.
Bishop, C. M. (2006). Pattern Recognition and Machine Learning. Springer.
Han, J., Kamber, M., & Pei, J. (2012). Data Mining: Concepts and Techniques (3rd ed.). Morgan Kaufmann.
James, G., Witten, D., Hastie, T., & Tibshirani, R. (2021). An Introduction to Statistical Learning (2nd ed.). Springer.
Kaufman, L., & Rousseeuw, P. J. (1990). Finding Groups in Data: An Introduction to Cluster Analysis. John Wiley & Sons.
MacQueen, J. (1967). Some Methods for Classification and Analysis of Multivariate Observations. Proceedings of the Fifth Berkeley Symposium on Mathematical Statistics and Probability, 281–297.
Pedregosa, F., Varoquaux, G., Gramfort, A., Michel, V., Thirion, B., Grisel, O., Blondel, M., Prettenhofer, P., Weiss, R., Dubourg, V., Vanderplas, J., Passos, A., Cournapeau, D., Brucher, M., Perrot, M., & Duchesnay, É. (2011). Scikit-learn: Machine Learning in Python. Journal of Machine Learning Research, 12, 2825–2830.
Rousseeuw, P. J. (1987). Silhouettes: A Graphical Aid to the Interpretation and Validation of Cluster Analysis. Journal of Computational and Applied Mathematics, 20, 53–65.
Shvachko, K., Kuang, H., Radia, S., & Chansler, R. (2010). The Hadoop Distributed File System. Proceedings of the 2010 IEEE 26th Symposium on Mass Storage Systems and Technologies, 1–10.
Tan, P.-N., Steinbach, M., Karpatne, A., & Kumar, V. (2019). Introduction to Data Mining (2nd ed.). Pearson.
Zaharia, M., Chowdhury, M., Franklin, M. J., Shenker, S., & Stoica, I. (2010). Spark: Cluster Computing with Working Sets. Proceedings of the 2nd USENIX Conference on Hot Topics in Cloud Computing.
Géron, A. (2022). Hands-On Machine Learning with Scikit-Learn, Keras, and TensorFlow (3rd ed.). O'Reilly Media. Provost, F., & Fawcett, T. (2013). Data Science for Business. O'Reilly Media. 
Hastie, T., Tibshirani, R., & Friedman, J. (2009). The Elements of Statistical Learning (2nd ed.). Springer. 


