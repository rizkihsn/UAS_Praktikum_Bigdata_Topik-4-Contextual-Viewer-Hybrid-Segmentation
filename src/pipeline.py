# ======================================================================
# PROJECT: Contextual Viewer Hybrid Segmentation (Topik 4 - UAS)
# FILE: src/pipeline.py (Merged Single-File Pipeline)
# ROLE: Senior Big Data Architect
# AUTHOR: Rizki Hasan
# ======================================================================

import os
import sys
import subprocess
import shutil

# ── Konfigurasi Central ──────────────────────────────────────────────────
SEED = 42
MAX_ITER = 10
TOL = 0.001
K_CANDIDATES = [2, 3, 4, 5, 6]

HDFS_INPUT = "hdfs://namenode:9000/user/bigdata/dataset/videodotcom_big.csv"
HDFS_PREPROCESSED = "hdfs://namenode:9000/user/bigdata/output/preprocessed_data.parquet"
HDFS_CLUSTER_TEMP = "hdfs://namenode:9000/tmp/cluster_result_csv"

LOCAL_SILO_CSV = "output/silhouette_scores.csv"
LOCAL_RESULT_CSV = "output/cluster_result.csv"
LOCAL_SILO_PNG = "output/silhouette_vs_k.png"
LOCAL_DIST_PNG = "output/cluster_distribution.png"
LOCAL_PROFILE_PNG = "output/multi_dimensional_cluster_profile.png"
LOCAL_SUMMARY_TXT = "output/business_summary.txt"


# ── Bagian 1: Pipeline Apache Spark (Dijalankan di Container) ──────────────
def run_spark_pipeline():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, count, when
    from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
    from pyspark.ml import Pipeline
    from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, LongType, DoubleType, TimestampType
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.evaluation import ClusteringEvaluator
    import csv

    print("\n" + "=" * 80)
    print("           MEMULAI DATA ENGINEERING & CLUSTERING (APACHE SPARK)")
    print("=" * 80)

    # 1. Inisialisasi Spark Session
    print("[INFO] Memulai Spark Session...")
    spark = SparkSession.builder \
        .appName("Contextual Viewer Hybrid Segmentation - Spark Pipeline") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.default.parallelism", "100") \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.memory.storageFraction", "0.5") \
        .getOrCreate()

    # 2. Skema Struktur Eksplisit
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

    # 3. Data Ingestion & Preprocessing
    print(f"[INFO] Membaca data mentah dari HDFS: {HDFS_INPUT}")
    df = spark.read.csv(HDFS_INPUT, header=True, schema=videodotcom_schema)

    # Filtering Missing Value
    feature_candidates = [
        "is_login", "platform", "playback_location", "os_name", "completed", "has_ad", 
        "is_premium", "play_duration", "autoplay", "content_type", "category_name",
        "average_bitrate", "total_bytes", "buffer_duration"
    ]
    df_clean = df.dropna(subset=feature_candidates)

    # Boolean Casting ke Integer
    boolean_cols = ["is_login", "completed", "has_ad", "is_premium", "autoplay"]
    for col_name in boolean_cols:
        df_clean = df_clean.withColumn(col_name + "_num", col(col_name).cast("integer"))

    # String Indexer & One-Hot Encoding
    categorical_cols = ["platform", "playback_location", "os_name", "content_type", "category_name"]
    indexers = [StringIndexer(inputCol=c, outputCol=c + "_index", handleInvalid="keep") for c in categorical_cols]
    encoder = OneHotEncoder(inputCols=[c + "_index" for c in categorical_cols], outputCols=[c + "_vec" for c in categorical_cols])

    # VectorAssembler & StandardScaler (Normalisasi wajib UAS)
    numeric_features = ["play_duration", "average_bitrate", "total_bytes", "buffer_duration"]
    boolean_features_num = [c + "_num" for c in boolean_cols]
    assembler_input_cols = boolean_features_num + numeric_features + [c + "_vec" for c in categorical_cols]
    
    assembler = VectorAssembler(inputCols=assembler_input_cols, outputCol="features", handleInvalid="skip")
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=False)

    pipeline = Pipeline(stages=indexers + [encoder, assembler, scaler])
    pipeline_model = pipeline.fit(df_clean)
    df_preprocessed = pipeline_model.transform(df_clean)

    # Simpan hasil Preprocessing ke HDFS (format Parquet)
    print(f"[INFO] Menyimpan preprocessed data ke Parquet HDFS: {HDFS_PREPROCESSED}")
    df_preprocessed.write.mode("overwrite").parquet(HDFS_PREPROCESSED)

    # 4. K-Means Clustering terdistribusi
    print("[INFO] Membaca data preprocessed dari Parquet...")
    df_cluster = spark.read.parquet(HDFS_PREPROCESSED)

    # Mencegah Out Of Memory (OOM): Latih Model pada 5% sample data
    print("[INFO] Melakukan sampling 5% data untuk mencegah JVM memory OOM...")
    df_train = df_cluster.sample(fraction=0.05, seed=SEED)
    df_train.cache()

    scores = []
    evaluator = ClusteringEvaluator(featuresCol="scaled_features", metricName="silhouette", distanceMeasure="squaredEuclidean")

    print("[INFO] Melakukan iterasi pencarian K optimal (K=2 s/d K=6)...")
    for k in K_CANDIDATES:
        kmeans = KMeans(featuresCol="scaled_features", predictionCol="cluster", k=k, seed=SEED, maxIter=MAX_ITER, tol=TOL)
        model = kmeans.fit(df_train)
        predictions = model.transform(df_train)
        silhouette = evaluator.evaluate(predictions)
        scores.append((k, silhouette, model))
        print(f"   -> K = {k} | Silhouette Score = {silhouette:.6f}")

    # Memilih model K optimal
    optimal_k, best_score, best_model = max(scores, key=lambda x: x[1])
    print(f"[INFO] K Optimal Terpilih: K={optimal_k} (Silhouette Score: {best_score:.6f})")

    # Menyimpan Silhouette Score ke CSV lokal master container
    os.makedirs("/output", exist_ok=True)
    with open("/output/silhouette_scores.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["K", "Silhouette"])
        for k, sil, _ in scores:
            writer.writerow([k, round(sil, 6)])
    print("[SUCCESS] Silhouette scores disimpan ke /output/silhouette_scores.csv")

    # Terapkan model final pada seluruh 3.9 juta baris
    print("[INFO] Menerapkan model terbaik ke seluruh baris data...")
    df_final = best_model.transform(df_cluster)

    # Seleksi kolom non-vector untuk output CSV
    cols_to_keep = [
        c for c in df_final.columns
        if not c.endswith("_index")
        and not c.endswith("_vec")
        and c not in ["features", "scaled_features"]
    ]
    df_out = df_final.select(*cols_to_keep)

    # Simpan hasil clustering ke HDFS
    print(f"[INFO] Menulis hasil klaster ke HDFS temp: {HDFS_CLUSTER_TEMP}")
    df_out.write.option("header", "true").mode("overwrite").csv(HDFS_CLUSTER_TEMP)

    spark.stop()
    print("[SUCCESS] Seluruh modul Apache Spark selesai dijalankan.")


# ── Bagian 2: Modul Visualisasi & Analisis Bisnis (Lokal Windows) ───────────
def run_local_pipeline():
    import pandas as pd
    import matplotlib.pyplot as plt
    import seaborn as sns
    from datetime import datetime

    print("\n" + "=" * 80)
    print("           MEMULAI VISUALISASI & ANALISIS BISNIS (LOKAL PYTHON)")
    print("=" * 80)

    # Set Seaborn theme
    sns.set_theme(style="whitegrid")

    # 1. Pemuatan Data
    print(f"[INFO] Membaca data Silhouette: {LOCAL_SILO_CSV}")
    scores_df = pd.read_csv(LOCAL_SILO_CSV)

    print(f"[INFO] Membaca data hasil cluster: {LOCAL_RESULT_CSV}...")
    results_df = pd.read_csv(LOCAL_RESULT_CSV, escapechar='\\', low_memory=False)

    # Bersihkan header duplikat
    if 'cluster' in results_df.columns:
        results_df = results_df[results_df['cluster'] != 'cluster']
        results_df['cluster'] = pd.to_numeric(results_df['cluster'], errors='coerce')

    results_df = results_df.dropna(subset=['cluster'])
    results_df['cluster'] = results_df['cluster'].astype(int)

    # Konversi kolom numerik & rate boolean (_num) ke tipe data numerik yang benar
    numeric_cols = [
        "play_duration", "average_bitrate", "total_bytes", "buffer_duration",
        "is_login_num", "completed_num", "has_ad_num", "autoplay_num", "is_premium_num"
    ]
    for c in numeric_cols:
        if c in results_df.columns:
            results_df[c] = pd.to_numeric(results_df[c], errors='coerce')

    # 2. Visualisasi 1: Silhouette vs K (Elbow Method)
    print("[INFO] Membuat visualisasi Silhouette Score vs K...")
    plt.figure(figsize=(9, 5.5))
    plt.plot(
        scores_df['K'], scores_df['Silhouette'], 
        marker='o', linestyle='-', linewidth=2.5, color='#4A148C', markersize=8, label='Silhouette Score'
    )
    best_row = scores_df.loc[scores_df['Silhouette'].idxmax()]
    best_k = int(best_row['K'])
    best_score = best_row['Silhouette']
    plt.annotate(
        f"Nilai K Terbaik: {best_k}\nScore: {best_score:.4f}",
        xy=(best_k, best_score),
        xytext=(best_k + 0.2, best_score - 0.03),
        arrowprops=dict(facecolor='#D32F2F', shrink=0.08, width=1.5, headwidth=7),
        fontsize=10.5, fontweight='bold',
        bbox=dict(boxstyle="round,pad=0.3", fc="#FFEB3B", ec="#FBC02D", alpha=0.8)
    )
    plt.title("Analisis Elbow Method Menggunakan Silhouette Score", fontweight='bold', pad=15)
    plt.xlabel("Nilai Cluster (K)")
    plt.ylabel("Silhouette Score")
    plt.xticks(scores_df['K'])
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.ylim(scores_df['Silhouette'].min() - 0.05, scores_df['Silhouette'].max() + 0.05)
    plt.tight_layout()
    plt.savefig(LOCAL_SILO_PNG, dpi=300)
    plt.close()
    print(f"[SUCCESS] Grafik disimpan ke: {LOCAL_SILO_PNG}")

    # 3. Visualisasi 2: Distribusi Anggota
    print("[INFO] Membuat visualisasi Distribusi Cluster...")
    plt.figure(figsize=(9, 5.5))
    dist_df = results_df['cluster'].value_counts().sort_index().reset_index()
    dist_df.columns = ['cluster', 'count']
    ax = sns.barplot(x='cluster', y='count', data=dist_df, palette='viridis', edgecolor='black', linewidth=1)
    for p in ax.patches:
        height = p.get_height()
        ax.annotate(
            f"{int(height):,}",
            xy=(p.get_x() + p.get_width() / 2, height),
            xytext=(0, 3), textcoords="offset points",
            ha='center', va='bottom', fontsize=10, fontweight='bold'
        )
    plt.title("Distribusi Jumlah Anggota per Segmen (Cluster)", fontweight='bold', pad=15)
    plt.xlabel("Segmen Cluster")
    plt.ylabel("Jumlah Pengguna (Baris)")
    plt.grid(axis='y', linestyle='--', alpha=0.4)
    plt.tight_layout()
    plt.savefig(LOCAL_DIST_PNG, dpi=300)
    plt.close()
    print(f"[SUCCESS] Grafik disimpan ke: {LOCAL_DIST_PNG}")

    # 4. Visualisasi 3: Heatmap Profil (Wajib Modul 4)
    print("[INFO] Membuat visualisasi Profil Heatmap...")
    numeric_candidates = ["play_duration", "average_bitrate", "total_bytes", "buffer_duration"]
    available_numerics = [c for c in numeric_candidates if c in results_df.columns]
    if available_numerics:
        profile_df = results_df.groupby('cluster')[available_numerics].mean()
        profile_norm = (profile_df - profile_df.min()) / (profile_df.max() - profile_df.min() + 1e-9)
        plt.figure(figsize=(10, 6.5))
        sns.heatmap(
            profile_norm, annot=profile_df, fmt=".2f", cmap="YlGnBu", cbar=True,
            cbar_kws={'label': 'Skala Relatif Fitur (0 - 1)'}, linewidths=.5,
            annot_kws={"size": 11, "weight": "bold"}
        )
        plt.title("Profil Karakteristik Segmen Pengguna (Heatmap)", fontweight='bold', pad=15)
        plt.xlabel("Fitur Perilaku Menonton")
        plt.ylabel("Cluster")
        plt.tight_layout()
        plt.savefig(LOCAL_PROFILE_PNG, dpi=300)
        plt.close()
        print(f"[SUCCESS] Grafik Heatmap disimpan ke: {LOCAL_PROFILE_PNG}")

    # 5. Analisis Bisnis (Business Insight)
    print("[INFO] Membuat analisis ringkasan dan strategi bisnis...")
    
    def get_dominant_value(series):
        return "N/A" if series.empty else series.mode().iloc[0]

    def get_top_n_distribution(series, n=3):
        dist = series.value_counts(normalize=True).head(n)
        return ", ".join([f"{val} ({pct:.1%})" for val, pct in dist.items()])

    def analyze_cluster(df_clus, clus_id):
        c_data = df_clus[df_clus["cluster"] == clus_id]
        total_all = len(df_clus)
        total_cluster = len(c_data)
        pct = (total_cluster / total_all) * 100

        analysis = {"cluster_id": clus_id, "jumlah_anggota": total_cluster, "persentase": round(pct, 2)}
        analysis["avg_play_duration"] = round(c_data["play_duration"].mean(), 2) if "play_duration" in c_data.columns else 0
        analysis["platform_dominan"] = get_dominant_value(c_data["platform"]) if "platform" in c_data.columns else "N/A"
        analysis["platform_top3"] = get_top_n_distribution(c_data["platform"]) if "platform" in c_data.columns else "N/A"
        analysis["kategori_dominan"] = get_dominant_value(c_data["category_name"]) if "category_name" in c_data.columns else "N/A"
        analysis["kategori_top3"] = get_top_n_distribution(c_data["category_name"]) if "category_name" in c_data.columns else "N/A"
        analysis["os_dominan"] = get_dominant_value(c_data["os_name"]) if "os_name" in c_data.columns else "N/A"
        analysis["content_type_dominan"] = get_dominant_value(c_data["content_type"]) if "content_type" in c_data.columns else "N/A"
        analysis["playback_location_dominan"] = get_dominant_value(c_data["playback_location"]) if "playback_location" in c_data.columns else "N/A"

        rate_mappings = [
            ("is_premium_num", "premium_rate"),
            ("completed_num", "completion_rate"),
            ("autoplay_num", "autoplay_rate"),
            ("is_login_num", "login_rate"),
            ("has_ad_num", "has_ad_rate")
        ]
        for col_name, key in rate_mappings:
            if col_name in c_data.columns:
                analysis[key] = round(c_data[col_name].mean() * 100, 2)
            else:
                analysis[key] = 0

        return analysis

    def generate_persona_label(analysis):
        parts = []
        if analysis["premium_rate"] > 50: parts.append("Premium")
        elif analysis["premium_rate"] > 15: parts.append("Semi-Premium")
        else: parts.append("Free-Tier")

        platform = analysis.get("platform_dominan", "").lower()
        if "android" in platform and "app" in platform: parts.append("Android App")
        elif "android" in platform: parts.append("Android")
        elif "ios" in platform: parts.append("iOS")
        elif "mobile" in platform: parts.append("Mobile Web")
        elif "desktop" in platform: parts.append("Desktop")
        elif "tv" in platform: parts.append("Smart TV")
        else: parts.append(analysis.get("platform_dominan", "Unknown"))

        avg_dur = analysis.get("avg_play_duration", 0)
        if avg_dur > 1800: parts.append("Binge Watcher")
        elif avg_dur > 600: parts.append("Movie Lover")
        elif avg_dur > 120: parts.append("Engaged Viewer")
        elif avg_dur > 30: parts.append("Casual Viewer")
        else: parts.append("Quick Browser")
        return " ".join(parts)

    def generate_recommendation(analysis):
        recs = []
        premium_rate = analysis.get("premium_rate", 0)
        avg_dur = analysis.get("avg_play_duration", 0)
        completion_rate = analysis.get("completion_rate", 0)
        autoplay_rate = analysis.get("autoplay_rate", 0)
        has_ad_rate = analysis.get("has_ad_rate", 0)
        login_rate = analysis.get("login_rate", 0)
        kategori = analysis.get("kategori_dominan", "")
        platform = analysis.get("platform_dominan", "")

        if premium_rate > 50:
            recs.append("RETENTION STRATEGY: Segmen didominasi premium. Pertahankan dengan benefit loyalitas.")
        elif premium_rate < 10 and avg_dur > 120:
            recs.append("PREMIUM UPSELLING: Pengguna aktif non-premium. Beri trial 7 hari gratis.")
        else:
            recs.append("FREEMIUM OPTIMIZATION: Optimalkan UX video player gratis agar lebih interaktif.")

        if completion_rate > 50:
            recs.append(f"PERSONALIZED RECOMMENDATION: Dorong konten '{kategori}' lebih banyak di home page.")
        elif completion_rate < 20:
            recs.append("CONTENT OPTIMIZATION: Completion rate rendah, kurangi durasi video atau kembangkan short-form.")
        else:
            recs.append("CONTENT CURATION: Tingkatkan kurasi dan kualitas tayangan utama.")

        if has_ad_rate > 70 and premium_rate < 20:
            recs.append("ADVERTISING OPTIMIZATION: Maksimalkan revenue iklan dengan format rewarded/non-skippable.")

        if "mobile" in platform.lower() or "android" in platform.lower():
            recs.append("PUSH NOTIFICATION: Gunakan push notification terjadwal sesuai kategori kesukaan.")
        elif "tv" in platform.lower():
            recs.append("CROSS SELLING: Tawarkan family package multi-device.")

        if autoplay_rate > 60:
            recs.append("AUTOPLAY ENGAGEMENT: Dorong total watch time dengan autoplay playlist beruntun.")

        if login_rate < 30:
            recs.append("LOGIN INCENTIVE: Batasi riwayat playlist jika belum login sebagai pendorong pendaftaran.")

        return recs

    clusters = sorted(results_df["cluster"].unique())
    lines = []
    lines.append("=" * 72)
    lines.append("  CONTEXTUAL VIEWER HYBRID SEGMENTATION")
    lines.append("  Business Insight Report - Rizki Hasan (UAS Big Data)")
    lines.append("=" * 72)
    lines.append(f"  Tanggal Analisis : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"  Total Data       : {len(results_df):,} records")
    lines.append(f"  Jumlah Cluster   : {len(clusters)} segmen")
    lines.append("=" * 72)

    for c_id in clusters:
        analysis = analyze_cluster(results_df, c_id)
        persona = generate_persona_label(analysis)
        recs = generate_recommendation(analysis)

        lines.append("=" * 72)
        lines.append(f"  CLUSTER {c_id}: \"{persona}\"")
        lines.append(f"  Jumlah Anggota: {analysis['jumlah_anggota']:,} ({analysis['persentase']}% dari total)")
        lines.append("=" * 72)
        lines.append(f"\n  [PERILAKU MENONTON]")
        lines.append(f"    Rata-rata Durasi Menonton : {analysis['avg_play_duration']:.2f} detik")
        lines.append(f"    Completion Rate           : {analysis['completion_rate']:.2f}%")
        lines.append(f"    Autoplay Rate             : {analysis['autoplay_rate']:.2f}%")
        lines.append(f"    Login Rate                : {analysis['login_rate']:.2f}%")
        lines.append(f"\n  [PLATFORM & TEKNOLOGI]")
        lines.append(f"    Platform Dominan          : {analysis['platform_dominan']}")
        lines.append(f"    Top 3 Platform            : {analysis['platform_top3']}")
        lines.append(f"    Operating System          : {analysis['os_dominan']}")
        lines.append(f"    Playback Location         : {analysis['playback_location_dominan']}")
        lines.append(f"\n  [PREFERENSI KONTEN]")
        lines.append(f"    Kategori Dominan          : {analysis['kategori_dominan']}")
        lines.append(f"    Top 3 Kategori            : {analysis['kategori_top3']}")
        lines.append(f"\n  [MONETISASI]")
        lines.append(f"    Premium Rate              : {analysis['premium_rate']:.2f}%")
        lines.append(f"    Ad Exposure Rate          : {analysis['has_ad_rate']:.2f}%")
        lines.append(f"\n  [REKOMENDASI BISNIS]")
        for idx, r in enumerate(recs, 1):
            lines.append(f"    {idx}. {r}")
        lines.append("")

    report_text = "\n".join(lines)
    with open(LOCAL_SUMMARY_TXT, "w", encoding="utf-8") as f:
        f.write(report_text)
    print(report_text)
    print(f"\n[SUCCESS] Analisis selesai. Laporan disimpan ke: {LOCAL_SUMMARY_TXT}")


# ── Bagian 3: Main Orchestrator (Orkestrasi Docker + Lokal) ────────────────
def check_hdfs_exists(path):
    """Cek apakah sebuah path di HDFS sudah ada via namenode container."""
    result = subprocess.run(
        ["docker", "exec", "namenode_rizkihsn", "hdfs", "dfs", "-test", "-e", path],
        capture_output=True
    )
    return result.returncode == 0

def check_container_file_exists(container, path):
    """Cek apakah file ada di dalam sebuah container Docker."""
    result = subprocess.run(
        ["docker", "exec", container, "test", "-f", path],
        capture_output=True
    )
    return result.returncode == 0

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--spark":
        # Dijalankan di dalam Spark Cluster
        run_spark_pipeline()
    else:
        # Dijalankan di Windows host
        print("=" * 80)
        print("  CONTEXTUAL VIEWER HYBRID SEGMENTATION (TOPIK 4)")
        print("  Merged Single-File Pipeline - Rizki Hasan (UAS Big Data)")
        print("=" * 80)

        # 1. Pastikan folder output ada
        os.makedirs("output", exist_ok=True)

        # 2. Cek apakah data hasil clustering sudah ada di HDFS
        hdfs_result_exists   = check_hdfs_exists(HDFS_CLUSTER_TEMP + "/_SUCCESS")
        silo_csv_exists      = check_container_file_exists("spark-master", "/output/silhouette_scores.csv")

        if hdfs_result_exists and silo_csv_exists:
            print("\n[INFO] Data hasil clustering SUDAH ADA di HDFS dari run sebelumnya.")
            print("[INFO] Melewati tahap Spark (Preprocessing + Clustering) — langsung ke tahap lokal.")
        else:
            # 2a. Salin skrip tunggal ke container spark-master
            print("\n[INFO] Data belum ada. Menyalin skrip ke container spark-master...")
            subprocess.run(["docker", "exec", "spark-master", "mkdir", "-p", "/src"])
            result_cp = subprocess.run(["docker", "cp", "src/pipeline.py", "spark-master:/src/pipeline.py"])
            if result_cp.returncode != 0:
                print("[ERROR] Gagal menyalin skrip ke Docker. Pastikan Docker Desktop sudah menyala.")
                sys.exit(result_cp.returncode)

            # 2b. Jalankan Spark (Preprocessing + Clustering)
            print("\n[INFO] Menjalankan Preprocessing & Clustering di Apache Spark...")
            result_spark = subprocess.run([
                "docker", "exec", "-i", "spark-master",
                "/spark/bin/spark-submit",
                "--master", "spark://spark-master:7077",
                "/src/pipeline.py", "--spark"
            ])
            if result_spark.returncode != 0:
                print("[ERROR] Pekerjaan Spark gagal!")
                # Cek sekali lagi — mungkin data sudah berhasil ditulis sebelum error exit
                if check_hdfs_exists(HDFS_CLUSTER_TEMP + "/_SUCCESS"):
                    print("[INFO] Namun data di HDFS terdeteksi — melanjutkan ke tahap berikutnya...")
                else:
                    sys.exit(result_spark.returncode)

        # 3. Gabungkan hasil partisi dari HDFS menggunakan getmerge di namenode
        print("\n[INFO] Menggabungkan berkas partisi hasil klaster di HDFS (getmerge)...")
        result_merge = subprocess.run([
            "docker", "exec", "-i", "namenode_rizkihsn",
            "hdfs", "dfs", "-getmerge",
            HDFS_CLUSTER_TEMP, "/tmp/cluster_result.csv"
        ])
        if result_merge.returncode != 0:
            print("[ERROR] Gagal melakukan getmerge dari HDFS.")
            sys.exit(result_merge.returncode)

        # 4. Salin hasil akhir ke Windows lokal
        print("\n[INFO] Menyalin hasil keluaran dari Docker ke Windows lokal...")
        subprocess.run(["docker", "cp", "namenode_rizkihsn:/tmp/cluster_result.csv", LOCAL_RESULT_CSV])
        subprocess.run(["docker", "cp", "spark-master:/output/silhouette_scores.csv", LOCAL_SILO_CSV])

        # Validasi file berhasil disalin
        if not os.path.exists(LOCAL_RESULT_CSV):
            print(f"[ERROR] File {LOCAL_RESULT_CSV} tidak ditemukan setelah docker cp.")
            sys.exit(1)
        if not os.path.exists(LOCAL_SILO_CSV):
            print(f"[ERROR] File {LOCAL_SILO_CSV} tidak ditemukan setelah docker cp.")
            sys.exit(1)

        print(f"[SUCCESS] cluster_result.csv ({os.path.getsize(LOCAL_RESULT_CSV) // (1024*1024)} MB) tersalin.")
        print(f"[SUCCESS] silhouette_scores.csv tersalin.")

        # 5. Jalankan Visualisasi dan Insight lokal
        run_local_pipeline()

        print("\n" + "=" * 80)
        print("  SELESAI! Seluruh pipeline sukses berjalan dalam satu file pipeline.py")
        print("  Output gambar & dokumen teks tersedia lengkap di folder: output/")
        print("=" * 80)
