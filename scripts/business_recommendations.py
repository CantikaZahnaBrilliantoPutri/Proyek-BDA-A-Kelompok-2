"""
BUSINESS RECOMMENDATION ENGINE
=============================

Input  : /app/data/model_output.csv
Output : Rekomendasi bisnis (restock, prioritas, insight)
"""

import pandas as pd

# ─────────────────────────────────────────────
# 1. LOAD DATA
# ─────────────────────────────────────────────
df = pd.read_csv("/app/data/model_output.csv")

print("Total data:", len(df))

# ─────────────────────────────────────────────
# 2. RULE-BASED DECISION
# ─────────────────────────────────────────────

def get_priority(row):
    if row["predicted_stockout"] == 1:
        if row["predicted_rop"] > row["stock_on_hand"]:
            return "HIGH"
        else:
            return "MEDIUM"
    else:
        return "LOW"

df["priority"] = df.apply(get_priority, axis=1)

# ─────────────────────────────────────────────
# 3. HITUNG REKOMENDASI ORDER
# ─────────────────────────────────────────────

df["recommended_order"] = (
    df["predicted_rop"] - df["stock_on_hand"]
)

# kalau negatif → tidak perlu order
df["recommended_order"] = df["recommended_order"].apply(
    lambda x: max(x, 0)
)

# ─────────────────────────────────────────────
# 4. BUAT INSIGHT NARATIF
# ─────────────────────────────────────────────

def generate_insight(row):
    if row["priority"] == "HIGH":
        return (
            "Produk ini berisiko tinggi mengalami stockout. "
            "Disarankan segera melakukan restock untuk menghindari kehilangan penjualan."
        )
    elif row["priority"] == "MEDIUM":
        return (
            "Produk menunjukkan potensi stockout dalam waktu dekat. "
            "Monitoring dan restock dalam waktu dekat sangat disarankan."
        )
    else:
        return (
            "Stok produk masih dalam kondisi aman. "
            "Belum diperlukan tindakan restock dalam waktu dekat."
        )

df["insight"] = df.apply(generate_insight, axis=1)

# ─────────────────────────────────────────────
# 5. RINGKASAN BISNIS
# ─────────────────────────────────────────────

total_high = len(df[df["priority"] == "HIGH"])
total_med  = len(df[df["priority"] == "MEDIUM"])
total_low  = len(df[df["priority"] == "LOW"])

print("\n=== BUSINESS SUMMARY ===")
print(f"Produk PRIORITAS TINGGI : {total_high}")
print(f"Produk PRIORITAS SEDANG : {total_med}")
print(f"Produk PRIORITAS RENDAH: {total_low}")

# ─────────────────────────────────────────────
# 6. SIMPAN HASIL
# ─────────────────────────────────────────────

output_path = "/app/data/business_recommendations.csv"
df.to_csv(output_path, index=False)

print("\nRekomendasi berhasil disimpan ke:")
print(output_path)

# ─────────────────────────────────────────────
# 7. SAMPLE OUTPUT
# ─────────────────────────────────────────────

print("\n=== SAMPLE RECOMMENDATIONS ===")
print(df[[
    "stock_on_hand",
    "predicted_rop",
    "priority",
    "recommended_order",
    "insight"
]].head(10))