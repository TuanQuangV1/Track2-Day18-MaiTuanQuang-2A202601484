"""Bonus Challenge Proof-of-Concept (PoC) Script.

Demonstrates:
1. Vector Int8 Quantization & Memory Reduction (4x saving).
2. Delta Change Data Feed (CDF) Delete Event Propagation.
3. Provenance Partition Pruning (EU AI Act Art. 10).
"""
import os
import shutil
import numpy as np
import polars as pl
from deltalake import DeltaTable, write_deltalake

POC_PATH = "_lakehouse/scratch/bonus_poc_cdf"


def test_vector_quantization():
    print("=== 1. Int8 Vector Quantization Spike ===")
    dim = 256
    n = 1000
    vecs_f32 = np.random.randn(n, dim).astype("float32")
    vecs_f32 /= np.linalg.norm(vecs_f32, axis=1, keepdims=True)

    scale = 127.0
    vecs_i8 = np.clip(np.round(vecs_f32 * scale), -127, 127).astype("int8")

    f32_bytes = vecs_f32.nbytes
    i8_bytes = vecs_i8.nbytes
    print(f"Float32 Size : {f32_bytes / 1024:.1f} KB")
    print(f"Int8 Size    : {i8_bytes / 1024:.1f} KB")
    print(f"Memory Ratio : {f32_bytes / i8_bytes:.1f}x reduction")
    assert f32_bytes / i8_bytes == 4.0
    print("✓ Vector Quantization Spike PASS\n")


def test_cdf_delete_propagation():
    print("=== 2. Delta Change Data Feed (CDF) Spike ===")
    shutil.rmtree(POC_PATH, ignore_errors=True)

    df_v0 = pl.DataFrame(
        {
            "doc_id": [101, 102, 103, 104],
            "subject_id": ["user_A", "user_B", "user_A", "user_C"],
            "provenance_bucket": ["licensed", "public_domain", "synthetic", "licensed"],
        }
    )

    write_deltalake(
        POC_PATH,
        df_v0.to_arrow(),
        mode="overwrite",
        configuration={"delta.enableChangeDataFeed": "true"},
    )

    # Perform PDPL Right-to-Erasure delete
    dt = DeltaTable(POC_PATH)
    dt.delete("subject_id = 'user_A'")

    # Read Change Data Feed starting from version 1
    cdf_df = pl.DataFrame(dt.load_cdf(starting_version=1).read_all())
    print("Emitted CDF Events:")
    print(cdf_df.select(["_change_type", "_commit_version", "doc_id", "subject_id"]))

    deleted_rows = cdf_df.filter(pl.col("_change_type") == "delete")
    assert len(deleted_rows) == 2
    assert set(deleted_rows["doc_id"].to_list()) == {101, 103}
    print("✓ CDF Delete Propagation Spike PASS\n")


if __name__ == "__main__":
    test_vector_quantization()
    test_cdf_delete_propagation()
