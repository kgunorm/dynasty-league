"""Step 1: Load source files and unify into a single DataFrame."""
import uuid
import pandas as pd


def load_all_sources(config: dict) -> pd.DataFrame:
    """Read each configured source, apply field mappings, and concatenate."""
    frames = []
    for idx, source in enumerate(config["sources"]):
        path = source["path"]
        system_id = source["system_id"]
        field_map = source["field_map"]  # canonical → source column name
        # Invert map: source col → canonical col
        rename_map = {v: k for k, v in field_map.items()}

        if path.endswith(".xlsx") or path.endswith(".xls"):
            sheet = source.get("sheet_name", 0)
            df = pd.read_excel(path, sheet_name=sheet)
        else:
            df = pd.read_csv(path)

        # Keep only columns that appear in the field_map values
        cols_needed = [c for c in field_map.values() if c in df.columns]
        df = df[cols_needed].copy()
        df.rename(columns=rename_map, inplace=True)

        # Ensure canonical columns exist (fill missing with None)
        for canonical in ["full_name", "email", "phone", "address", "updated_at"]:
            if canonical not in df.columns:
                df[canonical] = None

        df["_source_id"] = system_id
        df["_source_row"] = df.index
        df["_record_id"] = [str(uuid.uuid4()) for _ in range(len(df))]

        # Parse updated_at to datetime
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    return combined
