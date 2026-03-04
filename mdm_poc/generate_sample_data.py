"""
Creates demo source files under data/ to exercise all matching tiers.

Coverage:
  - 2 hard-match pairs (same norm_name + norm_phone, different emails)
  - 1 name typo → REVIEW tier
  - 1 swapped first/last name → tests WRatio token-sort
  - Conflicting field values with different updated_at timestamps
  - Unique records in each source
"""
import os
import pandas as pd

os.makedirs("data", exist_ok=True)

# ── Source A (CRM) ──────────────────────────────────────────────────────────
source_a = pd.DataFrame(
    {
        "Name": [
            "Alice Johnson",       # hard-match with source_b record 0
            "Bob Martinez",        # hard-match with source_c record 0
            "Carol Williams",      # unique to A
            "David Lee",           # typo variant in source_b → REVIEW
            "Johnson Alice",       # swapped name; matches source_b record 4 via WRatio
        ],
        "Email": [
            "alice.j@crm.com",
            "bob.m@crm.com",
            "carol.w@crm.com",
            "david.lee@crm.com",
            "alice.j2@crm.com",
        ],
        "Phone": [
            "555-100-2000",
            "555-200-3000",
            "555-300-4000",
            "555-400-5000",
            "555-100-2000",       # same phone as Alice Johnson row → hard-match test
        ],
        "Address": [
            "12 Oak St, Springfield, IL 62701",
            "45 Elm Ave, Chicago, IL 60601",
            "78 Maple Blvd, Peoria, IL 61602",
            "99 Pine Rd, Rockford, IL 61101",
            "12 Oak Street, Springfield IL 62701",  # slight variation
        ],
        "LastModified": [
            "2024-06-01",
            "2024-05-15",
            "2024-07-20",
            "2024-03-10",
            "2024-06-02",
        ],
    }
)

# ── Source B (Marketing) ────────────────────────────────────────────────────
source_b = pd.DataFrame(
    {
        "contact_name": [
            "Alice Johnson",       # hard-match with source_a record 0
            "Davd Lee",            # typo of David Lee → REVIEW
            "Emily Chen",          # unique to B
            "Frank Torres",        # unique to B
            "Alice Johnson",       # same name, same phone as source_a row 4 → hard-match
        ],
        "contact_email": [
            "alice.johnson@marketing.com",   # different email → conflict
            "d.lee@marketing.com",
            "emily.c@marketing.com",
            "frank.t@marketing.com",
            "alice.j2@marketing.com",        # slightly different
        ],
        "mobile": [
            "5551002000",          # normalized same as source_a row 0
            "5554005000",
            "5556007000",
            "5557008000",
            "5551002000",          # same as source_a row 4
        ],
        "mailing_address": [
            "12 Oak Street, Springfield, IL 62701",
            "99 Pine Road, Rockford, IL 61101",
            "33 Cedar Ln, Naperville, IL 60540",
            "21 Birch Ct, Aurora, IL 60505",
            "12 Oak St, Springfield IL 62701",
        ],
        "record_updated": [
            "2024-08-15",          # more recent than source_a → wins on email
            "2024-04-01",
            "2024-09-01",
            "2024-02-28",
            "2024-01-10",          # older than source_a row 4
        ],
    }
)

# ── Source C (ERP) — XLSX ───────────────────────────────────────────────────
source_c = pd.DataFrame(
    {
        "FullName": [
            "Bob Martinez",        # hard-match with source_a record 1
            "Grace Kim",           # unique to C
            "Hank Patel",          # unique to C
        ],
        "EmailAddr": [
            "b.martinez@erp.internal",   # conflict with source_a email
            "grace.k@erp.internal",
            "hank.p@erp.internal",
        ],
        "PhoneNumber": [
            "+1-555-200-3000",     # normalizes to same as source_a row 1
            "555-800-9000",
            "555-900-1111",
        ],
        "StreetAddress": [
            "45 Elm Avenue, Chicago, IL 60601",
            "55 Walnut Dr, Evanston, IL 60201",
            "67 Spruce Pl, Joliet, IL 60432",
        ],
        "ModifiedDate": [
            "2023-11-20",          # older → source_a email wins
            "2024-10-05",
            "2024-08-30",
        ],
    }
)

source_a.to_csv("data/source_a.csv", index=False)
source_b.to_csv("data/source_b.csv", index=False)

with pd.ExcelWriter("data/source_c.xlsx", engine="openpyxl") as writer:
    source_c.to_excel(writer, sheet_name="Contacts", index=False)

print("Sample data created:")
print(f"  data/source_a.csv  ({len(source_a)} rows)")
print(f"  data/source_b.csv  ({len(source_b)} rows)")
print(f"  data/source_c.xlsx ({len(source_c)} rows, sheet='Contacts')")
print(f"\nTotal source rows: {len(source_a) + len(source_b) + len(source_c)}")
print("Expected merges:")
print("  AUTO_MERGE: Alice Johnson (A0+B0), Alice Johnson (A4+B4), Bob Martinez (A1+C0)")
print("  REVIEW    : David Lee / Davd Lee (A3+B1)")
print("  Unique    : Carol Williams, Emily Chen, Frank Torres, Grace Kim, Hank Patel")
