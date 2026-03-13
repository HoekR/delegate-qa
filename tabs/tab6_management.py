"""Tab 6 – Delegate Management: fill names, replace misidentifications, add new, bulk remap, sandbox."""
from __future__ import annotations

from pathlib import Path
from typing import Callable

import pandas as pd
import streamlit as st

from utils import (
    REPUBLIC_ADD_PREFIX,
    load_sandboxed_records,
    next_republic_add_id,
    save_new_delegates,
    save_remappings,
    save_sandboxed,
)


def render(
    tab,
    *,
    df_abbrd: pd.DataFrame | None,
    df_p: pd.DataFrame,
    known_delegate_ids: list,
    name_col: str,
    ABBRD_CANDIDATES: list[Path],
    save_correction: Callable,
    n_enriched_persons: int,
    n_remapped_rows: int,
    sandboxed: set[str],
) -> None:
    with tab:
        st.title("👤 Delegate Management")
        st.caption(
            "Three tools: (1) fill missing name data for unnamed delegates from **abbrd.xlsx**, "
            "(2) replace a wrongly-identified delegate from abbrd, "
            "(3) add a completely new delegate with an auto-generated `republic_add_<##>` ID."
        )

        if df_abbrd is None:
            st.error(
                "Could not find abbrd.xlsx. Searched in:\n\n"
                + "\n".join(f"- `{p}`" for p in ABBRD_CANDIDATES)
                + "\n\n**Quickest fix:** copy or symlink `abbrd.xlsx` into the workspace folder "
                f"(`{Path(__file__).parent.parent}`) and clear cache."
            )
            st.stop()

        abbrd_id_col = "id_persoon" if "id_persoon" in df_abbrd.columns else df_abbrd.columns[0]
        abbrd_name_col = next(
            (c for c in ("naam", "name", "fullname", "full_name", "achternaam") if c in df_abbrd.columns),
            abbrd_id_col,
        )
        abbrd_hlife_col = "hlife" if "hlife" in df_abbrd.columns else None

        with st.expander("abbrd.xlsx columns", expanded=False):
            st.write(list(df_abbrd.columns))
            st.dataframe(df_abbrd.head(5), width="stretch")

        # -------------------------------------------------------------------
        # SECTION 1 – Fill unnamed delegates from abbrd
        # -------------------------------------------------------------------
        st.subheader("1️⃣  Fill unnamed / incomplete delegates from abbrd")
        st.caption(
            f"At startup **{n_enriched_persons}** persons rows were automatically enriched from abbrd "
            "(blank fields only — existing data was never overwritten). "
            "Delegates that still have no name are listed below for manual lookup."
        )

        if not df_p.empty:
            unnamed_mask = df_p[name_col].isna() | (df_p[name_col].astype(str).str.strip() == "")
            df_unnamed = (
                df_p[unnamed_mask][["delegate_id", name_col]].copy()
                if name_col in df_p.columns else df_p[unnamed_mask]
            )
            if df_unnamed.empty:
                st.success("All delegates in the persons file have a name.")
            else:
                st.metric("Unnamed delegates", len(df_unnamed))
                st.dataframe(df_unnamed, width="stretch", height=200)

                sel_unnamed_id = st.selectbox(
                    "Pick a delegate_id to fill",
                    df_unnamed["delegate_id"].tolist(),
                    key="mgmt_fill_id",
                )
                abbrd_lookup = st.text_input(
                    f"Search abbrd by `{abbrd_id_col}` (leave blank to use the delegate_id above)",
                    key="mgmt_abbrd_lookup",
                )
                lookup_val = abbrd_lookup.strip() if abbrd_lookup.strip() else str(sel_unnamed_id)

                abbrd_match = df_abbrd[
                    df_abbrd[abbrd_id_col].astype(str).str.strip() == lookup_val
                ]
                if abbrd_match.empty:
                    abbrd_match = df_abbrd[
                        df_abbrd[abbrd_id_col].astype(str).str.contains(lookup_val, na=False, case=False)
                    ]
                if abbrd_match.empty:
                    if lookup_val:
                        st.warning(f"No row found in abbrd for `{abbrd_id_col}` = '{lookup_val}'")
                else:
                    st.dataframe(abbrd_match.head(5), width="stretch")
                    chosen_abbrd_row = abbrd_match.iloc[0]
                    if st.button("✔️ Save enrichment to new_delegates", key="mgmt_fill_save"):
                        rec = chosen_abbrd_row.to_dict()
                        rec["delegate_id"] = sel_unnamed_id
                        st.session_state["new_delegates"] = [
                            r for r in st.session_state["new_delegates"]
                            if str(r.get("delegate_id")) != str(sel_unnamed_id)
                        ]
                        st.session_state["new_delegates"].append(rec)
                        save_new_delegates(st.session_state["new_delegates"])
                        st.success(
                            f"Enrichment for delegate_id={sel_unnamed_id} saved. "
                            "Clear Streamlit cache (hamburger → Clear cache) to reload."
                        )

        # -------------------------------------------------------------------
        # SECTION 2 – Replace misidentified delegate from abbrd
        # -------------------------------------------------------------------
        st.markdown("---")
        st.subheader("2️⃣  Replace occurrence with correct delegate from abbrd")
        st.caption(
            "Find a row in the occurrences file where the delegate is wrong, "
            "then look up the correct person in abbrd and save the correction."
        )

        occ_row_id = st.number_input(
            "Occurrence row index (from other tabs)", min_value=0, step=1, key="mgmt_occ_row"
        )
        abbrd_replace_q = st.text_input(
            f"Search abbrd for correct delegate (by name or `{abbrd_id_col}`)",
            key="mgmt_repl_q",
        )
        if abbrd_replace_q.strip():
            q = abbrd_replace_q.strip()
            hits = df_abbrd[
                df_abbrd[abbrd_id_col].astype(str).str.contains(q, case=False, na=False)
                | df_abbrd[abbrd_name_col].astype(str).str.contains(q, case=False, na=False)
            ]
            if hits.empty:
                st.warning("No matches in abbrd.")
            else:
                st.dataframe(
                    hits[[abbrd_id_col, abbrd_name_col]
                         + ([abbrd_hlife_col] if abbrd_hlife_col else [])].head(10),
                    width="stretch",
                )
                sel_abbrd_pid = st.selectbox(
                    f"Select `{abbrd_id_col}` to use as new delegate_id",
                    hits[abbrd_id_col].tolist(),
                    key="mgmt_repl_pid",
                )
                if st.button("💾 Save correction", key="mgmt_repl_save"):
                    save_correction(int(occ_row_id), sel_abbrd_pid)
                    st.success(f"Saved: row {int(occ_row_id)} → {sel_abbrd_pid}")

        # -------------------------------------------------------------------
        # SECTION 3 – Add a completely new delegate
        # -------------------------------------------------------------------
        st.markdown("---")
        st.subheader("3️⃣  Add new delegate (republic_add_⟨##⟩)")
        st.caption(
            "For cases where the correct person is not in abbrd or the persons file. "
            "A clash-free `republic_add_<##>` ID is generated automatically."
        )

        next_id = next_republic_add_id(df_p, st.session_state["new_delegates"])
        st.info(f"Next available ID: **`{next_id}`**")

        with st.form("add_delegate_form"):
            nd_naam     = st.text_input("Naam (surname, firstname)", key="nd_naam")
            nd_birth    = st.number_input("Birth year (0 = unknown)", min_value=0, max_value=1900, step=1, key="nd_birth")
            nd_death    = st.number_input("Death year (0 = unknown)", min_value=0, max_value=1900, step=1, key="nd_death")
            nd_hlife    = st.number_input("hlife (estimated midpoint year, 0 = unknown)", min_value=0, max_value=1900, step=1, key="nd_hlife")
            nd_province = st.text_input("Province", key="nd_prov")
            nd_source   = st.text_area("Source / notes", key="nd_source")
            submitted   = st.form_submit_button("➕ Add delegate")

        if submitted:
            if not nd_naam.strip():
                st.error("Naam is required.")
            else:
                safe_id = next_republic_add_id(df_p, st.session_state["new_delegates"])
                rec = {
                    "delegate_id": safe_id,
                    name_col:      nd_naam.strip(),
                    "birth_year":  int(nd_birth) if nd_birth else pd.NA,
                    "death_year":  int(nd_death) if nd_death else pd.NA,
                    "hlife":       int(nd_hlife) if nd_hlife else pd.NA,
                    "provincie":   nd_province.strip(),
                    "source":      nd_source.strip(),
                    "added_by":    "manual",
                }
                existing_ids = {str(r.get("delegate_id")) for r in st.session_state["new_delegates"]}
                if str(safe_id) in existing_ids:
                    st.error("ID clash detected — please press Add again to get a fresh ID.")
                else:
                    st.session_state["new_delegates"].append(rec)
                    save_new_delegates(st.session_state["new_delegates"])
                    st.success(
                        f"Added `{safe_id}` ({nd_naam.strip()}). "
                        "Clear Streamlit cache to merge into the overview."
                    )

        if st.session_state["new_delegates"]:
            st.markdown("---")
            st.subheader("Currently stored new delegates")
            nd_df = pd.DataFrame(st.session_state["new_delegates"])
            st.dataframe(nd_df, width="stretch", height=200)
            if st.button("🗑️ Delete last entry", key="nd_delete_last"):
                st.session_state["new_delegates"].pop()
                save_new_delegates(st.session_state["new_delegates"])
                st.rerun()

        # -------------------------------------------------------------------
        # SECTION 4 – Bulk ID remapping
        # -------------------------------------------------------------------
        st.markdown("---")
        st.subheader("4️⃣  Bulk remap delegate IDs")
        st.caption(
            "Maps every occurrence of **from_id** → **to_id** in the occurrences file before "
            "the merge. Applied at load time; changes take effect after a cache clear. "
            f"Currently **{n_remapped_rows}** rows were remapped this run."
        )

        known_ids = sorted(known_delegate_ids)
        col_from, col_to = st.columns(2)
        remap_from = col_from.selectbox("From delegate_id (wrong)", [""] + known_ids, key="remap_from")
        remap_to   = col_to.text_input("To delegate_id (correct, any string or int)", key="remap_to")
        if st.button("➕ Add remap rule", key="remap_add"):
            if remap_from and remap_to.strip():
                rule = {"from_id": str(remap_from), "to_id": str(remap_to).strip()}
                existing_froms = {r["from_id"] for r in st.session_state["remappings"]}
                if rule["from_id"] in existing_froms:
                    st.warning(f"A rule for {remap_from} already exists — delete it first.")
                else:
                    st.session_state["remappings"].append(rule)
                    save_remappings(st.session_state["remappings"])
                    st.success(f"Rule saved: {remap_from} → {remap_to.strip()}. Clear cache to apply.")
            else:
                st.warning("Fill both fields before adding.")

        st.caption("Or upload a CSV with columns `from_id` and `to_id` to add many rules at once:")
        remap_csv = st.file_uploader("CSV remap table", type=["csv"], key="remap_csv")
        if remap_csv is not None:
            try:
                remap_upload_df = pd.read_csv(remap_csv, dtype=str)
                if {"from_id", "to_id"}.issubset(remap_upload_df.columns):
                    existing_froms = {r["from_id"] for r in st.session_state["remappings"]}
                    added, skipped = 0, 0
                    for _, row_r in remap_upload_df.iterrows():
                        fid = str(row_r["from_id"]).strip()
                        tid = str(row_r["to_id"]).strip()
                        if fid and tid and fid not in existing_froms:
                            st.session_state["remappings"].append({"from_id": fid, "to_id": tid})
                            existing_froms.add(fid)
                            added += 1
                        else:
                            skipped += 1
                    save_remappings(st.session_state["remappings"])
                    st.success(
                        f"Added {added} rules ({skipped} skipped — duplicates or blanks). Clear cache to apply."
                    )
                else:
                    st.error("CSV must have columns `from_id` and `to_id`.")
            except Exception as e:
                st.error(f"Could not parse CSV: {e}")

        if st.session_state["remappings"]:
            st.markdown("**Current remap rules:**")
            remap_df = pd.DataFrame(st.session_state["remappings"])
            st.dataframe(remap_df, width="stretch", height=min(50 + 35 * len(remap_df), 400))

            del_from = st.selectbox(
                "Delete rule for from_id:",
                [""] + [r["from_id"] for r in st.session_state["remappings"]],
                key="remap_del",
            )
            if st.button("🗑️ Delete selected rule", key="remap_del_btn"):
                if del_from:
                    st.session_state["remappings"] = [
                        r for r in st.session_state["remappings"] if r["from_id"] != del_from
                    ]
                    save_remappings(st.session_state["remappings"])
                    st.success(f"Deleted rule for {del_from}. Clear cache to apply.")

            if st.button("🗑️ Delete ALL remap rules", key="remap_del_all"):
                st.session_state["remappings"] = []
                save_remappings([])
                st.success("All rules deleted.")
        else:
            st.info("No remap rules defined yet.")

        # -------------------------------------------------------------------
        # SECTION 5 – Sandbox known-wrong IDs
        # -------------------------------------------------------------------
        st.markdown("---")
        st.subheader("5️⃣  Sandbox known-wrong delegate IDs")
        st.caption(
            "Mark a delegate_id as *known incorrect but unfixable* — "
            "it will be flagged with 🔒 in the Overview grid so you can skip it during review. "
            "Sandboxed rows are never deleted from the data; this is purely a visual marker."
        )

        known_ids_sb = sorted(known_delegate_ids)
        sb_col1, sb_col2 = st.columns([3, 2])
        sb_add_id = sb_col1.selectbox(
            "Delegate ID to sandbox",
            [""] + [i for i in known_ids_sb if i not in sandboxed],
            key="sb_add_id",
        )
        sb_reason = sb_col2.text_input("Reason (optional)", key="sb_reason")

        if st.button("🔒 Add to sandbox", key="sb_add_btn", disabled=not sb_add_id):
            records = load_sandboxed_records()
            records.append({"id": str(sb_add_id), "reason": sb_reason.strip()})
            save_sandboxed(records)
            st.session_state["sandboxed"] = {r["id"] for r in records}
            st.success(f"Sandboxed `{sb_add_id}`. Overview will show 🔒 on next rerun.")
            st.rerun()

        sb_records = load_sandboxed_records()
        if sb_records:
            sb_df = pd.DataFrame(sb_records)
            if name_col in df_p.columns and "delegate_id" in df_p.columns:
                name_lkp = dict(zip(
                    df_p["delegate_id"].astype(str),
                    df_p[name_col].astype(str),
                ))
                sb_df["name"] = sb_df["id"].map(name_lkp).fillna("")
            st.dataframe(sb_df, width="stretch", height=min(50 + 35 * len(sb_df), 300))

            sb_del_id = st.selectbox(
                "Remove from sandbox:",
                [""] + [r["id"] for r in sb_records],
                key="sb_del_id",
            )
            if st.button("🗑️ Remove from sandbox", key="sb_del_btn", disabled=not sb_del_id):
                records = [r for r in sb_records if r["id"] != sb_del_id]
                save_sandboxed(records)
                st.session_state["sandboxed"] = {r["id"] for r in records}
                st.success(f"Removed `{sb_del_id}` from sandbox.")
                st.rerun()
        else:
            st.info("No IDs sandboxed yet.")

