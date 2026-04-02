"""Tab 8 — Unique Delegate Manager.

This tab shows the `persons` dataset and lets the user edit delegate metadata.
Edits are staged into `delegate_edits.json` and applied to `df_p` before any merge.
"""

from __future__ import annotations

import os
import re

import numpy as np
import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

from datetime import datetime

# Keep legacy mode optional but tab8 is now designed like tab0 as default.
TAB8_LEGACY_RERUN: bool = os.getenv("TAB8_LEGACY_RERUN", "0") == "1"

def _tab8_rerun() -> None:
    if TAB8_LEGACY_RERUN:
        if hasattr(st, "experimental_rerun"):
            st.experimental_rerun()
        elif hasattr(st, "rerun"):
            st.rerun()
    else:
        # Default tab8 behavior avoids explicit rerun; button press triggers rerun automatically.
        pass

from utils import (
    load_delegate_edits,
    load_new_delegates,
    next_republic_add_id,
    rerun,
    save_delegate_edits,
    save_new_delegates,
)

# Mapping of abbrd columns → persons columns (used when pre-filling missing delegate rows).
ABBRD_FIELD_MAP: dict[str, str] = {
    "fullname": "fullname",
    "id_persoon": "cons_id_str",
    "voornaam": "voornaam",
    "tussenvoegsel": "tussenvoegsel",
    "geslachtsnaam": "geslachtsnaam",
    "geboortejaar": "geboortejaar",
    "overlijden": "overlijdensjaar",
    "beginjaar": "minjaar",
    "eindjaar": "maxjaar",
    "hlife": "hlife",
    "provincie": "provincie",
}


def render(
    tab,
    *,
    df_p: pd.DataFrame,
    df_abbrd: pd.DataFrame | None,
    name_col: str,
    summary: pd.DataFrame | None = None,
) -> None:
    with tab:
        st.title("🧾 Delegate manager")
        st.caption(
            "Edit delegate metadata (name, years, province) and save changes to `delegate_edits.json`. "
            "Changes are applied to the app immediately and used in all other tabs."
        )


        edits = load_delegate_edits()

        def _normalize_id(val) -> str:
            if val is None or pd.isna(val):
                return ""
            if isinstance(val, (int, np.integer)):
                return str(int(val))
            if isinstance(val, float) or isinstance(val, np.floating):
                if float(val).is_integer():
                    return str(int(val))
                return str(val)
            s = str(val).strip()
            if s.endswith(".0") and s[:-2].isdigit():
                return s[:-2]
            return s

        def _is_missing(val) -> bool:
            # Treat empty/NA values as missing when deciding whether to fill from abbrd.
            if val is None:
                return True
            if isinstance(val, str) and val.strip().lower() in {"", "nan", "none"}:
                return True
            return pd.isna(val)

        # Config-driven defaults for abbrd lookup
        cfg = st.session_state.get("config", {})
        abbrd_cfg = cfg.get("abbrd", {})
        abbrd_id_col_default = abbrd_cfg.get("id_col", "id_persoon")
        abbrd_name_col_default = abbrd_cfg.get("name_col", "fullname")
        abbrd_max_preview_fields = int(abbrd_cfg.get("max_preview_fields", 6))
        abbrd_auto_refresh = bool(abbrd_cfg.get("auto_refresh", False))
        abbrd_field_map = abbrd_cfg.get("field_map", ABBRD_FIELD_MAP)
        if not isinstance(abbrd_field_map, dict):
            abbrd_field_map = ABBRD_FIELD_MAP

        # ------------------------------------------------------------------
        # Quick-add missing delegate IDs (lookup + supplement from abbrd)
        # ------------------------------------------------------------------
        st.subheader("Quick add delegate by ID")
        st.caption(
            "Paste or type the delegate_id here and click Add — the delegate is immediately "
            "added to the Overview (via `new_delegates.json`). If the ID exists in abbrd, "
            "we will auto-fill any available fields."
        )
        quick_id = st.text_input("Delegate ID to add", key="quick_add_id")
        if st.button("Add delegate by ID"):
            if not quick_id.strip():
                st.warning("Enter a delegate ID first.")
            else:
                did = quick_id.strip()
                existing_person_ids = {
                    str(x) for x in (df_p["delegate_id"].astype(str) if "delegate_id" in df_p.columns else [])
                }
                existing_new_ids = {str(r.get("delegate_id")) for r in st.session_state["new_delegates"]}
                if did in existing_person_ids or did in existing_new_ids:
                    st.info("This delegate already exists in the dataset.")
                else:
                    rec = {"delegate_id": did}
                    # If available, supplement from abbrd using our field map
                    if df_abbrd is not None:
                        abbrd_id_col = abbrd_id_col_default
                        if abbrd_id_col not in df_abbrd.columns:
                            abbrd_id_col = "id_persoon" if "id_persoon" in df_abbrd.columns else df_abbrd.columns[0]
                        norm_did = _normalize_id(did)
                        abbrd_ids = df_abbrd[abbrd_id_col].map(_normalize_id)
                        match = df_abbrd[abbrd_ids == norm_did]
                        if match.empty:
                            match = df_abbrd[abbrd_ids.str.contains(norm_did, na=False, case=False)]
                        if not match.empty:
                            r0 = match.iloc[0]
                            for abbrd_col, person_col in ABBRD_FIELD_MAP.items():
                                if person_col == "delegate_id":
                                    continue
                                if abbrd_col in r0 and pd.notna(r0[abbrd_col]):
                                    val = r0[abbrd_col]
                                    if person_col in {"birth_year", "death_year", "minjaar", "maxjaar", "hlife"}:
                                        try:
                                            val = int(val)
                                        except Exception:
                                            pass
                                    rec[person_col] = val
                    st.session_state["new_delegates"].append(rec)
                    save_new_delegates(st.session_state["new_delegates"])
                    st.success(f"Added delegate `{did}` and merged into Overview.")

                    # st.session_state["sel_delegate_id"] = did
                    if st.session_state.get("DEBUG"):
                        msg = f"tab8_add_delegate: {did}"
                        st.session_state["debug_last_action"] = msg
                        history = st.session_state.get("debug_history", [])
                        history.append(msg)
                        st.session_state["debug_history"] = history
                    _tab8_rerun()

        # ------------------------------------------------------------------
        # Fill missing delegate metadata from abbrd (moved from Delegate Mgmt)
        # ------------------------------------------------------------------
        cfg = st.session_state.get("config", {})
        abbrd_cfg = cfg.get("abbrd", {})
        abbrd_id_col = abbrd_cfg.get("id_col", "id_persoon")
        abbrd_name_col = abbrd_cfg.get("name_col", "fullname")

        if df_abbrd is not None:
            if abbrd_id_col not in df_abbrd.columns:
                abbrd_id_col = "id_persoon" if "id_persoon" in df_abbrd.columns else df_abbrd.columns[0]
            if abbrd_name_col not in df_abbrd.columns:
                abbrd_name_col = next(
                    (c for c in ("naam", "name", "fullname", "full_name", "achternaam") if c in df_abbrd.columns),
                    abbrd_id_col,
                )

            st.subheader("Fill missing delegate data from abbrd")
            if not df_p.empty and name_col in df_p.columns:
                unnamed = df_p[df_p[name_col].astype(str).str.strip() == ""]
                if unnamed.empty:
                    st.info("No delegates need enrichment from abbrd.")
                else:
                    sel_id = st.selectbox(
                        "Delegate ID to enrich",
                        unnamed["delegate_id"].astype(str).tolist(),
                        key="delegates_enrich_id",
                    )
                    lookup = st.text_input(
                        "Lookup in abbrd (leave blank to use selected ID)",
                        value=str(sel_id),
                        key="delegates_enrich_lookup",
                    ).strip()
                    if lookup:
                        match = df_abbrd[df_abbrd[abbrd_id_col].astype(str).str.contains(lookup, case=False, na=False)]
                        if match.empty:
                            st.warning("No match found in abbrd.")
                        else:
                            st.dataframe(match.head(5), width="stretch")
                            if st.button("Apply enrichment to delegate", key="delegates_enrich_apply"):
                                row = match.iloc[0].to_dict()
                                row["delegate_id"] = str(sel_id)
                                edits[str(sel_id)] = {
                                    k: v
                                    for k, v in row.items()
                                    if k != abbrd_id_col
                                }
                                print(edits)
                                save_delegate_edits(edits)
                                st.success("Applied enrichment to delegate.")
                                rerun()
            else:
                st.warning("abbrd data not available — cannot auto-fill missing delegates.")
        else:
            st.warning("abbrd data not loaded; fill-from-abbrd is unavailable.")

        # Build display frame: apply edits for display purposes too
        df_display = df_p.copy()

        # Build display frame: apply edits for display purposes too
        df_display = df_p.copy()
        if not df_display.empty:
            df_display["delegate_id"] = df_display["delegate_id"].astype(str)
        for did, changes in edits.items():
            if did in df_display["delegate_id"].values:
                for k, v in changes.items():
                    if k != "delegate_id":
                        df_display.loc[df_display["delegate_id"] == did, k] = v
            else:
                row = {"delegate_id": did}
                row.update(changes)
                df_display = pd.concat([df_display, pd.DataFrame([row])], ignore_index=True)

        # Inject any IDs referenced in occurrences but missing from persons.
        # This lets the user “review” them here and/or add metadata to make them real.
        if summary is not None and "delegate_id" in summary.columns:
            extra_ids = set(summary["delegate_id"].astype(str).unique()) - set(
                df_display["delegate_id"].astype(str).unique()
            )
            if extra_ids:
                with st.expander(
                    f"{len(extra_ids)} delegate IDs appear in occurrences but are missing from persons",
                    expanded=False,
                ):
                    st.info(
                        "These delegate IDs are referenced in the occurrences dataset but have no matching "
                        "entry in the persons dataset. You can edit their metadata here and they will also "
                        "appear in the Overview tab."
                    )
                    st.markdown(
                        "**Missing delegate IDs (editable in the table below):** "
                        f"{', '.join(sorted(extra_ids)[:10])}"
                        + (" ..." if len(extra_ids) > 10 else "")
                    )
                extra_rows = []
                for did in sorted(extra_ids):
                    row = {"delegate_id": did}

                    # If abbrd has this ID, prefill known fields.
                    if df_abbrd is not None and abbrd_id_col is not None:
                        norm_did = _normalize_id(did)
                        abbrd_ids = df_abbrd[abbrd_id_col].map(_normalize_id)
                        match = df_abbrd[abbrd_ids == norm_did]
                        if match.empty:
                            match = df_abbrd[abbrd_ids.str.contains(norm_did, na=False, case=False)]
                        if not match.empty:
                            r0 = match.iloc[0]
                            # Copy all mapped fields from abbrd into the person row.
                            for abbrd_col, person_col in ABBRD_FIELD_MAP.items():
                                if abbrd_col not in r0 or pd.isna(r0[abbrd_col]):
                                    continue
                                if person_col == "delegate_id":
                                    continue
                                # For numeric targets, attempt int conversion.
                                if person_col in {"birth_year", "death_year", "minjaar", "maxjaar", "hlife"}:
                                    try:
                                        row[person_col] = int(r0[abbrd_col])
                                    except Exception:
                                        row[person_col] = r0[abbrd_col]
                                else:
                                    row[person_col] = str(r0[abbrd_col])

                extra_rows = pd.DataFrame(extra_rows)
                for col in df_display.columns:
                    if col not in extra_rows.columns:
                        extra_rows[col] = pd.NA
                df_display = pd.concat([df_display, extra_rows[df_display.columns]], ignore_index=True)

        # Add an explicit “Found in abbrd” status column so users can easily filter/review.
        # Normalize IDs to avoid mismatches due to numeric vs string representations.
        def _id_status(did: object) -> str:
            nd = _normalize_id(did)
            if not nd:
                return "❌"
            return "✅" if nd in abbrd_ids else "❌"

        if df_abbrd is not None and abbrd_id_col is not None:
            abbrd_ids = {
                _normalize_id(x)
                for x in df_abbrd[abbrd_id_col].tolist()
                if pd.notna(x)
            }
            df_display["abbrd_status"] = df_display["delegate_id"].apply(_id_status)
        else:
            df_display["abbrd_status"] = "❌"

        # Add a quick missing-field count so it’s easy to spot incomplete delegate rows.
        missing_cols = [c for c in set(ABBRD_FIELD_MAP.values()) if c in df_display.columns and c != "delegate_id"]
        if missing_cols:
            df_display["missing_fields"] = (
                df_display[missing_cols]
                .apply(
                    lambda row: sum(
                        1
                        for v in row
                        if v is None or v == "" or (isinstance(v, float) and pd.isna(v))
                    ),
                    axis=1,
                )
            )
        else:
            df_display["missing_fields"] = 0

        # ------------------------------------------------------------------
        # Delegate table (editable)
        # ------------------------------------------------------------------
        st.markdown("---")
        st.subheader("Delegate list")

        st.markdown("**Search / filter**")
        search = st.text_input("Filter delegates (name / id)", key="delegates_search")
        show_missing_abbrd = st.checkbox(
            "Show only delegates not found in abbrd (❌)",
            value=False,
            key="delegates_show_missing_abbrd",
        )
        if show_missing_abbrd:
            df_display = df_display[df_display["abbrd_status"] == "❌"]

        if search.strip():
            mask = (
                df_display[name_col].astype(str).str.contains(search, case=False, na=False)
                | df_display["delegate_id"].astype(str).str.contains(search, case=False, na=False)
            )
            df_display = df_display[mask]

        st.markdown("**Delegate table (edit cells, then click Save)**")
        gb = GridOptionsBuilder.from_dataframe(df_display)
        gb.configure_default_column(editable=True, sortable=True, filter=True, resizable=True)
        gb.configure_column("delegate_id", editable=False)
        grid_opts = gb.build()

        # Enable single-row selection so users can create a delegate from one specific row.
        grid_opts["rowSelection"] = "single"
        grid_opts["suppressRowClickSelection"] = False

        response = AgGrid(
            df_display,
            gridOptions=grid_opts,
            update_mode=GridUpdateMode.VALUE_CHANGED,
            height=500,
            fit_columns_on_grid_load=True,
            key="delegates_grid",
        )

        updated = response.get("data")
        selected = response.get("selected_rows")
        if selected is None:
            selected = []
        elif isinstance(selected, pd.DataFrame):
            selected = selected.to_dict("records")

        # NOTE: selection in this tab is local (Overview controls global selection).
        # We still expose the selected row's ID so it can be copied or used for local actions.
        if selected:
            sel = selected[0]
            sel_id = str(sel.get("delegate_id", ""))
            if sel_id:
                st.text_input("Selected delegate ID (copy)", value=sel_id, key="tab8_selected_id", disabled=True)
                st.markdown(
                    f"""
                    <button onclick="navigator.clipboard.writeText('{sel_id}').then(()=>{{alert('Copied {sel_id}!')}})">Copy ID</button>
                    """,
                    unsafe_allow_html=True,
                )
        if updated is not None:
            if isinstance(updated, pd.DataFrame):
                updated_df = updated
            elif isinstance(updated, list):
                updated_df = pd.DataFrame(updated)
            elif isinstance(updated, dict):
                updated_df = pd.DataFrame([updated])
            else:
                updated_df = None

            if updated_df is not None and not updated_df.empty:
                new_edits: dict[str, dict] = {}
                for _, row in updated_df.iterrows():
                    did = str(row.get("delegate_id", ""))
                    if not did:
                        continue
                    current = edits.get(did, {})
                    # Save only fields that differ from originals
                    for col in updated_df.columns:
                        if col == "delegate_id":
                            continue
                        val = row.get(col)
                        if pd.isna(val):
                            val = None
                        if current.get(col) != val:
                            current[col] = val
                    if current:
                        new_edits[did] = current
                if new_edits != edits:
                    save_delegate_edits(new_edits)
                    st.success("Saved delegate edits.")
                    rerun()

        if selected:
            st.caption("Row selected (use the button below to create a delegate from this row).")
        else:
            st.caption("Select a row in the table to enable the ‘Create from selected row’ action.")

        if selected and st.button("Create delegate from selected row"):
            sel = selected[0]
            did = str(sel.get("delegate_id", ""))
            if not did:
                st.warning("Selected row has no delegate_id.")
            else:
                existing_person_ids = {
                    str(x) for x in (df_p["delegate_id"].astype(str) if "delegate_id" in df_p.columns else [])
                }
                existing_new_ids = {str(r.get("delegate_id")) for r in st.session_state["new_delegates"]}
                if did in existing_person_ids or did in existing_new_ids:
                    st.info("This delegate already exists in persons or has already been added.")
                else:
                    rec = {k: v for k, v in sel.items() if pd.notna(v) and k != "abbrd_status"}
                    rec["delegate_id"] = did
                    st.session_state["new_delegates"].append(rec)
                    save_new_delegates(st.session_state["new_delegates"])
                    st.success(f"Created delegate `{did}` from selected row and added to Overview.")
                    rerun()

        def _refresh_ids(ids: list[str], show_report: bool = True) -> None:
            """Refresh only the given IDs, optionally reporting what was matched/filled."""
            if df_abbrd is None or abbrd_id_col is None:
                if show_report:
                    st.warning("abbrd data is not available; cannot refresh.")
                return

            # Normalize requested IDs so we refresh the same thing as the debug output.
            norm_requested = { _normalize_id(i) for i in ids if i and str(i).strip() }
            if not norm_requested:
                st.warning("No valid IDs provided to refresh.")
                return

            abbrd_ids_normalized = df_abbrd[abbrd_id_col].map(_normalize_id)

            changed = False
            report = []
            # Fields we attempt to fill from abbrd
            target_fields = [v for v in ABBRD_FIELD_MAP.values() if v != "delegate_id"]
            for rec in st.session_state["new_delegates"]:
                did = str(rec.get("delegate_id", ""))
                norm_did = _normalize_id(did)
                if not norm_did or norm_did not in norm_requested:
                    continue

                before_vals = {f: rec.get(f) for f in target_fields}

                match = df_abbrd[abbrd_ids_normalized == norm_did]
                if match.empty:
                    match = df_abbrd[
                        abbrd_ids_normalized.str.contains(norm_did, na=False, case=False)
                    ]

                match_count = len(match)
                match_ids = []
                if match_count > 0 and abbrd_id_col in match.columns:
                    match_ids = match[abbrd_id_col].astype(str).dropna().unique().tolist()[:5]

                missing = []
                updates = []
                if match_count > 0:
                    for abbrd_col, person_col in ABBRD_FIELD_MAP.items():
                        if person_col == "delegate_id":
                            continue
                        if person_col in rec and not _is_missing(rec.get(person_col)):
                            continue

                        # Look for the first non-null value for this field across all matches
                        for _, r0 in match.iterrows():
                            if abbrd_col in r0 and pd.notna(r0[abbrd_col]):
                                missing.append(person_col)
                                val = r0[abbrd_col]
                                if person_col in {"birth_year", "death_year", "minjaar", "maxjaar", "hlife"}:
                                    try:
                                        val = int(val)
                                    except Exception:
                                        pass
                                rec[person_col] = val
                                new_val = rec.get(person_col)
                                if _is_missing(before_vals.get(person_col)) and not _is_missing(new_val):
                                    updates.append(f"{person_col}: {before_vals.get(person_col)!r} → {new_val!r}")
                                changed = True
                                break

                report.append({
                    "delegate_id": did,
                    "normalized_id": norm_did,
                    "abbrd_match": match_count > 0,
                    "abbrd_match_count": match_count,
                    "abbrd_matched_ids": ", ".join(match_ids) if match_ids else "(none)",
                    "missing_before": ", ".join(missing) if missing else "(none)",
                    "updated": "; ".join(updates) if updates else "(none)",
                })

            if changed:
                save_new_delegates(st.session_state["new_delegates"])
                if show_report:
                    st.success("Refreshed supplemented delegates from abbrd.")
            else:
                if show_report:
                    st.info("No supplements were updated (either no matches in abbrd or all fields already filled).")
            if show_report:
                st.dataframe(pd.DataFrame(report), width="stretch")

        # Auto-refresh supported delegates on first render (config-driven)
        if abbrd_auto_refresh and not st.session_state.get("abbrd_auto_refreshed", False):
            ids = [str(rec.get("delegate_id", "")) for rec in st.session_state["new_delegates"] if rec.get("delegate_id")]
            if ids:
                st.info("Auto-refreshing supplemented delegates from abbrd...")
                _refresh_ids(ids, show_report=False)
            st.session_state["abbrd_auto_refreshed"] = True

        if st.button("Refresh supplemented delegates from abbrd"):
            _refresh_ids([str(rec.get("delegate_id", "")) for rec in st.session_state["new_delegates"]])

        st.markdown("---")
        st.subheader("Refresh specific IDs")
        st.caption("Paste a list of delegate IDs (comma- or newline-separated) to refresh only those.")
        id_list = st.text_area("Delegate IDs to refresh", key="refresh_id_list")
        if st.button("Refresh only these IDs"):
            ids = [s.strip() for s in re.split(r"[,\n]+", id_list) if s.strip()]
            if not ids:
                st.warning("Enter at least one delegate ID.")
            else:
                _refresh_ids(ids)

        st.markdown("---")
        st.subheader("Debug abbrd lookup")
        st.caption("See how each ID is normalized and whether it matches any rows in abbrd.")
        dbg_ids = st.text_area("Delegate IDs to inspect", key="debug_id_list")
        if st.button("Inspect IDs in abbrd"):
            ids = [s.strip() for s in re.split(r"[,\n]+", dbg_ids) if s.strip()]
            if not ids:
                st.warning("Enter at least one delegate ID.")
            else:
                diag = []
                for did in ids:
                    norm_did = _normalize_id(did)
                    abbrd_ids = df_abbrd[abbrd_id_col].map(_normalize_id) if df_abbrd is not None else pd.Series([])
                    matches = []
                    if df_abbrd is not None:
                        matches = df_abbrd[abbrd_ids == norm_did]

                    preview = "(none)"
                    if df_abbrd is not None and not matches.empty:
                        r0 = matches.iloc[0]
                        preview_fields = []
                        for abbrd_col, person_col in ABBRD_FIELD_MAP.items():
                            if abbrd_col in r0 and pd.notna(r0[abbrd_col]):
                                preview_fields.append(f"{person_col}={r0[abbrd_col]}")
                        if preview_fields:
                            preview = "; ".join(preview_fields[:abbrd_max_preview_fields])

                    diag.append({
                        "delegate_id": did,
                        "normalized": norm_did,
                        "abbrd_match_count": len(matches) if df_abbrd is not None else 0,
                        "abbrd_match_ids": ", ".join(
                            matches[abbrd_id_col].astype(str).unique()[:5].tolist()
                        ) if (df_abbrd is not None and not matches.empty) else "(none)",
                        "abbrd_preview": preview,
                    })
                st.dataframe(pd.DataFrame(diag), width="stretch")

        if st.button("Copy missing delegates to persons (add to Overview)"):
            existing_person_ids = {
                str(x) for x in (df_p["delegate_id"].astype(str) if "delegate_id" in df_p.columns else [])
            }
            existing_new_ids = {str(r.get("delegate_id")) for r in st.session_state["new_delegates"]}
            added: list[str] = []
            if updated is not None:
                df_to_iterate = updated
            else:
                df_to_iterate = df_display
            for _, row in pd.DataFrame(df_to_iterate).iterrows():
                did = str(row.get("delegate_id", ""))
                if not did or did in existing_person_ids or did in existing_new_ids:
                    continue
                rec = {k: v for k, v in row.items() if pd.notna(v) and k != "abbrd_status"}
                rec["delegate_id"] = did
                st.session_state["new_delegates"].append(rec)
                added.append(did)
            if added:
                save_new_delegates(st.session_state["new_delegates"])
                st.success(f"Added {len(added)} missing delegate(s) to Overview: {', '.join(added[:10])}{'...' if len(added)>10 else ''}")
                rerun()
            else:
                st.info("No new missing delegates found (either already present in persons or already added).")

        if st.button("Clear all delegate edits"):
            save_delegate_edits({})
            rerun()

        # ------------------------------------------------------------------
        # Add new delegates (republic_add_⟨##⟩)
        # ------------------------------------------------------------------
        st.markdown("---")
        st.subheader("➕ Add new delegate")
        st.caption(
            "Create a new delegate record for IDs that are not present in the persons file or abbrd. "
            "These entries are stored separately (in `new_delegates.json`) and merged into the Overview table."
        )

        next_id = next_republic_add_id(df_p, st.session_state["new_delegates"])
        st.info(f"Next available ID: **`{next_id}`**")

        with st.form("add_delegate_form"):
            nd_delegate_id = st.text_input(
                "Delegate ID (leave blank for auto-generated)",
                value=str(next_id),
                key="nd_delegate_id",
            )
            nd_naam = st.text_input("Naam (surname, firstname) — optional", key="nd_naam")
            nd_birth = st.number_input(
                "Birth year (0 = unknown)", min_value=0, max_value=1900, step=1, key="nd_birth"
            )
            nd_death = st.number_input(
                "Death year (0 = unknown)", min_value=0, max_value=1900, step=1, key="nd_death"
            )
            nd_hlife = st.number_input(
                "hlife (estimated midpoint year, 0 = unknown)",
                min_value=0,
                max_value=1900,
                step=1,
                key="nd_hlife",
            )
            nd_province = st.text_input("Province", key="nd_prov")
            nd_source = st.text_area("Source / notes", key="nd_source")
            submitted = st.form_submit_button("➕ Add delegate")

        if submitted:
            desired_id = nd_delegate_id.strip() if nd_delegate_id.strip() else str(next_id)
            safe_id = desired_id

            existing_person_ids = (
                set(map(str, df_p["delegate_id"])) if "delegate_id" in df_p.columns else set()
            )
            existing_new_ids = {str(r.get("delegate_id")) for r in st.session_state["new_delegates"]}
            if safe_id in existing_person_ids or safe_id in existing_new_ids:
                st.error(f"Delegate ID `{safe_id}` already exists. Pick a different ID.")
            else:
                abbrd_match = df_abbrd[
                    df_abbrd[abbrd_id_col].astype(str).str.strip() == safe_id
                ]
                if abbrd_match.empty:
                    abbrd_match = df_abbrd[
                        df_abbrd[abbrd_id_col].astype(str).str.contains(safe_id, na=False, case=False)
                    ]

                rec = {"delegate_id": safe_id, "added_by": "manual"}
                if nd_naam.strip():
                    rec[name_col] = nd_naam.strip()
                elif not abbrd_match.empty:
                    rec[name_col] = str(abbrd_match.iloc[0][abbrd_name_col])

                if nd_birth:
                    rec["birth_year"] = int(nd_birth)
                elif not abbrd_match.empty:
                    for by in ("birth_year", "geboortejaar", "geboorte", "born", "birth"):
                        if by in abbrd_match.columns and pd.notna(abbrd_match.iloc[0].get(by)):
                            rec["birth_year"] = int(abbrd_match.iloc[0][by])
                            break
                if nd_death:
                    rec["death_year"] = int(nd_death)
                elif not abbrd_match.empty:
                    for dy in ("death_year", "sterfjaar", "overlijden", "died", "death"):
                        if dy in abbrd_match.columns and pd.notna(abbrd_match.iloc[0].get(dy)):
                            rec["death_year"] = int(abbrd_match.iloc[0][dy])
                            break
                if nd_hlife:
                    rec["hlife"] = int(nd_hlife)
                elif not abbrd_match.empty and "hlife" in abbrd_match.columns:
                    h = abbrd_match.iloc[0].get("hlife")
                    if pd.notna(h):
                        rec["hlife"] = int(h)

                if nd_province.strip():
                    rec["provincie"] = nd_province.strip()
                elif not abbrd_match.empty and "provincie" in abbrd_match.columns:
                    prov = abbrd_match.iloc[0].get("provincie")
                    if pd.notna(prov):
                        rec["provincie"] = str(prov)

                if nd_source.strip():
                    rec["source"] = nd_source.strip()

                st.session_state["new_delegates"].append(rec)
                save_new_delegates(st.session_state["new_delegates"])
                st.success(
                    f"Added `{safe_id}`. Clear Streamlit cache to merge into the overview."
                )

        if st.session_state["new_delegates"]:
            st.markdown("---")
            st.subheader("Currently stored new delegates")
            nd_df = pd.DataFrame(st.session_state["new_delegates"])
            st.dataframe(nd_df, width="stretch", height=200)
            if st.button("🗑️ Delete last entry", key="nd_delete_last"):
                st.session_state["new_delegates"].pop()
                save_new_delegates(st.session_state["new_delegates"])
                rerun()

            st.markdown("---")
            st.subheader("Save added delegates")
            st.caption("Export the currently stored new delegates to a JSON file or an Excel workbook.")

            # Compare in-memory delegates with what’s already persisted to disk.
            stored = load_new_delegates()
            stored_by_id = {str(r.get("delegate_id")): r for r in stored if r.get("delegate_id") is not None}
            current_by_id = {str(r.get("delegate_id")): r for r in st.session_state["new_delegates"] if r.get("delegate_id") is not None}

            stored_ids = set(stored_by_id)
            current_ids = set(current_by_id)
            new_ids = sorted(current_ids - stored_ids)
            removed_ids = sorted(stored_ids - current_ids)
            changed_ids = []
            for did in current_ids & stored_ids:
                if current_by_id[did] != stored_by_id[did]:
                    changed_ids.append(did)

            st.markdown(
                f"**In memory:** {len(current_ids)} record(s)  \
                 **Persisted on disk:** {len(stored_ids)} record(s)"
            )
            if new_ids:
                st.info(f"{len(new_ids)} new delegate(s) since last save: {', '.join(new_ids[:10])}{'...' if len(new_ids)>10 else ''}")
            if changed_ids:
                st.warning(f"{len(changed_ids)} updated delegate(s) since last save: {', '.join(changed_ids[:10])}{'...' if len(changed_ids)>10 else ''}")
            if removed_ids:
                st.error(f"{len(removed_ids)} delegate(s) removed since last save: {', '.join(removed_ids[:10])}{'...' if len(removed_ids)>10 else ''}")

            df_new = pd.DataFrame(st.session_state["new_delegates"])
            col1, col2, col3 = st.columns([3, 3, 2])
            with col1:
                if st.button("Save all to Excel", key="save_new_delegates_excel"):
                    out_path = "new_delegates_export.xlsx"
                    df_new.to_excel(out_path, index=False)
                    st.success(f"Saved {len(df_new)} delegate(s) to {out_path}")
            with col2:
                if st.button("Save unsaved to Excel", key="save_new_delegates_excel_diff"):
                    out_path = f"new_delegates_diff_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
                    df_diff = pd.DataFrame([current_by_id[d] for d in new_ids + changed_ids])
                    df_diff.to_excel(out_path, index=False)
                    st.success(f"Saved {len(df_diff)} delegate(s) to {out_path}")
            with col3:
                if st.button("Save all to JSON", key="save_new_delegates_json"):
                    save_new_delegates(st.session_state["new_delegates"])
                    st.success("Saved new delegates to new_delegates.json")
