The most useful check for each cluster is: what does the year distribution look like — is there a clear gap around the death year (two generations) or a gradual tail (wrong lifespan dates in uq)? 

Created [](file:///Users/rikhoekstra/develop/streamlit_worksheet/_check_cluster.py)

Ran terminal command: .venv/bin/python3 _check_cluster.py 14024 16307 19809 16189 13729 16120 2>/dev/null

The output tells you clearly what's happening in each case — most are **mixed clusters** (unrelated names lumped together), not just wrong lifespan dates:

| id | Name in uq | Problem | Evidence |
|---|---|---|---|
| **14024** | Heinsius, Anthonie (†1720) | Completely mixed — top patterns include `Romswinckel`, `van Slingelandt`, `Heinsius` | 2245 post-death rows are unrelated delegates |
| **16307** | Rouse, Lucas Gijsbert (b.1728) | 2284 pre-birth rows have patterns `Isselmuyden`, `Vestrinck` — different people | corrections moved those here accidentally |
| **19809** | Reede, Godart Adriaan van (†1730) | 992 post-death rows: patterns `Gevaerts`, `Harrevelt`, `van Blocklandt` | other delegates mixed in |
| **16189** | Rechteren tot Gramsbergen (†1780) | 970 rows in 1790s: patterns `Rouse`, `de Schepper`, `Jacobson` | different delegates mixed in |
| **13729** | Merens, Allard (†1716) | patterns `de Neyn`, `Abbekerck-Crap` alongside `Meerens` | two different people merged |
| **16120** | Hoorn, Hendrik Nicolaasz. van (†1759) | 554 post-death rows: patterns `Van Palland tot Glinthuys`, `Nilant` | unrelated delegates |

**Easiest way to check in the app:** filter the occurrences tab for that delegate_id, sort by year, and look at the pattern column for the out-of-range rows — the foreign pattern names will tell you immediately who they belong to. Then split those rows to the correct delegate.

You can also run _check_cluster.py with any id at any time:
```
.venv/bin/python3 _check_cluster.py 14024
```

Made changes.



Ran terminal command: cd /Users/rikhoekstra/develop/streamlit_worksheet && .venv/bin/python3 -c "
import pandas as pd

baked = pd.read_parquet('delegates_18ee_w_correcties_baked.parquet')
uq = pd.read_parquet('uq_delegates_baked_20260423.parquet')

targets = [15127, 432, 19808, 17128, 14226, 13573]

for tid in targets:
    sub = baked[baked['delegate_id'] == tid]
    info = uq[uq['cons_id_str'] == tid][['cons_id_str','fullname','geboortejaar','overlijdensjaar']].to_dict('records')
    print(f'\n=== {tid} {info} ===')
    viol = sub[sub['j'] < sub['birth'] - 2 if 'birth' in sub.columns else sub.index.isin([])]
    # violations = outside lifespan
    birth = sub['birth'].iloc[0] if len(sub) else None
    death = sub['death'].iloc[0] if len(sub) else None
    outside = sub[(sub['j'] < birth) | (sub['j'] > death)] if birth and death else sub
    top = outside.groupby('pattern').size().sort_values(ascending=False).head(8)
    print(f'  birth={birth} death={death}, {len(outside)} violations')
    print(top.to_string())
" 2>/dev/null

Read [](file:///Users/rikhoekstra/Library/Application%20Support/Code/User/workspaceStorage/bc962666d4afbbd6f80c7b43e4c4e150/GitHub.copilot-chat/chat-session-resources/9a6054ad-737a-4f72-b74a-d54515c03d49/toolu_bdrk_015cZzu5PKmwBj7JPAxcKH9v__vscode-1777026802756/content.txt#1-1), lines 1 to 80

The IDs aren't matching — likely a type mismatch. Let me check: 

Ran terminal command: cd /Users/rikhoekstra/develop/streamlit_worksheet && .venv/bin/python3 -c "
import pandas as pd

baked = pd.read_parquet('delegates_18ee_w_correcties_baked.parquet')
uq = pd.read_parquet('uq_delegates_baked_20260423.parquet')

print('baked delegate_id dtype:', baked['delegate_id'].dtype)
print('uq cons_id_str dtype:', uq['cons_id_str'].dtype)
print('baked delegate_id sample:', baked['delegate_id'].head(5).tolist())

targets = [15127, 432, 19808, 17128, 14226, 13573]
# try string match
for tid in targets:
    sub = baked[baked['delegate_id'].astype(str) == str(tid)]
    uq_row = uq[uq['cons_id_str'].astype(str) == str(tid)][['fullname','geboortejaar','overlijdensjaar']]
    if len(sub) == 0:
        print(f'{tid}: NOT FOUND in baked')
        continue
    birth = uq_row['geboortejaar'].iloc[0] if len(uq_row) else None
    death = uq_row['overlijdensjaar'].iloc[0] if len(uq_row) else None
    fname = uq_row['fullname'].iloc[0] if len(uq_row) else '?'
    outside = sub[(sub['j'] < birth) | (sub['j'] > death)] if birth and death else sub
    top = outside.groupby('pattern').size().sort_values(ascending=False).head(6)
    print(f'\n=== {tid} {fname} birth={birth} death={death} ({len(outside)} viols) ===')
    print(top.to_string())
" 2>/dev/null

Read [](file:///Users/rikhoekstra/Library/Application%20Support/Code/User/workspaceStorage/bc962666d4afbbd6f80c7b43e4c4e150/GitHub.copilot-chat/chat-session-resources/9a6054ad-737a-4f72-b74a-d54515c03d49/toolu_bdrk_01UnBZZ1fHJ85cn912rTiJse__vscode-1777026802758/content.txt)

Now let me look up the correct target delegates for each in uq: 

Ran terminal command: cd /Users/rikhoekstra/develop/streamlit_worksheet && .venv/bin/python3 -c "
import pandas as pd
uq = pd.read_parquet('uq_delegates_baked_20260423.parquet')

# Search for correct delegates by surname
searches = {
    'Wassenaer Starrenburg': 'wassenaer',
    'Steyn': 'steyn',
    'Essenius/van Essen (early)': 'essen',
    'Heeckeren (early, pre-1742)': 'heeckeren',
    'Iddekinge (post-1719)': 'iddekinge',
    'Pesters': 'pesters',
    'Bleyswijk (late, 1779)': 'bleyswijk|bleyswijck',
}

for label, term in searches.items():
    hits = uq[uq['fullname'].str.lower().str.contains(term, na=False)][['cons_id_str','fullname','geboortejaar','overlijdensjaar']]
    print(f'\n--- {label} ---')
    print(hits.to_string())
" 2>/dev/null

Now let me also check for Lestevenon (later), Parvé/Santheuvel, and look at what years the violations are by year for 19808:  

Ran terminal command: cd /Users/rikhoekstra/develop/streamlit_worksheet && .venv/bin/python3 -c "
import pandas as pd
uq = pd.read_parquet('uq_delegates_baked_20260423.parquet')

# Additional lookups
for term in ['lestevenon', 'parve|santheuvel', 'pesters']:
    hits = uq[uq['fullname'].str.lower().str.contains(term, na=False)][['cons_id_str','fullname','geboortejaar','overlijdensjaar']]
    print(f'\n--- {term} ---')
    print(hits.to_string())

# Heeckeren violations year range
baked = pd.read_parquet('delegates_18ee_w_correcties_baked.parquet')
h = baked[baked['delegate_id'].astype(str) == '19808']
birth, death = 1742, 1796
outside = h[(h['j'] < birth) | (h['j'] > death)]
print('\n--- 19808 violation years ---')
print(outside.groupby('j').size().sort_index().to_string())
" 2>/dev/null

Here's the full diagnosis for each:

| delegate_id | name (wrong lifespan) | top violation pattern | → correct delegate | ID |
|---|---|---|---|---|
| **15127** | Wassenaer, Carel Lodewijk (†1751, viols 1754-1795) | "Van Wassenaer" (1517) | Wassenaer Obdam, Carel George van (1733-1800) | 12372 |
| | | "Steyn" (1474) | Steyn, Pieter (1706-1772) | **13568** |
| | | "Van Wassenaer Starrenburg" (540) | Wassenaer, Willem Lodewijk Starrenburg (1744-1833) | **13555** |
| | | "Parvé/Santheuvel" (473) | Santheuvel, Bartholomeus van den (1730-1784) | **13563** |
| | | "Bleyswijck" (411) | Bleyswijk, Pieter van (1724-1790) | **14029** |
| **432** | Essenius, Samuel (born 1755, viols 1705-1752) | "van Essen/Essenius" (3243) | Essenius, Leonard (1676-1748) | **16932** |
| **19808** | Heeckeren van Brantsenburg (born 1742, viols 1705-1740) | "van Heeckeren van Brantsenburg" (1317) | Heeckeren tot den Brantsenburg, Walraven Robbert van (1704-1758) | **17295** |
| **17128** | Iddekinge, Rembt (†1719, viols 1720-1747) | "Iddekinge" (1314) | Iddekinge, Tobias Jan van (1689-1759) | **17965** |
| **14226** | Lestevenon, Daniel (†1736, viols 1737-1795) | "Pesters" (720) | Pesters, Balthasar Constantijn (1731-1822) | **19203** |
| | | "van Citters" (567) | (already known → 14076/15995) | |
| | | "Groeninx/Lestevenon" (280) | Lestevenon, Willem Anne (1750-1816) | **13998** |
| **13573** | Bleyswijk, Hendrik (†1703, viols 1779-1787) | "Bleyswijck" (1157) | Bleyswijk, Pieter van (1724-1790) | **14029** |

These can all be fixed in the app by selecting the pattern and reassigning to the target ID. Want me to help with anything specific, like checking Bentinck or the "Pesters" year split?