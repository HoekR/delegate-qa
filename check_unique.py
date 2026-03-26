from utils import build_merged, _compute_delegate_summary, load_data, source_mtimes, load_remappings
import importlib, utils

importlib.reload(utils)
utils.build_merged.clear()

# load and merge

df_p, df_i, df_abbrd = load_data(source_mtimes())
remappings = load_remappings()
df_merged, n_placeholder, n_remapped, summary = build_merged(df_p, df_i, df_abbrd, extra_delegates=None, remappings=remappings, name_col='fullname')
summary2 = _compute_delegate_summary(df_merged, df_p, 'fullname')

print('unique counts raw: df_p', df_p['delegate_id'].nunique(), 'df_i', df_i['delegate_id'].nunique(), 'df_merged', df_merged['delegate_id'].nunique())
print('dtype', df_p['delegate_id'].dtype, df_i['delegate_id'].dtype, df_merged['delegate_id'].dtype, summary['delegate_id'].dtype)
print('13613 summary rows', summary[summary['delegate_id']=='13613'].shape)

print('assert checks...')
assert df_p['delegate_id'].dtype == object
assert df_i['delegate_id'].dtype == object
assert df_merged['delegate_id'].dtype == object
assert summary['delegate_id'].dtype == object
assert df_p['delegate_id'].nunique() == df_p['delegate_id'].astype(str).nunique()
assert df_i['delegate_id'].nunique() == df_i['delegate_id'].astype(str).nunique()
assert df_merged['delegate_id'].nunique() == df_merged['delegate_id'].astype(str).nunique()
print('unique delegates assertions passed')
