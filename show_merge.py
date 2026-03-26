from utils import build_merged, _compute_delegate_summary, load_data, source_mtimes, load_remappings
import importlib, utils

importlib.reload(utils)
utils.build_merged.clear()

# load and merge

df_p, df_i, df_abbrd = load_data(source_mtimes())
remappings = load_remappings()
df_merged, n_placeholder, n_remapped, summary = build_merged(
    df_p, df_i, df_abbrd,
    extra_delegates=None,
    remappings=remappings,
    name_col='fullname'
)
summary2 = _compute_delegate_summary(df_merged, df_p, 'fullname')

print('build_merged rows', df_merged.shape[0], 'cols', df_merged.shape[1])
print('n_placeholder', n_placeholder, 'n_remapped', n_remapped)
print('summary rows', summary.shape[0], 'cols', summary.shape[1])
print('summary2 rows', summary2.shape[0], 'cols', summary2.shape[1])
print('13613 in summary', summary[summary['delegate_id']=='13613'].shape)
print('13613 in summary2', summary2[summary2['delegate_id']=='13613'].shape)
print('dup delegate summary', summary['delegate_id'].duplicated().sum())
print('dup delegate summary2', summary2['delegate_id'].duplicated().sum())
print('dtype merged delegate_id', df_merged['delegate_id'].dtype)
print('dtype persons delegate_id', df_p['delegate_id'].dtype)

print('\n=== merged sample 13613 ===')
print(df_merged[df_merged['delegate_id']=='13613'].head(5).to_string(index=False))
print('\n=== summary 13613 ===')
print(summary[summary['delegate_id']=='13613'].to_string(index=False))
