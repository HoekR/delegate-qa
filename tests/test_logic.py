import pandas as pd
import pytest
from logic import get_filtered_df

def make_df():
    return pd.DataFrame({
        'name': ['Alice', 'Bob', 'Charlie', 'David'],
        'dept': ['HR', 'IT', 'HR', 'Finance'],
        'id': [1, 2, 3, 4],
    })

def test_global_search():
    df = make_df()
    result = get_filtered_df(df, 'ali', mode='global')
    assert len(result) == 1
    assert result.iloc[0]['name'] == 'Alice'
    result = get_filtered_df(df, 'hr', mode='global')
    assert len(result) == 2

def test_column_search():
    df = make_df()
    result = get_filtered_df(df, 'bob', mode='column', column='name')
    assert len(result) == 1
    assert result.iloc[0]['name'] == 'Bob'
    result = get_filtered_df(df, 'finance', mode='column', column='dept')
    assert len(result) == 1
    assert result.iloc[0]['name'] == 'David'

def test_strict_search():
    df = make_df()
    result = get_filtered_df(df, 'alice', mode='strict')
    assert len(result) == 1
    assert result.iloc[0]['name'] == 'Alice'
    result = get_filtered_df(df, 'hr', mode='strict')
    assert len(result) == 2

def test_empty_query_returns_all():
    df = make_df()
    result = get_filtered_df(df, '', mode='global')
    assert len(result) == 4
    result = get_filtered_df(df, None, mode='column', column='name')
    assert len(result) == 4

def test_mixed_case_search():
    df = make_df()
    result = get_filtered_df(df, 'ALICE', mode='global')
    assert len(result) == 1
    result = get_filtered_df(df, 'bOb', mode='column', column='name')
    assert len(result) == 1

def test_numeric_search():
    df = make_df()
    result = get_filtered_df(df, '1', mode='global')
    assert len(result) == 1
    assert result.iloc[0]['name'] == 'Alice'

def test_special_char_search():
    df = pd.DataFrame({'name': ['Ann-Marie', 'O\'Connor', 'Smith'], 'dept': ['A', 'B', 'C'], 'id': [10, 11, 12]})
    result = get_filtered_df(df, 'ann-marie', mode='global')
    assert len(result) == 1
    result = get_filtered_df(df, "o'connor", mode='global')
    assert len(result) == 1

def test_no_matches():
    df = make_df()
    result = get_filtered_df(df, 'zzz', mode='global')
    assert len(result) == 0

def test_invalid_column():
    df = make_df()
    import pytest
    with pytest.raises(ValueError, match="not_a_col"):
        get_filtered_df(df, 'bob', mode='column', column='not_a_col')

if __name__ == '__main__':
    pytest.main()
