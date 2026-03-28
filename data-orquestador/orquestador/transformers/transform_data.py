if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    data.columns = [col.lower() for col in data.columns]
    
    data.rename(columns={
        'vendorid': 'vendor_id',
        'ratecodeid': 'rate_code_id',
        'pulocationid': 'pu_location_id',
        'dolocationid': 'do_location_id'
    }, inplace = True)

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
