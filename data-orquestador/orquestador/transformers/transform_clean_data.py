if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from typing import Dict
import pandas as pd

def build_dim(data, col, mapping_name=None, mapping=None, null_id=None):
    """
    Construye un DataFrame de dimensión desde data raw, opcionalmente mapeando nombres.
    """
    dim = data[[col]].drop_duplicates().sort_values(col).reset_index(drop=True)
    if mapping:
        dim[mapping_name] = dim[col].map(mapping).fillna(mapping[null_id])
    return dim

def merge_dimensions(existing_dims, new, dim_name, id_col, name_col=None):
    """
    Une la dimensión existente con la nueva, asegurando valores únicos y ordenados.
    
    existing_dims: diccionario de dimensiones existentes
    new: nuevo DataFrame de la dimensión
    dim_name: clave en existing_dims
    id_col: nombre de la columna ID
    name_col: nombre de la columna con nombre descriptivo (opcional)
    """
    existing = existing_dims.get(dim_name, pd.DataFrame(columns=[id_col] + ([name_col] if name_col else [])))
    combined = pd.concat([existing, new], ignore_index=True)

    if name_col:
        combined = combined.drop_duplicates(subset=[id_col])
    else:
        combined = combined.drop_duplicates()

    combined = combined.sort_values(id_col).reset_index(drop=True)

    return combined

@transformer
def transform(data, *args, **kwargs) -> Dict[str, pd.DataFrame]:
    df = data['raw']
    existing_dims = data.get('dimensions', {})

    df = df.drop_duplicates()

    df.rename(columns={'payment_type': 'payment_type_id'}, inplace=True)

    df['vendor_id'] = df['vendor_id'].fillna(-1).astype('Int64')
    df['payment_type_id'] = df['payment_type_id'].fillna(5).astype('Int64')
    df['rate_code_id'] = df['rate_code_id'].fillna(99).astype('Int64')

    datetime_cols = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']

    for col in datetime_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    df['trip_duration_minutes'] = (
        df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']
    ).dt.total_seconds() / 60.0

    df['avg_speed_mph'] = (
        df['trip_distance'] / (df['trip_duration_minutes'] / 60.0)
        ).where(df['trip_duration_minutes'] != 0, 0)


    # Dimensions (https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
    dim_vendor = build_dim(df, 'vendor_id', mapping_name = 'vendor_name',
        mapping = {
            1: 'Creative Mobile Technologies, LLC',
            2: 'Curb Mobility, LLC',
            6: 'Myle Technologies Inc',
            7: 'Helix',
            -1: 'Unknown Vendor'
        },
        null_id = -1)

    dim_payment_type = build_dim(df, 'payment_type_id', mapping_name = 'payment_type_name',
        mapping = {
            0: 'Flex Fare trip',
            1: 'Credit card',
            2: 'Cash',
            3: 'No charge',
            4: 'Dispute',
            5: 'Unknown',
            6: 'Voided trip'
        },
        null_id = 5)

    dim_rate_code = build_dim(df, 'rate_code_id', mapping_name = 'rate_code_name',
        mapping = {
            1: 'Standard rate',
            2: 'JFK',
            3: 'Newark',
            4: 'Nassau or Westchester',
            5: 'Negotiated fare',
            6: 'Group ride',
            99: 'Null/unknown'
        },
        null_id = 99)

    dim_pickup_location = build_dim(df, 'pu_location_id')
    dim_dropoff_location = build_dim(df, 'do_location_id')

    new_dims = {
        'dim_vendor': dim_vendor,
        'dim_payment_type': dim_payment_type,
        'dim_pickup_location': dim_pickup_location,
        'dim_dropoff_location': dim_dropoff_location,
        'dim_rate_code': dim_rate_code
    }  

    output = {'facts': df}
    for dim_name, new_dim in new_dims.items():
        if dim_name in ['dim_vendor', 'dim_payment_type', 'dim_rate_code']:
            id_col = new_dim.columns[0]
            name_col = new_dim.columns[1]
        else:
            id_col = new_dim.columns[0]
            name_col = None
        output[dim_name] = merge_dimensions(existing_dims, new_dim, dim_name, id_col, name_col)

    return output


@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
