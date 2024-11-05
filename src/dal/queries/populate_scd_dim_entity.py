import deltalake
from deltalake import DeltaTable
import pandas as pd
import pyarrow as pa
import os
from dal import my_sqlite

dim_entity_schema = pa.schema([
    ('entity_id', pa.string()),
    ('entity_type', pa.string()),
    ('jurisdiction', pa.string()),
    ('user_level', pa.int32()),
    ('event_timestamp', pa.timestamp('us')),
    ('start_date', pa.date32()),
    ('end_date', pa.date32()),
    ('is_current', pa.bool_())
])


def populate_dim_entity():
    # Convert to DataFrame
    gold_dim_entity_path = 'data/delta/gold/dim_entity'
    silver_user_level_df = DeltaTable('data\\delta\\silver\\silver_user_level').to_pandas()

    # Step 1: Rank levels to determine the start and end dates for each user level change
    silver_user_level_df['event_timestamp'] = pd.to_datetime(silver_user_level_df['event_timestamp'])
    silver_user_level_df = silver_user_level_df.sort_values(by=['user_id', 'jurisdiction', 'event_timestamp'])

    silver_user_level_df['start_date'] = silver_user_level_df['event_timestamp']
    silver_user_level_df['end_date'] = silver_user_level_df.groupby(['user_id', 'jurisdiction'])['event_timestamp'].shift(-1)

    # Convert datetime columns to timezone-naive
    silver_user_level_df['event_timestamp'] = silver_user_level_df['event_timestamp'].dt.tz_localize(None)
    silver_user_level_df['start_date'] = silver_user_level_df['start_date'].dt.tz_localize(None)
    silver_user_level_df['end_date'] = silver_user_level_df['end_date'].dt.tz_localize(None)

    # Step 2: Determine the current levels, setting end_date to a far future date if it's NULL
    silver_user_level_df['end_date'] = silver_user_level_df['end_date'].fillna(pd.Timestamp('9999-12-31'))
    silver_user_level_df['is_current'] = silver_user_level_df['end_date'] == pd.Timestamp('9999-12-31')

    # Step 3: Create or load the Delta Lake table
    try:
        dim_entity_table = DeltaTable(gold_dim_entity_path)
    except Exception:
        # Write the empty DataFrame to create the Delta table
        dim_entity_table = DeltaTable.create(gold_dim_entity_path, schema=dim_entity_schema)

    # Step 4: Update existing records in dim_entity to set the end_date and mark them as non-current
    existing_records = dim_entity_table.to_pandas()
    existing_records = existing_records.merge(
        silver_user_level_df[['user_id', 'jurisdiction', 'start_date']],
        left_on=['entity_id', 'jurisdiction'],
        right_on=['user_id', 'jurisdiction'],
        how='left',
        suffixes=('', '_new')
    )
    existing_records['start_date_new'] = pd.to_datetime(existing_records['start_date_new']).dt.tz_localize(None)
    existing_records['start_date'] = pd.to_datetime(existing_records['start_date']).dt.tz_localize(None)

    existing_records.loc[
        (existing_records['is_current'] is True) & (existing_records['start_date_new'] > existing_records['start_date']),
        'end_date'
    ] = existing_records['start_date_new'] - pd.Timedelta(days=1)
    existing_records.loc[
        (existing_records['is_current'] is True) & (existing_records['start_date_new'] > existing_records['start_date']),
        'is_current'
    ] = False

    # Step 5: Insert new records into dim_entity, ensuring no duplicates
    new_records = silver_user_level_df.merge(
        existing_records[['entity_id', 'jurisdiction', 'start_date']],
        left_on=['user_id', 'jurisdiction', 'start_date'],
        right_on=['entity_id', 'jurisdiction', 'start_date'],
        how='left',
        indicator=True
    ).query('_merge == "left_only"').drop(columns=['_merge'])

    # Add missing columns to new_records
    new_records['entity_id'] = new_records['user_id']
    new_records['entity_type'] = 'human'
    new_records['user_level'] = new_records['level']

    # Combine updated existing records and new records
    df_dim_entity_combined = pd.concat([existing_records, new_records], ignore_index=True)

    # # Ensure the DataFrame matches the Delta table schema
    df_dim_entity_combined = df_dim_entity_combined[
        ['entity_id', 'entity_type', 'jurisdiction', 'user_level', 'event_timestamp', 'start_date', 'end_date', 'is_current']
    ]

    # Fill any remaining null values with appropriate defaults
    df_dim_entity_combined = df_dim_entity_combined.fillna({
        'entity_id': pd.NA,
        'entity_type': 'unknown',
        'jurisdiction': 'unknown',
        'user_level': 0,
        'event_timestamp': pd.Timestamp('1970-01-01'),
        'start_date': pd.Timestamp('1970-01-01'),
        'end_date': pd.Timestamp('9999-12-31'),
        'is_current': False
    })

    # Write the combined DataFrame back to Delta Lake
    print("Saving dim_entity as gold delta table")
    affected = deltalake.write_deltalake(gold_dim_entity_path, df_dim_entity_combined, mode='overwrite')

    print("Saving dim_entity to gold csv")
    csv_path = "data/csv/gold/dim_entity.csv"
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df_dim_entity_combined.to_csv(csv_path, index=False)

    ##temp
    print("Saving dim_entity to gold sqlite")
    my_sqlite.save_replace(df_dim_entity_combined, 'dim_entity')

    return affected
