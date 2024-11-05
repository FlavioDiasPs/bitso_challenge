import great_expectations as gx
import pandas as pd
from deltalake import DeltaTable

# Define paths to your Delta Lake tables
silver_deposit_path = "path_to_silver_deposit"
silver_event_path = "path_to_silver_event"
silver_user_id_path = "path_to_silver_user_id"
silver_user_level_path = "path_to_silver_user_level"
silver_withdrawals_path = "path_to_silver_withdrawals"

fact_transactions_path = "path_to_fact_transactions"
dim_entity_path = "path_to_dim_entity"
dim_date_path = "path_to_dim_date"

# Load Delta Lake tables into Pandas DataFrames
silver_deposit_df = DeltaTable(silver_deposit_path).to_pandas()
silver_event_df = DeltaTable(silver_event_path).to_pandas()
silver_user_id_df = DeltaTable(silver_user_id_path).to_pandas()
silver_user_level_df = DeltaTable(silver_user_level_path).to_pandas()
silver_withdrawals_df = DeltaTable(silver_withdrawals_path).to_pandas()

fact_transactions_df = DeltaTable(fact_transactions_path).to_pandas()
dim_entity_df = DeltaTable(dim_entity_path).to_pandas()
dim_date_df = DeltaTable(dim_date_path).to_pandas()

# Get the Great Expectations context
context = gx.get_context()

# Validation functions
def validate_silver_deposit(batch):
    batch.expect_column_to_exist("id")
    batch.expect_column_values_to_not_be_null("id")
    batch.expect_column_values_to_be_unique("id")
    batch.expect_column_to_exist("event_timestamp")
    batch.expect_column_values_to_not_be_null("event_timestamp")
    batch.expect_column_to_exist("user_id")
    batch.expect_column_values_to_not_be_null("user_id")
    batch.expect_column_to_exist("amount")
    batch.expect_column_values_to_not_be_null("amount")
    batch.expect_column_values_to_be_between("amount", min_value=0)
    batch.expect_column_to_exist("currency")
    batch.expect_column_values_to_not_be_null("currency")
    batch.expect_column_to_exist("tx_status")
    batch.expect_column_values_to_not_be_null("tx_status")

def validate_silver_event(batch):
    batch.expect_column_to_exist("id")
    batch.expect_column_values_to_not_be_null("id")
    batch.expect_column_values_to_be_unique("id")
    batch.expect_column_to_exist("event_timestamp")
    batch.expect_column_values_to_not_be_null("event_timestamp")
    batch.expect_column_to_exist("user_id")
    batch.expect_column_values_to_not_be_null("user_id")
    batch.expect_column_to_exist("event_name")
    batch.expect_column_values_to_not_be_null("event_name")

def validate_silver_user_id(batch):
    batch.expect_column_to_exist("user_id")
    batch.expect_column_values_to_not_be_null("user_id")
    batch.expect_column_values_to_be_unique("user_id")

def validate_silver_user_level(batch):
    batch.expect_column_to_exist("user_id")
    batch.expect_column_values_to_not_be_null("user_id")
    batch.expect_column_to_exist("jurisdiction")
    batch.expect_column_values_to_not_be_null("jurisdiction")
    batch.expect_column_to_exist("level")
    batch.expect_column_values_to_be_in_set("level", [1, 2, 3])

def validate_silver_withdrawals(batch):
    batch.expect_column_to_exist("id")
    batch.expect_column_values_to_not_be_null("id")
    batch.expect_column_values_to_be_unique("id")
    batch.expect_column_to_exist("event_timestamp")
    batch.expect_column_values_to_not_be_null("event_timestamp")
    batch.expect_column_to_exist("user_id")
    batch.expect_column_values_to_not_be_null("user_id")
    batch.expect_column_to_exist("amount")
    batch.expect_column_values_to_not_be_null("amount")
    batch.expect_column_values_to_be_between("amount", min_value=0)
    batch.expect_column_to_exist("currency")
    batch.expect_column_values_to_not_be_null("currency")

def validate_fact_transactions(batch):
    batch.expect_column_to_exist("transaction_id")
    batch.expect_column_values_to_not_be_null("transaction_id")
    batch.expect_column_values_to_be_unique("transaction_id")
    batch.expect_column_to_exist("transaction_timestamp")
    batch.expect_column_values_to_not_be_null("transaction_timestamp")
    batch.expect_column_to_exist("entity_id")
    batch.expect_column_values_to_not_be_null("entity_id")
    batch.expect_column_to_exist("amount")
    batch.expect_column_values_to_not_be_null("amount")
    batch.expect_column_values_to_be_between("amount", min_value=0)

def validate_dim_entity(batch):
    batch.expect_column_to_exist("entity_id")
    batch.expect_column_values_to_not_be_null("entity_id")
    batch.expect_column_values_to_be_unique("entity_id")
    batch.expect_column_to_exist("jurisdiction")
    batch.expect_column_values_to_not_be_null("jurisdiction")
    batch.expect_column_to_exist("user_level")
    batch.expect_column_values_to_not_be_null("user_level")

def validate_dim_date(batch):
    batch.expect_column_to_exist("full_date")
    batch.expect_column_values_to_not_be_null("full_date")
    batch.expect_column_values_to_be_in_set("day_of_week", ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"])
    batch.expect_column_values_to_be_between("month", min_value=1, max_value=12)
    batch.expect_column_values_to_be_between("year", min_value=2000, max_value=2100)

# Define silver tables
silver_tables = {
    "silver_deposit": (silver_deposit_df, validate_silver_deposit),
    "silver_event": (silver_event_df, validate_silver_event),
    "silver_user_id": (silver_user_id_df, validate_silver_user_id),
    "silver_user_level": (silver_user_level_df, validate_silver_user_level),
    "silver_withdrawals": (silver_withdrawals_df, validate_silver_withdrawals)
}

# Validate Silver Tables
for table_name, (df, validate_func) in silver_tables.items():
    data_source = context.data_sources.add_pandas(name=table_name)
    data_asset = data_source.add_dataframe_asset(name=f"{table_name}_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(f"{table_name}_batch_definition")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    
    validate_func(batch)
    
    # Create an expectation suite if it doesn't exist
    expectation_suite_name = f"{table_name}_expectation_suite"
    context.add_expectation_suite(expectation_suite_name, overwrite_existing=True)
    
    # Validate the batch against the expectations
    validation_result = batch.validate(
        expectation_suite=expectation_suite_name,
        result_format="COMPLETE"
    )
    
    print(f"Validation result for {table_name}:")
    print(validation_result)

# Define and validate Fact and Dimension Tables
fact_dim_tables = {
    "fact_transactions": (fact_transactions_df, validate_fact_transactions),
    "dim_entity": (dim_entity_df, validate_dim_entity),
    "dim_date": (dim_date_df, validate_dim_date)
}

for table_name, (df, validate_func) in fact_dim_tables.items():
    # Use the updated method to add a pandas data source
    data_source = context.data_sources.add_pandas(name=table_name)
    data_asset = data_source.add_dataframe_asset(name=f"{table_name}_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(f"{table_name}_batch_definition")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    
    validate_func(batch)
    
    # Create an expectation suite if it doesn't exist
    expectation_suite_name = f"{table_name}_expectation_suite"
    expectation_suite = context.suites.add(expectation_suite_name)
    
    # Validate the batch against the expectations
    validation_result = batch.validate(
        expectation_suite=expectation_suite,
        result_format="COMPLETE"
    )
    
    print(f"Validation result for {table_name}:")
    print(validation_result)