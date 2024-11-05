# import great_expectations as gx

# context = gx.get_context()
# assert type(context).__name__ == "EphemeralDataContext"
# # SETUP FOR THE EXAMPLE:
# data_source = context.data_sources.add_pandas(name="my_data_source")

# # Retrieve the Data Source
# data_source_name = "my_data_source"
# data_source = context.data_sources.get(data_source_name)

# # Define the Data Asset name
# data_asset_name = "my_dataframe_data_asset"

# # Add a Data Asset to the Data Source
# data_asset = data_source.add_dataframe_asset(name=data_asset_name)

# assert data_asset.name == data_asset_name