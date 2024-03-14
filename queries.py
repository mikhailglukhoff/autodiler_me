import constants

# upload to psql with updating data
set_columns = [f"{column} = EXCLUDED.{column}" for column in constants.psql_data['column_names'] if
               column != 'unique_id']

set_clause = ", ".join(set_columns)

upload_query = f"""
INSERT INTO 
{constants.psql_data['table_name']} 
({', '.join(constants.psql_data['column_names'])})
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (unique_id) DO UPDATE
SET 
    {set_clause};
"""
