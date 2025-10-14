## How to use it

The script requires a `csv` file with a list of table names and nocodb table IDs. Header is not required. If you have a header, you can set the `header` parameter to `True`. Example:

```csv
table_name_1,table_id_1
table_name_2,table_id_2
table_name_3,table_id_3
```

Set the environment variables in a `.env` file:

```
NOCODB_BASE_URL=https://app.nocodb.com/api/v2/
NOCODB_TOKEN=your_nocodb_token
```

