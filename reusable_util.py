import pandas as pd
import os
import datetime
import pyodbc
from cryptography.fernet import Fernet
from azure.storage.blob import ContainerClient
from azure.storage.blob import BlobServiceClient
import shutil

############### Function to create table dynamically. Supported dtypes: nvarchar, Date, Float, Int and DateTime
"""
Sample Syntax Call
create_table_sql_db(df, table_name = table_name, non_null_cols = [''], server = server,\
    database = database, username = username, password = password, only_date_cols = ['Date'], Drop_Table = True/False)
Sample data conversions to depict to correct column format when creating table
df['Varchar'] = df['Varchar'].astype(str)
df['Int'] = pd.to_numeric(df['Int'], downcast='integer')
df['Float'] = pd.to_numeric(df['Float'], downcast='float')
df['Date'] = pd.to_datetime(df['Date'])
df['DateTime'] = pd.to_datetime(df['DateTime'])
"""
def create_table_sql_db(df, table_name, server, database, username, password, only_date_cols = [''],\
                        Drop_Table = False, buffer_len = 20, manual_dtype = pd.DataFrame(), non_null_cols = ['']):
    df_col_typ = df.copy()
    all_cols = df_col_typ.columns.values
    L_Col_Nm = []
    L_Dtype = []
    L_max_len = []
    for col_nm in all_cols:
        L_Col_Nm.append(col_nm)
        L_Dtype.append(df_col_typ[col_nm].dtype)
        df_col_typ[col_nm] = df_col_typ[col_nm].astype(str)
        df_col_typ[col_nm] = df_col_typ[col_nm].str.strip()
        maxlength = [max(df_col_typ[col_nm].apply(len))]
        maxlength = maxlength[0]
        L_max_len.append(maxlength)
    
    
    df_final_dtypes = pd.DataFrame({'Column_Name': L_Col_Nm,'DataType': L_Dtype,'Max_Length': L_max_len})
    df_final_dtypes['Null_Indicator'] = 'Null'
    
    pandas_sql_db_dtype = {'object': 'nvarchar',
                           'int32': 'int',
                           'int64': 'int',
                           'int8': 'int',
                           'int16': 'int',
                           'Int32': 'int',
                           'Int64': 'int',
                           'float': 'float',
                           'float64': 'float',
                           'float32': 'float',
                           'datetime64[ns]': 'DATETIME'}
    try:
        df_final_dtypes = df_final_dtypes.replace({"DataType": pandas_sql_db_dtype})
    except:
        try:
            for key, value in pandas_sql_db_dtype.items():
                df_final_dtypes['DataType'] = df_final_dtypes['DataType'].replace(key, value)
        except:
            try:
                for key, value in pandas_sql_db_dtype.iteritems():
                    df_final_dtypes['DataType'] = df_final_dtypes['DataType'].replace(key, value)
            except:
                raise
    df_final_dtypes['Max_Length'] = df_final_dtypes['Max_Length'] + buffer_len
    if len(manual_dtype) > 0:
        df_final_dtypes = df_final_dtypes.append(manual_dtype)
        df_final_dtypes = data_prep(df_final_dtypes, null_impute_value = 'NULL', case_flag_no_change = True)
        df_final_dtypes.drop_duplicates(subset = 'Column_Name', keep = 'last', inplace = True)                
    
    for col_name in only_date_cols:
        df_final_dtypes.loc[df_final_dtypes.Column_Name == col_name, ['DataType']] = 'Date'
    ################## Creating table #####################
    create_syntax = "CREATE TABLE [dbo].[$table_name]("
    create_syntax = create_syntax.replace('$table_name', table_name)
    #create_syntax = create_syntax.replace('$table_name', 'test_create')
    create_col_data_syntax = '[$col_nm] [$dtype]($max_len) $null_ind'
    data_syntax = ''
    counter = 0
    for index, row in df_final_dtypes.iterrows():
        col_nm = row['Column_Name']
        dtype = row['DataType']
        max_len = row['Max_Length']
        
        null_ind = row['Null_Indicator']
        
        non_len_columns = ['INT', 'FLOAT', 'DATETIME', 'DATE', 'smallint', 'REAL']
        non_len_columns = [x.upper() for x in non_len_columns]
        if dtype.upper() in non_len_columns:
            create_col_data_syntax_new = create_col_data_syntax.replace('($max_len)', '')
        else:
            create_col_data_syntax_new = create_col_data_syntax
        
        create_col_data_syntax_new = create_col_data_syntax_new.replace('$col_nm', str(col_nm))
        create_col_data_syntax_new = create_col_data_syntax_new.replace('$dtype', str(dtype))
        create_col_data_syntax_new = create_col_data_syntax_new.replace('$max_len', str(max_len))
        if col_nm in non_null_cols:
            create_col_data_syntax_new = create_col_data_syntax_new.replace('$null_ind', str('NOT NULL'))
        else:
            create_col_data_syntax_new = create_col_data_syntax_new.replace('$null_ind', str(null_ind))
        
        if counter == 0:
            data_syntax = create_col_data_syntax_new
        else:
            data_syntax = data_syntax + ',' + create_col_data_syntax_new
            
        counter = counter + 1
        
        #counter = counter + 1
        
    complete_create_table_syntax = create_syntax + data_syntax + ')'
    
    #Creating Connection with SQL to create table
    conn_str = (
    r'DRIVER={SQL Server};'
    r'SERVER=' + server +';'
    r'DATABASE=' + database +';'
    r'UID=' + username + ';'
    r'PWD=' + password + ';'
    )
    
    cnxn = pyodbc.connect(conn_str)
    crsr = cnxn.cursor()
    table_exist_sql = "select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='?'".replace('?', table_name)
    df_table_present = pd.read_sql(table_exist_sql, cnxn)
    if len(df_table_present) > 0 and Drop_Table == False:
        crsr.close()
        cnxn.close()
        dt = datetime.datetime.now()

        return print(str(dt.strftime("%Y-%m-%d %H:%M:%S")),'SQL Table:', table_name, 'already exist in DB:', database)
    try:
        crsr.execute('DROP TABLE [' + table_name +'];')
        #crsr.execute('DROP TABLE Gating_Criteria;')
    except:
        pass
    try:
        crsr.execute(complete_create_table_syntax)
    except:
        print(complete_create_table_syntax)
        crsr.close()
        cnxn.close()
        raise
    cnxn.commit()
    crsr.close()
    cnxn.close()
    dt = datetime.datetime.now()
    print(str(dt.strftime("%Y-%m-%d %H:%M:%S")),'Table creation Successful. Table Name:', table_name, 'in DB:', database, 'of server:', server)


############### Function to insert in table dynamically.
"""
Sample Syntax Call
insert_sql_db(df, table_name = table_name, server = server, database = database, username = username, password = password)
"""
def insert_sql_db(df, table_name, server, database, username, password, truncate = True, row_inserts = 1000):
    df = df.sort_index(axis=1, ascending = False)
    columns_names = list(df.columns.values)
    insert_sql_template = 'INSERT INTO [dbo].[$table_nm] ([$cols]) VALUES'
    insert_sql_new = insert_sql_template.replace('$table_nm', table_name)
    insert_sql_values_template = "('$col')"
    insert_sql_values_new = insert_sql_values_template
    for col in columns_names:
        insert_col_str = '[' + col + ']' + ',[$cols]'
        insert_sql_new = insert_sql_new.replace('[$cols]', insert_col_str)
        insert_col_value_str = "'$" + col + "'" + ",'$col'"
        insert_sql_values_new = insert_sql_values_new.replace("'$col'", insert_col_value_str)
    
    insert_sql_new = insert_sql_new.replace(',[$cols]', '')
    insert_sql_values_new = insert_sql_values_new.replace(",'$col'", '')
    
    rows_completed = 0
    insert_sql_complete_statement = ''
    
    conn_str = (
        r'DRIVER={SQL Server};'
        r'SERVER=' + server +';'
        r'DATABASE=' + database +';'
        r'UID=' + username + ';'
        r'PWD=' + password + ';'
    ) 
    cnxn = pyodbc.connect(conn_str)
    crsr = cnxn.cursor()
    truncate_str = 'TRUNCATE TABLE [?];'.replace('?', table_name)
    if truncate:
        crsr.execute(truncate_str)
    
    for i, row in df.iterrows():
            
        insert_sql_values = insert_sql_values_new
        
        for col in columns_names:
            placeholder = "$" + col + ""
            insert_actual_val = str(row[col])
            insert_actual_val = insert_actual_val.replace("'", "''")
            insert_sql_values = insert_sql_values.replace(placeholder, insert_actual_val)
            
                
        if insert_sql_complete_statement == '':
            insert_sql_complete_statement = insert_sql_new + insert_sql_values
        else:
            insert_sql_complete_statement = insert_sql_complete_statement + ',' + insert_sql_values
            
            
            
        rows_completed = rows_completed + 1
        if rows_completed % row_inserts == 0 or len(df) == rows_completed:  
            try:
                insert_sql_complete_statement = insert_sql_complete_statement.replace("'NULL'", "NULL")
                insert_sql_complete_statement = insert_sql_complete_statement.replace('"NULL"', "NULL")
                crsr.execute(insert_sql_complete_statement)  
            except:
                print(insert_sql_complete_statement)
                crsr.close()
                cnxn.close()
                raise
            
            insert_sql_complete_statement = ''
            if rows_completed == 0:
                pass
            else:
                
                dt = datetime.datetime.now()
                print(str(dt.strftime("%Y-%m-%d %H:%M:%S")),'Rows Inserted:', rows_completed, 'out of', len(df))
    dt = datetime.datetime.now()
    print(str(dt.strftime("%Y-%m-%d %H:%M:%S")),'Commiting the changes made to Table:', table_name)
    cnxn.commit()
    crsr.close()
    cnxn.close()
    dt = datetime.datetime.now()
    print(str(dt.strftime("%Y-%m-%d %H:%M:%S")),'Insert Successful')

################## Function to encrypt a string
"""
Sample syntax Call
encryption_key, encrypted_string = encrypt_string(string = test_string)
test_string = 'l1Myf6blv61!'
"""
def encrypt_string(string):
    string_encoded = string.encode()
    key = Fernet.generate_key()
    f = Fernet(key)
    encrypted_string = f.encrypt(string_encoded)
    key_decoded = key.decode()
    encrypted_string_decoded = encrypted_string.decode()
    return key_decoded, encrypted_string_decoded

################## Function to decrypt a string
"""
Sample syntax Call
decrypted_string = decrypt_string(string_encrypted = encrypted_string, key = encryption_key)
"""
def decrypt_string(string_encrypted, key):
    string_encrypted_encoded = string_encrypted.encode()
    f = Fernet(key)
    decrypted_string = f.decrypt(string_encrypted_encoded)
    decrypted_string_decoded = decrypted_string.decode()
    return decrypted_string_decoded

################ Data Prep Function ############
def data_prep(df, null_impute_value = '', col_names = [''], type_conversion = 'str', drop_dups = True, dup_keep = 'first',\
              case_flag_upper = True, case_flag_lower = False, case_flag_no_change = False):
    type_conversion = type_conversion.strip()
    if col_names == ['']:
        col_names = df.columns.values
    df = df.fillna(null_impute_value)
    for col in col_names:
        if type_conversion.upper() == 'str'.upper():
            df[col] = df[col].astype(str)
            df[col] = df[col].str.strip()
            if case_flag_no_change:
                pass
            elif case_flag_upper:
                df[col] = df[col].str.upper()
            elif case_flag_lower:
                df[col] = df[col].str.lower()
        elif type_conversion.upper() == 'int'.upper():
            try:
                df[col] = df[col].astype(int)
            except:
                df[col] = pd.to_numeric(df[col], downcast = 'integer')
                df[col] = df[col].astype(pd.Int64Dtype())
        elif type_conversion.upper() == 'float'.upper():
            df[col] = pd.to_numeric(df[col], downcast = 'float')
        elif type_conversion.upper() == 'datetime'.upper():
            df[col] = pd.to_datetime(df[col])
        elif type_conversion.upper() == 'numeric'.upper():
            df[col] = pd.to_numeric(df[col])
    
    if drop_dups:
        df = df.drop_duplicates(keep = dup_keep)
    
    return df
    
    
######################### Get data from Azure ######################
def azure_blob_local_df(account_url, container_name, credential, download_folder_azure, blob_path, maunal_delim = ','):
    containerClient = ContainerClient(account_url=account_url, container_name=container_name, credential=credential)

    blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
    download_folder_azure = download_folder_azure + '/temp'
    try:
        shutil.rmtree(download_folder_azure)
    except:
        pass
    
    try:
        os.makedirs(download_folder_azure)
    except FileExistsError:
        # directory already exists
        pass
    #blob_service.get_blob_to_path(CONTAINERNAME,BLOBNAME,LOCALFILENAME)
    #blob_path = blob_path.strip()
    for pages in containerClient.list_blobs().by_page():
        for page in pages:
            if blob_path.lower() in page.name.lower():
                pass
            else:
                continue
            dt = datetime.datetime.now()
            print(str(dt.strftime("%Y-%m-%d %H:%M:%S")), 'Extracting Files From Azure Blob', page.name)
            blob_name = page.name.replace("/","\\")
            FileName = page.name.replace("/","_")
            
    #         FileName = pageName[len(pageName)-1]
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=page.name)
            download_file_path = download_folder_azure
            download_file_path = os.path.join(download_file_path, FileName)
            with open(download_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
    
    filename = max([download_folder_azure + "\\" + f for f in os.listdir(download_folder_azure)],key=os.path.getctime)
    dt = datetime.datetime.now()
    print(str(dt.strftime("%Y-%m-%d %H:%M:%S")), 'Importing', filename)
    if '.csv' in filename.lower():
        try:
            df = pd.read_csv(filename, dtype = str, sep = maunal_delim)  
        except:
            df = pd.read_csv(filename, dtype = str, sep = maunal_delim, encoding='latin-1')
    elif '.xlsx' in filename.lower():
        df = pd.read_excel(filename, dtype = str)
    else:
        raise ValueError('Filetype conversion to dataframe not currently supported by function for file:', filename)
    
    os.remove(filename)
    shutil.rmtree(download_folder_azure)
    return df

################## Get Data from SQL DB ###################
def get_data_SQL(server, database, username, password, table_nm, driver_name = 'SQL Server', col_names = ['*'], sql = ''):
    conn_str = (
            r'DRIVER={' + driver_name +'};'
            r'SERVER=' + server +';'
            r'DATABASE=' + database +';'
            r'UID=' + username + ';'
            r'PWD=' + password + ';'
        ) 
        #cnxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    dt = datetime.datetime.now()
    print(str(dt.strftime("%Y-%m-%d %H:%M:%S")), 'Getting Data from Table', table_nm)
    try:
        cnxn = pyodbc.connect(conn_str)
    except:
        dt = datetime.datetime.now()
        print(str(dt.strftime("%Y-%m-%d %H:%M:%S")), 'Please check DB connection string details such as username, password etc. Not able to connect with DB.')
        raise
        
    if sql == '':   
        sql = 'SELECT * from ?'.replace('?', table_nm)
        cols = ''
        for item in col_names:
            if cols == '':
                item = '[' + item + ']'
                cols = item
            else:
                item = '[' + item + ']'
                cols = cols + ', ' + item
        cols = cols.replace(r'[*]', '*')
        sql = sql.replace('*', cols)
    
    try:
        df = pd.read_sql(sql, cnxn)
    except:
        dt = datetime.datetime.now()
        print(str(dt.strftime("%Y-%m-%d %H:%M:%S")), 'Not able to query table. Exiting program.', table_nm)
        raise
        
    cnxn.close()
    
    return df

################################################# Search Blob for data ###############

def search_blob(account_url, container_name, credential, search_by_string) : 
    containerClient = ContainerClient(account_url=account_url, container_name=container_name, credential=credential)
     
    #blob_service.get_blob_to_path(CONTAINERNAME,BLOBNAME,LOCALFILENAME)
    
    
    for pages in containerClient.list_blobs().by_page():
        for page in pages:
            if search_by_string.lower() in page.name.lower():
                pass
            else:
                continue
            dt = datetime.datetime.now()
            print(str(dt.strftime("%Y-%m-%d %H:%M:%S")), 'Probable File From Azure Blob', page.name)
            
            
######################### Get data from Azure without headers ######################
def azure_blob_local_without_header_df(account_url, container_name, credential, download_folder_azure, blob_path, maunal_delim = ','):
    containerClient = ContainerClient(account_url=account_url, container_name=container_name, credential=credential)

    blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
    download_folder_azure = download_folder_azure + '/temp'
    try:
        shutil.rmtree(download_folder_azure)
    except:
        pass
    
    try:
        os.makedirs(download_folder_azure)
    except FileExistsError:
        # directory already exists
        pass
    #blob_service.get_blob_to_path(CONTAINERNAME,BLOBNAME,LOCALFILENAME)
    #blob_path = blob_path.strip()
    for pages in containerClient.list_blobs().by_page():
        for page in pages:
            if blob_path.lower() in page.name.lower():
                pass
            else:
                continue
            dt = datetime.datetime.now()
            print(str(dt.strftime("%Y-%m-%d %H:%M:%S")), 'Extracting Files From Azure Blob', page.name)
            blob_name = page.name.replace("/","\\")
            FileName = page.name.replace("/","_")
            
    #         FileName = pageName[len(pageName)-1]
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=page.name)
            download_file_path = download_folder_azure
            download_file_path = os.path.join(download_file_path, FileName)
            with open(download_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
    
    filename = max([download_folder_azure + "\\" + f for f in os.listdir(download_folder_azure)],key=os.path.getctime)
    dt = datetime.datetime.now()
    print(str(dt.strftime("%Y-%m-%d %H:%M:%S")), 'Importing', filename)
    if '.csv' in filename.lower():
        df = pd.read_csv(filename, dtype = str, sep = maunal_delim, header=None)  
    elif '.xlsx' in filename.lower():
        df = pd.read_excel(filename, dtype = str, header=None)
    else:
        raise ValueError('Filetype conversion to dataframe not currently supported by function for file:', filename)
    
    os.remove(filename)
    shutil.rmtree(download_folder_azure)
    return df

def priority_overwrite_df_column(df, col1 = 'Column_Name', col2 = 'New_Column_Name'):
    L1 = []
    df = data_prep(df, case_flag_no_change = True)
    for index, row in df.iterrows():
        col1_val = str(row[col1]).strip()
        col2_val = str(row[col2]).strip()
        
        if col2_val.upper() == 'NUll'.upper() or col2_val == '':
            L1.append(col1_val)
        else:
            L1.append(col2_val)
    
    df[col1] = L1
    
    return df
        
def final_df_manual_mapping_treatment(df, manual_dtype):
    manual_dtype.fillna('Null', inplace = True)
    manual_dtype['Record_No'] = manual_dtype.index
    
    #Getting the latest data
    try:
    	manual_dtype['Date'] = pd.to_datetime(manual_dtype['Date'])
    	manual_dtype.sort_values(by = 'Date', ascending = False, inplace = True)
    except:
    	pass
    #Getting the latest info in the mapping file
    manual_dtype = data_prep(manual_dtype, case_flag_no_change = True)
    manual_dtype.drop_duplicates(subset = 'Column_Name', keep = 'first', inplace = True)
    manual_dtype['Record_No'] = pd.to_numeric(manual_dtype['Record_No'])
    manual_dtype.sort_values(by = 'Record_No', ascending = True, inplace = True)
    
    #Getting list of columns to be excluded or now inactive in latest mapping
    L_cols_exclude_ind = list(set(manual_dtype['Column_Name'].loc[manual_dtype['Exclude_Indicator'].str.upper() == 'Y'.upper()]))
    L_cols_inactive = list(set(manual_dtype['Column_Name'].loc[manual_dtype['Active/Inactive'].str.upper() == 'Inactive'.upper()]))
    L_remove_cols = L_cols_exclude_ind + L_cols_inactive
    
    #Removing Cols from final df and data type mapping df
    df.drop(L_remove_cols, axis = 1, inplace = True)
    manual_dtype = manual_dtype.loc[~manual_dtype['Column_Name'].isin(L_remove_cols)]
    
    #Renaming the columns on the basis of mapping file
    rename_dict = dict(zip(manual_dtype.Column_Name, manual_dtype.New_Column_Name))
    rename_dict = {k:v for k,v in rename_dict.items() if v != 'Null'}
    
    df.rename(columns = rename_dict, inplace = True)
    #Changing column_name and treating the df for data prep
    manual_dtype = priority_overwrite_df_column(manual_dtype)
    
    return df, manual_dtype

def mapping_sql_db_comparison(df_mapping_default, server, database, username, password, table_nm, driver_name = 'SQL Server'):
    df_mapping = df_mapping_default.copy()
    conn_str = (
            r'DRIVER={' + driver_name +'};'
            r'SERVER=' + server +';'
            r'DATABASE=' + database +';'
            r'UID=' + username + ';'
            r'PWD=' + password + ';'
        ) 
        #cnxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    dt = datetime.datetime.now()
    print(str(dt.strftime("%Y-%m-%d %H:%M:%S")), 'Getting Schema Data from Table', table_nm)
    try:
        cnxn = pyodbc.connect(conn_str)
    except:
        dt = datetime.datetime.now()
        print(str(dt.strftime("%Y-%m-%d %H:%M:%S")), 'Please check DB connection string details such as username, password etc. Not able to connect with DB.')
        raise
    
    table_schema_sql = "select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='?'".replace('?', table_nm)
    df_table_schema = pd.read_sql(table_schema_sql, cnxn)
    if len(df_table_schema) == 0:
        dt = datetime.datetime.now()
        print(str(dt.strftime("%Y-%m-%d %H:%M:%S")), "Table doesn't exist")
        return 'NA'
    
    df_table_schema.columns.values
    df_table_schema = df_table_schema[['COLUMN_NAME', 'IS_NULLABLE', 'DATA_TYPE', 'CHARACTER_MAXIMUM_LENGTH']]
    df_table_schema = data_prep(df_table_schema)
    df_table_schema['CHARACTER_MAXIMUM_LENGTH'] = pd.to_numeric(df_table_schema['CHARACTER_MAXIMUM_LENGTH'])
    
    df_mapping = data_prep(df_mapping)
    df_mapping = priority_overwrite_df_column(df_mapping)
    df_mapping = df_mapping[['Column_Name', 'DataType', 'Max_Length', 'Null_Indicator']]
    
    df_mapping['Null_Indicator'] = df_mapping['Null_Indicator'].replace('NULL', 'YES')
    df_mapping['Null_Indicator'] = df_mapping['Null_Indicator'].replace('NOT NULL', 'NO')
    df_mapping['Max_Length'] = df_mapping['Max_Length'].replace('NULL', '')
    df_mapping['Max_Length'] = pd.to_numeric(df_mapping['Max_Length'])
    
    df_comparison = pd.merge(df_table_schema, df_mapping,
                             left_on = ['COLUMN_NAME', 'IS_NULLABLE', 'DATA_TYPE', 'CHARACTER_MAXIMUM_LENGTH'],\
                                 right_on = ['Column_Name', 'Null_Indicator', 'DataType', 'Max_Length'], how = 'outer')
    
    df_comparison = df_comparison.loc[df_comparison['COLUMN_NAME'] != 'LOAD_DATE_TIME']
    print(str(dt.strftime("%Y-%m-%d %H:%M:%S")), 'Comparing mapping with existing schema in DB for', table_nm)
    
    if len(df_comparison) == len(df_mapping):
        #Same Schema ind '1'
        return '1'
    else:
        #Diff Schema ind '0'
        return '0'