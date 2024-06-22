class ETLProcess:
    def __init__(self, extract, transform, load):
        self.extract = extract
        self.transform = transform
        self.load = load

    def run(self, transform_columns, table_name):
        # Step 1: Extract
        extracted_file = self.extract.execute()
        print(extracted_file)
        
        # Step 2: Transform and save to Parquet
        parquet_path = self.transform.execute(extracted_file, transform_columns)
        print(type(parquet_path))
        print(parquet_path)
        
        # Step 3: Load the transformed data into the database
        self.load.execute(parquet_path, table_name)

    def run_extract(self):
        # Step 1: Extract
        extracted_file = self.extract.execute()
        print(extracted_file)
        return extracted_file
        
    def run_transform(self, transform_columns, extracted_file):
        # Step 2: Transform and save to Parquet
        parquet_path = self.transform.execute(extracted_file, transform_columns)
        print(parquet_path)
        return parquet_path
    
    def run_load(self, table_name, parquet_path):
        # Step 3: Load the transformed data into the database
        self.load.execute(parquet_path, table_name)