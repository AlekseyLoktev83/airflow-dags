
def generate_hdfs_to_s3_copy_file_command(s3_connection_name, filename, src_path, dst_path):
    entity_file = os.path.basename(src_path)
    
    s3_connection_name = BaseHook.get_connection(s3_connection_name)
    
    
    script = f'''
    hdfs dfs -get {src_path} /tmp/{filename} && s3cmd put /tmp/{filename} {dst_path} --host=storage.yandexcloud.net --access_key={azure_conn.login} --secret_key={azure_conn.password} --host-bucket="%(bucket)s.storage.yandexcloud.net" && rm -rf /tmp/{filename}   
    
    '''

    return script
