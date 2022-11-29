import os
from airflow.hooks.base_hook import BaseHook

def generate_adls_to_hdfs_copy_folder_command(azure_connection_name, src_path, dst_path, folders_from_tail_count = 1):
    entity_subfolder = os.sep.join(os.path.normpath(src_path).split(os.sep)[-folders_from_tail_count:])
    
    azure_conn = BaseHook.get_connection(azure_connection_name)
    
    
    script = f"""
    export AZCOPY_AUTO_LOGIN_TYPE=SPN
    export AZCOPY_SPA_APPLICATION_ID={azure_conn.login} 
    export AZCOPY_SPA_CLIENT_SECRET={azure_conn.password}
    export AZCOPY_TENANT_ID={azure_conn.extra_dejson['extra__azure__tenantId']}
    
    azcopy copy {src_path} /tmp/entity --recursive && hdfs dfs -rm -f {dst_path}{entity_subfolder}/* && hdfs dfs -mkdir -p {dst_path}{entity_subfolder} && hdfs dfs -put -f /tmp/entity/*/* {dst_path}{entity_subfolder} && rm -rf /tmp/entity   
    
    """

    return script


def generate_adls_to_hdfs_copy_file_command(azure_connection_name, src_path, dst_path):
    entity_file = os.path.basename(src_path)
    
    azure_conn = BaseHook.get_connection(azure_connection_name)
    
    
    script = f"""
    export AZCOPY_AUTO_LOGIN_TYPE=SPN
    export AZCOPY_SPA_APPLICATION_ID={azure_conn.login} 
    export AZCOPY_SPA_CLIENT_SECRET={azure_conn.password}
    export AZCOPY_TENANT_ID={azure_conn.extra_dejson['extra__azure__tenantId']}
    
    azcopy copy {src_path} /tmp/{entity_file} && hdfs dfs -rm -f {dst_path}{entity_file} && hdfs dfs -mkdir -p {dst_path} && hdfs dfs -put -f /tmp/{entity_file} {dst_path}{entity_file} && rm -rf /tmp/{entity_file}   
    
    """

    return script
