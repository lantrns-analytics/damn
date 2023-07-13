import boto3


def get_folder_stats(bucket, prefix):
    s3 = boto3.client('s3')
    
    latest_modification = None
    total_size = 0
    num_files = 0
    
    paginator = s3.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for content in page.get('Contents', []):
            if latest_modification is None or content["LastModified"] > latest_modification:
                latest_modification = content["LastModified"]
            
            total_size += content.get("Size", 0)
            num_files += 1

    return latest_modification, total_size, num_files


def list_objects_and_folders(bucket, prefix):
    s3 = boto3.client('s3')
    items = []
    
    paginator = s3.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
        for content in page.get('Contents', []):
            items.append({
                'object_type': 'file',
                'key': content['Key'],
                'num_files': 1,
                'file_size': content.get("Size", 0),
                'last_modified_ts': content['LastModified']
            })
        
        for common_prefix in page.get('CommonPrefixes', []):
            folder_name = common_prefix["Prefix"]
            last_modified, total_size, num_files = get_folder_stats(bucket, folder_name)
            items.append({
                'object_type': 'folder',
                'key': folder_name,
                'num_files': num_files,
                'file_size': total_size,
                'last_modified_ts': last_modified
            })
    
    items.sort(key=lambda x: x['last_modified_ts'], reverse=True)
    
    return items