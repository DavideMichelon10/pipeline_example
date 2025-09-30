def get_bucket() -> str:
    return 'openmeteo3423534654'

def get_prefix(start_dt, end_dt) -> str:
    return f'{start_dt}@{end_dt}'

def get_bucket_and_prefix(start_dt, end_dt) -> tuple[str, str]:
    bucket = get_bucket()
    prefix = get_prefix(start_dt, end_dt)
    return bucket, prefix